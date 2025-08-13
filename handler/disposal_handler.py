# handlers/disposal_handler.py
import os, base64, re, traceback, json, time
from datetime import datetime
from typing import Dict, Any, List
import pandas as pd

EXCEL_PATH = "remittance.xlsx"
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")

def log(msg: str): print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [DISPOSAL] {msg}", flush=True)

# ---------------- Email helpers ----------------
def header_value(msg, name: str):
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name","").lower()==name.lower(): return h.get("value","")
    return ""

def body_text(payload):
    def walk(parts):
        out=[]
        for p in parts:
            mime=p.get("mimeType","")
            body=p.get("body",{})
            data=body.get("data")
            if p.get("parts"): out+=walk(p["parts"])
            elif mime in ("text/plain","text/html") and data:
                out.append(base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8","ignore"))
        return out
    if payload.get("mimeType","").startswith("multipart/"):
        text="\n".join(walk(payload.get("parts",[]) or []))
    else:
        data=payload.get("body",{}).get("data")
        text=base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8","ignore") if data else ""
    text = re.sub(r"<[^>]+>"," ",text)
    text = re.sub(r"[ \t]+"," ",text)
    text = re.sub(r"\n{2,}","\n",text)
    return text.strip()

# ---------------- Excel helpers ----------------
def read_df():
    if os.path.exists(EXCEL_PATH):
        return pd.read_excel(EXCEL_PATH, dtype=str).fillna("")
    return pd.DataFrame()

def write_df(df):
    with pd.ExcelWriter(EXCEL_PATH, engine="openpyxl") as w:
        df.to_excel(w, index=False, sheet_name="Remittances")

def ensure_cols(df, cols):
    for c in cols:
        if c not in df.columns: df[c]=""
    return df

def upsert_selected(df: pd.DataFrame, pk_col: str, updates: Dict[str, Any], allowed_cols: List[str]) -> pd.DataFrame:
    """
    Upsert using pk_col; only write keys in allowed_cols.
    If row exists: update only differing allowed columns.
    If new: create with allowed columns only (others untouched).
    """
    safe_updates = {k: ("" if v is None else str(v)) for k,v in updates.items() if k in allowed_cols}
    df = ensure_cols(df, list(set(allowed_cols + [pk_col])))
    pk = safe_updates.get(pk_col, "")
    if not pk:
        # No PK -> append minimal row with whatever allowed fields we have
        return pd.concat([df, pd.DataFrame([safe_updates])], ignore_index=True)

    mask = df[pk_col].astype(str) == pk
    if mask.any():
        idx = df[mask].index[0]
        for k,v in safe_updates.items():
            old = str(df.at[idx,k]) if k in df.columns else ""
            if (old or "") != (v or ""):
                df.at[idx,k] = v
        return df
    else:
        return pd.concat([df, pd.DataFrame([safe_updates])], ignore_index=True)

# ---------------- LLM extraction ----------------
SYSTEM = "You are a strict information extraction engine. Return ONLY valid JSON."

# Canonical financial fields we care about (bank-agnostic)
FINANCIAL_CANON = [
    # Parties (remitter / beneficiary)
    "RemitterName",           # full name of the sending person/company
    "RemitterReference",      # sender’s reference/ID if present
    "BeneficiaryName",
    "BeneficiaryAccount",

    # IDs / Keys
    "InwardReference",        # inward/IRM/transaction reference (bank side)

    # Currency / amounts / dates
    "CurrencyCode",           # ISO 4217 like USD/EUR/INR
    "AmountFCY",              # amount in foreign currency (string as shown)
    "AmountINR",              # INR amount if stated (string)
    "ExchangeRate",           # as shown (string)
    "ValueDate",              # credit/value date as-is
    "CreditDate",             # if separately mentioned

    # Banking details / purpose
    "RemittingBankName",
    "RemittingBankSWIFT",     # 8 or 11 alphanumerics
    "PurposeCode",
    "PurposeDescription"
]

USER_TMPL = """You will receive the FULL EMAIL BODY of a bank inward remittance notification (intimation/disposal).
Extract ONLY FINANCIAL/TRANSACTION details (ignore greetings, signatures, disclaimers, instructions, addresses).

Bank-agnostic guidance:
- Keys may vary (e.g., INW_NO, IRM, OSN, UTR, Reference, Sender Ref). Infer meaning by context.
- RemitterName = full name of the sender/remitter (person or company).
- RemitterReference = the sender’s reference/ID if present (do not invent).
- InwardReference = bank’s inward/IRM/transaction reference for this credit.
- CurrencyCode must be ISO 4217 (e.g., USD, EUR, INR).
- RemittingBankSWIFT is 8 or 11 alphanumerics if present.
- Keep dates and numbers exactly as shown. Do NOT reformat.
- If a field is absent/unclear, set it to null. Do NOT hallucinate.

Return ONLY this JSON object:
{
  "IsRelevant": true,
  "Confidence": <0..1>,
  "FinancialFields": {
%(fields_json)s
  }
}

EMAIL BODY:
\"\"\"%(input_text)s\"\"\""""

# Heuristic filter to shrink to finance-relevant lines + small context
FINANCIAL_HINTS = [
    r"\b(inw[_\s-]?no|inward|irm|osn|utr|ref(?:erence)?)\b",
    r"\b(value\s*date|credit\s*date|date)\b",
    r"\b(currency|cur|ccy|usd|eur|gbp|inr)\b",
    r"\b(amount|fcy\s*amt|fcy\s*amount|inr\s*amount|exchange\s*rate|x?rate)\b",
    r"\b(remitter|remitting|ordering|sender)\b",
    r"\b(beneficiary|bene(?:f)?|account|a/c|acc(?:ount)?)\b",
    r"\b(swift|bic|ifsc|sort)\b",
    r"\b(purpose|purpose\s*code|reason)\b",
]
def extract_financial_window(text: str, ctx_lines: int = 2, max_chars: int = 8000) -> str:
    lines = (text or "").splitlines()
    keep = [False] * len(lines)
    pat = re.compile("|".join(FINANCIAL_HINTS), flags=re.I)
    for i, ln in enumerate(lines):
        if pat.search(ln or ""):
            for j in range(max(0, i-ctx_lines), min(len(lines), i+ctx_lines+1)):
                keep[j] = True
    filtered = "\n".join(l for i, l in enumerate(lines) if keep[i]) or text
    return filtered[:max_chars]

def call_openai_financials(email_body: str, model: str = OPENAI_MODEL) -> Dict[str, Any]:
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    client = OpenAI(api_key=api_key)

    # Pre-trim to reduce tokens
    trimmed = extract_financial_window(email_body, ctx_lines=2, max_chars=int(os.environ.get("OPENAI_MAX_CHARS", "8000")))

    def _prompt(txt: str) -> str:
        return USER_TMPL % {
            "fields_json": json.dumps(FINANCIAL_CANON, ensure_ascii=False, indent=4),
            "input_text": txt
        }

    attempts = 3
    shrink_steps = [1.0, 0.6, 0.35]
    for attempt in range(attempts):
        factor = shrink_steps[attempt] if attempt < len(shrink_steps) else 0.25
        use_txt = trimmed[: int(len(trimmed) * factor)]
        try:
            log(f"LLM: START disposal financials (try={attempt+1}/{attempts}, model={model}, chars={len(use_txt)})")
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role":"system","content":SYSTEM},
                          {"role":"user","content":_prompt(use_txt)}],
                temperature=0,
                response_format={"type":"json_object"},
            )
            content = resp.choices[0].message.content
            log(f"LLM: END disposal financials (resp chars={len(content)})")
            return json.loads(content)
        except Exception as e:
            msg = str(e)
            if "rate limit" in msg.lower() or "429" in msg:
                log(f"LLM rate limit on try {attempt+1}: {e}")
                time.sleep(2 * (attempt + 1))  # brief backoff to avoid blocking the whole runner
                continue
            log(f"LLM error (non-retriable): {e}")
            raise

    raise RuntimeError("OpenAI rate limit: all retries exhausted for disposal financials.")

# ---------------- Handler entrypoint ----------------
def handle(msg, ctx):
    """
    - Send EMAIL BODY to OpenAI to extract financial fields (bank-agnostic).
    - Upsert only FINANCIAL columns into remittance.xlsx.
    - PK preference: RemitterReference (RemitterPK) else InwardReference (InwardPK).
    - Never touch non-financial columns (they may be filled by other functions).
    """
    try:
        subject = header_value(msg,"Subject")
        sender  = header_value(msg,"From")
        date    = header_value(msg,"Date")
        body    = body_text(msg.get("payload", {}))

        log("Handling Inward Remittance Intimation (financials via LLM)")
        log(f"Subject: {subject}")
        log(f"From   : {sender}")

        # 1) LLM extract
        focused_preview = extract_financial_window(body)
        log(f"Focused financial text chars: {len(focused_preview)} (original {len(body)})")
        result = call_openai_financials(body)
        fields = (result.get("FinancialFields") or {}) if result.get("IsRelevant") else {}
        confidence = result.get("Confidence", 0.0)

        # 2) Build safe update dict (canon + pk mirrors)
        updates: Dict[str, Any] = {}
        for k in FINANCIAL_CANON:
            v = fields.get(k)
            if v is not None and v != "":
                updates[k] = str(v)

        # Derive PKs (primary = RemitterReference if present, else InwardReference)
        remitter_ref = fields.get("RemitterReference") or ""
        inward_ref   = fields.get("InwardReference") or ""
        pk_col = "RemitterPK" if remitter_ref else "InwardPK"
        pk_val = remitter_ref if remitter_ref else inward_ref

        updates["RemitterPK"] = remitter_ref
        updates["InwardPK"]   = inward_ref

        # Minimal email meta (do not overwrite other systems’ columns)
        updates["EMAIL_Type"]    = "DisposalEmail"
        updates["EmailSubject"]  = subject or ""
        updates["EmailFrom"]     = sender or ""
        updates["EmailDate"]     = date or ""

        # 3) Allowed columns to touch (financial only + PK + minimal meta)
        ALLOWED_TO_UPDATE = list(set(
            FINANCIAL_CANON
            + ["RemitterPK","InwardPK","EMAIL_Type","EmailSubject","EmailFrom","EmailDate"]
        ))

        # 4) Upsert into Excel by chosen PK; only update allowed columns
        df = read_df()
        df = upsert_selected(df, pk_col=pk_col, updates=updates, allowed_cols=ALLOWED_TO_UPDATE)
        write_df(df)

        # 5) Logs / summary
        log("=== Disposal LLM Summary ===")
        log(f"Confidence     : {round(float(confidence or 0.0),3)}")
        log(f"Primary Key    : {pk_col} = {pk_val}")
        for k in [
            "RemitterName","RemitterReference","InwardReference","CurrencyCode",
            "AmountFCY","AmountINR","ExchangeRate","ValueDate","CreditDate",
            "RemittingBankName","RemittingBankSWIFT","BeneficiaryName","BeneficiaryAccount",
            "PurposeCode","PurposeDescription"
        ]:
            if k in updates:
                log(f"{k:20s}: {updates.get(k,'')}")
        log("Upserted financials into remittance.xlsx (financial fields only).")
        log("============================")

    except Exception as e:
        log(f"ERROR in disposal handler: {e}")
        traceback.print_exc()
