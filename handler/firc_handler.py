# handlers/firc_handler.py
import os, base64, re, traceback, json, time, tempfile
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional
import pandas as pd

EXCEL_PATH = "remittance.xlsx"
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
DRIVE_FOLDER_ID = os.environ.get("DRIVE_FOLDER_ID", "").strip()  # optional; else a "FIRC" folder is created/found

def log(msg: str): print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] [FIRC] {msg}", flush=True)

# ---------------- Gmail helpers ----------------
def header_value(msg, name: str):
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name","").lower()==name.lower(): return h.get("value","")
    return ""

def list_attachments(msg):
    atts=[]
    def walk(parts):
        for p in parts:
            if p.get("parts"): walk(p["parts"])
            fn=p.get("filename") or ""
            body=p.get("body",{})
            if fn and body.get("attachmentId"):
                atts.append({"id": body["attachmentId"], "filename": fn, "mimeType": p.get("mimeType","")})
    payload=msg.get("payload",{})
    if payload.get("parts"): walk(payload["parts"])
    return atts

def download_attachment(gmail, message_id, attachment_id, filename, folder="downloads"):
    os.makedirs(folder, exist_ok=True)
    att=gmail.users().messages().attachments().get(userId="me", messageId=message_id, id=attachment_id).execute()
    data=att.get("data")
    raw=base64.urlsafe_b64decode(data.encode("utf-8"))
    path=os.path.join(folder, filename)
    with open(path,"wb") as f: f.write(raw)
    return path

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

# ---------------- Google Drive helpers ----------------
def drive_service():
    """Drive API with drive.file scope; token stored in token_drive.json."""
    from google.oauth2.credentials import Credentials
    from googleapiclient.discovery import build
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    SCOPES = ["https://www.googleapis.com/auth/drive.file"]
    token_path = "token_drive.json"
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                raise RuntimeError("credentials.json not found for Drive auth.")
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    return build("drive", "v3", credentials=creds)

def ensure_drive_folder(svc, name: str = "FIRC") -> str:
    """Find or create a folder by name in My Drive; return folder ID."""
    if DRIVE_FOLDER_ID:
        log(f"Using DRIVE_FOLDER_ID from env: {DRIVE_FOLDER_ID}")
        return DRIVE_FOLDER_ID
    q = f"name = '{name}' and mimeType = 'application/vnd.google-apps.folder' and trashed = false"
    res = svc.files().list(q=q, spaces="drive", fields="files(id,name)", pageSize=10).execute()
    items = res.get("files", [])
    if items:
        folder_id = items[0]["id"]
        log(f"Drive folder found: {name} ({folder_id})")
        return folder_id
    file_metadata = {"name": name, "mimeType": "application/vnd.google-apps.folder"}
    new = svc.files().create(body=file_metadata, fields="id").execute()
    folder_id = new["id"]
    log(f"Drive folder created: {name} ({folder_id})")
    return folder_id

def upload_pdf_to_drive(svc, file_path: str, folder_id: str) -> Dict[str, str]:
    """Upload local PDF to Drive folder; return {'id':..., 'url':...}."""
    from googleapiclient.http import MediaFileUpload
    file_name = os.path.basename(file_path)
    media = MediaFileUpload(file_path, mimetype="application/pdf", resumable=True)
    body = {"name": file_name, "parents": [folder_id]}
    created = svc.files().create(body=body, media_body=media, fields="id,webViewLink,webContentLink").execute()
    file_id = created.get("id")
    url = created.get("webViewLink") or f"https://drive.google.com/file/d/{file_id}/view"
    log(f"Uploaded to Drive: {file_name} (id={file_id})")
    return {"id": file_id, "url": url}

# ---------------- PDF helpers (incl. password handling) ----------------
def is_pdf_encrypted(pdf_path: str) -> bool:
    try:
        from PyPDF2 import PdfReader
        r = PdfReader(pdf_path)
        return bool(getattr(r, "is_encrypted", False))
    except Exception:
        # if can't open, assume might be encrypted
        return True

def gather_candidate_passwords(email_body: str, sender: str = "", subject: str = "") -> List[str]:
    """
    Build candidate list from:
      - ctx match password (handled in handle() and merged in)
      - env: PDF_PASSWORD (global); YESBANK_PDF_PASSWORD / HDFCBANK_PDF_PASSWORD / ICICI_PDF_PASSWORD (customize as needed)
      - env: PDF_PASSWORDS (comma-separated)
      - passwords.json (domains/senders/subjects maps)
      - hints in body: lines like "Password: XXXXXX" or "pwd: XXXXX"
    """
    cands: List[str] = []

    # Env globals
    if os.environ.get("PDF_PASSWORD"): cands.append(os.environ["PDF_PASSWORD"].strip())
    for bank_env in ("YESBANK_PDF_PASSWORD","HDFCBANK_PDF_PASSWORD","ICICI_PDF_PASSWORD"):
        v = os.environ.get(bank_env)
        if v: cands.append(v.strip())
    multi = os.environ.get("PDF_PASSWORDS","")
    if multi:
        cands += [p.strip() for p in multi.split(",") if p.strip()]

    # passwords.json (optional)
    try:
        if os.path.exists("passwords.json"):
            with open("passwords.json","r",encoding="utf-8") as f:
                j = json.load(f)
            domains = j.get("domains", {})
            senders = j.get("senders", {})
            subjmap = j.get("subjects", {})

            sender_l = (sender or "").lower()
            for k, vals in senders.items():
                if k.lower() in sender_l and isinstance(vals, list):
                    cands += vals
            for dom, vals in domains.items():
                if isinstance(vals, list) and dom.lower() in sender_l:
                    cands += vals

            subj_l = (subject or "").lower()
            for ss, vals in subjmap.items():
                if ss.lower() in subj_l and isinstance(vals, list):
                    cands += vals
    except Exception:
        pass

    # Hints in body
    hints = re.findall(r'(?:password|pwd)\s*[:\-]\s*([A-Za-z0-9@#_\-\.]+)', email_body or "", flags=re.I)
    cands += [h.strip() for h in hints if h.strip()]

    # Dedup preserve order
    seen=set(); out=[]
    for p in cands:
        if p not in seen:
            seen.add(p); out.append(p)
    return out

def try_decrypt_pdf(src_path: str, candidates: List[str]) -> Tuple[str, Optional[str]]:
    """
    Try to open/decrypt the PDF with any candidate. Return (path_to_use, used_password_or_None).
    If decrypted, write a temp copy and return its path.
    """
    # PyPDF2 attempt
    try:
        from PyPDF2 import PdfReader, PdfWriter
        reader = PdfReader(src_path)
        if not getattr(reader, "is_encrypted", False):
            return src_path, None
        for pw in candidates:
            try:
                if reader.decrypt(pw) == 0:
                    continue
                writer = PdfWriter()
                for page in reader.pages:
                    writer.add_page(page)
                fd, tmp = tempfile.mkstemp(suffix=".pdf"); os.close(fd)
                with open(tmp, "wb") as out:
                    writer.write(out)
                return tmp, pw
            except Exception:
                continue
    except Exception:
        pass

    # pikepdf fallback
    try:
        import pikepdf
        for pw in candidates:
            try:
                with pikepdf.open(src_path, password=pw) as pdf:
                    fd, tmp = tempfile.mkstemp(suffix=".pdf"); os.close(fd)
                    pdf.save(tmp)
                    return tmp, pw
            except Exception:
                continue
    except Exception:
        pass

    return src_path, None

def read_pdf_text(pdf_path: str) -> str:
    text = ""
    try:
        import pdfplumber
        with pdfplumber.open(pdf_path) as pdf:
            text = "\n".join((p.extract_text() or "") for p in pdf.pages)
    except Exception:
        try:
            from PyPDF2 import PdfReader
            r = PdfReader(pdf_path)
            text = "\n".join((p.extract_text() or "") for p in r.pages)
        except Exception as e:
            raise RuntimeError(f"Failed to read PDF: {e}")
    text = text.replace("\u00a0", " ")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
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

def upsert_by_inward(df: pd.DataFrame, updates: Dict[str, Any]) -> pd.DataFrame:
    """Upsert by InwardPK; create a row if not exists, else update provided columns."""
    updates = {k: ("" if v is None else str(v)) for k, v in updates.items()}
    df = ensure_cols(df, list(updates.keys()) + ["InwardPK"])
    inward_pk = updates.get("InwardPK", "")
    if not inward_pk:
        return pd.concat([df, pd.DataFrame([updates])], ignore_index=True)
    mask = df["InwardPK"].astype(str) == inward_pk
    if mask.any():
        idx = df[mask].index[0]
        for k, v in updates.items():
            old = str(df.at[idx, k]) if k in df.columns else ""
            if (old or "") != (v or ""):
                df.at[idx, k] = v
        return df
    else:
        return pd.concat([df, pd.DataFrame([updates])], ignore_index=True)

# ---------------- LLM extraction ----------------
SYSTEM = "You are a strict information extraction engine. Return ONLY valid JSON."

FIRC_CANON = [
    # Linkage / keys
    "InwardReference", "FIRCNumber", "IssueDate",
    # Parties / accounts
    "BeneficiaryName", "BeneficiaryAccount",
    "RemitterName", "RemitterReference", "RemittingBankName", "RemittingBankSWIFT",
    # Amounts / currency / rates / dates
    "CurrencyCode", "AmountFCY", "AmountINR", "ExchangeRate", "ValueDate", "CreditDate",
    # GST/Tax line items (when applicable)
    "GSTInvoiceNumber", "CustomerGSTIN", "BankGSTIN",
    "IGSTRatePercent", "IGSTAmount", "CGSTRatePercent", "CGSTAmount",
    "SGSTRatePercent", "SGSTAmount", "UTGSTRatePercent", "UTGSTAmount",
    "CessRatePercent", "CessAmount", "GrandTotal",
    # Purpose
    "PurposeCode", "PurposeDescription"
]

USER_TMPL = """You will receive the FULL TEXT of a bank-issued FIRC / Debit-cum-Credit Advice PDF (converted to text).
Extract as MANY FINANCIAL/TRANSACTION details as possible (bank-agnostic). Keep values exactly as printed.

Guidelines:
- Keys/labels vary (e.g., INW_NO, IRM, OSN, UTR, Ref, FIRC No, Advice No). Infer meaning from context.
- InwardReference = the bank’s inward/IRM/transaction reference (REQUIRED to proceed).
- CurrencyCode must be ISO 4217 (USD, EUR, INR, etc.).
- RemittingBankSWIFT is 8 or 11 alphanumerics if present.
- Preserve original formatting for dates and numbers; do NOT reformat.
- If a field is absent/unclear, set it to null. Do NOT hallucinate.

Return ONLY this JSON:
{
  "IsRelevant": true,
  "Confidence": <0..1>,
  "Fields": {
%(fields_json)s
  }
}

FIRC/ADVICE TEXT:
\"\"\"%(input_text)s\"\"\""""

def call_openai_firc(pdf_text: str, model: str = OPENAI_MODEL) -> Dict[str, Any]:
    from openai import OpenAI
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    client = OpenAI(api_key=api_key)

    max_chars = int(os.environ.get("OPENAI_MAX_CHARS", "12000"))
    txt = pdf_text[:max_chars]

    def _prompt(t: str) -> str:
        return USER_TMPL % {
            "fields_json": json.dumps(FIRC_CANON, ensure_ascii=False, indent=4),
            "input_text": t
        }

    attempts = 3
    shrink_steps = [1.0, 0.6, 0.35]
    for attempt in range(attempts):
        factor = shrink_steps[attempt] if attempt < len(shrink_steps) else 0.25
        use_txt = txt[: int(len(txt) * factor)]
        try:
            log(f"LLM: START FIRC extract (try={attempt+1}/{attempts}, model={model}, chars={len(use_txt)})")
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role":"system","content":SYSTEM},
                          {"role":"user","content":_prompt(use_txt)}],
                temperature=0,
                response_format={"type":"json_object"},
            )
            content = resp.choices[0].message.content
            log(f"LLM: END FIRC extract (resp chars={len(content)})")
            return json.loads(content)
        except Exception as e:
            msg = str(e)
            if "rate limit" in msg.lower() or "429" in msg:
                log(f"LLM rate limit: {e}")
                time.sleep(2 * (attempt + 1))
                continue
            log(f"LLM error (non-retriable): {e}")
            raise
    raise RuntimeError("OpenAI rate limit: all retries exhausted for FIRC extraction.")

# ---------------- Handler entrypoint ----------------
def handle(msg, ctx):
    """
    For each PDF attachment:
      1) download PDF
      2) if encrypted → try password(s) (from rules 'pdf_password', env, passwords.json, or email body hints)
      3) read text (decrypted or original)
      4) LLM extract; proceed ONLY if InwardReference is present
      5) upload PDF to Drive
      6) upsert Excel by InwardPK (create row if not exists; else update provided fields)
    """
    try:
        gmail   = ctx.get("gmail")
        match   = ctx.get("match", {}) or {}
        subject = header_value(msg,"Subject")
        sender  = header_value(msg,"From")
        date    = header_value(msg,"Date")
        email_body_for_hints = body_text(msg.get("payload", {}))

        log("Handling FIRC / Debit-cum-Credit Advice")
        log(f"Subject: {subject}")
        log(f"From   : {sender}")

        atts = list_attachments(msg)
        pdfs = [a for a in atts if (a.get("filename","").lower().endswith(".pdf"))]

        if not pdfs:
            log("No PDF attachments found; nothing to do.")
            return

        # Prepare Drive (optional but recommended)
        try:
            dsvc = drive_service()
            folder_id = ensure_drive_folder(dsvc, name="FIRC")
        except Exception as e:
            log(f"Drive setup failed: {e}")
            dsvc = None
            folder_id = ""

        df = read_df()
        updated_any = False

        # Password provided directly from rules?
        direct_pw = match.get("pdf_password")
        if direct_pw:
            log("Using password provided by rules.")

        for a in pdfs:
            local_path = download_attachment(gmail, msg["id"], a["id"], a["filename"])
            log(f"Saved PDF: {local_path}")

            # If encrypted, try to decrypt
            dec_used = None
            if is_pdf_encrypted(local_path):
                log("PDF appears encrypted. Trying candidate passwords...")
                candidates = []
                if direct_pw:
                    candidates.append(direct_pw)
                # merge with guessed candidates
                candidates += gather_candidate_passwords(email_body_for_hints, sender=sender, subject=subject)
                # dedup again
                seen=set(); cands=[]
                for p in candidates:
                    if p not in seen:
                        seen.add(p); cands.append(p)
                if not cands:
                    log("No password candidates available; will upload as-is and skip extraction.")
                else:
                    dec_path, used_pw = try_decrypt_pdf(local_path, cands)
                    if dec_path != local_path:
                        log("PDF decrypted successfully with a candidate password.")
                        local_path = dec_path
                        dec_used = used_pw
                    else:
                        log("Failed to decrypt PDF with provided candidates; will upload as-is and skip extraction.")

            # Read text (only if not encrypted, or decrypted successfully)
            can_extract = True
            if is_pdf_encrypted(local_path) and dec_used is None:
                can_extract = False

            pdf_text = ""
            if can_extract:
                try:
                    pdf_text = read_pdf_text(local_path)
                except Exception as e:
                    log(f"ERROR reading PDF '{local_path}': {e}")
                    can_extract = False

            # LLM extraction (only if we have text)
            fields = {}
            inward_ref = ""
            conf = 0.0
            if can_extract and pdf_text:
                result = call_openai_firc(pdf_text)
                if result.get("IsRelevant", False):
                    fields = result.get("Fields") or {}
                    inward_ref = (fields.get("InwardReference") or "").strip()
                    conf = result.get("Confidence", 0.0)
                else:
                    log("LLM says document not relevant; skipping this PDF.")

            # Proceed only with InwardReference
            if not inward_ref:
                log("No InwardReference extracted; will still upload PDF to Drive for record, but skip Excel update.")
                # upload even if not extractable
                if dsvc and folder_id:
                    try:
                        up = upload_pdf_to_drive(dsvc, local_path, folder_id)
                        log(f"Uploaded (no extract): {up.get('url','')}")
                    except Exception as e:
                        log(f"Drive upload failed for {local_path}: {e}")
                continue

            # Upload PDF to Drive
            drive_id, drive_url = "", ""
            if dsvc and folder_id:
                try:
                    up = upload_pdf_to_drive(dsvc, local_path, folder_id)
                    drive_id = up.get("id","")
                    drive_url = up.get("url","")
                except Exception as e:
                    log(f"Drive upload failed for {local_path}: {e}")

            # Build updates dict
            updates: Dict[str, Any] = {}
            for k, v in (fields or {}).items():
                if v is not None and v != "":
                    updates[k] = str(v)
            updates["InwardPK"] = inward_ref

            # Merge local SavedPDFs history
            prev = str(df.loc[df["InwardPK"]==inward_ref, "SavedPDFs"].iloc[0]) if ("InwardPK" in df.columns and (df["InwardPK"]==inward_ref).any() and "SavedPDFs" in df.columns) else ""
            updates["SavedPDFs"] = ", ".join(sorted(set([p.strip() for p in (prev + ("," if prev else "") + os.path.basename(local_path)).split(",") if p.strip()])))
            # Drive refs
            if drive_id:
                updates["DriveFileId"]  = drive_id
            if drive_url:
                updates["DriveFileUrl"] = drive_url
            # Minimal email meta
            updates["EMAIL_Type"]   = "FIRC"
            updates["EmailSubject"] = subject or ""
            updates["EmailFrom"]    = sender or ""
            updates["EmailDate"]    = date or ""

            # Upsert by InwardPK
            df = upsert_by_inward(df, updates)
            updated_any = True

            # Summary logs
            log("=== FIRC LLM Summary ===")
            log(f"Confidence       : {round(float(conf or 0.0),3)}")
            log(f"InwardReference  : {inward_ref}")
            if dec_used:
                log(f"DecryptionPwd    : (candidate used)")
            if drive_id:
                log(f"DriveFileId      : {drive_id}")
                log(f"DriveFileUrl     : {drive_url}")
            for k in ["FIRCNumber","IssueDate","CurrencyCode","AmountFCY","AmountINR","ExchangeRate","ValueDate","RemitterName","BeneficiaryName","GSTInvoiceNumber","CustomerGSTIN","BankGSTIN","GrandTotal"]:
                if k in updates:
                    log(f"{k:16s}: {updates.get(k,'')}")
            log("=========================")

        if updated_any:
            write_df(df)
            log("Upserted FIRC data into remittance.xlsx")
        else:
            log("No rows updated/created from the PDFs.")

    except Exception as e:
        log(f"ERROR: {e}")
        traceback.print_exc()
