# rules.py
from typing import List, Dict, Any
from dataclasses import dataclass
import re

# Keep in sync with runner.EmailContext (types here are just hints)
@dataclass
class EmailContext:
    id: str
    internal_ts: int
    From_: str
    ToCcBcc: list
    Subject: str
    Date: str
    Body: str
    Attachments: list  # each item: {"id","filename","mimeType"}

# ---------- normalization & helpers ----------
def _norm_spaces(s: str) -> str:
    """Lowercase + collapse all whitespace to single spaces."""
    s = (s or "")
    s = s.lower()
    s = re.sub(r"\s+", " ", s)  # collapse newlines/tabs/multiple spaces
    return s.strip()

def _contains(hay: str, needle: str) -> bool:
    return _norm_spaces(needle) in _norm_spaces(hay)

def _contains_all(hay: str, needles: List[str]) -> bool:
    norm_hay = _norm_spaces(hay)
    return all(_norm_spaces(n) in norm_hay for n in needles if n is not None)

def has_attachment(ctx: EmailContext) -> bool:
    return bool(ctx.Attachments)

def attachment_ext_is(ctx: EmailContext, exts: List[str], require_all: bool = False) -> bool:
    files = [((a.get("filename") or "").lower()) for a in (ctx.Attachments or [])]
    exts = [e.lower().lstrip(".") for e in (exts or [])]

    def has_ext(ext: str) -> bool:
        return any(f.endswith("." + ext) for f in files)

    if require_all:
        return all(has_ext(ext) for ext in exts)
    return any(has_ext(ext) for ext in exts)

def attachment_name_contains(ctx: EmailContext, needles: List[str]) -> bool:
    """Case-insensitive substring check on attachment filenames."""
    files = [((a.get("filename") or "").lower()) for a in (ctx.Attachments or [])]
    needles = [(n or "").lower() for n in (needles or [])]
    return any(any(n in f for n in needles) for f in files)

# ---------- main rules ----------
def categorize(ctx: EmailContext) -> List[Dict[str, Any]]:
    matches: List[Dict[str, Any]] = []

    subj = ctx.Subject or ""
    body = ctx.Body or ""

    # ===========================================================
    # Category 1: inward_remmittance_intimation
    # if:
    #   subject contains "DISPOSAL REQUIRED FOR FCY INWARD "
    #   AND body contains each of:
    #       "From: FCYinward.disposal@hdfcbank.com <FCYinward.disposal@hdfcbank.com>"
    #       "To: Promoters Canopus <promoters@canopusinfosystems.com>"
    #       "We are in receipt of following inward remittance."
    #       "Kindly provide following disposal instructions"
    #       "INW_NO"
    # ===========================================================
    inward_subject_ok = _contains(subj, "DISPOSAL REQUIRED FOR FCY INWARD")
    inward_body_ok = _contains_all(
        body,
        [
            "FCYinward.disposal@hdfcbank.com",
            "promoters@canopusinfosystems.com",
            "We are in receipt of following inward remittance.",
            "Kindly provide following disposal instructions",
            "INW_NO",
        ],
    )

    if inward_subject_ok and inward_body_ok:
        matches.append({
            "name": "inward_remmittance_intimation",
            "handler_module": "handlers.disposal_handler",
            "handler_function": "handle",
            "stop_after_match": True,
            "why": {
                "subject_contains": inward_subject_ok,
                "body_contains_all": inward_body_ok
            }
        })

    # ===========================================================
    # Category 2: HDFC Bank FIRC
    # if:
    #   subject contains "Debit Cum Credit Advice For FCY Inward Remittance"
    #   AND body contains each of:
    #       "From: Inward.Remittances@hdfcbank.com <Inward.Remittances@hdfcbank.com>"
    #       "To: Promoters Canopus <promoters@canopusinfosystems.com>"
    #       "Please find the attached Debit Cum Credit advice for Inward"
    #       "For any queries,Please write to us at firchelpdesk@hdfcbank.com "
    #   AND has attachment
    #   AND attachment type includes PDF
    # ===========================================================
    firc_subject_ok = _contains(subj, "Debit Cum Credit Advice For FCY Inward Remittance")
    firc_body_ok = _contains_all(
        body,
        [
            "Inward.Remittances@hdfcbank.com",
            "promoters@canopusinfosystems.com",
            "Please find the attached Debit Cum Credit advice for Inward",
            "For any queries,Please write to us at firchelpdesk@hdfcbank.com",
        ],
    )
    firc_attach_ok = has_attachment(ctx) and attachment_ext_is(ctx, ["pdf"], require_all=False)

    if firc_subject_ok and firc_body_ok and firc_attach_ok:
        matches.append({
            "name": "FIRC",
            "handler_module": "handlers.firc_handler",
            "handler_function": "handle",
            "stop_after_match": True,
            "why": {
                "subject_contains": firc_subject_ok,
                "body_contains_all": firc_body_ok,
                "has_pdf_attachment": firc_attach_ok
            }
        })

    # ===========================================================
    # Category 3: Yes Bank FIRC
    # if:
    #   subject contains "Debit Cum Credit Advice For FCY Inward Remittance"
    #   AND body contains each of:
    #       "From: Inward.Remittances@hdfcbank.com <Inward.Remittances@hdfcbank.com>"
    #       "To: Promoters Canopus <promoters@canopusinfosystems.com>"
    #       "Please find the attached Debit Cum Credit advice for Inward"
    #       "For any queries,Please write to us at firchelpdesk@hdfcbank.com "
    #   AND has attachment
    #   AND attachment type includes PDF
    # ===========================================================
    firc_subject_ok = _contains(subj, "Miscellaneous_Advices - INWARD REMITTANCE - YBL REF NO")
    firc_body_ok = _contains_all(
        body,
        [
            "yestouch@yesbank.in",
            "We attach herewith the transaction advice for trade transaction reference"
        ],
    )
    firc_attach_ok = has_attachment(ctx) and attachment_ext_is(ctx, ["pdf"], require_all=False) and attachment_name_contains(ctx, ["firc"])

    if firc_subject_ok and firc_body_ok and firc_attach_ok:
        matches.append({
            "name": "FIRC",
            "handler_module": "handlers.firc_handler",
            "handler_function": "handle",
            "stop_after_match": True,
            "pdf_password": "21893044",
            "why": {
                "subject_contains": firc_subject_ok,
                "body_contains_all": firc_body_ok,
                "has_pdf_attachment": firc_attach_ok
            }
        })

    return matches
