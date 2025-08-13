#!/usr/bin/env python3
import os, time, json, base64, re, importlib, traceback
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional

from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

STATE_FILE = "state.json"
GMAIL_SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

# ---------- Logging ----------
def now(): return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
def log(msg: str): print(f"[{now()}] {msg}", flush=True)

# ---------- Gmail helpers ----------
def google_service(api: str, version: str, scopes: List[str]):
    creds = None
    token_path = f"token_{api}.json"
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, scopes)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists("credentials.json"):
                raise RuntimeError("credentials.json not found.")
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", scopes)
            creds = flow.run_local_server(port=0)
        with open(token_path, "w") as f: f.write(creds.to_json())
    return build(api, version, credentials=creds)

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE,"r") as f: return json.load(f)
    return {"last_internal_ts": 0, "processed_ids": []}

def save_state(state):
    state["processed_ids"] = state.get("processed_ids", [])[-5000:]
    with open(STATE_FILE,"w") as f: json.dump(state, f, indent=2)

def header_value(msg: Dict[str, Any], name: str) -> str:
    for h in msg.get("payload", {}).get("headers", []):
        if h.get("name","").lower() == name.lower():
            return h.get("value","")
    return ""

def get_recipients(msg: Dict[str, Any]) -> List[str]:
    tos = header_value(msg, "To"); ccs = header_value(msg, "Cc"); bccs = header_value(msg, "Bcc")
    parts = []
    for block in (tos, ccs, bccs):
        if block:
            parts += [p.strip() for p in re.split(r"[;,]", block) if p.strip()]
    return parts

def get_body_text(payload: Dict[str, Any]) -> str:
    def walk(parts):
        out=[]
        for p in parts:
            mime=p.get("mimeType","")
            body=p.get("body",{})
            data=body.get("data")
            if p.get("parts"): out+=walk(p["parts"])
            elif mime in ("text/plain","text/html") and data:
                decoded = base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8","ignore")
                out.append(decoded)
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

def list_attachments(msg: Dict[str, Any]) -> List[Dict[str, Any]]:
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

def list_new_messages(gmail, last_ts_ms: int) -> List[Dict[str, Any]]:
    """
    Fetch only messages strictly AFTER last_ts_ms.
    Uses Gmail 'after:<unix_seconds>' and then double-checks internalDate > last_ts_ms.
    """
    after_seconds = max(0, last_ts_ms // 1000)
    query = f'in:inbox after:{after_seconds}'
    log(f"Gmail: listing with query: {query}")
    msgs=[]; total=0
    resp=gmail.users().messages().list(userId="me", q=query, maxResults=100).execute()
    ids=resp.get("messages",[])
    while True:
        if not ids: break
        total += len(ids)
        for m in ids:
            full=gmail.users().messages().get(userId="me", id=m["id"], format="full").execute()
            msgs.append(full)
        tok=resp.get("nextPageToken")
        if not tok: break
        resp=gmail.users().messages().list(userId="me", q=query, pageToken=tok, maxResults=100).execute()
        ids=resp.get("messages",[])
    # Strictly after guard (ms precision)
    fresh=[m for m in msgs if int(m.get("internalDate",0)) > last_ts_ms]
    log(f"Gmail: fetched {total} stubs; {len(msgs)} full; {len(fresh)} strictly after {last_ts_ms}")
    fresh.sort(key=lambda x:int(x.get("internalDate",0)))
    return fresh

# ---------- EmailContext passed into rules.py ----------
@dataclass
class EmailContext:
    id: str
    internal_ts: int
    From_: str
    ToCcBcc: List[str]
    Subject: str
    Date: str
    Body: str
    Attachments: List[Dict[str, Any]]

# ---------- Dispatch ----------
def call_handler(handler_module: str, handler_function: str, msg: Dict[str, Any], ctx: Dict[str, Any]):
    try:
        mod = importlib.import_module(handler_module)
        fn = getattr(mod, handler_function)
        fn(msg, ctx)
    except Exception as e:
        log(f"Handler error for {handler_module}.{handler_function}: {e}")
        traceback.print_exc()

def main():
    log("Building Gmail service...")
    gmail = google_service("gmail","v1", GMAIL_SCOPES)

    state = load_state()
    last_ts = int(state.get("last_internal_ts", 0))
    processed = set(state.get("processed_ids", []))

    # Initialize on first run: start from NOW (do not process historical)
    if last_ts == 0:
        last_ts = int(time.time() * 1000)
        state["last_internal_ts"] = last_ts
        save_state(state)
        log(f"Initialized last_internal_ts to NOW = {last_ts} (ms). Historical emails will NOT be processed.")

    # import rules module
    import rules

    log("Runner started. Polling every 30s ...")
    while True:
        try:
            # hot reload rules each cycle so you can edit without restarting
            importlib.reload(rules)

            log("Polling Gmail...")
            messages = list_new_messages(gmail, last_ts)

            for msg in messages:
                mid = msg["id"]; its = int(msg.get("internalDate", 0))
                if mid in processed: 
                    continue

                sender   = header_value(msg, "From")
                subject  = header_value(msg, "Subject")
                date_hdr = header_value(msg, "Date")
                body     = get_body_text(msg.get("payload", {}))
                rcpts    = get_recipients(msg)
                atts     = list_attachments(msg)

                log(f"New message id={mid} ts={its}")
                log(f"From: {sender}")
                log(f"Subject: {subject}")

                email_ctx = EmailContext(
                    id=mid, internal_ts=its,
                    From_=sender, ToCcBcc=rcpts, Subject=subject, Date=date_hdr,
                    Body=body, Attachments=atts
                )

                # your Python rules decide categories and targets
                try:
                    matches = rules.categorize(email_ctx)
                except Exception as e:
                    log(f"rules.categorize() error: {e}")
                    traceback.print_exc()
                    matches = []

                if not matches:
                    log("No rule matched. Ignoring.")
                else:
                    for m in matches:
                        log(f"Matched category: {m.get('name')} -> {m.get('handler_module')}.{m.get('handler_function')}")
                        ctx = {
                            "gmail": gmail,
                            "message_metadata": {
                                "id": mid, "internalDate": its,
                                "From": sender, "Subject": subject, "Date": date_hdr
                            },
                            "match": m
                        }
                        call_handler(m["handler_module"], m["handler_function"], msg, ctx)
                        if m.get("stop_after_match", True):
                            log("Stop after match: True")
                            break

                processed.add(mid)
                # Move the watermark forward to the latest processed internalDate
                if its > last_ts:
                    last_ts = its

            if messages:
                state["last_internal_ts"] = last_ts
                state["processed_ids"] = list(processed)
                save_state(state)

        except Exception as e:
            log(f"ERROR: {e}")
            traceback.print_exc()

        time.sleep(10)

if __name__ == "__main__":
    main()
