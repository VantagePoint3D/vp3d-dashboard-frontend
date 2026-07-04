"""
VantagePoint 3D — Job Tracker API Server
FastAPI backend: proxies Spiro data, stores team notes in Google Sheets,
fetches client feedback from Go High Level.

Run locally:  uvicorn api_server:app --reload --port 8000
Deploy:       Railway (see Procfile)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Optional

import httpx
from cachetools import TTLCache
from dotenv import load_dotenv
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

load_dotenv()

# ── CONFIG ────────────────────────────────────────────────
SPIRO_API_KEY     = os.getenv("SPIRO_API_KEY", "")
SPIRO_BASE_URL    = os.getenv("SPIRO_API_BASE_URL", "https://api.spiro.media/api/v1")
DASHBOARD_API_KEY = os.getenv("DASHBOARD_API_KEY", "")
GOOGLE_SHEET_ID   = os.getenv("GOOGLE_SHEET_ID", "")
GOOGLE_CREDS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "google_credentials.json")
ALLOWED_ORIGINS   = os.getenv("ALLOWED_ORIGINS", "*").split(",")

# GHL (Go High Level)
GHL_API_KEY     = os.getenv("GHL_API_KEY", "")
GHL_LOCATION_ID = os.getenv("GHL_LOCATION_ID", "8gHIkZhM1JGLBhOPJ9x7")
GHL_BASE_URL    = "https://services.leadconnectorhq.com"
GHL_API_VERSION = "2021-07-28"

# GHL "Feedback and Reviews" pipeline and stage IDs
GHL_FEEDBACK_PIPELINE = "zXS1IaswSqTYaBb6xXHp"
GHL_STAGE_SENTIMENT = {
    "50691dd5-20f7-4723-94f8-cb779fb8c9e5": "negative",   # Negative Feedback
    "77e0cc72-71c2-44d8-84c9-3754c5f5d6c2": "positive",   # Positive Feedback
    "61bae09b-8592-4640-899c-2313d5c13450": "pending",     # Feedback Request sent
    "bc8eb0d7-e7fc-4895-a0ad-b96045c6f961": "pending",     # New Review Request
    "38e50c82-4c36-4ea2-ad28-8af940983c62": "pending",     # Review Link Clicked
    "6f0354f6-a8ee-424f-a61c-086e5b2a5578": "positive",    # Review Confirmed
    "ab3245bf-b104-4c77-a91f-dba37d8e92a5": "no_review",   # No Review
    "faf0358a-f3a8-4f05-a522-8248083d21de": "archived",    # Archive
}
GHL_FEEDBACK_COMMENT_FIELD = "feedback_comments"

# Google Sheets column layout
SHEET_COLS = ["orderId", "feedback", "errors", "preShootCall", "postDeliveryCall", "reviewReceived", "updatedAt"]

# ── APP ───────────────────────────────────────────────────
app = FastAPI(
    title="VantagePoint 3D — Job Tracker API",
    description="Proxy for Spiro data, Google Sheets notes, and GHL feedback",
    version="2.1.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── CACHES ────────────────────────────────────────────────
appt_cache         = TTLCache(maxsize=100,  ttl=300)   # 5 min
order_cache        = TTLCache(maxsize=1000, ttl=600)   # 10 min
media_cache        = TTLCache(maxsize=1000, ttl=1200)  # 20 min
ghl_feedback_cache = TTLCache(maxsize=1,   ttl=900)    # 15 min


# ── AUTH ──────────────────────────────────────────────────
def require_key(x_api_key: Optional[str] = Header(None)):
    if DASHBOARD_API_KEY and x_api_key != DASHBOARD_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid or missing API key")
    return x_api_key


# ── SPIRO HTTP CLIENT ─────────────────────────────────────
async def spiro(path: str, params: dict | None = None) -> dict:
    if not SPIRO_API_KEY:
        raise HTTPException(status_code=503, detail="SPIRO_API_KEY not configured")
    headers = {
        "Authorization": f"Bearer {SPIRO_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(
            f"{SPIRO_BASE_URL}{path}", headers=headers, params=params or {}
        )
        if resp.status_code == 401:
            raise HTTPException(status_code=502, detail="Spiro auth failed — check SPIRO_API_KEY")
        if resp.status_code == 404:
            return {}
        resp.raise_for_status()
        return resp.json()


# ── GHL HTTP CLIENT ───────────────────────────────────────
def ghl_headers() -> dict:
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Version": GHL_API_VERSION,
        "Content-Type": "application/json",
    }


# ── GOOGLE SHEETS CLIENT ──────────────────────────────────
def get_sheet():
    if not GOOGLE_SHEET_ID or not os.path.exists(GOOGLE_CREDS_FILE):
        return None
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(
            GOOGLE_CREDS_FILE,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        gc = gspread.authorize(creds)
        return gc.open_by_key(GOOGLE_SHEET_ID).sheet1
    except Exception as e:
        print(f"[sheets] connection error: {e}")
        return None


def ensure_headers(sheet) -> None:
    try:
        existing = sheet.row_values(1)
        if existing != SHEET_COLS:
            sheet.delete_rows(1)
            sheet.insert_row(SHEET_COLS, index=1)
    except Exception:
        pass


def row_to_dict(row: dict) -> dict:
    return {
        "orderId":          str(row.get("orderId", "")),
        "feedback":         row.get("feedback", ""),
        "errors":           row.get("errors", ""),
        "preShootCall":     row.get("preShootCall", "") in (True, "True", "TRUE", "true", "1"),
        "postDeliveryCall": row.get("postDeliveryCall", "") in (True, "True", "TRUE", "true", "1"),
        "reviewReceived":   row.get("reviewReceived", ""),
        "updatedAt":        row.get("updatedAt", ""),
    }


# ── HEALTH ────────────────────────────────────────────────
@app.get("/health", tags=["System"])
def health():
    return {
        "status":  "ok",
        "service": "VP3D Job Tracker API",
        "spiro":   bool(SPIRO_API_KEY),
        "sheets":  bool(GOOGLE_SHEET_ID and os.path.exists(GOOGLE_CREDS_FILE)),
        "ghl":     bool(GHL_API_KEY),
    }


# ── APPOINTMENTS ──────────────────────────────────────────
@app.get("/api/appointments", tags=["Spiro"])
async def get_appointments(
    from_date: Optional[str] = Query(None, alias="from"),
    to_date:   Optional[str] = Query(None, alias="to"),
    page:      int = Query(1, ge=1),
    page_size: int = Query(25, ge=1, le=500),
    _: str = Depends(require_key),
):
    cache_key = f"appts|{from_date}|{to_date}|{page}|{page_size}"
    if cache_key in appt_cache:
        return appt_cache[cache_key]

    params: dict = {"page": page, "pageSize": page_size}
    if from_date:
        params["filter[arrivalWindowStart][gte]"] = from_date
    if to_date:
        params["filter[arrivalWindowStart][lte]"] = to_date

    data = await spiro("/appointments", params)
    appt_cache[cache_key] = data
    return data


# ── ORDER ─────────────────────────────────────────────────
@app.get("/api/orders/{order_id}", tags=["Spiro"])
async def get_order(order_id: str, _: str = Depends(require_key)):
    if order_id in order_cache:
        return order_cache[order_id]
    data = await spiro(f"/orders/{order_id}")
    # Spiro wraps all responses as { "data": {...}, "meta": null } — unwrap so
    # the frontend can access order fields directly (e.g. order.bundle, order.pricing)
    result = data.get("data", data) if isinstance(data, dict) else data
    order_cache[order_id] = result
    return result


# ── MEDIA COUNTS ──────────────────────────────────────────
@app.get("/api/media/{order_id}", tags=["Spiro"])
async def get_media(order_id: str, _: str = Depends(require_key)):
    if order_id in media_cache:
        return media_cache[order_id]

    photos = videos = None
    try:
        ph = await spiro(f"/orders/{order_id}/photos")
        photos = ph.get("meta", {}).get("resultCount") or len(ph.get("data", []))
    except Exception:
        pass
    try:
        vi = await spiro(f"/orders/{order_id}/videos")
        videos = vi.get("meta", {}).get("resultCount") or len(vi.get("data", []))
    except Exception:
        pass

    result = {"photos": photos, "videos": videos}
    media_cache[order_id] = result
    return result


# ── NOTES ─────────────────────────────────────────────────
class NotePayload(BaseModel):
    feedback:         Optional[str]  = None
    errors:           Optional[str]  = None
    preShootCall:     Optional[bool] = None
    postDeliveryCall: Optional[bool] = None
    reviewReceived:   Optional[str]  = None


@app.get("/api/notes", tags=["Notes"])
def get_all_notes(_: str = Depends(require_key)):
    sheet = get_sheet()
    if not sheet:
        return {}
    try:
        ensure_headers(sheet)
        return {
            str(r["orderId"]): row_to_dict(r)
            for r in sheet.get_all_records()
            if r.get("orderId")
        }
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sheets error: {e}")


@app.post("/api/notes/{order_id}", tags=["Notes"])
def upsert_note(order_id: str, payload: NotePayload, _: str = Depends(require_key)):
    sheet = get_sheet()
    if not sheet:
        raise HTTPException(
            status_code=503,
            detail="Google Sheets not configured — set GOOGLE_SHEET_ID and add google_credentials.json",
        )
    try:
        ensure_headers(sheet)
        rows = sheet.get_all_records()
        now  = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")

        def merge(new_val, old_val, is_bool=False):
            if new_val is None:
                return old_val
            if is_bool:
                return str(new_val)
            return new_val

        for i, row in enumerate(rows, start=2):
            if str(row.get("orderId")) == order_id:
                sheet.update(f"A{i}:G{i}", [[
                    order_id,
                    merge(payload.feedback,         row.get("feedback", "")),
                    merge(payload.errors,           row.get("errors", "")),
                    merge(payload.preShootCall,     row.get("preShootCall", ""), is_bool=True),
                    merge(payload.postDeliveryCall, row.get("postDeliveryCall", ""), is_bool=True),
                    merge(payload.reviewReceived,   row.get("reviewReceived", "")),
                    now,
                ]])
                return {"ok": True, "action": "updated"}

        sheet.append_row([
            order_id,
            payload.feedback         or "",
            payload.errors           or "",
            str(payload.preShootCall)     if payload.preShootCall     is not None else "",
            str(payload.postDeliveryCall) if payload.postDeliveryCall is not None else "",
            payload.reviewReceived   or "",
            now,
        ])
        return {"ok": True, "action": "created"}

    except Exception as e:
        raise HTTPException(status_code=503, detail=f"Sheets error: {e}")


# ── GHL FEEDBACK ──────────────────────────────────────────
@app.get("/api/ghl/feedback", tags=["GHL"])
async def get_ghl_feedback(_: str = Depends(require_key)):
    """
    Returns feedback from GHL's 'Feedback and Reviews' pipeline.

    Response is keyed by lowercase contact NAME (Spiro appointments expose
    agentName but not agentEmail, so name is the only reliable join key).
    Each value is an ARRAY of feedback records sorted newest-first so the
    frontend can pick the entry closest in time to a specific shoot delivery.

    Example:
      {
        "joel elliott": [
          { "sentiment": "positive", "comments": "Great work!", "createdAt": "2026-06-14T...", "name": "Joel Elliott" },
          { "sentiment": "pending",  "comments": "",             "createdAt": "2026-06-01T...", "name": "Joel Elliott" }
        ]
      }

    Matching strategy (implemented in the frontend):
      For each Spiro appointment, look up S.ghlFeedback[agentName.lower()],
      then pick the entry whose createdAt is closest to shootDate + 2 days.
      Falls back to the most recent entry if none is within 30 days.

    Cached for 15 minutes.
    """
    if not GHL_API_KEY:
        return {}

    if "feedback" in ghl_feedback_cache:
        return ghl_feedback_cache["feedback"]

    # keyed by lowercase agent name → list of feedback dicts
    # (Spiro appointments expose agentName but not agentEmail, so we key by name)
    result: dict[str, list] = {}

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            # ── Step 1: paginate ALL opps in the Feedback pipeline ──
            all_opps: list = []
            start_after_id: str | None = None
            start_after:    int | None = None

            while True:
                params: dict = {
                    "location_id": GHL_LOCATION_ID,
                    "pipeline_id": GHL_FEEDBACK_PIPELINE,
                    "limit":       100,
                }
                if start_after_id:
                    params["startAfterId"] = start_after_id
                    params["startAfter"]   = str(start_after)

                resp = await client.get(
                    f"{GHL_BASE_URL}/opportunities/search",
                    headers=ghl_headers(),
                    params=params,
                )
                if not resp.is_success:
                    print(f"[ghl] search failed: {resp.status_code} {resp.text[:200]}")
                    break

                data  = resp.json()
                meta  = data.get("meta", {})
                batch = data.get("opportunities", [])
                all_opps.extend(batch)

                if len(batch) < 100 or not meta.get("startAfterId"):
                    break

                start_after_id = meta["startAfterId"]
                start_after    = meta.get("startAfter")

            print(f"[ghl] fetched {len(all_opps)} opportunities total")

            # ── Step 2: build per-name feedback list ──
            priority_pairs: list = []  # (name_key, contactId, entry_idx)

            for opp in all_opps:
                contact    = opp.get("contact", {})
                name       = (contact.get("name") or "").strip()
                name_key   = name.lower()
                stage_id   = opp.get("pipelineStageId", "")
                sentiment  = GHL_STAGE_SENTIMENT.get(stage_id, "pending")
                contact_id = contact.get("id", "")
                created_at = opp.get("createdAt", "")

                if not name_key:
                    continue  # skip records with no name

                entry = {
                    "name":      name,
                    "sentiment": sentiment,
                    "comments":  "",
                    "createdAt": created_at,
                    "stageId":   stage_id,
                }

                result.setdefault(name_key, []).append(entry)

                if sentiment in ("positive", "negative") and contact_id:
                    priority_pairs.append((name_key, contact_id, len(result[name_key]) - 1))

            # ── Step 3: fetch feedback_comments for positive/negative contacts ──
            seen_contacts: set[str] = set()
            for name_key, contact_id, idx in priority_pairs[:50]:
                if contact_id in seen_contacts:
                    continue
                seen_contacts.add(contact_id)
                try:
                    cr = await client.get(
                        f"{GHL_BASE_URL}/contacts/{contact_id}",
                        headers=ghl_headers(),
                    )
                    if cr.is_success:
                        cdata  = cr.json().get("contact", {})
                        custom = {
                            cf.get("fieldKey", "").replace("contact.", ""): cf.get("value", "")
                            for cf in (cdata.get("customFields") or [])
                        }
                        comments = str(custom.get(GHL_FEEDBACK_COMMENT_FIELD, "") or "")
                        if comments and name_key in result and idx < len(result[name_key]):
                            result[name_key][idx]["comments"] = comments
                except Exception as e:
                    print(f"[ghl] contact fetch error ({contact_id}): {e}")

            # ── Step 4: sort each name's list newest-first ──
            for name_key in result:
                result[name_key].sort(
                    key=lambda e: e.get("createdAt", ""), reverse=True
                )

    except Exception as e:
        print(f"[ghl] feedback error: {e}")
        return {}

    ghl_feedback_cache["feedback"] = result
    return result


# ── CACHE CLEAR ───────────────────────────────────────────
@app.post("/api/cache/clear", tags=["System"])
def clear_cache(_: str = Depends(require_key)):
    appt_cache.clear()
    order_cache.clear()
    media_cache.clear()
    ghl_feedback_cache.clear()
    return {"ok": True, "message": "All caches cleared"}
