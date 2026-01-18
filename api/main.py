import io
import os
import re
import uuid
import hashlib
from datetime import datetime, date, time
from typing import Optional

import pandas as pd
import pytz
from dateutil import parser as dtparser
from openpyxl import load_workbook

from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, Response
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware

from supabase import create_client, Client

# =====================================================
# CONFIG
# =====================================================

SUPABASE_URL = "https://dlxqgelylxcakrmbkyun.supabase.co"
SUPABASE_KEY = "sb_publishable_mgjSTslsZ_ObnIRxCL10AQ_ix5NSBpz"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#app = FastAPI(root_path="/api" if os.environ.get("VERCEL") else "")
app = FastAPI()


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(
    directory=os.path.join(os.path.dirname(BASE_DIR), "templates")
)

app.add_middleware(
    SessionMiddleware,
    secret_key="SECRET_SESSION_A_CHANGER",
    same_site="lax",
    https_only=True
)

# =====================================================
# HELPERS URL
# =====================================================

def url_for(request: Request, path: str) -> str:
    return request.scope.get("root_path", "") + path

def get_current_user(request: Request) -> Optional[str]:
    return request.session.get("user")

def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

# =====================================================
# PARSING EXCEL (INCHANGÉ LOGIQUE)
# =====================================================

def normalize_group_label(x):
    if x is None: return None
    s = str(x).strip()
    if not s: return None
    m = re.search(r'G\s*\.?\s*(\d+)', s, re.I)
    if m: return f'G {m.group(1)}'
    return s

def is_time_like(x):
    if x is None: return False
    if isinstance(x, (datetime, time, pd.Timestamp)): return True
    return bool(re.match(r'^\d{1,2}[:hH]\d{2}', str(x)))

def to_time(x):
    if isinstance(x, time): return x
    try:
        return dtparser.parse(str(x)).time()
    except Exception:
        return None

def to_date(x):
    if isinstance(x, date): return x
    try:
        return dtparser.parse(str(x), dayfirst=True).date()
    except Exception:
        return None

def get_merged_map(xls, sheet):
    wb = load_workbook(xls, data_only=True)
    ws = wb[sheet]
    merged = {}
    for m in ws.merged_cells.ranges:
        for r in range(m.min_row, m.max_row + 1):
            for c in range(m.min_col, m.max_col + 1):
                merged[(r - 1, c - 1)] = True
    return merged

def parse_sheet_to_events(content, sheet):
    file_io = io.BytesIO(content)
    try:
        df = pd.read_excel(file_io, sheet_name=sheet, header=None)
    except Exception:
        return []

    file_io.seek(0)
    merged_map = get_merged_map(file_io, sheet)
    events = []

    for r in range(len(df)):
        if not is_time_like(df.iat[r, 0]): continue

        for c in range(df.shape[1]):
            summary = df.iat[r, c]
            if pd.isna(summary): continue

            start = to_time(df.iat[r, c])
            end = to_time(df.iat[r + 1, c]) if r + 1 < len(df) else None
            date_val = to_date(df.iat[0, c])

            if not (start and end and date_val): continue

            events.append({
                "summary": str(summary),
                "start": datetime.combine(date_val, start).isoformat(),
                "end": datetime.combine(date_val, end).isoformat(),
                "teachers": [],
                "description": "",
                "groups": []
            })

    return events

# =====================================================
# ICS
# =====================================================

def build_vtimezone():
    return """BEGIN:VTIMEZONE
TZID:Europe/Paris
BEGIN:STANDARD
TZOFFSETFROM:+0200
TZOFFSETTO:+0100
DTSTART:19701025T030000
RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU
END:STANDARD
BEGIN:DAYLIGHT
TZOFFSETFROM:+0100
TZOFFSETTO:+0200
DTSTART:19700329T020000
RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU
END:DAYLIGHT
END:VTIMEZONE"""

def events_to_ics(events):
    tz = pytz.timezone("Europe/Paris")
    lines = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//EDT//FR",
        build_vtimezone()
    ]

    for e in events:
        uid = uuid.uuid4()
        start = tz.localize(datetime.fromisoformat(e["start"]))
        end = tz.localize(datetime.fromisoformat(e["end"]))

        lines += [
            "BEGIN:VEVENT",
            f"UID:{uid}",
            f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}",
            f"DTSTART;TZID=Europe/Paris:{start.strftime('%Y%m%dT%H%M%S')}",
            f"DTEND;TZID=Europe/Paris:{end.strftime('%Y%m%dT%H%M%S')}",
            f"SUMMARY:{e['summary']}",
            "END:VEVENT"
        ]

    lines.append("END:VCALENDAR")
    return "\n".join(lines)

# =====================================================
# ROUTES
# =====================================================

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url_for(request, "/login"), 303)

    plannings = supabase.table("plannings").select("*").execute().data

    return templates.TemplateResponse("index.html", {
        "request": request,
        "plannings": plannings
    })

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    hashed = hash_password(password)
    res = supabase.table("users").select("*") \
        .eq("username", username).eq("password_hash", hashed).execute()

    if not res.data:
        return templates.TemplateResponse("login.html", {
            "request": request,
            "error": "Identifiants incorrects"
        })

    request.session["user"] = username
    return RedirectResponse(url_for(request, "/"), 303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url_for(request, "/login"), 303)

@app.post("/create")
async def create_calendar(request: Request, promo_name: str = Form(...), school_year: str = Form(...)):
    if not get_current_user(request):
        return RedirectResponse(url_for(request, "/login"), 303)

    slug = f"{promo_name}-{school_year}".upper().replace(" ", "-")
    supabase.table("plannings").upsert({
        "slug": slug,
        "name": promo_name,
        "year": school_year
    }).execute()

    return RedirectResponse(url_for(request, "/"), 303)

@app.post("/upload/{slug}")
async def upload_excel(request: Request, slug: str, file: UploadFile = File(...)):
    if not get_current_user(request):
        return RedirectResponse(url_for(request, "/login"), 303)

    slug = slug.upper()
    content = await file.read()

    p1 = parse_sheet_to_events(content, "EDT P1")
    p2 = parse_sheet_to_events(content, "EDT P2")

    supabase.table("plannings").update({
        "events_p1": p1,
        "events_p2": p2,
        "updated_at": datetime.now().isoformat()
    }).eq("slug", slug).execute()

    return RedirectResponse(url_for(request, "/"), 303)

@app.get("/ics/{slug}_{group}.ics")
async def ics(slug: str, group: str):
    slug = slug.upper()
    group = group.lower()

    if group not in ("p1", "p2"):
        raise HTTPException(404)

    res = supabase.table("plannings") \
        .select(f"events_{group}") \
        .eq("slug", slug).execute()

    if not res.data:
        raise HTTPException(404)

    ics = events_to_ics(res.data[0][f"events_{group}"])

    return Response(
        content=ics,
        media_type="text/calendar",
        headers={"Content-Disposition": f"attachment; filename={slug}_{group.upper()}.ics"}
    )
