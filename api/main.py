import io
import os
import re
import uuid
import hashlib
from datetime import datetime, date, time
from typing import List

from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from supabase import create_client, Client
import pandas as pd
from dateutil import parser as dtparser
from openpyxl import load_workbook
import pytz

# --- CONFIGURATION ---
SUPABASE_URL = "https://dlxqgelylxcakrmbkyun.supabase.co"
SUPABASE_KEY = "sb_publishable_mgjSTslsZ_ObnIRxCL10AQ_ix5NSBpz"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
app.add_middleware(SessionMiddleware, secret_key="CLE_SECRETTE_PLANNING")

# Gestion des chemins pour Vercel
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(BASE_DIR), "templates"))

# --- UTILS PARSING ---
def normalize_group_label(x):
    if x is None or (not isinstance(x, str) and pd.isna(x)): return None
    s = str(x).strip()
    m = re.search(r'G\s*\.?\s*(\d+)', s, re.I)
    if m: return f'G {m.group(1)}'
    m2 = re.search(r'^(?:groupe)?\s*(\d+)$', s, re.I)
    if m2: return f'G {m2.group(1)}'
    return s

def is_time_like(x):
    if x is None: return False
    if isinstance(x, (pd.Timestamp, datetime, time)): return True
    s = str(x).strip()
    return bool(re.match(r'^\d{1,2}[:hH]\d{2}(\s*[AaPp][Mm]\.?)*$', s))

def to_time(x):
    if isinstance(x, time): return x
    if isinstance(x, (pd.Timestamp, datetime)): return x.time()
    s = str(x).strip().replace('h', ':').replace('H', ':')
    try: return dtparser.parse(s).time()
    except: return None

def to_date(x):
    if isinstance(x, (pd.Timestamp, datetime)): return x.date()
    if isinstance(x, date): return x
    try: return dtparser.parse(str(x), dayfirst=True, fuzzy=True).date()
    except: return None

def get_merged_map(xls_fileobj, sheet_name):
    wb = load_workbook(xls_fileobj, data_only=True)
    ws = wb[sheet_name]
    merged_map = {}
    for merged in ws.merged_cells.ranges:
        for r in range(merged.min_row, merged.max_row + 1):
            for c in range(merged.min_col, merged.max_col + 1):
                merged_map[(r - 1, c - 1)] = (merged.min_row - 1, merged.min_col - 1, merged.max_row - 1, merged.max_col - 1)
    return merged_map

def parse_sheet_to_events(file_content, sheet_name):
    file_io = io.BytesIO(file_content)
    try:
        df = pd.read_excel(file_io, sheet_name=sheet_name, header=None)
    except: return []
    
    file_io.seek(0)
    merged_map = get_merged_map(file_io, sheet_name)
    nrows, ncols = df.shape
    s_rows = [i for i in range(nrows) if isinstance(df.iat[i,0], str) and re.match(r'^\s*S\s*\d+', df.iat[i,0], re.I)]
    h_rows = [i for i in range(nrows) if isinstance(df.iat[i,0], str) and re.match(r'^\s*H\s*\d+', df.iat[i,0], re.I)]
    
    raw_events = []
    for r in h_rows:
        p = max([s for s in s_rows if s <= r], default=None)
        if p is None: continue
        date_row, group_row = p + 1, p + 2
        date_cols = [c for c in range(ncols) if date_row < nrows and to_date(df.iat[date_row, c]) is not None]

        for c in date_cols:
            for col in (c, c + 1):
                if col >= ncols: continue
                summary = df.iat[r, col]
                if pd.isna(summary): continue
                
                # Heures
                times = []
                for off in range(1, 13):
                    if r+off < nrows and is_time_like(df.iat[r+off, col]):
                        times.append(to_time(df.iat[r+off, col]))
                if len(times) < 2: continue
                
                # Date
                d = to_date(df.iat[date_row, c])
                start_dt = datetime.combine(d, times[0])
                end_dt = datetime.combine(d, times[1])
                
                # Groupes
                gl = normalize_group_label(df.iat[group_row, col] if group_row < nrows else None)
                groups = {gl} if gl else set()
                if col == c:
                    merged = merged_map.get((r, col))
                    if merged and (r, col+1) in merged_map:
                        gl_next = normalize_group_label(df.iat[group_row, col+1] if group_row < nrows else None)
                        if gl_next: groups.add(gl_next)

                raw_events.append({
                    'summary': str(summary).strip(),
                    'start': start_dt.isoformat(),
                    'end': end_dt.isoformat(),
                    'groups': list(groups)
                })
    return raw_events

# --- ICS GENERATION ---
def build_ics(events):
    lines = ["BEGIN:VCALENDAR", "VERSION:2.0", "PRODID:-//EDT//FR", "CALSCALE:GREGORIAN", "BEGIN:VTIMEZONE", "TZID:Europe/Paris", "BEGIN:DAYLIGHT", "TZOFFSETFROM:+0100", "TZOFFSETTO:+0200", "TZNAME:CEST", "DTSTART:19700329T020000", "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU", "END:DAYLIGHT", "BEGIN:STANDARD", "TZOFFSETFROM:+0200", "TZOFFSETTO:+0100", "TZNAME:CET", "DTSTART:19701025T030000", "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU", "END:STANDARD", "END:VTIMEZONE"]
    for ev in events:
        lines.extend(["BEGIN:VEVENT", f"UID:{uuid.uuid4()}", f"DTSTAMP:{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}", f"DTSTART;TZID=Europe/Paris:{datetime.fromisoformat(ev['start']).strftime('%Y%m%dT%H%M%S')}", f"DTEND;TZID=Europe/Paris:{datetime.fromisoformat(ev['end']).strftime('%Y%m%dT%H%M%S')}", f"SUMMARY:{ev['summary']}", f"DESCRIPTION:Groupes: {', '.join(ev['groups'])}", "END:VEVENT"])
    lines.append("END:VCALENDAR")
    return "\r\n".join(lines)

# --- ROUTES ---
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = request.session.get("user")
    if not user: return RedirectResponse("/login")
    res = supabase.table("plannings").select("*").execute()
    return templates.TemplateResponse("index.html", {"request": request, "user": user, "plannings": res.data, "base_url": str(request.base_url)})

@app.get("/login", response_class=HTMLResponse)
async def login_pg(request: Request): return templates.TemplateResponse("login.html", {"request": request, "mode": "login"})

@app.get("/register", response_class=HTMLResponse)
async def reg_pg(request: Request): return templates.TemplateResponse("login.html", {"request": request, "mode": "register"})

@app.post("/login")
async def login_sub(request: Request, username: str = Form(...), password: str = Form(...)):
    h = hashlib.sha256(password.encode()).hexdigest()
    res = supabase.table("users").select("*").eq("username", username).eq("password_hash", h).execute()
    if res.data:
        request.session["user"] = username
        return RedirectResponse("/", status_code=303)
    return HTMLResponse("Erreur login")

@app.post("/register")
async def reg_sub(request: Request, username: str = Form(...), password: str = Form(...), verification: str = Form(...)):
    if verification.upper() != "EUROVISION": return HTMLResponse("Code faux")
    h = hashlib.sha256(password.encode()).hexdigest()
    supabase.table("users").insert({"username": username, "password_hash": h}).execute()
    request.session["user"] = username
    return RedirectResponse("/", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse("/login")

@app.post("/create")
async def create_plan(request: Request, promo_name: str = Form(...), school_year: str = Form(...)):
    slug = f"{promo_name}-{school_year}".lower().replace(" ", "-")
    supabase.table("plannings").insert({"slug": slug, "name": promo_name, "year": school_year}).execute()
    return RedirectResponse("/", status_code=303)

@app.post("/upload/{slug}")
async def upload(slug: str, request: Request, file: UploadFile = File(...)):
    content = await file.read()
    p1 = parse_sheet_to_events(content, "EDT P1")
    p2 = parse_sheet_to_events(content, "EDT P2")
    now_fr = datetime.now(pytz.timezone('Europe/Paris')).isoformat()
    supabase.table("plannings").update({"events_p1": p1, "events_p2": p2, "updated_at": now_fr}).eq("slug", slug).execute()
    return RedirectResponse("/?success=1", status_code=303)

@app.get("/ics/{slug}_{group}.ics")
async def get_ics(slug: str, group: str):
    res = supabase.table("plannings").select(f"events_{group.lower()}").eq("slug", slug.lower()).execute()
    if not res.data: raise HTTPException(404)
    return Response(content=build_ics(res.data[0][f"events_{group.lower()}"]), media_type="text/calendar")
