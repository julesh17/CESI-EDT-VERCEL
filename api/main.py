import io
import os
import re
import uuid
import hashlib
import json
from datetime import datetime, date, time, timedelta
from typing import List, Optional

# --- BIBLIOTHEQUES WEB ---
from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException, Depends, status
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.middleware.sessions import SessionMiddleware
from supabase import create_client, Client

# --- BIBLIOTHEQUES ANALYSE EXCEL ---
import pandas as pd
from dateutil import parser as dtparser
from openpyxl import load_workbook
import pytz

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Tes clés Supabase (Directement dans le code pour simplifier ta vie, 
# même si normalement on utilise des variables d'environnement)
SUPABASE_URL = "https://dlxqgelylxcakrmbkyun.supabase.co"
SUPABASE_KEY = "sb_publishable_mgjSTslsZ_ObnIRxCL10AQ_ix5NSBpz"

# Création du client Database
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

#app = FastAPI()
app = FastAPI(root_path="/api" if os.environ.get("VERCEL") else "")
#templates = Jinja2Templates(directory="templates")

import os
#BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
#templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory=os.path.join(os.path.dirname(BASE_DIR), "templates"))

# Sécurité pour les cookies de session (impératif pour la connexion)
app.add_middleware(SessionMiddleware, secret_key="SECRET_ALEATOIRE_A_CHANGER_SI_TU_VEUX")

# ==========================================
# 2. TON CODE DE PARSING (Légèrement adapté)
# ==========================================

def normalize_group_label(x):
    if x is None: return None
    try:
        if pd.isna(x): return None
    except Exception: pass
    s = str(x).strip()
    if not s: return None
    m = re.search(r'G\s*\.?\s*(\d+)', s, re.I)
    if m: return f'G {m.group(1)}'
    m2 = re.search(r'^(?:groupe)?\s*(\d+)$', s, re.I)
    if m2: return f'G {m2.group(1)}'
    return s

def is_time_like(x):
    if x is None: return False
    if isinstance(x, (pd.Timestamp, datetime, time)): return True
    s = str(x).strip()
    if not s: return False
    if re.match(r'^\d{1,2}[:hH]\d{2}(\s*[AaPp][Mm]\.?)*$', s): return True
    return False

def to_time(x):
    if x is None: return None
    if isinstance(x, time): return x
    if isinstance(x, pd.Timestamp): return x.to_pydatetime().time()
    if isinstance(x, datetime): return x.time()
    s = str(x).strip()
    if not s: return None
    s2 = s.replace('h', ':').replace('H', ':')
    try:
        dt = dtparser.parse(s2, dayfirst=True)
        return dt.time()
    except Exception: return None

def to_date(x):
    if x is None: return None
    if isinstance(x, pd.Timestamp): return x.to_pydatetime().date()
    if isinstance(x, datetime): return x.date()
    if isinstance(x, date): return x
    s = str(x).strip()
    if not s: return None
    try:
        dt = dtparser.parse(s, dayfirst=True, fuzzy=True)
        return dt.date()
    except Exception: return None

def get_merged_map(xls_fileobj, sheet_name):
    wb = load_workbook(xls_fileobj, data_only=True)
    ws = wb[sheet_name]
    merged_map = {}
    for merged in ws.merged_cells.ranges:
        r1, r2 = merged.min_row, merged.max_row
        c1, c2 = merged.min_col, merged.max_col
        for r in range(r1, r2 + 1):
            for c in range(c1, c2 + 1):
                merged_map[(r - 1, c - 1)] = (r1 - 1, c1 - 1, r2 - 1, c2 - 1)
    return merged_map

def find_week_rows(df):
    return [i for i in range(len(df)) if isinstance(df.iat[i, 0], str) and re.match(r'^\s*S\s*\d+', df.iat[i, 0].strip(), re.I)]

def find_slot_rows(df):
    return [i for i in range(len(df)) if isinstance(df.iat[i, 0], str) and re.match(r'^\s*H\s*\d+', df.iat[i, 0].strip(), re.I)]

def parse_sheet_to_events(file_content, sheet_name):
    """Version adaptée pour FastAPI (prend bytes, pas Streamlit upload)"""
    file_io = io.BytesIO(file_content)
    
    try:
        df = pd.read_excel(file_io, sheet_name=sheet_name, header=None)
    except ValueError:
        return [] # Feuille non trouvée

    # Reset IO for openpyxl
    file_io.seek(0)
    merged_map = get_merged_map(file_io, sheet_name)

    nrows, ncols = df.shape
    s_rows = find_week_rows(df)
    h_rows = find_slot_rows(df)
    raw_events = []

    for r in h_rows:
        p_candidates = [s for s in s_rows if s <= r]
        if not p_candidates: continue
        p = max(p_candidates)
        date_row = p + 1
        group_row = p + 2

        date_cols = [c for c in range(ncols) if date_row < nrows and to_date(df.iat[date_row, c]) is not None]

        for c in date_cols:
            for col in (c, c + 1):
                if col >= ncols: continue
                summary = df.iat[r, col]
                if pd.isna(summary) or summary is None: continue
                summary_str = str(summary).strip()
                if not summary_str: continue

                # Teachers
                teachers = []
                if (r + 2) < nrows:
                    for off in range(2, 6):
                        idx = r + off
                        if idx >= nrows: break
                        t = df.iat[idx, col]
                        if t is None or pd.isna(t): continue
                        s = str(t).strip()
                        if not s: continue
                        if not is_time_like(s) and to_date(s) is None:
                            teachers.append(s)
                teachers = list(dict.fromkeys(teachers))

                # Stop Index
                stop_idx = None
                for off in range(1, 12):
                    idx = r + off
                    if idx >= nrows: break
                    if is_time_like(df.iat[idx, col]):
                        stop_idx = idx
                        break
                if stop_idx is None: stop_idx = min(r + 7, nrows)

                # Description
                desc_parts = []
                for idx in range(r + 1, stop_idx):
                    if idx >= nrows: break
                    cell = df.iat[idx, col]
                    if pd.isna(cell) or cell is None: continue
                    s = str(cell).strip()
                    if not s: continue
                    if to_date(cell) is not None: continue
                    if s in teachers or s == summary_str: continue
                    desc_parts.append(s)
                desc_text = " | ".join(dict.fromkeys(desc_parts))

                # Times
                start_val, end_val = None, None
                for off in range(1, 13):
                    idx = r + off
                    if idx >= nrows: break
                    v = df.iat[idx, col]
                    if is_time_like(v):
                        if start_val is None: start_val = v
                        elif end_val is None and v != start_val:
                            end_val = v
                            break
                if start_val is None or end_val is None: continue
                start_t, end_t = to_time(start_val), to_time(end_val)
                if start_t is None or end_t is None: continue

                # DateTime construction
                d = to_date(df.iat[date_row, c])
                if d is None: continue
                dtstart = datetime.combine(d, start_t)
                dtend = datetime.combine(d, end_t)

                # Groups
                gl = normalize_group_label(df.iat[group_row, col] if group_row < nrows else None)
                gl_next = normalize_group_label(df.iat[group_row, col + 1] if (col + 1) < ncols else None)
                is_left_col = (col == c)
                groups = set()
                if is_left_col:
                    merged = merged_map.get((r, col))
                    if merged and (r, col + 1) in merged_map:
                        if gl: groups.add(gl)
                        if gl_next: groups.add(gl_next)
                    else:
                        if gl: groups.add(gl)
                else:
                    if gl: groups.add(gl)

                raw_events.append({
                    'summary': summary_str,
                    'teachers': set(teachers),
                    'descriptions': set([desc_text]) if desc_text else set(),
                    'start': dtstart,
                    'end': dtend,
                    'groups': groups
                })

    # Fusion
    merged = {}
    for e in raw_events:
        key = (e['summary'], e['start'], e['end'])
        if key not in merged:
            merged[key] = {
                'summary': e['summary'],
                'teachers': set(),
                'descriptions': set(),
                'start': e['start'],
                'end': e['end'],
                'groups': set()
            }
        merged[key]['teachers'].update(e.get('teachers', set()))
        merged[key]['descriptions'].update(e.get('descriptions', set()))
        merged[key]['groups'].update(e.get('groups', set()))

    # Conversion en liste de dictionnaires JSON-serializable
    final_list = []
    for v in merged.values():
        final_list.append({
            'summary': v['summary'],
            'teachers': sorted(list(v['teachers'])),
            'description': " | ".join(sorted(list(v['descriptions']))) if v['descriptions'] else "",
            'start': v['start'].isoformat(), # Important pour JSON
            'end': v['end'].isoformat(),     # Important pour JSON
            'groups': sorted(list(v['groups']))
        })
    return final_list

# ==========================================
# 3. GENERATION ICS
# ==========================================

def build_paris_vtimezone_text():
    return "\n".join([
        "BEGIN:VTIMEZONE",
        "TZID:Europe/Paris",
        "X-LIC-LOCATION:Europe/Paris",
        "BEGIN:DAYLIGHT",
        "TZOFFSETFROM:+0100",
        "TZOFFSETTO:+0200",
        "TZNAME:CEST",
        "DTSTART:19700329T020000",
        "RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=-1SU",
        "END:DAYLIGHT",
        "BEGIN:STANDARD",
        "TZOFFSETFROM:+0200",
        "TZOFFSETTO:+0100",
        "TZNAME:CET",
        "DTSTART:19701025T030000",
        "RRULE:FREQ=YEARLY;BYMONTH=10;BYDAY=-1SU",
        "END:STANDARD",
        "END:VTIMEZONE"
    ])

def escape_ical_text(s: str) -> str:
    if s is None: return ""
    s = str(s)
    s = s.replace('\\', '\\\\').replace('\n', '\\n').replace(',', '\\,').replace(';', '\\;')
    return s

def events_to_ics_string(events, tzname='Europe/Paris'):
    tz = pytz.timezone(tzname)
    body = [build_paris_vtimezone_text()]
    
    for ev in events:
        uid = str(uuid.uuid4())
        # Les dates sont des strings ISO stockées en JSON, on doit les reparser
        start_dt = datetime.fromisoformat(ev['start'])
        end_dt = datetime.fromisoformat(ev['end'])
        
        # Gestion Timezone
        if start_dt.tzinfo is None: start_loc = tz.localize(start_dt)
        else: start_loc = start_dt.astimezone(tz)
        
        if end_dt.tzinfo is None: end_loc = tz.localize(end_dt)
        else: end_loc = end_dt.astimezone(tz)

        dtstart = start_loc.strftime('%Y%m%dT%H%M%S')
        dtend = end_loc.strftime('%Y%m%dT%H%M%S')
        
        summary = escape_ical_text(ev['summary'])
        
        desc_lines = []
        if ev.get('description'): desc_lines.append(ev['description'])
        if ev.get('teachers'): desc_lines.append('Enseignant(s): ' + ' / '.join(ev['teachers']))
        groups = ev.get('groups', [])
        if groups:
            if len(groups) == 1: desc_lines.append('Groupe: ' + groups[0])
            else: desc_lines.append('Groupes: ' + ' et '.join(groups))
        
        description = escape_ical_text('\n'.join(desc_lines))

        body.extend([
            'BEGIN:VEVENT',
            f'UID:{uid}',
            f'DTSTAMP:{datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")}',
            f'DTSTART;TZID={tzname}:{dtstart}',
            f'DTEND;TZID={tzname}:{dtend}',
            f'SUMMARY:{summary}',
            f'DESCRIPTION:{description}',
            'END:VEVENT'
        ])

    return '\n'.join([
        'BEGIN:VCALENDAR',
        'VERSION:2.0',
        'PRODID:-//EDT Export//FR',
        'CALSCALE:GREGORIAN',
    ] + body + ['END:VCALENDAR'])

# ==========================================
# 4. GESTION COMPTES & ROUTES
# ==========================================

def get_current_user(request: Request):
    return request.session.get("user")

def hash_password(password: str):
    return hashlib.sha256(password.encode()).hexdigest()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    
    # Récupérer les calendriers
    response = supabase.table("plannings").select("slug, name, year, updated_at").execute()
    plannings = response.data
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "user": user, 
        "plannings": plannings,
        "base_url": str(request.base_url)
    })

# --- LOGIN / REGISTER ---

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "mode": "login"})

@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request, "mode": "register"})

@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    # Vérification Supabase
    hashed = hash_password(password)
    res = supabase.table("users").select("*").eq("username", username).eq("password_hash", hashed).execute()
    
    if len(res.data) > 0:
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    else:
        return templates.TemplateResponse("login.html", {
            "request": request, "mode": "login", "error": "Identifiants incorrects"
        })

@app.post("/register")
async def register_submit(
    request: Request, 
    username: str = Form(...), 
    password: str = Form(...), 
    verification: str = Form(...)
):
    if verification.strip().upper() != "EUROVISION":
        return templates.TemplateResponse("login.html", {
            "request": request, "mode": "register", "error": "Code de vérification incorrect !"
        })
    
    # Vérif si user existe
    check = supabase.table("users").select("*").eq("username", username).execute()
    if len(check.data) > 0:
        return templates.TemplateResponse("login.html", {
            "request": request, "mode": "register", "error": "Ce pseudo est déjà pris."
        })
    
    # Création
    hashed = hash_password(password)
    supabase.table("users").insert({"username": username, "password_hash": hashed}).execute()
    
    # Connexion auto
    request.session["user"] = username
    return RedirectResponse(url="/", status_code=303)

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

# --- ACTIONS CALENDRIER ---

@app.post("/create")
async def create_calendar(request: Request, promo_name: str = Form(...), school_year: str = Form(...)):
    if not get_current_user(request): return RedirectResponse("/login")
    
    slug = f"{promo_name}-{school_year}".upper().replace(" ", "-")
    data = {"slug": slug, "name": promo_name, "year": school_year}
    try:
        supabase.table("plannings").insert(data).execute()
    except Exception:
        pass # Déjà existant probablement
    return RedirectResponse("/", status_code=303)

@app.post("/upload/{slug}")
async def upload_excel(slug: str, request: Request, file: UploadFile = File(...)):
    if not get_current_user(request): return RedirectResponse("/login")
    
    content = await file.read()
    
    # Parsing using adapted functions
    events_p1 = parse_sheet_to_events(content, "EDT P1")
    events_p2 = parse_sheet_to_events(content, "EDT P2")
    
    supabase.table("plannings").update({
        "events_p1": events_p1,
        "events_p2": events_p2,
        "updated_at": datetime.now(pytz.timezone('Europe/Paris')).isoformat()
    }).eq("slug", slug).execute()
    
    return RedirectResponse("/", status_code=303)

# --- URL PUBLIQUE POUR OUTLOOK (Pas de Login nécessaire) ---

@app.get("/ics/{slug}_{group}.ics")
async def get_ics_file(slug: str, group: str):
    # On force la recherche en majuscule pour correspondre à ton souhait
    search_slug = slug.upper() 
    
    res = supabase.table("plannings").select(f"events_{group.lower()}").eq("slug", search_slug).execute()
    
    # Si on ne trouve rien, on essaie en minuscule au cas où
    if not res.data:
        res = supabase.table("plannings").select(f"events_{group.lower()}").eq("slug", slug.lower()).execute()
    
    if not res.data:
        # Si on ne trouve toujours rien, on renvoie une erreur plus parlante
        raise HTTPException(status_code=404, detail=f"Calendrier {search_slug} non trouvé dans la base")
        
    # group doit être "P1" ou "P2"
    if group not in ["P1", "P2"]: raise HTTPException(404)
    
    res = supabase.table("plannings").select(f"events_{group.lower()}").eq("slug", slug).execute()
    if not res.data: raise HTTPException(404)
    
    events_json = res.data[0].get(f"events_{group.lower()}", [])
    ics_content = events_to_ics_string(events_json)
    
    return Response(content=ics_content, media_type="text/calendar", headers={
        "Content-Disposition": f"attachment; filename={slug}_{group}.ics"
    })
