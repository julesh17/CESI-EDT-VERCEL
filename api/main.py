import io
import os
import re
import uuid
import hashlib
import json
from datetime import datetime, date, time
from typing import List, Optional, Any

# --- BIBLIOTHEQUES WEB ---
from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from supabase import create_client, Client

from fastapi.staticfiles import StaticFiles

# --- BIBLIOTHEQUES ANALYSE EXCEL ---
import pandas as pd
import pytz
from dateutil import parser as dtparser
from openpyxl import load_workbook

app.mount("/static", StaticFiles(directory="static"), name="static")

# ==========================================
# 1. CONFIGURATION
# ==========================================

# Fonction simple pour voir les traces dans les logs Vercel
def log_msg(msg: str):
    print(f"[LOG VERCEL] {msg}")

SUPABASE_URL = "https://dlxqgelylxcakrmbkyun.supabase.co"
SUPABASE_KEY = "sb_publishable_mgjSTslsZ_ObnIRxCL10AQ_ix5NSBpz"

# Initialisation Supabase
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    log_msg(f"Error initializing Supabase: {e}")

app = FastAPI()

# Configuration Templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
# Sur Vercel, ajustement chemin templates si besoin. 
# Si tes templates sont à la racine, 'templates' suffit.
templates = Jinja2Templates(directory="templates")

# Sécurité Session
app.add_middleware(
    SessionMiddleware,
    secret_key="SECRET_SESSION_A_CHANGER_POUR_PROD",
    same_site="lax",
    https_only=True 
)

# ==========================================
# 2. PARSING (Logique STREAMLIT intégrée)
# ==========================================

def normalize_group_label(x):
    if x is None:
        return None
    try:
        if pd.isna(x):
            return None
    except Exception:
        pass
    s = str(x).strip()
    if not s:
        return None
    m = re.search(r'G\s*\.?\s*(\d+)', s, re.I)
    if m:
        return f'G {m.group(1)}'
    m2 = re.search(r'^(?:groupe)?\s*(\d+)$', s, re.I)
    if m2:
        return f'G {m2.group(1)}'
    return s


def is_time_like(x):
    if x is None:
        return False
    if isinstance(x, (pd.Timestamp, datetime, time)):
        return True
    s = str(x).strip()
    if not s:
        return False
    # accepte formats hh:mm, hhm m, 9h30, 9:30, 9AM, 9 PM ...
    if re.match(r'^\d{1,2}[:hH]\d{2}(\s*[AaPp][Mm]\.?)*$', s):
        return True
    return False


def to_time(x):
    if x is None:
        return None
    if isinstance(x, time):
        return x
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().time()
    if isinstance(x, datetime):
        return x.time()
    s = str(x).strip()
    if not s:
        return None
    s2 = s.replace('h', ':').replace('H', ':')
    try:
        dt = dtparser.parse(s2, dayfirst=True)
        return dt.time()
    except Exception:
        return None


def to_date(x):
    if x is None:
        return None
    if isinstance(x, pd.Timestamp):
        return x.to_pydatetime().date()
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    s = str(x).strip()
    if not s:
        return None
    try:
        dt = dtparser.parse(s, dayfirst=True, fuzzy=True)
        return dt.date()
    except Exception:
        return None


def get_merged_map(xls_fileobj, sheet_name):
    """
    Retourne un dict {(row0,col0): (r1,c1,r2,c2)} pour gérer les cellules fusionnées.
    """
    wb = load_workbook(xls_fileobj, data_only=True)
    if sheet_name not in wb.sheetnames:
        return {}
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


def parse_sheet_to_events_json(file_content: bytes, sheet_name: str) -> List[dict]:
    """
    Parse le fichier Excel (bytes) et retourne une liste de dicts JSON-serializable.
    C'est ici qu'on intègre toute la logique robuste du Streamlit.
    """
    # Création d'un BytesIO pour pandas
    file_io = io.BytesIO(file_content)
    
    try:
        df = pd.read_excel(file_io, sheet_name=sheet_name, header=None)
    except ValueError:
        log_msg(f"Feuille {sheet_name} introuvable.")
        return []
    except Exception as e:
        log_msg(f"Erreur lecture Excel: {e}")
        return []

    # Reset du pointeur pour openpyxl car pandas l'a lu
    file_io.seek(0)
    merged_map = get_merged_map(file_io, sheet_name)

    nrows, ncols = df.shape
    s_rows = find_week_rows(df)
    h_rows = find_slot_rows(df)

    raw_events = []

    for r in h_rows:
        # Trouver la semaine associée (le S_row juste au-dessus)
        p_candidates = [s for s in s_rows if s <= r]
        if not p_candidates:
            continue
        p = max(p_candidates)
        date_row = p + 1
        group_row = p + 2

        # Identifier les colonnes qui contiennent une date valide
        date_cols = [c for c in range(ncols) if date_row < nrows and to_date(df.iat[date_row, c]) is not None]

        for c in date_cols:
            # On itère sur la colonne c et c+1 (gestion des demi-colonnes ou fusions)
            for col in (c, c + 1):
                if col >= ncols:
                    continue
                
                summary = df.iat[r, col]
                if pd.isna(summary) or summary is None:
                    continue
                summary_str = str(summary).strip()
                if not summary_str:
                    continue

                # --- 1. Enseignants ---
                teachers = []
                if (r + 2) < nrows:
                    for off in range(2, 6): # Cherche 2 à 5 lignes en dessous
                        idx = r + off
                        if idx >= nrows: break
                        t = df.iat[idx, col]
                        if t is None or pd.isna(t): continue
                        s = str(t).strip()
                        if not s: continue
                        # Si ce n'est ni une heure ni une date, c'est probablement un prof
                        if not is_time_like(s) and to_date(s) is None:
                            teachers.append(s)
                teachers = list(dict.fromkeys(teachers)) # Dedup

                # --- 2. Index de fin (Stop Index) ---
                stop_idx = None
                for off in range(1, 12):
                    idx = r + off
                    if idx >= nrows: break
                    if is_time_like(df.iat[idx, col]):
                        stop_idx = idx
                        break
                if stop_idx is None:
                    stop_idx = min(r + 7, nrows)

                # --- 3. Description ---
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

                # --- 4. Heures (Start/End) ---
                start_val, end_val = None, None
                for off in range(1, 13):
                    idx = r + off
                    if idx >= nrows: break
                    v = df.iat[idx, col]
                    if is_time_like(v):
                        if start_val is None:
                            start_val = v
                        elif end_val is None and v != start_val:
                            end_val = v
                            break
                
                if start_val is None or end_val is None:
                    continue
                
                start_t, end_t = to_time(start_val), to_time(end_val)
                if start_t is None or end_t is None:
                    continue

                # --- 5. Construction DateTime ---
                d = to_date(df.iat[date_row, c])
                if d is None:
                    continue
                dtstart = datetime.combine(d, start_t)
                dtend = datetime.combine(d, end_t)

                # --- 6. Groupes ---
                gl = normalize_group_label(df.iat[group_row, col] if group_row < nrows else None)
                gl_next = normalize_group_label(df.iat[group_row, col + 1] if (col + 1) < ncols else None)
                is_left_col = (col == c)
                
                groups = set()
                if is_left_col:
                    merged = merged_map.get((r, col))
                    # Si fusion détectée avec la colonne suivante
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

    # --- Fusion des événements identiques ---
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

    # --- Conversion en JSON Serializable pour Supabase ---
    final_list = []
    for v in merged.values():
        final_list.append({
            'summary': v['summary'],
            'teachers': sorted(list(v['teachers'])),
            'description': " | ".join(sorted(list(v['descriptions']))) if v['descriptions'] else "",
            'start': v['start'].isoformat(), # Important: datetime -> string ISO
            'end': v['end'].isoformat(),     # Important: datetime -> string ISO
            'groups': sorted(list(v['groups']))
        })

    return final_list

# ==========================================
# 3. GENERATEUR ICS
# ==========================================

def escape_ical_text(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = s.replace('\\', '\\\\').replace('\n', '\\n').replace(',', '\\,').replace(';', '\\;')
    return s

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

def events_to_ics_string(events: List[dict], tzname='Europe/Paris') -> str:
    """
    Transforme la liste d'events (JSON/Dicts) en string .ics
    """
    tz = pytz.timezone(tzname)
    
    header = [
        'BEGIN:VCALENDAR',
        'VERSION:2.0',
        'PRODID:-//EDT Export//FR',
        'CALSCALE:GREGORIAN',
    ]
    
    body = [build_paris_vtimezone_text()]

    for ev in events:
        uid = str(uuid.uuid4())
        
        # Désérialisation ISO String -> Datetime
        try:
            start_dt = datetime.fromisoformat(ev['start'])
            end_dt = datetime.fromisoformat(ev['end'])
        except ValueError:
            continue # Ignorer si date invalide

        # Localize (L'excel est en heure locale naïve, on le force en Europe/Paris)
        if start_dt.tzinfo is None:
            start_loc = tz.localize(start_dt)
        else:
            start_loc = start_dt.astimezone(tz)
            
        if end_dt.tzinfo is None:
            end_loc = tz.localize(end_dt)
        else:
            end_loc = end_dt.astimezone(tz)

        dtstart = start_loc.strftime('%Y%m%dT%H%M%S')
        dtend = end_loc.strftime('%Y%m%dT%H%M%S')
        
        summary = escape_ical_text(ev['summary'])

        desc_lines = []
        if ev.get('description'):
            desc_lines.append(ev['description'])
        
        # Gestion liste profs (peut être vide)
        teachers = ev.get('teachers', [])
        if teachers:
            desc_lines.append('Enseignant(s): ' + ' / '.join(teachers))
            
        # Gestion liste groupes (peut être vide)
        groups = ev.get('groups', [])
        if groups:
            if len(groups) == 1:
                desc_lines.append('Groupe: ' + groups[0])
            else:
                desc_lines.append('Groupes: ' + ' et '.join(groups))

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

    footer = ['END:VCALENDAR']
    return '\n'.join(header + body + footer)

# ==========================================
# 4. ROUTES & AUTH
# ==========================================

def get_current_user(request: Request):
    return request.session.get("user")

def hash_password(password: str):
    return hashlib.sha256(password.encode()).hexdigest()

@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    user = get_current_user(request)
    if not user:
        # url_for gère les préfixes Vercel automatiquement si configuré, 
        # sinon une string brute "/login" fonctionne aussi généralement.
        return RedirectResponse(url="/login", status_code=303)
    
    # Récupérer les calendriers
    response = supabase.table("plannings").select("slug, name, year, updated_at").execute()
    plannings = response.data
    
    return templates.TemplateResponse("index.html", {
        "request": request, 
        "user": user, 
        "plannings": plannings
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
    # --- VALIDATION EUROVISION ---
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
    try:
        supabase.table("users").insert({"username": username, "password_hash": hashed}).execute()
        # Connexion auto
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        log_msg(f"Erreur inscription: {e}")
        return templates.TemplateResponse("login.html", {
            "request": request, "mode": "register", "error": "Erreur technique lors de l'inscription."
        })

@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)

# --- ACTIONS CALENDRIER ---

@app.post("/create")
async def create_calendar(request: Request, promo_name: str = Form(...), school_year: str = Form(...)):
    if not get_current_user(request): return RedirectResponse("/login")
    
    slug = f"{promo_name}-{school_year}".lower().replace(" ", "-")
    data = {"slug": slug, "name": promo_name, "year": school_year}
    try:
        supabase.table("plannings").insert(data).execute()
        log_msg(f"Nouveau planning créé: {slug}")
    except Exception as e:
        log_msg(f"Erreur création (existe probablement déjà): {e}")
        
    return RedirectResponse("/", status_code=303)

@app.post("/upload/{slug}")
async def upload_excel(slug: str, request: Request, file: UploadFile = File(...)):
    if not get_current_user(request): return RedirectResponse("/login")
    
    log_msg(f"Début upload pour {slug}")
    content = await file.read()
    
    # Parsing using the robust functions
    events_p1 = parse_sheet_to_events_json(content, "EDT P1")
    events_p2 = parse_sheet_to_events_json(content, "EDT P2")
    
    log_msg(f"Events trouvés - P1: {len(events_p1)}, P2: {len(events_p2)}")
    
    try:
        supabase.table("plannings").update({
            "events_p1": events_p1,
            "events_p2": events_p2,
            "updated_at": datetime.now().isoformat()
        }).eq("slug", slug).execute()
    except Exception as e:
        log_msg(f"Erreur mise à jour BDD: {e}")
    
    return RedirectResponse("/", status_code=303)

# --- URL PUBLIQUE POUR OUTLOOK (Pas de Login nécessaire) ---

@app.get("/ics/{slug}/{group}.ics")
async def get_ics_file(slug: str, group: str):
    # group doit être "P1" ou "P2" (insensible à la casse dans l'URL, converti en logique)
    group_clean = group.upper()
    if group_clean not in ["P1", "P2"]:
        raise HTTPException(404, detail="Groupe inconnu (utiliser P1 ou P2)")
    
    # Récupération en base
    try:
        # .ilike pour insensible à la casse sur le slug
        res = supabase.table("plannings").select(f"events_{group_clean.lower()}").ilike("slug", slug).execute()
        
        if not res.data:
            raise HTTPException(404, detail="Planning introuvable")
        
        events_json = res.data[0].get(f"events_{group_clean.lower()}", [])
        
        if not events_json:
            # On génère quand même un ICS vide valide pour éviter les erreurs Outlook
            events_json = []

        ics_content = events_to_ics_string(events_json)
        
        filename = f"{slug}_{group_clean}.ics"
        
        return Response(content=ics_content, media_type="text/calendar", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })
        
    except Exception as e:
        log_msg(f"Erreur ICS generation: {e}")
        raise HTTPException(500, detail="Erreur interne")
