import io
import os
import re
import uuid
import hashlib
from datetime import datetime, date, time
from typing import List, Dict, Tuple, Optional

# --- BIBLIOTHEQUES WEB ---
from fastapi import FastAPI, Request, Form, UploadFile, File, HTTPException
from fastapi.responses import HTMLResponse, Response, RedirectResponse
from fastapi.templating import Jinja2Templates
from starlette.middleware.sessions import SessionMiddleware
from supabase import create_client, Client

# --- BIBLIOTHEQUES ANALYSE EXCEL ---
import pandas as pd
import pytz
from dateutil import parser as dtparser
from openpyxl import load_workbook

# ==========================================
# 1. CONFIGURATION
# ==========================================

def log_msg(msg: str):
    print(f"[LOG VERCEL] {msg}")

SUPABASE_URL = "https://dlxqgelylxcakrmbkyun.supabase.co"
SUPABASE_KEY = "sb_publishable_mgjSTslsZ_ObnIRxCL10AQ_ix5NSBpz"

try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
except Exception as e:
    log_msg(f"Error initializing Supabase: {e}")

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory="templates")

app.add_middleware(
    SessionMiddleware,
    secret_key="SECRET_SESSION_A_CHANGER_POUR_PROD",
    same_site="lax",
    https_only=True
)

PARIS_TZ = pytz.timezone("Europe/Paris")

# ==========================================
# 2. PARSING EDT
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
    result = []
    for i in range(len(df)):
        val = df.iat[i, 0]
        if val is None:
            continue
        if isinstance(val, str) and re.match(r'^\s*S\s*\.?\s*\d+', val.strip(), re.I):
            result.append(i)
            continue
        if isinstance(val, (int, float)) and not isinstance(val, bool):
            try:
                n = int(val)
                if 1 <= n <= 53 and float(val) == n:
                    result.append(i)
            except (ValueError, TypeError):
                pass
    return result


def find_slot_rows(df):
    return [
        i for i in range(len(df))
        if isinstance(df.iat[i, 0], str)
        and re.match(r'^\s*H\s*\d+', df.iat[i, 0].strip(), re.I)
    ]


def parse_sheet_to_events_json(file_content: bytes, sheet_name: str) -> List[dict]:
    file_io = io.BytesIO(file_content)
    try:
        df = pd.read_excel(file_io, sheet_name=sheet_name, header=None)
    except ValueError:
        log_msg(f"Feuille {sheet_name} introuvable.")
        return []
    except Exception as e:
        log_msg(f"Erreur lecture Excel: {e}")
        return []

    file_io.seek(0)
    merged_map = get_merged_map(file_io, sheet_name)

    nrows, ncols = df.shape
    s_rows = find_week_rows(df)
    h_rows = find_slot_rows(df)

    log_msg(f"[{sheet_name}] Semaines trouvées: {len(s_rows)}, Créneaux trouvés: {len(h_rows)}")

    raw_events = []

    for r in h_rows:
        p_candidates = [s for s in s_rows if s <= r]
        if not p_candidates:
            continue
        p = max(p_candidates)
        date_row = p + 1
        group_row = p + 2

        date_cols = [
            c for c in range(ncols)
            if date_row < nrows and to_date(df.iat[date_row, c]) is not None
        ]

        for c in date_cols:
            for col in (c, c + 1):
                if col >= ncols:
                    continue

                summary = df.iat[r, col]
                if pd.isna(summary) or summary is None:
                    continue
                summary_str = str(summary).strip()
                if not summary_str:
                    continue

                teachers = []
                if (r + 2) < nrows:
                    for off in range(2, 6):
                        idx = r + off
                        if idx >= nrows:
                            break
                        t = df.iat[idx, col]
                        if t is None or pd.isna(t):
                            continue
                        s = str(t).strip()
                        if not s:
                            continue
                        if not is_time_like(s) and to_date(s) is None:
                            teachers.append(s)
                teachers = list(dict.fromkeys(teachers))

                stop_idx = None
                for off in range(1, 12):
                    idx = r + off
                    if idx >= nrows:
                        break
                    if is_time_like(df.iat[idx, col]):
                        stop_idx = idx
                        break
                if stop_idx is None:
                    stop_idx = min(r + 7, nrows)

                desc_parts = []
                for idx in range(r + 1, stop_idx):
                    if idx >= nrows:
                        break
                    cell = df.iat[idx, col]
                    if pd.isna(cell) or cell is None:
                        continue
                    s = str(cell).strip()
                    if not s:
                        continue
                    if to_date(cell) is not None:
                        continue
                    if s in teachers or s == summary_str:
                        continue
                    desc_parts.append(s)
                desc_text = " | ".join(dict.fromkeys(desc_parts))

                start_val, end_val = None, None
                for off in range(1, 13):
                    idx = r + off
                    if idx >= nrows:
                        break
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

                d = to_date(df.iat[date_row, c])
                if d is None:
                    continue
                dtstart = datetime.combine(d, start_t)
                dtend = datetime.combine(d, end_t)

                gl = normalize_group_label(df.iat[group_row, col] if group_row < nrows else None)
                gl_next = normalize_group_label(df.iat[group_row, col + 1] if (col + 1) < ncols else None)
                is_left_col = (col == c)

                groups = set()
                if is_left_col:
                    merged = merged_map.get((r, col))
                    if merged and (r, col + 1) in merged_map:
                        if gl:
                            groups.add(gl)
                        if gl_next:
                            groups.add(gl_next)
                    else:
                        if gl:
                            groups.add(gl)
                else:
                    if gl:
                        groups.add(gl)

                raw_events.append({
                    'summary': summary_str,
                    'teachers': set(teachers),
                    'descriptions': set([desc_text]) if desc_text else set(),
                    'start': dtstart,
                    'end': dtend,
                    'groups': groups
                })

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

    final_list = []
    for v in merged.values():
        final_list.append({
            'summary': v['summary'],
            'teachers': sorted(list(v['teachers'])),
            'description': " | ".join(sorted(list(v['descriptions']))) if v['descriptions'] else "",
            'start': v['start'].isoformat(),
            'end': v['end'].isoformat(),
            'groups': sorted(list(v['groups']))
        })

    return final_list


# ==========================================
# 2b. PARSING MAQUETTE
# ==========================================

def parse_maquette_sheet(file_content: bytes) -> List[dict]:
    """
    Parse la feuille 'Maquette' du fichier Excel.
    Structure attendue (0-indexé) :
      col 2  = Matière
      col 3-7 = Enseignants 1-5
      col 8  = CM/TD
      col 9  = TP
      col 10 = Autonomie
      col 11 = Examen
      col 12 = Total
      col 13 = Commentaires
      col 14 = Coefficient
    Ligne 0 = groupe de colonnes (headers niveau 1)
    Ligne 1 = headers
    Ligne 2+ = données
    """
    file_io = io.BytesIO(file_content)
    try:
        xls = pd.ExcelFile(file_io)
        maquette_sheet = next((s for s in xls.sheet_names if 'maquette' in s.lower()), None)
        if not maquette_sheet:
            log_msg("Feuille Maquette introuvable.")
            return []
    except Exception as e:
        log_msg(f"Erreur lecture feuilles maquette: {e}")
        return []

    file_io.seek(0)
    try:
        df = pd.read_excel(file_io, sheet_name=maquette_sheet, header=None)
    except Exception as e:
        log_msg(f"Erreur lecture maquette: {e}")
        return []

    if df.shape[1] < 13:
        log_msg("Maquette: pas assez de colonnes.")
        return []

    IGNORE_SUBJECTS = {
        "erasmus day", "forum international", "période entreprise", "periode entreprise",
        "férié", "ferie", "mission à l'international", "mission a l'international",
        "matière", "matières", "divers"
    }

    rows_out = []
    # Contexte de section (semestre / UE) — propagation vers le bas
    current_semester = None
    current_ue = None

    for i in range(2, len(df)):  # skip les 2 lignes de headers
        row = df.iloc[i]

        # Mise à jour du semestre (col 0) si non vide
        if pd.notna(row.iloc[0]) and str(row.iloc[0]).strip():
            current_semester = str(row.iloc[0]).strip()

        # Mise à jour de l'UE (col 1) si non vide
        if pd.notna(row.iloc[1]) and str(row.iloc[1]).strip():
            current_ue = str(row.iloc[1]).strip()

        # Matière (col 2)
        subj_raw = row.iloc[2] if df.shape[1] > 2 else None
        if subj_raw is None or pd.isna(subj_raw):
            continue
        subj = str(subj_raw).strip()
        if not subj:
            continue
        if subj.lower() in IGNORE_SUBJECTS:
            continue

        # Enseignants (cols 3-7)
        teachers = []
        for tc in range(3, 8):
            if tc >= df.shape[1]:
                break
            t = row.iloc[tc]
            if pd.notna(t) and str(t).strip() and str(t).strip() not in [' ', '']:
                teachers.append(str(t).strip())

        def safe_float(v):
            try:
                f = float(v)
                return f if not pd.isna(f) else None
            except Exception:
                return None

        cm_td    = safe_float(row.iloc[8])  if df.shape[1] > 8  else None
        tp       = safe_float(row.iloc[9])  if df.shape[1] > 9  else None
        autonomie= safe_float(row.iloc[10]) if df.shape[1] > 10 else None
        examen   = safe_float(row.iloc[11]) if df.shape[1] > 11 else None
        total    = safe_float(row.iloc[12]) if df.shape[1] > 12 else None
        comment  = str(row.iloc[13]).strip() if df.shape[1] > 13 and pd.notna(row.iloc[13]) else ""
        coeff    = safe_float(row.iloc[14]) if df.shape[1] > 14 else None

        rows_out.append({
            "subject":    subj,
            "semester":   current_semester,
            "ue":         current_ue,
            "teachers":   teachers,
            "cm_td":      cm_td,
            "tp":         tp,
            "autonomie":  autonomie,
            "examen":     examen,
            "total":      total,
            "comment":    comment,
            "coeff":      coeff,
        })

    log_msg(f"Maquette: {len(rows_out)} matières extraites.")
    return rows_out


# ==========================================
# 2c. PARSING ENSEIGNANTS (tarifs)
# ==========================================

def parse_teachers_sheet(file_content: bytes) -> List[dict]:
    """
    Parse la feuille 'Enseignants' du fichier Excel.
    Structure (0-indexé) :
      col 1 = Nom (format "NOM, Prénom" — identique au format utilisé dans EDT P1/P2)
      col 2 = Organisme
      col 3 = Majoration (1 = oui / vide = non, utilisé par la formule Excel)
      col 4 = Pourcentage de majoration
      col 5 = Tarif horaire théorique
      col 6 = Tarif horaire effectif (= théorique si majoration, sinon théorique x1.25)
      col 7 = Région

    On stocke le tarif EFFECTIF (col 6), qui est celui réellement utilisé dans les
    formules de facturation du classeur Excel (VLOOKUP ... colonne 6).
    """
    file_io = io.BytesIO(file_content)
    try:
        xls = pd.ExcelFile(file_io)
        teachers_sheet = next((s for s in xls.sheet_names if 'enseignant' in s.lower()), None)
        if not teachers_sheet:
            log_msg("Feuille Enseignants introuvable.")
            return []
    except Exception as e:
        log_msg(f"Erreur lecture feuilles enseignants: {e}")
        return []

    file_io.seek(0)
    try:
        df = pd.read_excel(file_io, sheet_name=teachers_sheet, header=None)
    except Exception as e:
        log_msg(f"Erreur lecture enseignants: {e}")
        return []

    if df.shape[1] < 7:
        log_msg("Enseignants: pas assez de colonnes.")
        return []

    def safe_float(v):
        try:
            f = float(v)
            return f if not pd.isna(f) else None
        except Exception:
            return None

    rows_out = []
    seen_names = set()
    for i in range(len(df)):
        name_raw = df.iat[i, 1]
        if name_raw is None or pd.isna(name_raw):
            continue
        name = str(name_raw).strip()
        if not name:
            continue
        # En cas de doublon (le fichier peut lister 2x le même enseignant), garder la 1ère occurrence
        if name in seen_names:
            continue
        seen_names.add(name)

        rate = safe_float(df.iat[i, 6])  # tarif effectif (colonne G / index 6)
        rows_out.append({
            "name": name,
            "organism": str(df.iat[i, 2]).strip() if pd.notna(df.iat[i, 2]) else "",
            "hourly_rate": rate if rate is not None else 0.0,
        })

    log_msg(f"Enseignants: {len(rows_out)} tarifs extraits.")
    return rows_out




# ==========================================
# 2d. PARSING OPUS/FNG (PDF des salles)
# ==========================================

def clean_pdf_cell(value) -> str:
    """Nettoie une cellule extraite d'un PDF Opus/FNG."""
    if value is None:
        return ""
    s = str(value).replace("\r", "\n")
    s = re.sub(r"\s*\n\s*", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_opus_date(value) -> Optional[date]:
    """Extrait une date JJ/MM/AA d'une cellule Opus pouvant contenir le jour de semaine."""
    s = clean_pdf_cell(value)
    m = re.search(r"(\d{1,2}/\d{1,2}/\d{2,4})", s)
    if not m:
        return None
    raw = m.group(1)
    for fmt in ("%d/%m/%y", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def parse_opus_time(value) -> Optional[time]:
    """
    Extrait une heure Opus. Le PDF coupe parfois les minutes sur deux lignes
    (ex: "12:3\n0"), donc on supprime les blancs avant de parser.
    """
    s = clean_pdf_cell(value).replace(" ", "")
    s = re.sub(r"[^0-9:]", "", s)
    m = re.match(r"^(\d{1,2}):(\d{1,2})$", s)
    if not m:
        return None
    hour = int(m.group(1))
    minute_txt = m.group(2)
    if len(minute_txt) == 1:
        minute_txt = minute_txt + "0"
    minute = int(minute_txt)
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        return None
    return time(hour, minute)


def normalize_room_label(value) -> str:
    """Normalise une salle, y compris les salles sur deux lignes (ex: A012 Pléïades 2)."""
    return clean_pdf_cell(value).strip()


def parse_opus_targets(group_value: str) -> List[dict]:
    """
    Transforme le libellé Groupe du PDF Opus en cibles internes.
      - Groupe session complète -> P1 + P2
      - Promo 1 Groupe 1 -> P1 / G 1
      - Promo 2 -> toute la P2
    Les lignes hors promo (ex: Jury) sont ignorées.
    """
    s = clean_pdf_cell(group_value)
    if not s:
        return []
    sl = s.lower()

    if "session" in sl and "compl" in sl:
        return [{"promo": "p1", "subgroup": None}, {"promo": "p2", "subgroup": None}]

    promo_match = re.search(r"promo\s*([12])", sl)
    if not promo_match:
        return []

    promo = "p" + promo_match.group(1)
    group_match = re.search(r"groupe\s*(\d+)", sl)
    subgroup = f"G {int(group_match.group(1))}" if group_match else None
    return [{"promo": promo, "subgroup": subgroup}]


def merge_pdf_table_rows(left: list, right: list) -> list:
    """
    Fusionne deux fragments de ligne Opus.

    Cas réel observé dans le PDF fourni : une séance en bas de page peut finir sur
    la page suivante. Exemple page 23 -> 24 :
      ['mar.', '13:30', '17:3', 'Electrotechnique et électronique de', ...]
      ['30/09/25', '', '0', 'puissance', 'promos', ...]
    La fusion redonne une ligne exploitable par les parseurs regex.
    """
    left = (left + [None] * 9)[:9]
    right = (right + [None] * 9)[:9]
    merged = []
    for a, b in zip(left, right):
        sa = clean_pdf_cell(a)
        sb = clean_pdf_cell(b)
        if sa and sb:
            merged.append(f"{sa} {sb}")
        else:
            merged.append(sa or sb)
    return merged


def is_opus_header_row(row: list) -> bool:
    row = (row + [None] * 9)[:9]
    joined = " ".join(clean_pdf_cell(c).lower() for c in row)
    return "date" in joined and "début" in joined and "fin" in joined and "salle" in joined


def row_has_meaningful_text(row: list) -> bool:
    return any(clean_pdf_cell(c) for c in (row or []))


def row_looks_like_event_fragment(row: list) -> bool:
    """Détecte une ligne qui ressemble au début ou à un morceau de séance Opus."""
    row = (row + [None] * 9)[:9]
    return bool(
        clean_pdf_cell(row[0])
        or parse_opus_time(row[1])
        or parse_opus_time(row[2])
        or clean_pdf_cell(row[3])
        or clean_pdf_cell(row[4])
        or clean_pdf_cell(row[5])
        or clean_pdf_cell(row[6])
    )


def row_looks_like_page_continuation(previous_row: list, current_row: list) -> bool:
    """
    Vrai si current_row semble compléter previous_row après un saut de page.

    On évite volontairement les lignes de salles additionnelles normales : elles
    portent généralement date + début + fin + salle, et doivent être rattachées au
    créneau précédent plutôt que fusionnées avec la dernière ligne de la page.
    """
    if not previous_row or not current_row:
        return False
    previous_row = (previous_row + [None] * 9)[:9]
    current_row = (current_row + [None] * 9)[:9]

    if not row_has_meaningful_text(current_row):
        return False
    if not row_looks_like_event_fragment(previous_row):
        return False
    if is_opus_header_row(current_row):
        return False

    current_has_start = parse_opus_time(current_row[1]) is not None
    current_has_targets = bool(parse_opus_targets(clean_pdf_cell(current_row[5])))

    # Une vraie nouvelle séance a presque toujours un début. Une continuation de
    # haut de page complète plutôt la date, la fin, la matière, le découpage ou la salle.
    if current_has_start or current_has_targets:
        return False

    previous_missing_date = parse_opus_date(previous_row[0]) is None
    current_supplies_date = parse_opus_date(current_row[0]) is not None
    previous_end_txt = clean_pdf_cell(previous_row[2])
    current_end_txt = clean_pdf_cell(current_row[2])

    # Signaux forts vus dans le PDF : date seule en haut de page, minute finale
    # isolée, ou mots de fin de cellule matière/découpage/salle.
    if previous_missing_date and current_supplies_date:
        return True
    if re.match(r"^\d{1,2}:\d$", previous_end_txt.replace(" ", "")) and re.match(r"^\d$", current_end_txt):
        return True
    if any(clean_pdf_cell(current_row[i]) for i in (3, 4, 6, 7, 8)) and not current_has_start:
        return True

    return False


def parse_opus_pdf_rooms(file_content: bytes) -> List[dict]:
    """
    Parse le PDF Opus/FNG et retourne uniquement les lignes exploitables pour les salles.

    On ne s'appuie volontairement que sur : date, heure de début, heure de fin,
    groupe et salle. Les matières et intervenants d'Opus sont ignorés car ils peuvent
    différer du planning principal.

    Points importants validés sur le PDF fourni :
      - les dates sont extraites par regex JJ/MM/AA, jamais par dateutil sur "mar." ;
      - les heures coupées ("12:3\n0", "17:3" + "0") sont recomposées ;
      - les salles multi-lignes ("A012 Pléïades\n2") sont conservées ;
      - les lignes coupées entre deux pages sont fusionnées avant parsing ;
      - les salles supplémentaires sur une ligne séparée sont rattachées au même créneau.
    """
    try:
        import pdfplumber
    except Exception as exc:
        raise RuntimeError("La dépendance pdfplumber est requise pour lire les PDF Opus.") from exc

    entries: List[dict] = []
    last_entry_by_slot: Dict[Tuple[date, time, time], dict] = {}

    table_settings = {
        "vertical_strategy": "lines",
        "horizontal_strategy": "lines",
        "snap_tolerance": 3,
        "join_tolerance": 3,
        "edge_min_length": 3,
        "min_words_vertical": 1,
        "min_words_horizontal": 1,
        "text_x_tolerance": 2,
        "text_y_tolerance": 3,
    }

    def process_row(row: list, pages: List[int]):
        row = (row + [None] * 9)[:9]
        if is_opus_header_row(row):
            return

        day = parse_opus_date(row[0])
        start_t = parse_opus_time(row[1])
        end_t = parse_opus_time(row[2])
        if not (day and start_t and end_t):
            return

        room = normalize_room_label(row[6])
        if not room:
            return

        group_label = clean_pdf_cell(row[5])
        targets = parse_opus_targets(group_label)
        slot_key = (day, start_t, end_t)

        if targets:
            entry = {
                "page": pages[0] if pages else None,
                "pages": pages,
                "start": datetime.combine(day, start_t),
                "end": datetime.combine(day, end_t),
                "targets": targets,
                "room": room,
                "rooms": [room],
                "pdf_group": group_label,
            }
            entries.append(entry)
            last_entry_by_slot[slot_key] = entry
        else:
            # Salle sur une ligne de continuation : on la rattache au créneau précédent.
            previous = last_entry_by_slot.get(slot_key)
            if previous:
                previous["rooms"].append(room)
                previous["room"] = " / ".join(dict.fromkeys(previous["rooms"]))
                previous_pages = previous.setdefault("pages", [])
                for p in pages:
                    if p not in previous_pages:
                        previous_pages.append(p)

    pending_last_row = None
    pending_last_pages: List[int] = []

    with pdfplumber.open(io.BytesIO(file_content)) as pdf:
        for page_number, page in enumerate(pdf.pages, start=1):
            try:
                table = page.extract_table(table_settings) or page.extract_table() or []
            except Exception:
                table = []

            data_rows = []
            for raw_row in table:
                row = (list(raw_row or []) + [None] * 9)[:9]
                if is_opus_header_row(row):
                    continue
                if not row_has_meaningful_text(row):
                    continue
                data_rows.append(row)

            # Si la page précédente s'est terminée sur un fragment, vérifier si la
            # première ligne de cette page le complète.
            if pending_last_row is not None:
                if data_rows and row_looks_like_page_continuation(pending_last_row, data_rows[0]):
                    merged_row = merge_pdf_table_rows(pending_last_row, data_rows[0])
                    process_row(merged_row, pending_last_pages + [page_number])
                    data_rows = data_rows[1:]
                else:
                    process_row(pending_last_row, pending_last_pages)
                pending_last_row = None
                pending_last_pages = []

            if not data_rows:
                continue

            # On garde toujours la dernière ligne de la page en attente afin de
            # pouvoir la fusionner avec le haut de page suivant si Opus l'a coupée.
            for row in data_rows[:-1]:
                process_row(row, [page_number])
            pending_last_row = data_rows[-1]
            pending_last_pages = [page_number]

    if pending_last_row is not None:
        process_row(pending_last_row, pending_last_pages)

    # Fusion de sécurité : si le même créneau/cible apparaît plusieurs fois, on regroupe les salles.
    merged: Dict[Tuple[datetime, datetime, str, Optional[str]], dict] = {}
    for entry in entries:
        for target in entry["targets"]:
            key = (entry["start"], entry["end"], target["promo"], target.get("subgroup"))
            if key not in merged:
                merged[key] = {
                    "start": entry["start"],
                    "end": entry["end"],
                    "promo": target["promo"],
                    "subgroup": target.get("subgroup"),
                    "rooms": [],
                    "pdf_groups": set(),
                    "pages": set(),
                }
            merged[key]["rooms"].extend(entry.get("rooms") or [entry["room"]])
            if entry.get("pdf_group"):
                merged[key]["pdf_groups"].add(entry["pdf_group"])
            for p in (entry.get("pages") or [entry.get("page")]):
                if p:
                    merged[key]["pages"].add(p)

    out = []
    for value in merged.values():
        unique_rooms = list(dict.fromkeys([r for r in value["rooms"] if r]))
        if not unique_rooms:
            continue
        out.append({
            "start": value["start"],
            "end": value["end"],
            "promo": value["promo"],
            "subgroup": value["subgroup"],
            "room": " / ".join(unique_rooms),
            "pdf_groups": sorted(value["pdf_groups"]),
            "pages": sorted(p for p in value["pages"] if p),
        })

    return out

def parse_iso_datetime(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        return None


def calendar_bounds(events_p1: List[dict], events_p2: List[dict]) -> Tuple[Optional[datetime], Optional[datetime]]:
    starts, ends = [], []
    for ev in (events_p1 or []) + (events_p2 or []):
        st = parse_iso_datetime(ev.get("start"))
        en = parse_iso_datetime(ev.get("end"))
        if st:
            starts.append(st)
        if en:
            ends.append(en)
    if not starts or not ends:
        return None, None
    return min(starts), max(ends)


def room_target_matches_event(entry: dict, ev: dict, promo: str) -> bool:
    """Teste si une entrée salle Opus s'applique à un événement interne."""
    if entry.get("promo") != promo:
        return False

    subgroup = entry.get("subgroup")
    if not subgroup:
        return True

    raw_groups = ev.get("groups") or []
    if isinstance(raw_groups, str):
        raw_groups = [raw_groups]
    normalized_groups = {normalize_group_label(g) for g in raw_groups if normalize_group_label(g)}
    return subgroup in normalized_groups


def intervals_are_compatible(opus_start: datetime, opus_end: datetime, event_start: datetime, event_end: datetime) -> bool:
    """
    Vérifie la compatibilité horaire entre Opus et le calendrier interne.

    Le rapprochement exact était trop strict : Opus regroupe parfois une séance
    08:30-12:30 alors que le calendrier interne a deux séances 08:30-10:30 et
    10:30-12:30. On accepte donc les cas où un intervalle contient l'autre.
    Les chevauchements simplement partiels ne sont pas acceptés, afin d'éviter
    d'injecter une salle sur une séance voisine.
    """
    if not (opus_start and opus_end and event_start and event_end):
        return False
    if opus_start.date() != event_start.date():
        return False

    # Tolérance légère pour absorber les arrondis/minutes recomposées depuis le PDF.
    tolerance = pd.Timedelta(minutes=1).to_pytimedelta()

    event_inside_opus = (opus_start - tolerance) <= event_start and event_end <= (opus_end + tolerance)
    opus_inside_event = (event_start - tolerance) <= opus_start and opus_end <= (event_end + tolerance)
    return event_inside_opus or opus_inside_event


def calendar_range_overlaps(entry_start: datetime, entry_end: datetime, cal_start: datetime, cal_end: datetime) -> bool:
    """Vrai si l'entrée Opus touche la période couverte par le calendrier."""
    if not (entry_start and entry_end and cal_start and cal_end):
        return False
    return entry_end >= cal_start and entry_start <= cal_end


def inject_rooms_into_events(events_p1: List[dict], events_p2: List[dict], room_entries: List[dict]) -> dict:
    """
    Injecte/écrase les salles dans les événements existants.

    Règles de rapprochement :
      - même promo/groupe ;
      - même date ;
      - intervalle horaire exact OU intervalle Opus qui englobe la séance interne
        OU séance interne qui englobe l'intervalle Opus.

    Cela corrige les cas où Opus regroupe des créneaux de 4 h alors que le
    calendrier interne les découpe en deux créneaux de 2 h.
    """
    cal_start, cal_end = calendar_bounds(events_p1, events_p2)
    if not cal_start or not cal_end:
        return {
            "events_p1": events_p1 or [],
            "events_p2": events_p2 or [],
            "parsed_rooms": len(room_entries or []),
            "usable_rooms": 0,
            "matched_events": 0,
            "unmatched_rooms": len(room_entries or []),
        }

    usable_entries = [
        e for e in (room_entries or [])
        if e.get("start") and e.get("end") and calendar_range_overlaps(e["start"], e["end"], cal_start, cal_end)
    ]

    entry_used = [False] * len(usable_entries)

    def update_list(events: List[dict], promo: str) -> Tuple[List[dict], int]:
        updated = []
        matched_count = 0
        for ev in events or []:
            ev_copy = dict(ev)
            # Une injection Opus est une mise à jour des salles : on retire la salle
            # précédente puis on la remet seulement si le PDF contient un match.
            ev_copy.pop("room", None)

            st = parse_iso_datetime(ev_copy.get("start"))
            en = parse_iso_datetime(ev_copy.get("end"))
            if not st or not en:
                updated.append(ev_copy)
                continue

            rooms = []
            for idx, entry in enumerate(usable_entries):
                if (
                    intervals_are_compatible(entry.get("start"), entry.get("end"), st, en)
                    and room_target_matches_event(entry, ev_copy, promo)
                ):
                    if entry.get("room"):
                        rooms.append(entry["room"])
                        entry_used[idx] = True

            unique_rooms = list(dict.fromkeys(rooms))
            if unique_rooms:
                ev_copy["room"] = " / ".join(unique_rooms)
                matched_count += 1
            updated.append(ev_copy)
        return updated, matched_count

    new_p1, matched_p1 = update_list(events_p1 or [], "p1")
    new_p2, matched_p2 = update_list(events_p2 or [], "p2")

    return {
        "events_p1": new_p1,
        "events_p2": new_p2,
        "parsed_rooms": len(room_entries or []),
        "usable_rooms": len(usable_entries),
        "matched_events": matched_p1 + matched_p2,
        "unmatched_rooms": sum(1 for used in entry_used if not used),
        "calendar_start": cal_start.isoformat(),
        "calendar_end": cal_end.isoformat(),
    }


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

        try:
            start_dt = datetime.fromisoformat(ev['start'])
            end_dt = datetime.fromisoformat(ev['end'])
        except ValueError:
            continue

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
        room = ev.get('room') or ev.get('location') or ""
        if room:
            desc_lines.append('Salle: ' + str(room))

        if ev.get('description'):
            desc_lines.append(ev['description'])

        teachers = ev.get('teachers', [])
        if teachers:
            desc_lines.append('Enseignant(s): ' + ' / '.join(teachers))

        groups = ev.get('groups', [])
        if groups:
            if len(groups) == 1:
                desc_lines.append('Groupe: ' + groups[0])
            else:
                desc_lines.append('Groupes: ' + ' et '.join(groups))

        description = escape_ical_text('\n'.join(desc_lines))
        location = escape_ical_text(room)

        event_lines = [
            'BEGIN:VEVENT',
            f'UID:{uid}',
            f'DTSTAMP:{datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")}',
            f'DTSTART;TZID={tzname}:{dtstart}',
            f'DTEND;TZID={tzname}:{dtend}',
            f'SUMMARY:{summary}',
        ]
        if location:
            event_lines.append(f'LOCATION:{location}')
        event_lines.extend([
            f'DESCRIPTION:{description}',
            'END:VEVENT'
        ])
        body.extend(event_lines)

    footer = ['END:VCALENDAR']
    return '\n'.join(header + body + footer)


# ==========================================
# 4. ROUTES & AUTH
# ==========================================

def get_current_user(request: Request):
    return request.session.get("user")


def hash_password(password: str):
    return hashlib.sha256(password.encode()).hexdigest()


def convert_updated_at(plannings: list) -> list:
    for p in plannings:
        if p.get("updated_at"):
            try:
                dt = datetime.fromisoformat(p["updated_at"].replace("Z", "+00:00"))
                dt_paris = dt.astimezone(PARIS_TZ)
                p["updated_at"] = dt_paris.isoformat()
            except Exception:
                pass
    return plannings


@app.get("/", response_class=HTMLResponse)
async def home(request: Request, filter: str = "my"):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)

    query = supabase.table("plannings").select("slug, name, year, updated_at, creator")

    if filter == "my":
        query = query.eq("creator", user)

    response = query.execute()
    plannings = convert_updated_at(response.data)

    return templates.TemplateResponse(request, "index.html", {
        "user": user,
        "plannings": plannings,
        "current_filter": filter
    })


# --- LOGIN / REGISTER ---

@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"mode": "login"})


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse(request, "login.html", {"mode": "register"})


@app.post("/login")
async def login_submit(request: Request, username: str = Form(...), password: str = Form(...)):
    hashed = hash_password(password)
    res = supabase.table("users").select("*").eq("username", username).eq("password_hash", hashed).execute()

    if len(res.data) > 0:
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    else:
        return templates.TemplateResponse(request, "login.html", {
            "mode": "login",
            "error": "Identifiants incorrects"
        })


@app.post("/register")
async def register_submit(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    verification: str = Form(...)
):
    if verification.strip().upper() != "EUROVISION":
        return templates.TemplateResponse(request, "login.html", {
            "mode": "register",
            "error": "Code de vérification incorrect !"
        })

    check = supabase.table("users").select("*").eq("username", username).execute()
    if len(check.data) > 0:
        return templates.TemplateResponse(request, "login.html", {
            "mode": "register",
            "error": "Ce pseudo est déjà pris."
        })

    hashed = hash_password(password)
    try:
        supabase.table("users").insert({"username": username, "password_hash": hashed}).execute()
        request.session["user"] = username
        return RedirectResponse(url="/", status_code=303)
    except Exception as e:
        log_msg(f"Erreur inscription: {e}")
        return templates.TemplateResponse(request, "login.html", {
            "mode": "register",
            "error": "Erreur technique lors de l'inscription."
        })


@app.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/login", status_code=303)


# --- ACTIONS CALENDRIER ---

@app.post("/create")
async def create_calendar(request: Request, promo_name: str = Form(...), school_year: str = Form(...)):
    user = get_current_user(request)
    if not user:
        return RedirectResponse("/login")

    slug = f"{promo_name}-{school_year}".lower().replace(" ", "-")
    data = {"slug": slug, "name": promo_name, "year": school_year, "creator": user}
    try:
        supabase.table("plannings").insert(data).execute()
        log_msg(f"Nouveau planning créé: {slug} par {user}")
    except Exception as e:
        log_msg(f"Erreur création (existe probablement déjà): {e}")

    return RedirectResponse("/", status_code=303)


@app.post("/upload/{slug}")
async def upload_excel(slug: str, request: Request, file: UploadFile = File(...)):
    if not get_current_user(request):
        return RedirectResponse("/login")

    log_msg(f"Début upload pour {slug}")
    content = await file.read()

    try:
        xls = pd.ExcelFile(io.BytesIO(content))
        log_msg(f"Feuilles disponibles: {xls.sheet_names}")
    except Exception as e:
        log_msg(f"Erreur lecture feuilles: {e}")

    events_p1 = parse_sheet_to_events_json(content, "EDT P1")
    events_p2 = parse_sheet_to_events_json(content, "EDT P2")
    maquette_data = parse_maquette_sheet(content)
    teachers_data = parse_teachers_sheet(content)

    log_msg(f"Events trouvés - P1: {len(events_p1)}, P2: {len(events_p2)}, Maquette: {len(maquette_data)}, Enseignants: {len(teachers_data)}")

    try:
        supabase.table("plannings").update({
            "events_p1": events_p1,
            "events_p2": events_p2,
            "maquette_data": maquette_data,
            "teachers_data": teachers_data,
            "updated_at": datetime.now(PARIS_TZ).isoformat()
        }).eq("slug", slug).execute()
    except Exception as e:
        log_msg(f"Erreur mise à jour BDD: {e}")


    return RedirectResponse("/", status_code=303)


@app.post("/inject-rooms/{slug}")
async def inject_rooms_from_opus(slug: str, request: Request, file: UploadFile = File(...)):
    """Ajoute/met à jour les salles depuis un PDF Opus/FNG sans modifier les séances."""
    if not get_current_user(request):
        return RedirectResponse("/login")

    filename = (file.filename or "").lower()
    if filename and not filename.endswith(".pdf"):
        return RedirectResponse("/?error=rooms_bad_file", status_code=303)

    try:
        content = await file.read()
        if not content:
            return RedirectResponse("/?error=rooms_empty_file", status_code=303)

        res = supabase.table("plannings").select("events_p1, events_p2").ilike("slug", slug).execute()
        if not res.data:
            raise HTTPException(404, detail="Planning introuvable")

        current = res.data[0]
        events_p1 = current.get("events_p1", []) or []
        events_p2 = current.get("events_p2", []) or []

        room_entries = parse_opus_pdf_rooms(content)
        inject_result = inject_rooms_into_events(events_p1, events_p2, room_entries)

        supabase.table("plannings").update({
            "events_p1": inject_result["events_p1"],
            "events_p2": inject_result["events_p2"],
            "updated_at": datetime.now(PARIS_TZ).isoformat()
        }).eq("slug", slug).execute()

        log_msg(
            f"Salles Opus pour {slug}: PDF={inject_result['parsed_rooms']}, "
            f"dans période={inject_result['usable_rooms']}, "
            f"événements maj={inject_result['matched_events']}, "
            f"non rapprochées={inject_result['unmatched_rooms']}"
        )

        return RedirectResponse(
            f"/?success=rooms&rooms_matched={inject_result['matched_events']}&rooms_pdf={inject_result['usable_rooms']}&rooms_unmatched={inject_result['unmatched_rooms']}",
            status_code=303
        )

    except HTTPException:
        raise
    except Exception as e:
        log_msg(f"Erreur injection salles Opus: {e}")
        return RedirectResponse("/?error=rooms", status_code=303)


# --- URL PUBLIQUE POUR OUTLOOK ---

@app.get("/ics/{slug}/{group}.ics")
async def get_ics_file(slug: str, group: str):
    group_clean = group.upper()
    if group_clean not in ["P1", "P2"]:
        raise HTTPException(404, detail="Groupe inconnu (utiliser P1 ou P2)")

    try:
        res = supabase.table("plannings").select(f"events_{group_clean.lower()}").ilike("slug", slug).execute()

        if not res.data:
            raise HTTPException(404, detail="Planning introuvable")

        events_json = res.data[0].get(f"events_{group_clean.lower()}", []) or []
        ics_content = events_to_ics_string(events_json)
        filename = f"{slug}_{group_clean}.ics"

        return Response(content=ics_content, media_type="text/calendar", headers={
            "Content-Disposition": f"attachment; filename={filename}"
        })

    except HTTPException:
        raise
    except Exception as e:
        log_msg(f"Erreur ICS generation: {e}")
        raise HTTPException(500, detail="Erreur interne")


# --- VUE PUBLIQUE CALENDRIER ---

@app.get("/calendrier/{slug}", response_class=HTMLResponse)
async def view_calendar(slug: str, request: Request):
    res = supabase.table("plannings").select("slug, name, year").ilike("slug", slug).execute()
    if not res.data:
        raise HTTPException(404, detail="Planning introuvable")
    p = res.data[0]
    return templates.TemplateResponse(request, "calendar_view.html", {
        "slug": p["slug"],
        "planning_name": p["name"],
        "planning_year": p.get("year", ""),
    })


# --- VUE TABLEAU DE BORD (protégée) ---

@app.get("/dashboard/{slug}", response_class=HTMLResponse)
async def view_dashboard(slug: str, request: Request):
    user = get_current_user(request)
    if not user:
        return RedirectResponse(url="/login", status_code=303)
    res = supabase.table("plannings").select("slug, name, year").ilike("slug", slug).execute()
    if not res.data:
        raise HTTPException(404, detail="Planning introuvable")
    p = res.data[0]
    return templates.TemplateResponse(request, "dashboard.html", {
        "slug": p["slug"],
        "planning_name": p["name"],
        "planning_year": p.get("year", ""),
        "user": user,
    })


# --- APIS JSON ---

@app.get("/api/events/{slug}/{group}")
async def api_events(slug: str, group: str):
    group_clean = group.lower()
    if group_clean not in ["p1", "p2"]:
        raise HTTPException(404)
    try:
        res = supabase.table("plannings").select(f"events_{group_clean}").ilike("slug", slug).execute()
        if not res.data:
            raise HTTPException(404)
        return res.data[0].get(f"events_{group_clean}", []) or []
    except HTTPException:
        raise
    except Exception as e:
        log_msg(f"Erreur API events: {e}")
        raise HTTPException(500, detail="Erreur interne")


@app.get("/api/maquette/{slug}")
async def api_maquette(slug: str):
    """Retourne les données de la maquette pédagogique."""
    try:
        res = supabase.table("plannings").select("maquette_data").ilike("slug", slug).execute()
        if not res.data:
            raise HTTPException(404)
        return res.data[0].get("maquette_data", []) or []
    except HTTPException:
        raise
    except Exception as e:
        log_msg(f"Erreur API maquette: {e}")
        raise HTTPException(500, detail="Erreur interne")


@app.get("/api/dashboard-data/{slug}")
async def api_dashboard_data(slug: str):
    """Retourne en une seule requête : events_p1, events_p2, maquette_data, teachers_data."""
    try:
        res = supabase.table("plannings").select(
            "events_p1, events_p2, maquette_data, teachers_data"
        ).ilike("slug", slug).execute()
        if not res.data:
            raise HTTPException(404)
        d = res.data[0]
        return {
            "events_p1":     d.get("events_p1", []) or [],
            "events_p2":     d.get("events_p2", []) or [],
            "maquette_data": d.get("maquette_data", []) or [],
            "teachers_data": d.get("teachers_data", []) or [],
        }
    except HTTPException:
        raise
    except Exception as e:
        log_msg(f"Erreur API dashboard-data: {e}")
        raise HTTPException(500, detail="Erreur interne")
