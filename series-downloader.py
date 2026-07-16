#!/usr/bin/env python3
"""
BEAM Series Downloader — GitHub Actions Pipeline
=================================================
Reads Google Sheet (Series_Pipeline tab) → downloads episodes → detects audio languages →
renames files → uploads to Vidara → calls BEAM Worker upsert API → writes URLs back to sheet.
"""

import os
import re
import json
import shutil
import requests
import time
import subprocess
import zipfile
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from pymediainfo import MediaInfo
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

# ============================================================================
# CONFIGURATION
# ============================================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

VIDARA_API_KEY = os.environ.get("VIDARA_API_KEY", "").strip()
if not VIDARA_API_KEY:
    VIDARA_API_KEY = "de57ed8e0bd00f3c0c18db283f5377caf14ad141ffb74ee49f83cb5ed13ab9dc"

RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()

BEAM_WORKER_URL = "https://beamplay.beam-api.workers.dev"
ADMIN_EMAIL = os.environ.get("BEAM_ADMIN_EMAIL", "chanducharan2030@gmail.com")
ADMIN_PASSWORD = os.environ.get("BEAM_ADMIN_PASSWORD", "Chandu2030")

BASE_DIR = "./media_work"
TEMP_FOLDER = f"{BASE_DIR}/temp_downloads"
OUTPUT_FOLDER = f"{BASE_DIR}/processed"
SHEET_INDEX = 1  # Second tab (Series_Pipeline)

VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv")

for p in [TEMP_FOLDER, OUTPUT_FOLDER]:
    os.makedirs(p, exist_ok=True)

LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
    "aus": "Assamese", "tel": "Telugu", "hin": "Hindi", "tam": "Tamil", "mal": "Malayalam",
    "kan": "Kannada", "ben": "Bengali", "pan": "Punjabi", "guj": "Gujarati", "mar": "Marathi",
    "ori": "Oriya", "eng": "English", "jpn": "Japanese", "kor": "Korean", "spa": "Spanish",
    "fra": "French", "deu": "German", "rus": "Russian", "zho": "Chinese", "ita": "Italian",
    "por": "Portuguese", "ara": "Arabic", "tur": "Turkish", "und": "English"
}

# ============================================================================
# GOOGLE SHEETS AUTH
# ============================================================================
print("=" * 60)
print("BEAM SERIES DOWNLOADER — STARTING")
print("=" * 60)

try:
    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("GOOGLE_SHEETS_JSON is missing.")
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[OK] Connected to Google Sheets API")
except Exception as auth_err:
    print(f"[CRITICAL] Auth failed: {auth_err}")
    raise

sheet = gc.open_by_key(SPREADSHEET_ID).get_worksheet(SHEET_INDEX)
all_rows = sheet.get_all_records()
headers = [h.strip() if isinstance(h, str) else h for h in sheet.row_values(1)]

try:
    tmdb_id_col = headers.index("TMDB_ID") + 1
    tmdb_name_col = headers.index("TMDB_NAME") + 1
    season_col = headers.index("Season") + 1
    ep_col = headers.index("Episode") + 1
    quality_col = headers.index("Quality") + 1
    link_col = headers.index("Input_Link") + 1
    type_col = headers.index("Link_Type") + 1
    status_col = headers.index("DOWNLOAD_STATUS") + 1
    final_link_col = headers.index("FINAL_LINK") + 1
    error_col = headers.index("Error") + 1
except ValueError as e:
    raise Exception(f"Missing column header: {e}. Found headers: {headers}")

# ============================================================================
# HELPERS
# ============================================================================

def get_episode_number(filename, fallback_ep=None):
    if not filename: return fallback_ep
    patterns = [r'[Ss]\d{1,2}\s*[Ee](\d{1,3})', r'(\d{1,2})[xX]\d{1,3}', r'[Ee][Pp]?(\d{1,3})', r'episode[\s._-]?(\d{1,3})']
    for p in patterns:
        m = re.search(p, filename, re.IGNORECASE)
        if m: return int(m.group(1))
    return fallback_ep

def get_season_number(filename, fallback_season=None):
    if not filename: return fallback_season
    m = re.search(r'[Ss](\d{1,2})\s*[Ee]\d{1,3}', filename)
    if m: return int(m.group(1))
    m = re.search(r'(\d{1,2})[xX]\d{1,3}', filename)
    if m: return int(m.group(1))
    return fallback_season

def parse_media_languages(file_path):
    try:
        media = MediaInfo.parse(str(file_path))
        langs = []
        for track in media.tracks:
            if track.track_type == "Audio":
                code = track.language if track.language else "en"
                langs.append(LANG_MAP.get(code.lower(), "English"))
        return list(dict.fromkeys(langs)) or ["English"]
    except Exception:
        return ["English"]

def clean_string_for_vidara(text):
    """Deep scrubs strings to avoid all filesystem, double-space, and API processing bugs."""
    if not text:
        return ""
    # 1. Strip out dots completely
    text = text.replace(".", "")
    # 2. Replace slashes with a clean hyphen
    text = text.replace("/", "-")
    # 3. Strip out any remaining weird characters
    text = re.sub(r'[:*?"<>|]', "", text)
    # 4. Collapse any double or triple spaces into a single clean space
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def build_filename(series_name, season, episode, quality, languages):
    clean_title = clean_string_for_vidara(series_name)
    short_langs = [l[:3] for l in languages]
    return f"{clean_title} S{int(season):02d} E{int(episode):02d} {quality} {' + '.join(short_langs)}"

def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        return res.json().get("result", {}).get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except:
        return "https://api.vidara.so/v1/upload/server"

_folder_id_cache = {}

def get_or_create_vidara_folder(tmdb_name, season_num):
    clean_tmdb_name = clean_string_for_vidara(tmdb_name)
    cache_key = (clean_tmdb_name, int(season_num))
    if cache_key in _folder_id_cache:
        return _folder_id_cache[cache_key]

    folder_name = f"{clean_tmdb_name} Season {int(season_num):02d}"
    print(f"    [FOLDER] Creating: '{folder_name}'")
    create_url = f"https://api.vidara.so/v1/folder/create?api_key={VIDARA_API_KEY}&name={requests.utils.quote(folder_name)}"

    try:
        res = requests.get(create_url, timeout=30).json()
        if res.get("status") == 200:
            fld_id = res["result"]["folder_id"]
            _folder_id_cache[cache_key] = fld_id
            print(f"    [FOLDER] Created — ID: {fld_id}")
            return fld_id
        else:
            print(f"    [FOLDER] Warning: {res}")
            return None
    except Exception as e:
        print(f"    [FOLDER] Error: {e}")
        return None

def upload_to_vidara(file_path, custom_name, folder_id=None):
    upload_server = fetch_vidara_upload_server()
    print(f"      Uploading: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    payload_fields = {
        "api_key": VIDARA_API_KEY,
        "file": (custom_name, open(file_path, "rb"), "video/mp4")
    }
    if folder_id:
        payload_fields["fld_id"] = str(folder_id)
        payload_fields["folder_id"] = str(folder_id)

    encoder = MultipartEncoder(fields=payload_fields)
    monitor = MultipartEncoderMonitor(encoder)
    response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
    encoder.fields["file"][1].close()

    if response.status_code == 200:
        data = response.json()
        return data.get("filecode") or data.get("url") or data.get("result", {}).get("url")
    else:
        raise Exception(f"Vidara upload failed: {response.status_code}")

def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=30)
    res.raise_for_status()
    return res.json()["token"]

def beam_upsert(jwt, tmdb_id, season, episode, quality, languages, url):
    res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
        "content_type": "episode",
        "tmdb_id": int(tmdb_id),
        "season": int(season),
        "episode": int(episode),
        "url": url,
        "quality": quality,
        "audio_languages": languages
    }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)
    res.raise_for_status()
    return res.json()

def download_file(url, dest_path):
    cmd = ["aria2c", "-x", "16", "-s", "16", "-k", "1M", "--file-allocation=none", "--summary-interval=0", "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    try:
        if os.path.exists(dest_path): os.remove(dest_path)
        with requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"   [ERROR] Download failed: {e}")
        return False

# ============================================================================
# MAIN PIPELINE
# ============================================================================
print(f"\nProcessing {len(all_rows)} rows from Series_Pipeline...\n")
jwt = beam_login()

for idx, row in enumerate(all_rows):
    row_idx = idx + 2
    if str(row.get("DOWNLOAD_STATUS", "")).strip() != "":
        continue

    tmdb_id = str(row.get("TMDB_ID", "")).strip()
    tmdb_name = str(row.get("TMDB_NAME", "")).strip()
    season = str(row.get("Season", "")).strip()
    episode = row.get("Episode", 0)
    quality = str(row.get("Quality", "")).strip()
    input_link = str(row.get("Input_Link", "")).strip()
    link_type = str(row.get("Link_Type", "")).upper().strip()

    if not input_link or not tmdb_id or not quality:
        continue

    print(f"\n{'='*60}\nRow {row_idx}: {tmdb_name} — S{season} — {link_type}\n{'='*60}")

    try:
        if link_type == "ZIP":
            zip_path = os.path.join(TEMP_FOLDER, f"bundle_{row_idx}.zip")
            if not download_file(input_link, zip_path) or not zipfile.is_zipfile(zip_path):
                raise Exception("Invalid ZIP download")

            with zipfile.ZipFile(zip_path, 'r') as archive:
                video_files = [f for f in archive.infolist() if f.filename.lower().endswith(VIDEO_EXTENSIONS) and not os.path.basename(f.filename).startswith('.')]
                all_urls = []

                for vf in video_files:
                    extracted_path = archive.extract(vf, OUTPUT_FOLDER)
                    ep_num = get_episode_number(vf.filename, episode if episode else 1)
                    season_num = get_season_number(vf.filename, season)
                    languages = parse_media_languages(extracted_path)

                    clean_name = build_filename(tmdb_name, season_num, ep_num, quality, languages)
                    final_path = os.path.join(OUTPUT_FOLDER, clean_name)
                    shutil.move(extracted_path, final_path)

                    folder_id = get_or_create_vidara_folder(tmdb_name, season_num)
                    vidara_url = upload_to_vidara(final_path, clean_name, folder_id)
                    beam_upsert(jwt, tmdb_id, season_num, ep_num, quality, languages, vidara_url)
                    all_urls.append(vidara_url)

                    if os.path.exists(final_path): os.remove(final_path)

            sheet.update_cell(row_idx, final_link_col, "\n".join(all_urls))
            sheet.update_cell(row_idx, status_col, "Done")
            sheet.update_cell(row_idx, error_col, "")
            if os.path.exists(zip_path): os.remove(zip_path)

        elif link_type == "SINGLE":
            original_name = os.path.basename(input_link.split('?')[0]) or f"ep_{row_idx}.mkv"
            temp_path = os.path.join(TEMP_FOLDER, original_name)

            if not download_file(input_link, temp_path):
                raise Exception("Download failed")

            ep_num = get_episode_number(original_name, episode if episode else 1)
            season_num = get_season_number(original_name, season)
            languages = parse_media_languages(temp_path)

            clean_name = build_filename(tmdb_name, season_num, ep_num, quality, languages)
            final_path = os.path.join(OUTPUT_FOLDER, clean_name)
            shutil.move(temp_path, final_path)

            folder_id = get_or_create_vidara_folder(tmdb_name, season_num)
            vidara_url = upload_to_vidara(final_path, clean_name, folder_id)
            beam_upsert(jwt, tmdb_id, season_num, ep_num, quality, languages, vidara_url)

            sheet.update_cell(row_idx, final_link_col, vidara_url)
            sheet.update_cell(row_idx, status_col, "Done")
            sheet.update_cell(row_idx, error_col, "")
            if os.path.exists(final_path): os.remove(final_path)

    except Exception as e:
        print(f"   [ERROR] {e}")
        sheet.update_cell(row_idx, status_col, "Failed")
        sheet.update_cell(row_idx, error_col, str(e)[:500])
        for p in [locals().get('temp_path'), locals().get('zip_path'), locals().get('final_path')]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass

print(f"\n{'='*60}\nSERIES PIPELINE COMPLETE\n{'='*60}")
try: shutil.rmtree(BASE_DIR)
except: pass
