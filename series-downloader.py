#!/usr/bin/env python3
"""
BEAM Series Downloader — GitHub Actions Pipeline
=================================================
Reads Google Sheet (Series_Pipeline tab) → downloads episodes → detects audio languages →
renames files → uploads to Vidara → calls BEAM Worker upsert API → writes URLs back to sheet.

Supports:
  - SINGLE: one link = one episode (episode number from sheet or detected from filename)
  - ZIP: download zip, extract, process each video file (episode number detected from filename)

Sheet columns (Series_Pipeline tab):
  A: TMDB_ID
  B: TMDB_NAME
  C: Season
  D: Episode        (0 for ZIP = detect from filename inside zip)
  E: Quality
  F: Input_Link
  G: Link_Type       (SINGLE or ZIP)
  H: DOWNLOAD_STATUS (blank=pending, Done, Failed)
  I: FINAL_LINK      (Vidara URL — for SINGLE: single URL, for ZIP: newline-separated URLs)
  J: Error

Setup:
  GitHub Secrets needed:
    GOOGLE_SHEETS_JSON  — service account JSON
    SPREADSHEET_ID      — Google Sheet ID
    VIDARA_API_KEY      — Vidara API key
    BEAM_ADMIN_EMAIL    — beam-worker admin email
    BEAM_ADMIN_PASSWORD — beam-worker admin password
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
headers = sheet.row_values(1)
# Strip whitespace from headers
headers = [h.strip() if isinstance(h, str) else h for h in headers]

# Column indices (1-based)
try:
    tmdb_id_col = headers.index("TMDB_ID") + 1         # A
    tmdb_name_col = headers.index("TMDB_NAME") + 1     # B
    season_col = headers.index("Season") + 1           # C
    ep_col = headers.index("Episode") + 1              # D
    quality_col = headers.index("Quality") + 1         # E
    link_col = headers.index("Input_Link") + 1         # F
    type_col = headers.index("Link_Type") + 1          # G
    status_col = headers.index("DOWNLOAD_STATUS") + 1  # H
    final_link_col = headers.index("FINAL_LINK") + 1   # I
    error_col = headers.index("Error") + 1             # J
except ValueError as e:
    raise Exception(f"Missing column header: {e}. Found headers: {headers}")

# ============================================================================
# HELPERS
# ============================================================================

def get_episode_number(filename, fallback_ep=None):
    """Extract episode number from filename."""
    if not filename:
        return fallback_ep
    patterns = [
        r'[Ss]\d{1,2}[Ee](\d{1,3})',         # S01E05
        r'(\d{1,2})[xX]\d{1,3}',              # 1x05
        r'[Ee][Pp]?(\d{1,3})',                # E05, EP05
        r'episode[\s._-]?(\d{1,3})',          # Episode 5
    ]
    for p in patterns:
        m = re.search(p, filename, re.IGNORECASE)
        if m:
            return int(m.group(1))
    return fallback_ep

def get_season_number(filename, fallback_season=None):
    """Extract season number from filename."""
    if not filename:
        return fallback_season
    m = re.search(r'[Ss](\d{1,2})[Ee]\d{1,3}', filename)
    if m:
        return int(m.group(1))
    m = re.search(r'(\d{1,2})[xX]\d{1,3}', filename)
    if m:
        return int(m.group(1))
    return fallback_season

def parse_media_languages(file_path):
    """Detect audio languages from file using MediaInfo."""
    try:
        media = MediaInfo.parse(str(file_path))
        langs = []
        for track in media.tracks:
            if track.track_type == "Audio":
                code = track.language if track.language else "en"
                mapped = LANG_MAP.get(code.lower(), "English")
                langs.append(mapped)
        return list(dict.fromkeys(langs)) or ["English"]
    except Exception:
        return ["English"]

def build_filename(series_name, season, episode, quality, languages):
    """Build clean filename: Series S01E05 1080p Eng + Tel (no extension — Vidara works without it)"""
    short_langs = [l[:3] for l in languages]
    return f"{series_name} S{int(season):02d}E{int(episode):02d} {quality} {' + '.join(short_langs)}"

def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except:
        return "https://api.vidara.so/v1/upload/server"

def upload_to_vidara(file_path, custom_name):
    """Upload file to Vidara, return filecode/URL."""
    upload_server = fetch_vidara_upload_server()
    print(f"      Uploading: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")

    encoder = MultipartEncoder(fields={
        "api_key": VIDARA_API_KEY,
        "file": (custom_name, open(file_path, "rb"), "video/mp4")
    })
    monitor = MultipartEncoderMonitor(encoder)
    response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
    encoder.fields["file"][1].close()

    if response.status_code == 200:
        data = response.json()
        return data.get("filecode") or data.get("url") or data.get("result", {}).get("url")
    else:
        raise Exception(f"Vidara upload failed: {response.status_code}")

def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]

def beam_upsert(jwt, tmdb_id, season, episode, quality, languages, url):
    """Call BEAM worker upsert endpoint for episodes."""
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
    """Download using aria2c, fallback to requests."""
    cmd = [
        "aria2c", "-x", "16", "-s", "16", "-k", "1M",
        "--file-allocation=none", "--summary-interval=0",
        "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    print("   [WARN] aria2c failed, trying direct stream...")
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        headers = {"User-Agent": "Mozilla/5.0"}
        with requests.get(url, headers=headers, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"   [ERROR] Download failed: {e}")
        return False

# ============================================================================
# MAIN PIPELINE
# ============================================================================
print(f"\nProcessing {len(all_rows)} rows from Series_Pipeline...\n")

jwt = beam_login()
print("[OK] Logged into BEAM worker\n")

for idx, row in enumerate(all_rows):
    row_idx = idx + 2

    # ONLY process rows where DOWNLOAD_STATUS is blank — skip Done, Failed, and any other value
    status = str(row.get("DOWNLOAD_STATUS", "")).strip()
    if status.strip() != "":
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

    print(f"\n{'='*60}")
    print(f"Row {row_idx}: {tmdb_name} — S{season} — {quality} — {link_type}")
    print(f"{'='*60}")

    try:
        if link_type == "ZIP":
            # ── ZIP: download, extract, process each video file ──
            print(f"   Downloading ZIP from: {input_link[:80]}...")
            zip_path = os.path.join(TEMP_FOLDER, f"bundle_{row_idx}.zip")
            if not download_file(input_link, zip_path):
                raise Exception("ZIP download failed")

            if not zipfile.is_zipfile(zip_path):
                raise Exception("Downloaded file is not a valid ZIP")

            with zipfile.ZipFile(zip_path, 'r') as archive:
                video_files = [f for f in archive.infolist()
                               if f.filename.lower().endswith(VIDEO_EXTENSIONS)
                               and not os.path.basename(f.filename).startswith('.')]

                print(f"   Found {len(video_files)} video files in ZIP")
                all_urls = []

                for vf in video_files:
                    print(f"\n   Processing: {vf.filename}")
                    extracted_path = archive.extract(vf, OUTPUT_FOLDER)

                    # Detect episode number from filename
                    ep_num = get_episode_number(vf.filename)
                    if not ep_num:
                        ep_num = episode if episode else 1
                    season_num = get_season_number(vf.filename, season)

                    # Detect languages
                    languages = parse_media_languages(extracted_path)
                    print(f"      Episode: S{season_num:02d}E{ep_num:02d}, Languages: {languages}")

                    # Rename
                    clean_name = build_filename(tmdb_name, season_num, ep_num, quality, languages)
                    final_path = os.path.join(OUTPUT_FOLDER, clean_name)
                    shutil.move(extracted_path, final_path)
                    print(f"      Renamed: {clean_name}")

                    # Upload
                    vidara_url = upload_to_vidara(final_path, clean_name)
                    print(f"      Vidara URL: {vidara_url}")

                    # Upsert to DB
                    result = beam_upsert(jwt, tmdb_id, season_num, ep_num, quality, languages, vidara_url)
                    print(f"      DB: {result.get('action')} (id: {result.get('id')})")

                    all_urls.append(vidara_url)

                    # Cleanup
                    if os.path.exists(final_path):
                        os.remove(final_path)

            # Write all URLs to sheet
            sheet.update_cell(row_idx, final_link_col, "\n".join(all_urls))
            sheet.update_cell(row_idx, status_col, "Done")
            sheet.update_cell(row_idx, error_col, "")

            # Cleanup ZIP
            if os.path.exists(zip_path):
                os.remove(zip_path)

            print(f"\n   ✅ {len(all_urls)} episodes processed")

        elif link_type == "SINGLE":
            # ── SINGLE: one link = one episode ──
            print(f"   Downloading from: {input_link[:80]}...")
            original_name = os.path.basename(input_link.split('?')[0]) or f"ep_{row_idx}.mkv"
            temp_path = os.path.join(TEMP_FOLDER, original_name)

            if not download_file(input_link, temp_path):
                raise Exception("Download failed")

            # Detect episode number
            ep_num = get_episode_number(original_name, episode if episode else 1)
            season_num = get_season_number(original_name, season)

            # Detect languages
            languages = parse_media_languages(temp_path)
            print(f"   Episode: S{season_num:02d}E{ep_num:02d}, Languages: {languages}")

            # Rename
            clean_name = build_filename(tmdb_name, season_num, ep_num, quality, languages)
            final_path = os.path.join(OUTPUT_FOLDER, clean_name)
            shutil.move(temp_path, final_path)
            print(f"   Renamed: {clean_name}")

            # Upload
            vidara_url = upload_to_vidara(final_path, clean_name)
            print(f"   Vidara URL: {vidara_url}")

            # Upsert to DB
            result = beam_upsert(jwt, tmdb_id, season_num, ep_num, quality, languages, vidara_url)
            print(f"   DB: {result.get('action')} (id: {result.get('id')})")

            # Write to sheet
            sheet.update_cell(row_idx, final_link_col, vidara_url)
            sheet.update_cell(row_idx, status_col, "Done")
            sheet.update_cell(row_idx, error_col, "")

            # Cleanup
            if os.path.exists(final_path):
                os.remove(final_path)

        else:
            sheet.update_cell(row_idx, status_col, "Failed")
            sheet.update_cell(row_idx, error_col, f"Unknown Link_Type: {link_type}")

    except Exception as e:
        print(f"   [ERROR] {e}")
        sheet.update_cell(row_idx, status_col, "Failed")
        sheet.update_cell(row_idx, error_col, str(e)[:500])

        # Cleanup any leftover temp files
        for p in [locals().get('temp_path'), locals().get('zip_path'), locals().get('final_path')]:
            if p and os.path.exists(p):
                try: os.remove(p)
                except: pass

print(f"\n{'='*60}")
print("SERIES PIPELINE COMPLETE")
print(f"{'='*60}")

# Cleanup
try:
    shutil.rmtree(BASE_DIR)
except:
    pass
