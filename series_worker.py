import os
import re
import json
import shutil
import requests
import queue
import zipfile
import threading
import time
import subprocess
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from pymediainfo import MediaInfo
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor

print("=" * 60)
print("INITIALIZING WORKER CONTAINER ENVIRONMENT (SERIES ENGINE)")
print("=" * 60)

# ==============================================================================
# ENVIRONMENT AUTHENTICATION & CONFIGURATION
# ==============================================================================
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

try:
    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("Critical Secret 'GOOGLE_SHEETS_JSON' is missing.")
        
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[SUCCESS] Connected to Google Sheets API securely.")
except Exception as auth_err:
    print(f"[CRITICAL ERROR] Failed to load workspace credentials token: {auth_err}")
    raise

API_KEY = os.environ.get("VIDARA_API_KEY", "de57ed8e0bd00f3c0c18db283f5377caf14ad141ffb74ee49f83cb5ed13ab9dc").strip()
RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "1KYxYMv9hOKGvfKNpH2SnZ9cXrNMISuVcnhZ7__EgBac")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()
ADMIN_KEY = os.environ.get("ADMIN_KEY", "*Chandu2030#@").strip()

BASE_DIR = "./media_work"
TEMP_FOLDER = f"{BASE_DIR}/temp_downloads"
OUTPUT_FOLDER = f"{BASE_DIR}/processed"
SHEET_INDEX = 1  # TARGETS SECOND TAB: Series_Pipeline

ADMIN_BASE_URL = "https://streamio-api.usestoreme.workers.dev"
MAX_CONCURRENT_UPLOADS = 3
VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv")

for p in [TEMP_FOLDER, OUTPUT_FOLDER]:
    os.makedirs(p, exist_ok=True)

lang_map = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian", 
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish", "fr-ca": "French"
}

upload_queue = queue.Queue()
uploaded_links_tracker = {} # Maps row_idx -> list of generated final links
lock = threading.Lock()

# ==============================================================================
# SPREADSHEET INGESTION & HEADER MAPPING
# ==============================================================================
try:
    sheet = gc.open_by_key(SPREADSHEET_ID).get_worksheet(SHEET_INDEX)
    all_rows = sheet.get_all_records()
    headers = sheet.row_values(1)
except Exception as e:
    raise Exception(f"Failed pulling Series_Pipeline data structure: {e}")

try:
    tmdb_id_col = headers.index("TMDB_ID") + 1
    tmdb_name_col = headers.index("TMDB_NAME") + 1
    season_col = headers.index("Season") + 1
    ep_col = headers.index("Episode") + 1
    dl_link_col = headers.index("Input_Link") + 1
    type_col = headers.index("Link_Type") + 1
    status_col = headers.index("DOWNLOAD_STATUS") + 1
    final_link_col = headers.index("FINAL_LINK") + 1
    error_col = headers.index("Error")
except ValueError as e:
    raise Exception(f"Missing required pipeline structural columns: {e}")

# ==============================================================================
# PARSING & DATABASE INTEGRATION ENGINE
# ==============================================================================
def get_episode_code(name):
    n = name.lower()
    if m := re.search(r"s(\d{1,2})\s*e(\d{1,2})", n): return int(m.group(1)), int(m.group(2))
    if m := re.search(r"season\s*(\d{1,2}).*episode\s*(\d{1,2})", n): return int(m.group(1)), int(m.group(2))
    if m := re.search(r"(\d{1,2})\s*x\s*(\d{1,2})", n): return int(m.group(1)), int(m.group(2))
    if m := re.search(r"episode\s*(\d{1,3})", n): return 1, int(m.group(1))
    return None, None

def parse_media_languages(file_path):
    try:
        media = MediaInfo.parse(str(file_path))
        langs = []
        for track in media.tracks:
            if track.track_type == "Audio":
                code = track.language if track.language else "en"
                langs.append(lang_map.get(code.lower(), "English"))
        return list(dict.fromkeys(langs)) or ["English"]
    except Exception:
        return ["English"]

def fetch_active_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": API_KEY}, timeout=20)
        return res.json()["result"]["upload_server"]
    except Exception:
        return "https://api.vidara.so/v1/upload/server"

upload_server = fetch_active_upload_server()

def synchronize_cms_database(tmdb_id, season_num, episode_num, file_url, languages):
    """
    Automates layout discovery, initializes series/seasons when missing,
    and attaches links with standardized metadata.
    """
    print(f"\n======================================================================")
    print(f"STARTING BULLETPROOF SERIES INGESTION FOR TMDB: {tmdb_id}")
    print(f"TARGETING: S{season_num:02d}E{episode_num:02d}")
    
    admin_headers = {"x-admin-key": ADMIN_KEY, "Content-Type": "application/json"}
    
    # STEP 1: Core Content Ingestion Handshake
    print("[STEP 1] Running Master TMDB Import / Verification...")
    import_res = requests.post(f"{ADMIN_BASE_URL}/admin/import/series", headers=admin_headers, json={"tmdb_id": int(tmdb_id)}, timeout=30)
    if import_res.status_code not in [200, 201]:
        print(f"-> [ERROR] Failed TMDB baseline verification. Code: {import_res.status_code}")
        return False

    # STEP 2: Database Structural Index Discovery
    print("[STEP 2] Fetching internal layout mapping indices via TMDB ID...")
    meta_res = requests.get(f"{ADMIN_BASE_URL}/admin/series/{tmdb_id}", headers=admin_headers, timeout=30)
    if meta_res.status_code != 200:
        return False
        
    series_data = meta_res.json()
    series_db_id = series_data.get("id") or series_data.get("result", {}).get("id")
    
    # Locate targeted execution structural components
    seasons_list = series_data.get("seasons", []) or series_data.get("result", {}).get("seasons", [])
    season_db_id = None
    for s in seasons_list:
        if s.get("season_number") == int(season_num):
            season_db_id = s.get("id")
            break
            
    if not season_db_id:
        print(f"-> [INFO] Season missing. Syncing container array...")
        sync_res = requests.post(f"{ADMIN_BASE_URL}/admin/series/{series_db_id}/sync-seasons", headers=admin_headers, timeout=30)
        if sync_res.status_code in [200, 201]:
            # Refetch updated internal metadata layout
            series_data = requests.get(f"{ADMIN_BASE_URL}/admin/series/{tmdb_id}", headers=admin_headers, timeout=30).json()
            seasons_list = series_data.get("seasons", []) or series_data.get("result", {}).get("seasons", [])
            for s in seasons_list:
                if s.get("season_number") == int(season_num):
                    season_db_id = s.get("id")
                    break

    if not series_db_id or not season_db_id:
        print("-> [CRITICAL] Internal structural map location failure.")
        return False

    print(f"-> [SUCCESS] Series DB ID: [{series_db_id}] | Season DB ID: [{season_db_id}]")

    # STEP 3: Epizodic Target Isolation
    episodes_res = requests.get(f"{ADMIN_BASE_URL}/admin/season/{season_db_id}/episodes", headers=admin_headers, timeout=30)
    if episodes_res.status_code != 200:
        return False
        
    ep_list = episodes_res.json() if isinstance(episodes_res.json(), list) else episodes_res.json().get("result", [])
    episode_db_id = None
    for ep in ep_list:
        if ep.get("episode_number") == int(episode_num):
            episode_db_id = ep.get("id")
            break

    if not episode_db_id:
        print(f"-> [WARN] Target Episode entry footprint unindexed on backend.")
        return False

    # STEP 4: Injection of Complete Payload
    short_tags = [lang[:3] for lang in languages]
    link_payload = {
        "url": file_url,
        "quality": "1080p",
        "audio_languages": json.dumps(short_tags),
        "has_subtitles": 1
    }
    
    link_res = requests.post(f"{ADMIN_BASE_URL}/admin/episode/{episode_db_id}/link", headers=admin_headers, json=link_payload, timeout=30)
    print(f"[STEP 4] Injecting remote storage pointers -> status code: {link_res.status_code}")
    return link_res.status_code in [200, 201]

# ==============================================================================
# MULTI-THREADED ASYNC UPLOAD DISPATCHER
# ==============================================================================
def upload_processor_worker(worker_id):
    global upload_server
    while True:
        task = upload_queue.get()
        if task is None:
            upload_queue.task_done()
            break

        file_path = task["path"]
        row_idx = task["row_idx"]
        tmdb_id = task["tmdb_id"]
        season_num = task["season"]
        episode_num = task["episode"]
        custom_name = task["filename"]
        languages = task["langs"]

        with lock:
            print(f"[UPLOADER-{worker_id}] Pushing target stream: {custom_name}")

        attempts, success = 0, False
        while attempts < 3:
            try:
                encoder = MultipartEncoder(fields={"api_key": API_KEY, "file": (custom_name, open(file_path, "rb"), "video/mp4")})
                monitor = MultipartEncoderMonitor(encoder)
                res = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
                encoder.fields["file"][1].close()

                if res.status_code == 200:
                    data = res.json()
                    final_url = data.get("filecode") or data.get("url") or data.get("result", {}).get("url")
                    if final_url:
                        # Process database injection mapping sequence
                        db_synced = synchronize_cms_database(tmdb_id, season_num, episode_num, final_url, languages)
                        
                        with lock:
                            if row_idx not in uploaded_links_tracker:
                                uploaded_links_tracker[row_idx] = []
                            uploaded_links_tracker[row_idx].append(final_url)
                        success = True
                        break

                if "disk_full" in res.text:
                    upload_server = fetch_active_upload_server()
                attempts += 1
            except Exception:
                attempts += 1
                time.sleep(2)

        if os.path.exists(file_path):
            try: os.remove(file_path)
            except Exception: pass
        upload_queue.task_done()

# Start background uploader daemons
threads = []
for i in range(MAX_CONCURRENT_UPLOADS):
    t = threading.Thread(target=upload_processor_worker, args=(i+1,), daemon=True)
    t.start()
    threads.append(t)

# ==============================================================================
# PIPELINE INGESTION LOGIC Loop
# ==============================================================================
for idx, row in enumerate(all_rows):
    row_idx = idx + 2
    status = str(row.get("DOWNLOAD_STATUS", "")).strip()
    link_type = str(row.get("Link_Type", "")).upper().strip()
    input_link = str(row.get("Input_Link", "")).strip()
    tmdb_id = str(row.get("TMDB_ID", "")).strip()
    tmdb_name = str(row.get("TMDB_NAME", "")).strip()
    season = str(row.get("Season", "")).strip()
    episode = str(row.get("Episode", "")).strip()

    if status.lower() == "done" or not input_link or not tmdb_id:
        continue

    print(f"\nProcessing active row [{row_idx}] Engine Model Type -> {link_type}")

    if link_type == "ZIP":
        zip_filename = f"bundle_{row_idx}.zip"
        target_zip_path = os.path.join(TEMP_FOLDER, zip_filename)
        
        cmd = ["aria2c", "-x", "16", "-s", "16", "-k", "1M", "--file-allocation=none", "-d", TEMP_FOLDER, "-o", zip_filename, input_link]
        download_res = subprocess.run(cmd)

        if download_res.returncode != 0 or not os.path.exists(target_zip_path) or not zipfile.is_zipfile(target_zip_path):
            sheet.update_cell(row_idx, status_col, "Failed")
            sheet.update_cell(row_idx, error_col + 1, "Failed downloading/extracting ZIP package structure.")
            continue

        with zipfile.ZipFile(target_zip_path, 'r') as archive:
            valid_targets = [f for f in archive.infolist() if f.filename.lower().endswith(VIDEO_EXTENSIONS) and not f.filename.split('/')[-1].startswith('.')]
            
            for file_info in valid_targets:
                while upload_queue.qsize() >= (MAX_CONCURRENT_UPLOADS * 2):
                    time.sleep(2)

                extracted_path = archive.extract(file_info, OUTPUT_FOLDER)
                s_extracted, e_extracted = get_episode_code(os.path.basename(extracted_path))
                
                # Use track codes extracted from filename fallback to row structural defaults
                final_s = s_extracted if s_extracted is not None else int(season)
                final_e = e_extracted if e_extracted is not None else 1
                
                langs = parse_media_languages(extracted_path)
                short_langs = [l[:3] for l in langs]
                renamed_filename = f"{tmdb_name} S{final_s:02d}E{final_e:02d} {' '.join(short_langs)}{Path(extracted_path).suffix}"

                upload_queue.put({
                    "path": extracted_path, "row_idx": row_idx, "tmdb_id": tmdb_id,
                    "season": final_s, "episode": final_e, "filename": renamed_filename, "langs": langs
                })

        try: os.remove(target_zip_path)
        except Exception: pass

        # Wait until current rows items drain to prevent index shifting on multi-batch tracks
        upload_queue.join()
        
        links_pushed = uploaded_links_tracker.get(row_idx, [])
        if links_pushed:
            sheet.update_cell(row_idx, final_link_col, "\n".join(links_pushed))
            sheet.update_cell(row_idx, status_col, "Done")
            sheet.update_cell(row_idx, error_col + 1, "")
        else:
            sheet.update_cell(row_idx, status_col, "Failed")

    elif link_type == "SINGLE":
        file_ext_match = input_link.split('?')[0]
        ext = os.path.basename(file_ext_match)
        if not ext or "." not in ext:
            ext = f"episode_{episode}_{int(time.time())}.mkv"

        cmd = ["aria2c", "-x", "16", "-s", "16", "-k", "1M", "--file-allocation=none", "-d", TEMP_FOLDER, "-o", ext, input_link]
        download_res = subprocess.run(cmd)
        local_target_path = os.path.join(TEMP_FOLDER, ext)

        if download_res.returncode == 0 and os.path.exists(local_target_path):
            langs = parse_media_languages(local_target_path)
            short_langs = [l[:3] for l in langs]
            
            renamed_filename = f"{tmdb_name} S{int(season):02d}E{int(episode):02d} {' '.join(short_langs)}{Path(local_target_path).suffix}"
            final_moved_path = os.path.join(OUTPUT_FOLDER, renamed_filename)
            shutil.move(local_target_path, final_moved_path)

            upload_queue.put({
                "path": final_moved_path, "row_idx": row_idx, "tmdb_id": tmdb_id,
                "season": int(season), "episode": int(episode), "filename": renamed_filename, "langs": langs
            })
            
            upload_queue.join()
            
            links_pushed = uploaded_links_tracker.get(row_idx, [])
            if links_pushed:
                sheet.update_cell(row_idx, final_link_col, links_pushed[0])
                sheet.update_cell(row_idx, status_col, "Done")
                sheet.update_cell(row_idx, error_col + 1, "")
            else:
                sheet.update_cell(row_idx, status_col, "Failed")
        else:
            sheet.update_cell(row_idx, status_col, "Failed")
            sheet.update_cell(row_idx, error_col + 1, "Aria2 engine dropped input stream download pointer.")

# Clean up backgrounds
for _ in range(MAX_CONCURRENT_UPLOADS):
    upload_queue.put(None)
for t in threads:
    t.join()

try: shutil.rmtree(BASE_DIR)
except Exception: pass
print("\n[COMPLETE] Pipeline processing loop finalized successfully.")
