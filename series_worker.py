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
import asyncio
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
from pymediainfo import MediaInfo
from requests_toolbelt.multipart.encoder import MultipartEncoder, MultipartEncoderMonitor
from telethon import TelegramClient
from telethon.sessions import StringSession

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

# --- TELEGRAM SECURE SETTING INDICES ---
TG_API_ID = int(os.environ.get("TG_API_ID", 39631214))
TG_API_HASH = os.environ.get("TG_API_HASH", "341da0c5a267f02ccc36efe6582049e6").strip()
TG_CHANNEL_ID = int(os.environ.get("TG_CHANNEL_ID", -1003955675609))
TG_SESSION_STRING = os.environ.get("TG_SESSION_STRING", "1BVtsOKIBuyd2azy_Bxc7MjrHCDqkHzz5efA_jfCJgNL1_aOTcNyV2psFZWq58OdTntp7ALXuS6i1AudE96qvNfHsWYUK0VTSf-_0LEHEHvS7Qp4uSxUNDbXzVi5pnKikjNA7Rv7yH79WxpnzDuuRsMfVqs47rbx0h_xYSon3PK9mK6CRyLUKci9ywHrq5Xifp18ZPXXsbPAsmpoY4aJDElXU_LHWPwoK0Yo_VZ4TECeWuhcOwiK18mE0gzJaqWQE9vsxMNaufUxGgwaPuKDOlwmYuptsq5XdnRTChtlBcmAclEeWPcZNniJcKD-EOgaI5BONLdw8WL6rRxx53PukvGUEBMqmki4=").strip()

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

# Global tracking directory cache to prevent redundant API queries per thread execution
folder_id_cache = {}

# Initialize Telethon Client Context
tg_client = TelegramClient(StringSession(TG_SESSION_STRING), TG_API_ID, TG_API_HASH)

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

def get_or_create_vidara_folder(tmdb_id, tmdb_name, season_num):
    cache_key = (str(tmdb_id).strip(), int(season_num))
    with lock:
        if cache_key in folder_id_cache:
            return folder_id_cache[cache_key]
            
    folder_name = f"{tmdb_name.strip()} Season {int(season_num):02d}"
    print(f"[FOLDER SELECTION] Verifying/Creating remote storage structure: '{folder_name}'")
    create_folder_url = f"https://api.vidara.so/v1/folder/create?api_key={API_KEY}&name={requests.utils.quote(folder_name)}"
    
    try:
        res = requests.get(create_folder_url, timeout=30).json()
        if res.get("status") == 200:
            fld_id = res["result"]["folder_id"]
            with lock:
                folder_id_cache[cache_key] = fld_id
            print(f"-> [SUCCESS] Target operational root directory allocated with unique ID: {fld_id}")
            return fld_id
        else:
            print(f"-> [WARNING] Remote api failed generating folder structural indices, fallback to root context. Response: {res}")
            return None
    except Exception as err:
        print(f"-> [WARNING] Network transaction fault occurred creating cloud storage mapping nodes: {err}")
        return None

def synchronize_cms_database(tmdb_id, season_num, episode_num, file_url, languages):
    print(f"\n======================================================================")
    print(f"STARTING BULLETPROOF SERIES INGESTION FOR TMDB: {tmdb_id}")
    print(f"TARGETING: S{season_num:02d} E{episode_num:02d}")
    print(f"======================================================================")
    
    admin_headers = {
        "x-admin-key": ADMIN_KEY,
        "Content-Type": "application/json"
    }
    
    try:
        print("[STEP 1] Running Master TMDB Import / Verification...")
        series_res = requests.post(
            f"{ADMIN_BASE_URL}/admin/import/series",
            headers=admin_headers,
            json={"tmdb_id": int(tmdb_id)},
            timeout=30
        )

        if series_res.status_code == 409:
            print("-> [INFO] Series already initialized in database.")
        else:
            series_res.raise_for_status()
            print("-> [SUCCESS] New series imported successfully.")

        print(f"[STEP 2] Fetching internal layout mapping indices via TMDB ID...")
        seasons_res = requests.get(
            f"{ADMIN_BASE_URL}/series/{tmdb_id}/seasons",
            headers=admin_headers,
            timeout=30
        )
        seasons_res.raise_for_status()
        seasons_list = seasons_res.json()

        if not seasons_list or not isinstance(seasons_list, list):
            print(f"-> [ERROR] Failed to fetch a valid season mapping schema: {seasons_list}")
            return False

        internal_series_id = seasons_list[0].get("series_id")
        internal_season_id = None

        for target_season in seasons_list:
            s_num = target_season.get("season_number") or target_season.get("seasonNumber")
            if s_num == int(season_num):
                internal_season_id = target_season.get("id")
                break

        if not internal_series_id or not internal_season_id:
            print("-> [ERROR] Could not isolate internal relational DB IDs.")
            return False

        print(f"-> [SUCCESS] Series DB ID: [{internal_series_id}] | Season DB ID: [{internal_season_id}]")

        print(f"[STEP 3] Synchronizing season tracks via Internal Series ID [{internal_series_id}]...")
        sync_res = requests.post(
            f"{ADMIN_BASE_URL}/admin/series/{internal_series_id}/sync-season/{int(season_num)}",
            headers=admin_headers,
            timeout=45
        )

        if sync_res.status_code == 409:
            print("-> [INFO] Season structural metadata matches existing records.")
        else:
            sync_res.raise_for_status()
            print("-> [SUCCESS] Worker season-to-episode mapping synchronized.")

        print(f"[STEP 4] Fetching current episode table list for Season ID [{internal_season_id}]...")
        episodes_res = requests.get(
            f"{ADMIN_BASE_URL}/seasons/{internal_season_id}/episodes",
            headers=admin_headers,
            timeout=30
        )
        episodes_res.raise_for_status()
        episodes_list = episodes_res.json()

        internal_episode_id = None
        for ep in episodes_list:
            ep_num = ep.get("episode_number") or ep.get("episodeNumber")
            if ep_num == int(episode_num):
                internal_episode_id = ep.get("id")
                break

        if not internal_episode_id:
            print(f"-> [ERROR] Target episode tracking index missing for Episode {episode_num}.")
            return False

        print(f"-> [SUCCESS] Target Episode localized. Internal Episode Table ID: {internal_episode_id}")

        print(f"[STEP 5] Injecting stream content payload into Episode node: {internal_episode_id}...")
        link_payload = {
            "url": file_url,
            "quality": "1080p",
            "audio_languages": json.dumps(languages),
            "has_subtitles": 1
        }

        link_res = requests.post(
            f"{ADMIN_BASE_URL}/admin/episode/{internal_episode_id}/link",
            headers=admin_headers,
            json=link_payload,
            timeout=30
        )

        if link_res.status_code in [200, 201]:
            print(f"🎉 SERIES EPISODE LINKED SUCCESSFULLY TO CMS! ID: {internal_episode_id}")
            return True
        else:
            print(f"-> [ERROR] Failed link injection call: {link_res.status_code} - {link_res.text}")
            return False

    except Exception as exc:
        print(f"-> [CRITICAL RUNTIME ERROR] Pipeline sync step trace failure: {exc}")
        return False

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
        target_fld_id = task.get("folder_id")

        with lock:
            print(f"[UPLOADER-{worker_id}] Pushing target stream: {custom_name} into Folder ID: {target_fld_id}")

        attempts, success = 0, False
        while attempts < 3:
            try:
                payload_fields = {
                    "api_key": API_KEY, 
                    "file": (custom_name, open(file_path, "rb"), "video/mp4")
                }
                
                if target_fld_id:
                    payload_fields["fld_id"] = str(target_fld_id)
                    payload_fields["folder_id"] = str(target_fld_id)
                
                encoder = MultipartEncoder(fields=payload_fields)
                monitor = MultipartEncoderMonitor(encoder)
                res = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
                encoder.fields["file"][1].close()

                if res.status_code == 200:
                    data = res.json()
                    final_url = data.get("filecode") or data.get("url") or data.get("result", {}).get("url")
                    if final_url:
                        db_synced = synchronize_cms_database(tmdb_id, season_num, episode_num, final_url, languages)
                        
                        if db_synced:
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
# MAXIMUM SPEED PARALLEL TELEGRAM DOWNLOAD ENGINE (FAULT-TOLERANT)
# ==============================================================================
def telegram_progress_callback(current, total):
    percentage = (current / total) * 100
    current_mb = current / (1024 * 1024)
    total_mb = total / (1024 * 1024)
    print(f"\r⚡ BLAZING DOWNLOAD: {percentage:.1f}% | {current_mb:.1f}/{total_mb:.1f} MB", end="")

async def download_parallel_part(client, input_location, offset, size):
    """Downloads a single isolated slice part of the file block over a dedicated connection pool."""
    return await client.download_file(input_location, offset=offset, limit=size)

async def fast_parallel_download(client, message, destination_path, concurrency=8):
    """Splits the asset file context into concurrent parts to download them simultaneously."""
    file_size = message.file.size
    part_size = 512 * 1024  # Optimal 512KB chunk alignment matrix configuration
    
    total_parts = (file_size + part_size - 1) // part_size
    input_location = message.media.document
    
    print(f"🚀 Initializing Parallel Multi-Stream Engine ({concurrency} concurrent pipes)...")
    
    # Pre-allocate complete byte array directly into memory buffer context
    file_bytes = bytearray(file_size)
    part_idx = 0
    current_downloaded = 0
    
    while part_idx < total_parts:
        tasks = []
        task_mappings = []
        
        for _ in range(concurrency):
            if part_idx >= total_parts:
                break
                
            offset = part_idx * part_size
            remaining_bytes = file_size - offset
            current_part_size = min(part_size, remaining_bytes)
            
            task = asyncio.create_task(download_parallel_part(client, input_location, offset, current_part_size))
            tasks.append(task)
            task_mappings.append((offset, current_part_size))
            part_idx += 1
            
        # Fire simultaneous parallel HTTP connection hooks via gather
        results = await asyncio.gather(*tasks)
        
        # Ingest binary streams straight into allocated pointers
        for chunk_data, (offset, chunk_len) in zip(results, task_mappings):
            file_bytes[offset:offset + chunk_len] = chunk_data
            current_downloaded += chunk_len
            
        telegram_progress_callback(current_downloaded, file_size)

    # Atomic write step flush directly to deployment space drive
    with open(destination_path, 'wb') as f:
        f.write(file_bytes)
        
    return destination_path

async def process_telegram_download(message_id, destination_folder):
    if not tg_client.is_connected():
        await tg_client.connect()
        
    max_retries = 5
    attempt = 0
    
    while attempt < max_retries:
        try:
            msg = await tg_client.get_messages(TG_CHANNEL_ID, ids=int(message_id))
            if not msg or not msg.media:
                print(f"\n❌ [TG ERROR] Message ID {message_id} contains no structural media files.")
                return None
                
            raw_name = getattr(msg.file, 'name', f"tg_episode_{message_id}.mkv")
            target_path = os.path.join(destination_folder, raw_name)
            
            print(f"\n🛰---- Stream connected. Fetching Telegram item: {raw_name} (Attempt {attempt+1}/{max_retries})")
            
            # Execute max speed parallel memory downloader
            await fast_parallel_download(tg_client, msg, target_path, concurrency=8)
            
            print(f"\n✅ Telegram direct high-speed file extraction finished.")
            return target_path

        except (asyncio.CancelledError, Exception) as e:
            attempt += 1
            print(f"\n⚠️ [NET STALL/CANCELLED] Mid-download glitch handled: {e}")
            if attempt < max_retries:
                print(f"🔄 Hard resetting network session sockets in 6 seconds...")
                await asyncio.sleep(6)
                try:
                    await tg_client.disconnect()
                    await tg_client.connect()
                except Exception:
                    pass
            else:
                print(f"\n❌ Max download retries exhausted for Message ID: {message_id}")
                return None

def run_tg_download(message_id, destination_folder):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
    if loop.is_running():
        task = loop.create_task(process_telegram_download(message_id, destination_folder))
        while not task.done():
            time.sleep(0.2)
        return task.result()
    else:
        return loop.run_until_complete(process_telegram_download(message_id, destination_folder))

# ==============================================================================
# PIPELINE INGESTION LOGIC LOOP
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
                
                final_s = s_extracted if s_extracted is not None else int(season)
                final_e = e_extracted if e_extracted is not None else 1
                
                active_folder_id = get_or_create_vidara_folder(tmdb_id, tmdb_name, final_s)
                
                langs = parse_media_languages(extracted_path)
                short_langs = [l[:3] for l in langs]
                renamed_filename = f"{tmdb_name} S{final_s:02d} E{final_e:02d} {' '.join(short_langs)}{Path(extracted_path).suffix}"

                upload_queue.put({
                    "path": extracted_path, "row_idx": row_idx, "tmdb_id": tmdb_id,
                    "season": final_s, "episode": final_e, "filename": renamed_filename, 
                    "langs": langs, "folder_id": active_folder_id
                })

        try: os.remove(target_zip_path)
        except Exception: pass

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
            final_s = int(season)
            active_folder_id = get_or_create_vidara_folder(tmdb_id, tmdb_name, final_s)
            
            langs = parse_media_languages(local_target_path)
            short_langs = [l[:3] for l in langs]
            
            renamed_filename = f"{tmdb_name} S{final_s:02d} E{int(episode):02d} {' '.join(short_langs)}{Path(local_target_path).suffix}"
            final_moved_path = os.path.join(OUTPUT_FOLDER, renamed_filename)
            shutil.move(local_target_path, final_moved_path)

            upload_queue.put({
                "path": final_moved_path, "row_idx": row_idx, "tmdb_id": tmdb_id,
                "season": final_s, "episode": int(episode), "filename": renamed_filename, 
                "langs": langs, "folder_id": active_folder_id
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

    elif link_type == "TELEGRAM":
        # Process targeted message ID using the secure parallel engine
        local_target_path = run_tg_download(input_link, TEMP_FOLDER)

        if local_target_path and os.path.exists(local_target_path):
            final_s = int(season)
            active_folder_id = get_or_create_vidara_folder(tmdb_id, tmdb_name, final_s)
            
            langs = parse_media_languages(local_target_path)
            short_langs = [l[:3] for l in langs]
            
            renamed_filename = f"{tmdb_name} S{final_s:02d} E{int(episode):02d} {' '.join(short_langs)}{Path(local_target_path).suffix}"
            final_moved_path = os.path.join(OUTPUT_FOLDER, renamed_filename)
            shutil.move(local_target_path, final_moved_path)

            upload_queue.put({
                "path": final_moved_path, "row_idx": row_idx, "tmdb_id": tmdb_id,
                "season": final_s, "episode": int(episode), "filename": renamed_filename, 
                "langs": langs, "folder_id": active_folder_id
            })
            
            upload_queue.join()
            
            links_pushed = uploaded_links_tracker.get(row_idx, [])
            if links_pushed:
                sheet.update_cell(row_idx, final_link_col, links_pushed[0])
                sheet.update_cell(row_idx, status_col, "Done")
                sheet.update_cell(row_idx, error_col + 1, "")
            else:
                sheet.update_cell(row_idx, status_col, "Failed")
                sheet.update_cell(row_idx, error_col + 1, "Vidara upload failed for the processed Telegram asset.")
        else:
            sheet.update_cell(row_idx, status_col, "Failed")
            sheet.update_cell(row_idx, error_col + 1, f"Telegram client failed tracking or pulling Message ID: {input_link}")

# Clean up backgrounds
for _ in range(MAX_CONCURRENT_UPLOADS):
    upload_queue.put(None)
for t in threads:
    t.join()

try: shutil.rmtree(BASE_DIR)
except Exception: pass
print("\n[COMPLETE] Pipeline processing loop finalized successfully.")
