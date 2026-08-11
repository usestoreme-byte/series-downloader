#!/usr/bin/env python3
"""
BEAM Series Downloader v3 — GitHub Actions Pipeline (Semaphore + Filecode Tracking)
=====================================================
"""

import os
import re
import json
import shutil
import requests
import subprocess
import zipfile
import time
import threading
import queue
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
IA_ACCESS_KEY = os.environ.get("IA_ACCESS_KEY", "EQ6XJ3AACbxfK4n7").strip()
IA_SECRET_KEY = os.environ.get("IA_SECRET_KEY", "BlzN7vT0uJo7g3n2").strip()

RAW_SPREADSHEET_ID = os.environ.get("SPREADSHEET_ID", "")
SPREADSHEET_ID = RAW_SPREADSHEET_ID.replace("'", "").replace('"', '').strip()

BEAM_WORKER_URL = "https://beamplay.beam-api.workers.dev"
ADMIN_EMAIL = os.environ.get("BEAM_ADMIN_EMAIL", "")
ADMIN_PASSWORD = os.environ.get("BEAM_ADMIN_PASSWORD", "")

BASE_DIR = "./media_work"
TEMP_FOLDER = f"{BASE_DIR}/temp_downloads"
OUTPUT_FOLDER = f"{BASE_DIR}/processed"

VIDEO_EXTENSIONS = (".mp4", ".mkv", ".mov", ".avi", ".webm", ".flv")

for p in [TEMP_FOLDER, OUTPUT_FOLDER]:
    os.makedirs(p, exist_ok=True)

LANG_MAP = {
    "as": "Assamese", "te": "Telugu", "hi": "Hindi", "ta": "Tamil", "ml": "Malayalam",
    "kn": "Kannada", "bn": "Bengali", "pa": "Punjabi", "gu": "Gujarati", "mr": "Marathi",
    "or": "Oriya", "en": "English", "ja": "Japanese", "ko": "Korean", "es": "Spanish",
    "fr": "French", "de": "German", "ru": "Russian", "zh": "Chinese", "it": "Italian",
    "pt": "Portuguese", "ar": "Arabic", "tr": "Turkish",
    "id": "Indonesian", "ms": "Malay", "th": "Thai", "vi": "Vietnamese", "tl": "Filipino",
    "he": "Hebrew", "fa": "Persian", "ur": "Urdu", "ne": "Nepali", "si": "Sinhala",
    "my": "Burmese", "km": "Khmer", "lo": "Lao", "mn": "Mongolian",
    "nl": "Dutch", "sv": "Swedish", "no": "Norwegian", "da": "Danish", "fi": "Finnish",
    "pl": "Polish", "cs": "Czech", "sk": "Slovak", "hu": "Hungarian", "ro": "Romanian",
    "el": "Greek", "uk": "Ukrainian", "bg": "Bulgarian", "hr": "Croatian", "sr": "Serbian",
    "sl": "Slovenian", "bs": "Bosnian", "mk": "Macedonian", "sq": "Albanian",
    "lt": "Lithuanian", "lv": "Latvian", "et": "Estonian", "is": "Icelandic",
    "ga": "Irish", "cy": "Welsh", "eu": "Basque", "ca": "Catalan", "gl": "Galician",
    "af": "Afrikaans", "zu": "Zulu", "xh": "Xhosa", "sw": "Swahili", "am": "Amharic",
    "so": "Somali", "ha": "Hausa", "yo": "Yoruba", "ig": "Igbo", "st": "Sotho",
    "ka": "Georgian", "hy": "Armenian", "az": "Azerbaijani", "kk": "Kazakh",
    "uz": "Uzbek", "ky": "Kyrgyz", "tg": "Tajik", "tk": "Turkmen", "ps": "Pashto",
    "ku": "Kurdish", "sd": "Sindhi", "bo": "Tibetan", "dz": "Dzongkha",
    "jv": "Javanese", "su": "Sundanese", "ceb": "Cebuano", "haw": "Hawaiian",
    "mi": "Maori", "sm": "Samoan", "to": "Tongan", "fj": "Fijian",
    "eo": "Esperanto", "la": "Latin", "yi": "Yiddish", "mt": "Maltese",
    "lb": "Luxembourgish", "fo": "Faroese", "gd": "Scottish Gaelic", "br": "Breton",
    "co": "Corsican", "oc": "Occitan", "rm": "Romansh", "gn": "Guarani",
    "qu": "Quechua", "ay": "Aymara", "ht": "Haitian Creole",
}

UNKNOWN_TOKENS = {"", "und", "unknown", "unk", "n/a", "none"}

ISO2_TO_ISO3 = {
    "as": "asm", "te": "tel", "hi": "hin", "ta": "tam", "ml": "mal",
    "kn": "kan", "bn": "ben", "pa": "pan", "gu": "guj", "mr": "mar",
    "or": "ori", "en": "eng", "ja": "jpn", "ko": "kor", "es": "spa",
    "fr": "fre", "de": "ger", "ru": "rus", "zh": "chi", "it": "ita",
    "pt": "por", "ar": "ara", "tr": "tur",
    "id": "ind", "ms": "may", "th": "tha", "vi": "vie", "tl": "fil",
    "he": "heb", "fa": "per", "ur": "urd", "ne": "nep", "si": "sin",
    "my": "bur", "km": "khm", "lo": "lao", "mn": "mon",
    "nl": "dut", "sv": "swe", "no": "nor", "da": "dan", "fi": "fin",
    "pl": "pol", "cs": "cze", "sk": "slo", "hu": "hun", "ro": "rum",
    "el": "gre", "uk": "ukr", "bg": "bul", "hr": "hrv", "sr": "srp",
    "sl": "slv", "bs": "bos", "mk": "mac", "sq": "alb",
    "lt": "lit", "lv": "lav", "et": "est", "is": "ice",
    "ga": "gle", "cy": "wel", "eu": "baq", "ca": "cat", "gl": "glg",
    "af": "afr", "zu": "zul", "xh": "xho", "sw": "swa", "am": "amh",
    "so": "som", "ha": "hau", "yo": "yor", "ig": "ibo", "st": "sot",
    "ka": "geo", "hy": "arm", "az": "aze", "kk": "kaz",
    "uz": "uzb", "ky": "kir", "tg": "tgk", "tk": "tuk", "ps": "pus",
    "ku": "kur", "sd": "snd", "bo": "tib", "dz": "dzo",
    "jv": "jav", "su": "sun", "ceb": "ceb", "haw": "haw",
    "mi": "mao", "sm": "smo", "to": "ton", "fj": "fij",
    "eo": "epo", "la": "lat", "yi": "yid", "mt": "mlt",
    "lb": "ltz", "fo": "fao", "gd": "gla", "br": "bre",
    "co": "cos", "oc": "oci", "rm": "roh", "gn": "grn",
    "qu": "que", "ay": "aym", "ht": "hat",
}

NAME_TO_ISO3 = {}
for _code2, _name in LANG_MAP.items():
    _iso3 = ISO2_TO_ISO3.get(_code2)
    if _iso3 and _name not in NAME_TO_ISO3:
        NAME_TO_ISO3[_name] = _iso3

def iso3_for_language(language_name):
    return NAME_TO_ISO3.get(language_name, "und")

SERIES_AUDIO_LANG_OVERRIDES = {
    "1399": {"is": "English"},
}

def normalize_audio_lang(raw_code, raw_name=None, override_map=None):
    code = (raw_code or "").strip().lower()
    if override_map and code in override_map:
        return override_map[code]
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "Unknown"

def normalize_subtitle_lang(raw_code, raw_name=None):
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "English"

def get_episode_number_from_filename(filename, fallback_ep):
    if not filename:
        return fallback_ep
    patterns = [
        r'[Ss]\d{1,2}\s*[Ee](\d{1,3})',
        r'(?:^|[^0-9])(\d{1,2})[xX](\d{1,3})',
        r'[Ee][Pp]?[\s._-]?(\d{1,3})\b',
        r'episode[\s._-]?(\d{1,3})',
    ]
    for p in patterns:
        m = re.search(p, filename, re.IGNORECASE)
        if m:
            return int(m.group(m.lastindex))
    return fallback_ep

def natural_sort_key(name):
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r'(\d+)', name)]

def inspect_tracks(file_path, tmdb_id=None):
    override_map = SERIES_AUDIO_LANG_OVERRIDES.get(str(tmdb_id)) if tmdb_id is not None else None
    media = MediaInfo.parse(str(file_path))
    audio_tracks, subtitle_tracks = [], []
    audio_pos, sub_pos = 0, 0

    for track in media.tracks:
        if track.track_type == "Audio":
            title = str(getattr(track, "title", "")).lower()
            # Filter out commentary tracks
            if any(kw in title for kw in ["commentary", "director", "descriptive", "visual impairment", "dvs"]):
                print(f"         [PROC] Skipping commentary/descriptive audio track: {title or 'Unknown Title'}")
            else:
                lang = normalize_audio_lang(track.language, getattr(track, "language_full", None), override_map)
                audio_tracks.append({"stream_index": audio_pos, "language": lang})
            audio_pos += 1
        elif track.track_type == "Text":
            lang = normalize_subtitle_lang(track.language, getattr(track, "language_full", None))
            sub_fmt = str(getattr(track, "format", "")).lower()
            sub_codec = str(getattr(track, "codecid", "")).lower()
            subtitle_tracks.append({"stream_index": sub_pos, "language": lang, "format": sub_fmt, "codec": sub_codec})
            sub_pos += 1

    if not audio_tracks:
        audio_tracks = [{"stream_index": 0, "language": "Unknown"}]

    return audio_tracks, subtitle_tracks

def remux_single_audio(source_path, output_path, audio_track, subtitle_tracks, subtitle_srt_overrides=None):
    subtitle_srt_overrides = subtitle_srt_overrides or {}
    audio_stream_index = audio_track["stream_index"]
    audio_iso3 = iso3_for_language(audio_track["language"])

    cmd = ["ffmpeg", "-y", "-i", str(source_path)]
    override_input_idx = {}
    next_input = 1
    for sub in subtitle_tracks:
        override_path = subtitle_srt_overrides.get(sub["stream_index"])
        if override_path and os.path.exists(override_path):
            cmd += ["-i", str(override_path)]
            override_input_idx[sub["stream_index"]] = next_input
            next_input += 1

    cmd += ["-map", "0:v:0", "-map", f"0:a:{audio_stream_index}"]

    mapped_subs = []
    for sub in subtitle_tracks:
        if sub["stream_index"] in override_input_idx:
            mapped_subs.append(sub)
            cmd += ["-map", f"{override_input_idx[sub['stream_index']]}:0"]
            cmd += [f"-c:s:{len(mapped_subs)-1}", "copy"]
        else:
            fmt = sub.get("format", "").lower()
            codec = sub.get("codec", "").lower()
            is_safe = any(s in fmt or s in codec for s in [
                "subrip", "srt", "utf-8", "ass", "ssa", "pgs", "pgssub", "hdmv", "vobsub", "dvd_subtitle", "s_text", "s_hdmv"
            ])
            
            if not is_safe:
                print(f"         [WARN] Skipping unsupported subtitle format for MKV: {fmt or codec or 'Unknown'}")
                continue
                
            mapped_subs.append(sub)
            cmd += ["-map", f"0:s:{sub['stream_index']}"]

    cmd += ["-c", "copy", "-map_chapters", "-1"]
    cmd += ["-metadata:s:a:0", f"language={audio_iso3}"]
    
    for out_idx, sub in enumerate(mapped_subs):
        sub_iso3 = iso3_for_language(sub["language"])
        cmd += [f"-metadata:s:s:{out_idx}", f"language={sub_iso3}"]

    cmd.append(str(output_path))
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
        raise Exception(f"ffmpeg remux failed: {result.stderr[-500:] if result.stderr else 'unknown error'}")
    return True

def clean_string_for_vidara(text):
    if not text: return ""
    text = text.replace(".", "").replace("/", "-")
    text = re.sub(r'[:*?"<>|]', "", text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def build_filename(series_name, season, episode, quality, language):
    clean_title = clean_string_for_vidara(series_name)
    return f"{clean_title} S{int(season):02d} E{int(episode):02d} {quality} {language}.mkv"

def fetch_vidara_upload_server():
    try:
        res = requests.get("https://api.vidara.so/v1/upload/server", params={"api_key": VIDARA_API_KEY}, timeout=30)
        res.raise_for_status()
        data = res.json()
        return data.get("result", {}).get("upload_server") or data.get("upload_server") or "https://api.vidara.so/v1/upload/server"
    except Exception as e:
        print(f"      [WARN] Vidara server fetch failed: {e}")
        return "https://api.vidara.so/v1/upload/server"

_folder_id_cache = {}
_folder_lock = threading.Lock()

def get_or_create_vidara_folder(series_name, season_num, quality, languages):
    clean_name = clean_string_for_vidara(series_name)
    cache_key = (clean_name, int(season_num), quality)
    with _folder_lock:
        if cache_key in _folder_id_cache:
            return _folder_id_cache[cache_key]

    lang_str = " ".join(l[:3] for l in languages)
    folder_name = f"{clean_name} Season {int(season_num):02d} {quality} {lang_str}"
    create_url = f"https://api.vidara.so/v1/folder/create?api_key={VIDARA_API_KEY}&name={requests.utils.quote(folder_name)}"

    try:
        res = requests.get(create_url, timeout=30).json()
        if res.get("status") == 200:
            fld_id = res["result"]["folder_id"]
            with _folder_lock:
                _folder_id_cache[cache_key] = fld_id
            print(f"      [FOLDER] '{folder_name}' -> {fld_id}")
            return fld_id
        else:
            print(f"      [FOLDER] Warning: {res}")
            return None
    except Exception as e:
        print(f"      [FOLDER] Error: {e}")
        return None

def extract_vidara_urls(data):
    full_url = data.get("url") or data.get("result", {}).get("url")
    filecode = data.get("filecode") or data.get("result", {}).get("filecode")
    if not full_url and not filecode:
        raise Exception(f"Vidara upload returned no url/filecode: {data}")
    if not full_url: full_url = filecode
    if not filecode: filecode = full_url.rstrip("/").split("/")[-1]
    return full_url, filecode

def upload_to_vidara(file_path, custom_name, folder_id=None):
    upload_server = fetch_vidara_upload_server()
    print(f"      Uploading: {custom_name} ({round(os.path.getsize(file_path) / 1048576, 1)} MB)")
    fields = {"api_key": VIDARA_API_KEY}
    with open(file_path, "rb") as fh:
        fields["file"] = (custom_name, fh, "video/x-matroska")
        if folder_id:
            fields["fld_id"] = str(folder_id)
            fields["folder_id"] = str(folder_id)
        encoder = MultipartEncoder(fields=fields)
        monitor = MultipartEncoderMonitor(encoder)
        response = requests.post(upload_server, data=monitor, headers={"Content-Type": monitor.content_type}, timeout=None)
    if response.status_code == 200:
        return extract_vidara_urls(response.json())
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")

def extract_subtitle_to_srt(source_path, subtitle_stream_index, output_srt_path):
    cmd = ["ffmpeg", "-y", "-i", str(source_path), "-map", f"0:s:{subtitle_stream_index}", "-c:s", "srt", str(output_srt_path)]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_srt_path) or os.path.getsize(output_srt_path) < 10:
        raise Exception(f"ffmpeg subtitle extraction failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True

def extract_subtitle_raw_copy(source_path, subtitle_stream_index, output_path):
    cmd = ["ffmpeg", "-y", "-i", str(source_path), "-map", f"0:s:{subtitle_stream_index}", "-c:s", "copy", str(output_path)]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 10:
        raise Exception(f"ffmpeg raw subtitle copy failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True

def fix_common_ocr_errors(text):
    return text.replace("|", "I")

def ocr_pgs_from_source(source_path, language_code="en", timeout_seconds=600):
    cmd = ["pgsrip", "-l", language_code, str(source_path)]
    try:
        result = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=timeout_seconds)
    except FileNotFoundError:
        raise Exception("pgsrip is not installed/available on PATH")
    except subprocess.TimeoutExpired:
        raise Exception(f"pgsrip OCR timed out after {timeout_seconds}s")

    base, _ = os.path.splitext(str(source_path))
    expected_srt = f"{base}.{language_code}.srt"
    if not os.path.exists(expected_srt) or os.path.getsize(expected_srt) < 10:
        raise Exception(f"pgsrip produced no usable output: {(result.stderr or result.stdout)[-300:]}")

    with open(expected_srt, "r", encoding="utf-8", errors="replace") as f:
        corrected = fix_common_ocr_errors(f.read())
    with open(expected_srt, "w", encoding="utf-8") as f:
        f.write(corrected)
    return expected_srt

def slugify_for_ia(text, max_len=80):
    text = re.sub(r'[^a-zA-Z0-9\-_.]', '-', text or "")
    text = re.sub(r'-+', '-', text).strip('-_.')
    return (text.lower() or "item")[:max_len]

def upload_to_archive_org(file_path, bucket_hint, key_hint, content_type="application/x-subrip", extension="srt", wait_seconds=60):
    bucket = slugify_for_ia(f"beamplay-subs-{bucket_hint}")
    key = slugify_for_ia(key_hint) + f".{extension}"
    upload_url = f"https://s3.us.archive.org/{bucket}/{key}"
    headers = {
        "authorization": f"LOW {IA_ACCESS_KEY}:{IA_SECRET_KEY}",
        "x-amz-auto-make-bucket": "1", "x-archive-meta-mediatype": "texts",
        "x-archive-meta-collection": "opensource", "x-archive-ignore-preexisting-bucket": "1",
        "Content-Type": content_type,
    }
    with open(file_path, "rb") as fh: data = fh.read()
    response = requests.put(upload_url, data=data, headers=headers, timeout=60)
    if response.status_code not in (200, 201):
        raise Exception(f"Archive.org upload failed: {response.status_code} {response.text[:200]}")
    direct_url = f"https://archive.org/download/{bucket}/{key}"
    attempts = max(1, wait_seconds // 5)
    for _ in range(attempts):
        try:
            check = requests.head(direct_url, timeout=10, allow_redirects=True)
            if check.status_code == 200: return direct_url
        except Exception: pass
        time.sleep(5)
    return direct_url

LITTERBOX_API = "https://litterbox.catbox.moe/resources/internals/api.php"

def upload_to_litterbox(file_path, expire="72h"):
    with open(file_path, "rb") as fh:
        response = requests.post(LITTERBOX_API, data={"reqtype": "fileupload", "time": expire}, files={"fileToUpload": fh}, timeout=30)
    response.raise_for_status()
    url = response.text.strip()
    if not url.startswith("http"): raise Exception(f"Litterbox did not return a URL: {url[:200]}")
    return url

def host_subtitle_everywhere(sub_path, bucket_hint, key_hint, content_type="application/x-subrip", extension="srt"):
    hosted, errors = [], []
    try: hosted.append((upload_to_archive_org(sub_path, bucket_hint, key_hint, content_type=content_type, extension=extension), "Archive.org"))
    except Exception as e: errors.append(f"Archive.org: {e}")
    try: hosted.append((upload_to_litterbox(sub_path), "Litterbox"))
    except Exception as e: errors.append(f"Litterbox: {e}")
    if not hosted: raise Exception(" | ".join(errors))
    return hosted

def prepare_english_subtitle_urls(source_path, subtitle_tracks, bucket_hint, tmp_prefix):
    candidates, failures, srt_overrides = [], [], {}
    english_tracks = [s for s in subtitle_tracks if s["language"] == "English"]
    if not english_tracks: return candidates, failures, srt_overrides

    whole_file_ocr_tried, whole_file_ocr_srt, whole_file_ocr_error = False, None, None

    for idx, sub in enumerate(english_tracks):
        srt_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.srt")
        sup_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.sup")
        srt_err_msg = None
        try:
            extract_subtitle_to_srt(source_path, sub["stream_index"], srt_path)
            hosted = host_subtitle_everywhere(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
            candidates.append({"hosts": hosted, "format": "srt"})
            srt_overrides[sub["stream_index"]] = srt_path
            for url, host in hosted: print(f"         [SUB] English subtitle #{idx+1} (srt) hosted via {host} -> {url}")
            continue
        except Exception as e:
            srt_err_msg = str(e)
            safe_delete(srt_path)

        if not whole_file_ocr_tried:
            whole_file_ocr_tried = True
            try:
                produced_path = ocr_pgs_from_source(source_path, language_code="en")
                shutil.copy(produced_path, srt_path)
                safe_delete(produced_path)
                whole_file_ocr_srt = srt_path
            except Exception as e:
                whole_file_ocr_error = str(e)
                print(f"         [WARN] PGS OCR failed: {e}")
        elif whole_file_ocr_srt:
            shutil.copy(whole_file_ocr_srt, srt_path)

        if whole_file_ocr_srt and os.path.exists(srt_path):
            try:
                hosted = host_subtitle_everywhere(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
                candidates.append({"hosts": hosted, "format": "srt (OCR)"})
                srt_overrides[sub["stream_index"]] = srt_path
                for url, host in hosted: print(f"         [SUB] English subtitle #{idx+1} (OCR'd from PGS) hosted via {host} -> {url}")
                continue
            except Exception as host_err:
                safe_delete(srt_path)
                failures.append(f"track #{idx+1}: OCR succeeded but hosting failed ({host_err})")
                continue

        try:
            extract_subtitle_raw_copy(source_path, sub["stream_index"], sup_path)
            hosted = host_subtitle_everywhere(sup_path, bucket_hint, f"{tmp_prefix}_sub{idx}", content_type="application/octet-stream", extension="sup")
            candidates.append({"hosts": hosted, "format": "sup (OCR failed, raw)"})
            for url, host in hosted: print(f"         [SUB] English subtitle #{idx+1} (raw .sup, OCR failed) hosted via {host} -> {url}")
        except Exception as raw_err:
            failures.append(f"track #{idx+1}: srt failed ({srt_err_msg}); OCR failed ({whole_file_ocr_error}); raw backup failed ({raw_err})")
            print(f"         [WARN] Could not prepare English subtitle #{idx+1} via any method")
        finally:
            safe_delete(sup_path)

    return candidates, failures, srt_overrides

def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={"email": ADMIN_EMAIL, "password": ADMIN_PASSWORD}, timeout=30)
    res.raise_for_status()
    return res.json()["token"]

def beam_upsert(jwt, tmdb_id, season, episode, quality, language, url, max_attempts=5, base_delay=2):
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
                "content_type": "episode", "tmdb_id": int(tmdb_id), "season": int(season),
                "episode": int(episode), "url": url, "quality": quality, "audio_languages": [language]
            }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)
            if res.status_code >= 500: raise Exception(f"{res.status_code} Server Error: {res.text[:200]}")
            res.raise_for_status()
            return res.json()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"BEAM upsert rejected (not retrying): {e}")
        except Exception as e:
            last_err = e
            if attempt == max_attempts: break
            delay = base_delay * (2 ** (attempt - 1))
            print(f"         [BEAM] upsert attempt {attempt}/{max_attempts} failed ({e}); retrying in {delay}s...")
            time.sleep(delay)
    raise Exception(f"BEAM upsert failed after {max_attempts} attempts: {last_err}")

def download_file(url, dest_path):
    cmd = [
        "aria2c", "-x", "16", "-s", "16", "-j", "16", "-k", "1M",
        "--file-allocation=none", "--summary-interval=0", "--retry-wait=5",
        "--max-tries=8", "--timeout=45", "--connect-timeout=15",
        "--auto-file-renaming=false", "--disable-ipv6=true",
        "--max-connection-per-server=16", "--min-split-size=1M",
        "--user-agent=Mozilla/5.0", "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True
    print("      [WARN] aria2c failed, trying direct stream...")
    try:
        if os.path.exists(dest_path): os.remove(dest_path)
        with requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk: f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"      [ERROR] Direct stream failed: {e}")
        return False

def safe_delete(path):
    try:
        if path and os.path.exists(path): os.remove(path)
    except Exception as e:
        print(f"      [WARN] Could not delete {path}: {e}")

SHEET_CELL_CHAR_LIMIT = 49000

def format_error(episode, language, stage, reason):
    ep_part = f"E{episode} " if episode is not None else ""
    lang_part = f"[{language}] " if language else ""
    return f"{ep_part}{lang_part}FAILED @ {stage}: {reason}"[:SHEET_CELL_CHAR_LIMIT]


# ============================================================================
# PIPELINE WORKERS (SEMAPHORE OPTIMIZED + FILECODE TRACKING)
# ============================================================================

DISK_LIMIT = 2 # Max large files on disk simultaneously

def processor_worker(Q_PROCESS, Q_UPLOAD):
    """Stage 2: Takes downloaded files, runs ffmpeg/OCR, passes to upload queue."""
    while True:
        task = Q_PROCESS.get()
        if task is None:
            Q_UPLOAD.put(None)
            Q_PROCESS.task_done()
            break
            
        try:
            tmdb_id = task["tmdb_id"]
            series_name = task["series_name"]
            season = int(task["season"])
            episode = int(task["episode"])
            quality = task["quality"]
            source_path = task["source_path"]
            
            audio_tracks, subtitle_tracks = inspect_tracks(source_path, tmdb_id=tmdb_id)
            print(f"         [PROC] E{episode}: Audio: {[a['language'] for a in audio_tracks]}"
                  + (f" | Subs: {[s['language'] for s in subtitle_tracks]}" if subtitle_tracks else ""))

            subtitle_candidates, prep_failures, subtitle_srt_overrides = prepare_english_subtitle_urls(
                source_path, subtitle_tracks, f"{tmdb_id}-s{season}", f"{tmdb_id}_S{season}E{episode}_{quality}"
            )
            
            all_langs = []
            for t in audio_tracks:
                if t["language"] not in all_langs: all_langs.append(t["language"])
            folder_id = get_or_create_vidara_folder(series_name, season, quality, all_langs)

            task["sub_links"] = []
            if subtitle_candidates:
                for candidate in subtitle_candidates:
                    task["sub_links"].extend(url for url, _host in candidate["hosts"])
            
            task["sub_failures"] = []
            for fail_reason in prep_failures:
                task["sub_failures"].append(fail_reason)

            task["output_files"] = []
            seen_langs = set()
            for track in audio_tracks:
                lang = track["language"]
                if lang in seen_langs:
                    print(f"         [PROC] Skipping duplicate audio language for E{episode}: {lang}")
                    continue
                seen_langs.add(lang)
                
                output_name = build_filename(series_name, season, episode, quality, lang)
                output_path = os.path.join(OUTPUT_FOLDER, output_name)
                
                remux_single_audio(source_path, output_path, track, subtitle_tracks, subtitle_srt_overrides)
                task["output_files"].append({
                    "path": output_path, "name": output_name, "lang": lang, "folder_id": folder_id
                })
            
            for _idx, override_path in (subtitle_srt_overrides or {}).items():
                safe_delete(override_path)
                
            task["status"] = "processed"
        except Exception as e:
            task["status"] = "failed"
            task["error"] = f"Split/Process: {str(e)}"
            task["output_files"] = []
            
        safe_delete(task["source_path"])
        Q_UPLOAD.put(task)
        Q_PROCESS.task_done()


def uploader_worker(Q_UPLOAD, jwt, pipeline_sheet, pcol, row_completion, state_lock, sheet_lock, disk_semaphore):
    """Stage 3: Uploads processed files to Vidara/BEAM and records filecodes."""
    while True:
        task = Q_UPLOAD.get()
        if task is None:
            Q_UPLOAD.task_done()
            break
            
        row_idx = task["row_idx"]
        failed = False
        error_msg = ""
        notes = []
        filecodes = []
        beam_err_notes = []
        
        if task["status"] == "processed":
            try:
                for out_file in task["output_files"]:
                    try:
                        video_url, filecode = upload_to_vidara(out_file["path"], out_file["name"], out_file["folder_id"])
                        if filecode: filecodes.append(filecode)
                        try:
                            beam_upsert(jwt, task["tmdb_id"], task["season"], task["episode"], task["quality"], out_file["lang"], video_url)
                        except Exception as beam_err:
                            beam_err_notes.append(f"E{task['episode']} {out_file['lang']}: live ({video_url}) DB-pending — {beam_err}")
                            print(f"         [WARN] BEAM registration failed for E{task['episode']} {out_file['lang']}, video stays live: {beam_err}")
                        print(f"         [OK] S{int(task['season']):02d}E{int(task['episode']):02d} {out_file['lang']} uploaded ({video_url}).")
                    except Exception as e:
                        raise Exception(f"[E{task['episode']} / {out_file['lang']}] {e}")
                    finally:
                        safe_delete(out_file["path"])
            except Exception as e:
                failed = True
                error_msg = f"Upload: {str(e)}"
        else:
            failed = True
            error_msg = task.get("error", "Unknown processing error")
            
        for out_file in task.get("output_files", []):
            safe_delete(out_file["path"])
            
        disk_semaphore.release()
        
        ep_num = task['episode']
        fc_str = ",".join(filecodes) if filecodes else "N/A"
        
        if task.get("sub_links"):
            notes.append(f"E{ep_num} [FC:{fc_str}] Subs: " + " | ".join(task["sub_links"]))
        for fail_reason in task.get("sub_failures", []):
            notes.append(f"E{ep_num} [FC:{fc_str}] Sub Prep Failed: {fail_reason}")
            
        notes.extend(beam_err_notes)
            
        with state_lock:
            state = row_completion[row_idx]
            state["done"] += 1
            if failed: state["failed"] += 1
            state["notes"].extend(notes)
            
            if state["done"] == state["total"]:
                final_status = "Failed" if state["failed"] > 0 else "Done"
                final_notes = "\n".join(state["notes"])[:SHEET_CELL_CHAR_LIMIT] if state["notes"] else ""
                if final_status == "Failed":
                    final_notes = format_error(task["episode"] if task["link_type"] == "SINGLE" else None, None, "Split/Upload", error_msg)
                
                with sheet_lock:
                    pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], final_status)
                    pipeline_sheet.update_cell(row_idx, pcol["Error"], final_notes)
                    
        Q_UPLOAD.task_done()


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("BEAM SERIES DOWNLOADER v3 (SEMAPHORE + FC TRACKING) — STARTING")
    print("=" * 60)

    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str: raise ValueError("GOOGLE_SHEETS_JSON is missing.")
    creds_dict = json.loads(raw_json_str)
    creds = Credentials.from_service_account_info(creds_dict, scopes=SCOPES)
    gc = gspread.authorize(creds)
    print("[OK] Connected to Google Sheets API")

    spreadsheet = gc.open_by_key(SPREADSHEET_ID)
    master_sheet = spreadsheet.worksheet("Master")
    pipeline_sheet = spreadsheet.worksheet("Pipeline")
    archive_sheet = spreadsheet.worksheet("Archive")

    jwt = beam_login()
    print("[OK] Logged into BEAM worker\n")

    master_values = master_sheet.get_all_values()
    master_headers = [h.strip() for h in master_values[0]]

    MASTER_REQUIRED = [
        "Filename", "Status", "TMDB_ID", "TMDB_NAME", "YEAR", "Season",
        "Link_1080p", "Link_720p", "Link_480p",
        "DOWNLOAD_STATUS_1080p", "DOWNLOAD_STATUS_720p", "DOWNLOAD_STATUS_480p",
        "Link_Type_1080p", "Link_Type_720p", "Link_Type_480p",
        "Processed", "Duplicate_Check"
    ]
    missing = [h for h in MASTER_REQUIRED if h not in master_headers]
    if missing: raise Exception(f"Series_Master missing columns: {missing}")

    mcols = {name: master_headers.index(name) + 1 for name in MASTER_REQUIRED}
    QUALITY_STATUS_COL = {"1080p": "DOWNLOAD_STATUS_1080p", "720p": "DOWNLOAD_STATUS_720p", "480p": "DOWNLOAD_STATUS_480p"}
    QUALITY_LINK_COL = {"1080p": "Link_1080p", "720p": "Link_720p", "480p": "Link_480p"}

    master_rows_by_key = {}
    for i, row_cells in enumerate(master_values[1:], start=2):
        padded = row_cells + [""] * (len(master_headers) - len(row_cells))
        row = {master_headers[j]: padded[j] for j in range(len(master_headers))}
        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        season = str(row.get("Season", "")).strip()
        if tmdb_id and season: master_rows_by_key[(tmdb_id, season)] = i

    pipeline_values = pipeline_sheet.get_all_values()
    if not pipeline_values: raise Exception("Series_Pipeline is empty (no header row).")
    pipeline_headers = [h.strip() for h in pipeline_values[0]]

    PIPELINE_REQUIRED = ["TMDB_ID", "TMDB_NAME", "Season", "Episode", "Quality",
                          "Input_Link", "Link_Type", "DOWNLOAD_STATUS", "Error"]
    missing_p = [h for h in PIPELINE_REQUIRED if h not in pipeline_headers]
    if missing_p: raise Exception(f"Series_Pipeline missing columns: {missing_p}. Expected: {PIPELINE_REQUIRED}")

    pcol = {name: pipeline_headers.index(name) + 1 for name in PIPELINE_REQUIRED}

    pipeline_rows = []
    for i, row_cells in enumerate(pipeline_values[1:], start=2):
        padded = row_cells + [""] * (len(pipeline_headers) - len(row_cells))
        row = {pipeline_headers[j]: padded[j] for j in range(len(pipeline_headers))}
        row["_row_idx"] = i
        pipeline_rows.append(row)

    print(f"Loaded {len(pipeline_rows)} Pipeline rows.\n")

    touched_groups = set()
    
    Q_PROCESS = queue.Queue()
    Q_UPLOAD = queue.Queue()
    row_completion = {}
    state_lock = threading.Lock()
    sheet_lock = threading.Lock()
    disk_semaphore = threading.Semaphore(DISK_LIMIT)

    proc_thread = threading.Thread(target=processor_worker, args=(Q_PROCESS, Q_UPLOAD), daemon=True)
    up_thread = threading.Thread(target=uploader_worker, args=(Q_UPLOAD, jwt, pipeline_sheet, pcol, row_completion, state_lock, sheet_lock, disk_semaphore), daemon=True)
    proc_thread.start()
    up_thread.start()

    for row in pipeline_rows:
        row_idx = row["_row_idx"]
        status = str(row.get("DOWNLOAD_STATUS", "")).strip().lower()
        if status == "done": continue

        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        series_name = str(row.get("TMDB_NAME", "")).strip()
        season = str(row.get("Season", "")).strip()
        episode_raw = str(row.get("Episode", "")).strip()
        quality = str(row.get("Quality", "")).strip()
        link_type = str(row.get("Link_Type", "")).strip().upper()
        link = str(row.get("Input_Link", "")).strip()

        if not tmdb_id or not season or not quality or not link or link_type not in ("SINGLE", "ZIP"):
            continue

        episode = int(episode_raw) if episode_raw.isdigit() else 0
        touched_groups.add((tmdb_id, season, quality))

        label = f"E{episode}" if link_type == "SINGLE" else "ZIP (multi-episode)"
        print(f"\n{'='*60}\n{series_name} S{season} {label} — {quality} [Pipeline row {row_idx}]\n{'='*60}")

        with sheet_lock:
            pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Running")
            
        with state_lock:
            row_completion[row_idx] = {"total": 1, "done": 0, "failed": 0, "notes": [], "link_type": link_type}

        if link_type == "SINGLE":
            guessed_name = os.path.basename(link.split('?')[0]) or f"ep{episode}.mkv"
            temp_path = os.path.join(TEMP_FOLDER, f"{tmdb_id}_S{season}E{episode}_{quality}_{guessed_name}")
            
            disk_semaphore.acquire()
            
            if download_file(link, temp_path):
                task = {
                    "row_idx": row_idx, "tmdb_id": tmdb_id, "series_name": series_name,
                    "season": season, "episode": episode, "quality": quality, "link_type": "SINGLE",
                    "source_path": temp_path
                }
                Q_PROCESS.put(task)
            else:
                safe_delete(temp_path)
                disk_semaphore.release()
                with state_lock:
                    row_completion[row_idx]["done"] = 1
                    row_completion[row_idx]["failed"] = 1
                with sheet_lock:
                    pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Failed")
                    pipeline_sheet.update_cell(row_idx, pcol["Error"], format_error(episode, None, "Download", "Episode download failed after retries"))

        elif link_type == "ZIP":
            zip_path = os.path.join(TEMP_FOLDER, f"{tmdb_id}_S{season}_{quality}.zip")
            
            disk_semaphore.acquire()
            
            if not download_file(link, zip_path) or not zipfile.is_zipfile(zip_path):
                safe_delete(zip_path)
                disk_semaphore.release()
                with state_lock:
                    row_completion[row_idx]["done"] = 1
                    row_completion[row_idx]["failed"] = 1
                with sheet_lock:
                    pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Failed")
                    pipeline_sheet.update_cell(row_idx, pcol["Error"], format_error(None, None, "Download", "ZIP download failed or invalid"))
                continue

            try:
                with zipfile.ZipFile(zip_path, 'r') as archive:
                    video_entries = [
                        f for f in archive.infolist()
                        if f.filename.lower().endswith(VIDEO_EXTENSIONS)
                        and not os.path.basename(f.filename).startswith('.')
                    ]
                    video_entries.sort(key=lambda f: natural_sort_key(f.filename))
                    
                    if not video_entries:
                        raise Exception("No valid video files found inside ZIP.")
                        
                    with state_lock:
                        row_completion[row_idx]["total"] = len(video_entries)
                        
                    for pos, entry in enumerate(video_entries, start=1):
                        entry_basename = os.path.basename(entry.filename)
                        extract_target = os.path.join(TEMP_FOLDER, f"zipentry_{tmdb_id}_{quality}_{pos}_{entry_basename}")
                        
                        disk_semaphore.acquire()
                        
                        try:
                            with archive.open(entry) as src, open(extract_target, "wb") as dst:
                                shutil.copyfileobj(src, dst)
                        except Exception as extract_err:
                            disk_semaphore.release()
                            safe_delete(extract_target)
                            raise Exception(f"Failed to extract {entry_basename}: {extract_err}")
                            
                        ep_num = get_episode_number_from_filename(entry_basename, fallback_ep=pos)
                        task = {
                            "row_idx": row_idx, "tmdb_id": tmdb_id, "series_name": series_name,
                            "season": season, "episode": ep_num, "quality": quality, "link_type": "ZIP",
                            "source_path": extract_target
                        }
                        Q_PROCESS.put(task)
            except Exception as e:
                with state_lock:
                    state = row_completion[row_idx]
                    state["failed"] += 1
                    state["done"] = state["total"]
                    state["notes"].append(format_error(None, None, "ZIP Read", str(e)))
                with sheet_lock:
                    pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Failed")
                    pipeline_sheet.update_cell(row_idx, pcol["Error"], format_error(None, None, "ZIP Read", str(e)))
            finally:
                safe_delete(zip_path)
                disk_semaphore.release()

    Q_PROCESS.join()
    Q_UPLOAD.join()
    
    Q_PROCESS.put(None)
    proc_thread.join()
    up_thread.join()

    print(f"\n{'='*60}\nSyncing Master statuses...\n{'='*60}")

    pipeline_values_fresh = pipeline_sheet.get_all_values()
    fresh_pipeline_rows = []
    for row_cells in pipeline_values_fresh[1:]:
        padded = row_cells + [""] * (len(pipeline_headers) - len(row_cells))
        fresh_pipeline_rows.append({pipeline_headers[j]: padded[j] for j in range(len(pipeline_headers))})

    touched_series_seasons = set((t, s) for (t, s, q) in touched_groups)

    for (tmdb_id, season, quality) in touched_groups:
        matching = [
            r for r in fresh_pipeline_rows
            if str(r.get("TMDB_ID", "")).strip() == tmdb_id
            and str(r.get("Season", "")).strip() == season
            and str(r.get("Quality", "")).strip() == quality
        ]
        if not matching: continue

        statuses = [str(r.get("DOWNLOAD_STATUS", "")).strip().lower() for r in matching]
        if any(s == "failed" for s in statuses): agg = "Failed"
        elif all(s == "done" for s in statuses): agg = "Done"
        else: agg = "Running"

        master_row_idx = master_rows_by_key.get((tmdb_id, season))
        if master_row_idx:
            status_col = mcols[QUALITY_STATUS_COL[quality]]
            master_sheet.update_cell(master_row_idx, status_col, agg)
            print(f"  Master ({tmdb_id}, S{season}, {quality}) -> {agg}")

    print(f"\n{'='*60}\nChecking rows for archiving...\n{'='*60}")

    master_values_fresh = master_sheet.get_all_values()
    to_archive = []

    for i, row_cells in enumerate(master_values_fresh[1:], start=2):
        padded = row_cells + [""] * (len(master_headers) - len(row_cells))
        row = {master_headers[j]: padded[j] for j in range(len(master_headers))}

        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        season = str(row.get("Season", "")).strip()
        if not tmdb_id or not season: continue
        if (tmdb_id, season) not in touched_series_seasons: continue

        present_qualities = [q for q in ("1080p", "720p", "480p") if str(row.get(QUALITY_LINK_COL[q], "")).strip()]
        if not present_qualities: continue

        all_done = all(str(row.get(QUALITY_STATUS_COL[q], "")).strip().lower() == "done" for q in present_qualities)
        if all_done: to_archive.append((i, row, tmdb_id, season))

    for i, row, tmdb_id, season in sorted(to_archive, key=lambda x: x[0], reverse=True):
        print(f"Archiving {row.get('TMDB_NAME','')} S{season} (row {i})...")
        archive_row = [row.get(h, "") for h in master_headers]
        archive_sheet.append_row(archive_row, value_input_option="USER_ENTERED")
        master_sheet.delete_rows(i)

        pipeline_values_cleanup = pipeline_sheet.get_all_values()
        kept_for_warnings = 0
        for j in range(len(pipeline_values_cleanup) - 1, 0, -1):
            prow = pipeline_values_cleanup[j]
            p_padded = prow + [""] * (len(pipeline_headers) - len(prow))
            p_tmdb = str(p_padded[pcol["TMDB_ID"] - 1]).strip()
            p_season = str(p_padded[pcol["Season"] - 1]).strip()
            p_error = str(p_padded[pcol["Error"] - 1]).strip()
            if p_tmdb == tmdb_id and p_season == season:
                if p_error:
                    kept_for_warnings += 1
                    continue
                pipeline_sheet.delete_rows(j + 1)

        if kept_for_warnings:
            print(f"[OK] Archived. Kept {kept_for_warnings} Pipeline row(s) with unresolved notes — check their Error column.")
        else:
            print(f"[OK] Archived and cleaned up.")

    try: shutil.rmtree(BASE_DIR, ignore_errors=True)
    except Exception: pass

    print(f"\n{'='*60}\nSERIES PIPELINE COMPLETE\n{'='*60}")

if __name__ == "__main__":
    main()
