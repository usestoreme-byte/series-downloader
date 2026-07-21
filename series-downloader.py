#!/usr/bin/env python3
"""
BEAM Series Downloader v3 — GitHub Actions Pipeline
=====================================================
Series_Master   = one row per (TMDB_ID + Season). Links pasted per quality.
Series_Pipeline = one row per LINK (pushed by the Apps Script menu item).
                  SINGLE links get an explicit Episode number = that line's
                  position within the quality cell (never guessed).
                  ZIP links get Episode = 0; the real episode numbers are
                  read from each file's name INSIDE the zip.
Series_Archive  = one row per (TMDB_ID + Season), written only once every
                  quality that has a link on that Master row is Done.

Because a single quality can now be many Pipeline rows (one per SINGLE
episode, or one per zip), the Master status for a quality is an aggregate:
    - any row Failed          -> Failed
    - all rows Done           -> Done
    - otherwise (still queued/running) -> Running

Sheet writes are checkpoint-based:
    row start   -> Pipeline.DOWNLOAD_STATUS = Running
    row success -> Pipeline.DOWNLOAD_STATUS = Done, Error cleared
    row failure -> Pipeline.DOWNLOAD_STATUS = Failed, Error = details
    after the run -> Master's DOWNLOAD_STATUS_xxxx recomputed from ALL
                      matching Pipeline rows (not just ones touched this run)
No per-language sheet writes.
"""

import os
import re
import json
import shutil
import requests
import subprocess
import zipfile
import time
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

# Internet Archive S3-style credentials, used to host extracted English
# subtitles so Vidara can fetch them by direct URL.
# SECURITY NOTE: hardcoded here only because you asked to test quickly —
# swap these for a GitHub Secret (IA_ACCESS_KEY / IA_SECRET_KEY, same
# pattern as VIDARA_API_KEY above) before running this long-term. Anyone
# with read access to this file/repo gets full write access to your IA
# account with these sitting here in plain text.
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
}

UNKNOWN_TOKENS = {"", "und", "unknown", "unk", "n/a", "none"}

ISO2_TO_ISO3 = {
    "as": "asm", "te": "tel", "hi": "hin", "ta": "tam", "ml": "mal",
    "kn": "kan", "bn": "ben", "pa": "pan", "gu": "guj", "mr": "mar",
    "or": "ori", "en": "eng", "ja": "jpn", "ko": "kor", "es": "spa",
    "fr": "fre", "de": "ger", "ru": "rus", "zh": "chi", "it": "ita",
    "pt": "por", "ar": "ara", "tr": "tur",
}

NAME_TO_ISO3 = {}
for _code2, _name in LANG_MAP.items():
    _iso3 = ISO2_TO_ISO3.get(_code2)
    if _iso3 and _name not in NAME_TO_ISO3:
        NAME_TO_ISO3[_name] = _iso3


def iso3_for_language(language_name):
    return NAME_TO_ISO3.get(language_name, "und")


# ============================================================================
# NORMALIZATION
# ============================================================================

def normalize_audio_lang(raw_code, raw_name=None):
    """Audio must NEVER guess. Unrecognized/blank stays 'Unknown'."""
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "Unknown"


def normalize_subtitle_lang(raw_code, raw_name=None):
    """Subtitles: unknown/blank/und collapses to English."""
    code = (raw_code or "").strip().lower()
    if code in LANG_MAP:
        return LANG_MAP[code]
    name = (raw_name or "").strip()
    if name:
        for full in LANG_MAP.values():
            if name.lower() == full.lower():
                return full
    return "English"


# ============================================================================
# EPISODE DETECTION — ONLY used for entries inside a ZIP.
# SINGLE links never go through this; their episode number is explicit,
# written by the Apps Script push step (line position in the cell).
# ============================================================================

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


# ============================================================================
# MEDIAINFO
# ============================================================================

def inspect_tracks(file_path):
    """
    Returns:
        audio_tracks    = [ { "stream_index": int, "language": "English" }, ... ]
        subtitle_tracks = [ { "stream_index": int, "language": "English" }, ... ]
    """
    media = MediaInfo.parse(str(file_path))
    audio_tracks, subtitle_tracks = [], []
    audio_pos, sub_pos = 0, 0

    for track in media.tracks:
        if track.track_type == "Audio":
            lang = normalize_audio_lang(track.language, getattr(track, "language_full", None))
            audio_tracks.append({"stream_index": audio_pos, "language": lang})
            audio_pos += 1
        elif track.track_type == "Text":
            lang = normalize_subtitle_lang(track.language, getattr(track, "language_full", None))
            subtitle_tracks.append({"stream_index": sub_pos, "language": lang})
            sub_pos += 1

    if not audio_tracks:
        audio_tracks = [{"stream_index": 0, "language": "Unknown"}]

    return audio_tracks, subtitle_tracks


# ============================================================================
# FFMPEG — remux only, never re-encode
# ============================================================================

def remux_single_audio(source_path, output_path, audio_track, subtitle_tracks):
    audio_stream_index = audio_track["stream_index"]
    audio_iso3 = iso3_for_language(audio_track["language"])

    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", "0:v:0",
        "-map", f"0:a:{audio_stream_index}",
    ]
    for sub in subtitle_tracks:
        cmd += ["-map", f"0:s:{sub['stream_index']}"]

    cmd += ["-c", "copy", "-map_chapters", "-1"]
    cmd += ["-metadata:s:a:0", f"language={audio_iso3}"]
    for out_idx, sub in enumerate(subtitle_tracks):
        sub_iso3 = iso3_for_language(sub["language"])
        cmd += [f"-metadata:s:s:{out_idx}", f"language={sub_iso3}"]

    cmd.append(str(output_path))

    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 1024:
        raise Exception(f"ffmpeg remux failed: {result.stderr[-500:] if result.stderr else 'unknown error'}")
    return True


# ============================================================================
# NAMING / VIDARA / BEAM / DOWNLOAD
# ============================================================================

def clean_string_for_vidara(text):
    if not text:
        return ""
    text = text.replace(".", "")
    text = text.replace("/", "-")
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


def get_or_create_vidara_folder(series_name, season_num, language):
    """One folder per (series, season, language), e.g. 'Breaking Bad Season 1 English'."""
    clean_name = clean_string_for_vidara(series_name)
    cache_key = (clean_name, int(season_num), language)
    if cache_key in _folder_id_cache:
        return _folder_id_cache[cache_key]

    folder_name = f"{clean_name} Season {int(season_num):02d} {language}"
    create_url = f"https://api.vidara.so/v1/folder/create?api_key={VIDARA_API_KEY}&name={requests.utils.quote(folder_name)}"

    try:
        res = requests.get(create_url, timeout=30).json()
        if res.get("status") == 200:
            fld_id = res["result"]["folder_id"]
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
    """
    Returns (full_url, bare_filecode).

    full_url: whatever URL Vidara actually returned in `url` (or
    result.url), stored AS-IS into BEAM — Vidara's embed domain has changed
    more than once (vidara.so -> vidaraa.cc -> vidara.to), so reconstructing
    or hardcoding a domain is fragile. Store exactly what they give back.

    bare_filecode: just the last path segment, needed ONLY internally for
    the subtitle-attach API, which requires the bare code rather than a URL.
    """
    full_url = data.get("url") or data.get("result", {}).get("url")
    filecode = data.get("filecode") or data.get("result", {}).get("filecode")

    if not full_url and not filecode:
        raise Exception(f"Vidara upload returned no url/filecode: {data}")

    if not full_url:
        full_url = filecode

    if not filecode:
        filecode = full_url.rstrip("/").split("/")[-1]

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
        data = response.json()
        return extract_vidara_urls(data)  # (full_url, filecode)
    else:
        raise Exception(f"Vidara upload failed: {response.status_code} {response.text[:200]}")


# ============================================================================
# SUBTITLES — extract English tracks only, host them permanently and freely
# on Internet Archive, then tell Vidara to attach that URL to the uploaded video's filecode.
# Extracting straight from the same source file we split the audio from
# guarantees the subtitle timing matches — no separate re-sync possible.
# ============================================================================

def extract_subtitle_to_srt(source_path, subtitle_stream_index, output_srt_path):
    """
    Pulls ONE subtitle stream out of the source file as a standalone .srt.
    Text-based subtitle codecs (srt/ass/webvtt/etc.) convert to srt cleanly
    via -c:s srt. If a track is image-based (e.g. PGS/VobSub) ffmpeg can't
    convert it to srt and this will fail — that's expected and handled by
    the caller as a skip, not a hard error.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", f"0:s:{subtitle_stream_index}",
        "-c:s", "srt",
        str(output_srt_path)
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_srt_path) or os.path.getsize(output_srt_path) < 10:
        raise Exception(f"ffmpeg subtitle extraction failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True


def slugify_for_ia(text, max_len=80):
    """
    Internet Archive item/bucket identifiers and S3 keys only allow
    alphanumerics, -, _, . — anything else gets collapsed to a dash.
    """
    text = re.sub(r'[^a-zA-Z0-9\-_.]', '-', text or "")
    text = re.sub(r'-+', '-', text).strip('-_.')
    return (text.lower() or "item")[:max_len]


def upload_to_archive_org(file_path, bucket_hint, key_hint, content_type="application/x-subrip", wait_seconds=60):
    """
    Uploads via Internet Archive's S3-compatible endpoint. `bucket_hint`
    should be something stable per show+season so multiple subtitle files
    land in the same IA "item" instead of creating a new one per file.
    x-amz-auto-make-bucket creates that item automatically if it doesn't
    exist yet. Storage is free and permanent — no expiry to manage.

    IA can take anywhere from a few seconds to a couple minutes to make a
    freshly uploaded file publicly fetchable, so this polls the direct
    download URL briefly before handing it back — Vidara needs to fetch
    it immediately, so handing back a URL that 404s yet would just move
    the same failure mode over to a different host.
    """
    bucket = slugify_for_ia(f"beamplay-subs-{bucket_hint}")
    key = slugify_for_ia(key_hint) + ".srt"
    upload_url = f"https://s3.us.archive.org/{bucket}/{key}"

    headers = {
        "authorization": f"LOW {IA_ACCESS_KEY}:{IA_SECRET_KEY}",
        "x-amz-auto-make-bucket": "1",
        "x-archive-meta-mediatype": "texts",
        "x-archive-meta-collection": "opensource",
        "x-archive-ignore-preexisting-bucket": "1",
        "Content-Type": content_type,
    }

    with open(file_path, "rb") as fh:
        data = fh.read()

    response = requests.put(upload_url, data=data, headers=headers, timeout=60)
    if response.status_code not in (200, 201):
        raise Exception(f"Archive.org upload failed: {response.status_code} {response.text[:200]}")

    direct_url = f"https://archive.org/download/{bucket}/{key}"

    attempts = max(1, wait_seconds // 5)
    for _ in range(attempts):
        try:
            check = requests.head(direct_url, timeout=10, allow_redirects=True)
            if check.status_code == 200:
                return direct_url
        except Exception:
            pass
        time.sleep(5)

    # Didn't confirm propagation within the wait window — hand the URL back
    # anyway. Worst case Vidara's fetch fails once and this episode's
    # subtitle becomes one of the "manual attach" warnings, same as any
    # other subtitle-stage failure.
    print(f"         [WARN] Archive.org file not confirmed reachable after {wait_seconds}s, proceeding anyway: {direct_url}")
    return direct_url


def attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English"):
    res = requests.get(
        "https://api.vidara.so/v1/upload/sub",
        params={"api_key": VIDARA_API_KEY, "filecode": filecode, "sub_lang": sub_lang, "sub_url": sub_url},
        timeout=30
    )
    res.raise_for_status()
    data = res.json()
    if data.get("status") != 200:
        raise Exception(f"Vidara subtitle attach failed: {data}")
    return True


LITTERBOX_API = "https://litterbox.catbox.moe/resources/internals/api.php"


def upload_to_litterbox(file_path, expire="72h"):
    """
    Fallback host used ONLY if Archive.org's upload throws (network error,
    timeout, etc). Free, no-signup, temporary (72h is plenty for a subtitle
    to get attached). Response body is a plain-text direct URL.
    """
    with open(file_path, "rb") as fh:
        response = requests.post(
            LITTERBOX_API,
            data={"reqtype": "fileupload", "time": expire},
            files={"fileToUpload": fh},
            timeout=30
        )
    response.raise_for_status()
    url = response.text.strip()
    if not url.startswith("http"):
        raise Exception(f"Litterbox did not return a URL: {url[:200]}")
    return url


def host_subtitle_with_fallback(srt_path, bucket_hint, key_hint):
    """
    Tries Archive.org first (permanent, free). If that throws for any
    reason — network error, timeout, IA having a bad day — falls back to
    Litterbox instead of failing the whole subtitle outright. Only if BOTH
    fail does this raise, and the caller turns that into a clean warning
    in the Error column.
    """
    try:
        url = upload_to_archive_org(srt_path, bucket_hint, key_hint)
        return url, "Archive.org"
    except Exception as e_ia:
        print(f"         [WARN] Archive.org upload failed ({e_ia}), falling back to Litterbox...")
        try:
            url = upload_to_litterbox(srt_path)
            return url, "Litterbox"
        except Exception as e_lb:
            raise Exception(f"Archive.org failed ({e_ia}); Litterbox fallback also failed ({e_lb})")


def prepare_english_subtitle_urls(source_path, subtitle_tracks, bucket_hint, tmp_prefix):
    """
    Extracts every subtitle track normalized to 'English', uploads each to
    Internet Archive (one IA "item" per bucket_hint, reused across every
    subtitle for that show+season instead of creating a new item per file),
    and returns (urls, failure_reasons). Best-effort: a track that fails to
    extract/upload is skipped (reason recorded) rather than aborting the
    whole episode — the video itself matters more than a caption attach.
    """
    urls = []
    failures = []
    english_tracks = [s for s in subtitle_tracks if s["language"] == "English"]
    if not english_tracks:
        return urls, failures

    for idx, sub in enumerate(english_tracks):
        srt_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.srt")
        try:
            extract_subtitle_to_srt(source_path, sub["stream_index"], srt_path)
            url, host = host_subtitle_with_fallback(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
            urls.append(url)
            print(f"         [SUB] English subtitle #{idx+1} hosted -> {url}")
        except Exception as e:
            failures.append(f"track #{idx+1}: {e}")
            print(f"         [WARN] Could not prepare English subtitle #{idx+1}: {e}")
        finally:
            safe_delete(srt_path)

    return urls, failures


def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]


def beam_upsert(jwt, tmdb_id, season, episode, quality, language, url):
    res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
        "content_type": "episode",
        "tmdb_id": int(tmdb_id),
        "season": int(season),
        "episode": int(episode),
        "url": url,
        "quality": quality,
        "audio_languages": [language]
    }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)
    res.raise_for_status()
    return res.json()


def download_file(url, dest_path):
    cmd = [
        "aria2c", "-x", "8", "-s", "8", "-k", "5M",
        "--file-allocation=none", "--summary-interval=0", "--retry-wait=10",
        "--max-tries=8", "--timeout=45", "--auto-file-renaming=false",
        "-d", os.path.dirname(dest_path), "-o", os.path.basename(dest_path), url
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0 and os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024:
        return True

    print("      [WARN] aria2c failed, trying direct stream...")
    try:
        if os.path.exists(dest_path):
            os.remove(dest_path)
        with requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        f.write(chunk)
        return os.path.exists(dest_path) and os.path.getsize(dest_path) > 1024 * 1024
    except Exception as e:
        print(f"      [ERROR] Direct stream failed: {e}")
        return False


def safe_delete(path):
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        print(f"      [WARN] Could not delete {path}: {e}")


def format_error(episode, language, stage, reason):
    ep_line = f"Episode:\n{episode}\n\n" if episode is not None else ""
    lang_line = f"Language:\n{language}\n\n" if language else ""
    return (
        f"FAILED\n\n"
        f"{ep_line}"
        f"{lang_line}"
        f"Stage:\n{stage}\n\n"
        f"Reason:\n{reason}"
    )[:1500]


# ============================================================================
# CORE: split one already-on-disk video file into per-language uploads
# ============================================================================

def process_episode_file(jwt, tmdb_id, series_name, season_num, episode_num,
                          quality, source_path, already_done_langs, subtitle_warnings):
    """
    `already_done_langs` is a set (mutated in place) of languages already
    uploaded for THIS episode within THIS row's scope. Raises on failure.
    `subtitle_warnings` is a list (mutated in place) of human-readable notes
    for any caption that didn't get attached — always includes the direct
    Archive.org link when one was successfully generated, so it can be
    downloaded and attached to that filecode by hand.
    """
    audio_tracks, subtitle_tracks = inspect_tracks(source_path)
    print(f"         Audio: {[a['language'] for a in audio_tracks]}"
          + (f" | Subs: {[s['language'] for s in subtitle_tracks]}" if subtitle_tracks else ""))

    # Extract + host every English subtitle track ONCE for this episode —
    # same captions get attached to every audio-language video we upload
    # below, so there's no need to redo this per audio track.
    subtitle_urls, prep_failures = prepare_english_subtitle_urls(
        source_path, subtitle_tracks, f"{tmdb_id}-s{season_num}", f"{tmdb_id}_S{season_num}E{episode_num}_{quality}"
    )
    for fail_reason in prep_failures:
        subtitle_warnings.append(
            f"S{season_num}E{episode_num} {quality}: could not extract/host English subtitle — {fail_reason}"
        )

    for track in audio_tracks:
        lang = track["language"]
        if lang in already_done_langs:
            print(f"         Skipping duplicate language for E{episode_num}: {lang}")
            continue

        output_name = build_filename(series_name, season_num, episode_num, quality, lang)
        output_path = os.path.join(OUTPUT_FOLDER, output_name)

        try:
            remux_single_audio(source_path, output_path, track, [])  # subs handled via API below, not embedded
            folder_id = get_or_create_vidara_folder(series_name, season_num, lang)
            video_url, filecode = upload_to_vidara(output_path, output_name, folder_id)
            beam_upsert(jwt, tmdb_id, season_num, episode_num, quality, lang, video_url)
        except Exception as e:
            safe_delete(output_path)
            raise Exception(f"[E{episode_num} / {lang}] {e}")

        safe_delete(output_path)
        already_done_langs.add(lang)
        print(f"         [OK] S{int(season_num):02d}E{int(episode_num):02d} {lang} uploaded ({video_url}).")

        # Best-effort: attach every prepared English subtitle to this filecode.
        for sub_url in subtitle_urls:
            try:
                attach_subtitle_to_vidara(filecode, sub_url, sub_lang="English")
                print(f"         [SUB] Attached English caption to {filecode}")
            except Exception as e:
                warning = (
                    f"S{season_num}E{episode_num} {quality} [{lang}] video {video_url} (filecode {filecode}): "
                    f"video uploaded OK but subtitle attach failed ({e}). "
                    f"Download the caption yourself here: {sub_url}"
                )
                subtitle_warnings.append(warning)
                print(f"         [WARN] {warning}")


# ============================================================================
# CORE: process one Pipeline row
# ============================================================================

def process_single_row(jwt, tmdb_id, series_name, season, episode, quality, link):
    """One SINGLE-type link = one specific, already-known episode."""
    guessed_name = os.path.basename(link.split('?')[0]) or f"ep{episode}.mkv"
    temp_path = os.path.join(TEMP_FOLDER, f"{tmdb_id}_S{season}E{episode}_{quality}_{guessed_name}")

    if not download_file(link, temp_path):
        safe_delete(temp_path)
        raise Exception(("Download", "Episode download failed after retries"))

    subtitle_warnings = []
    try:
        process_episode_file(jwt, tmdb_id, series_name, season, episode, quality, temp_path, set(), subtitle_warnings)
    except Exception as e:
        safe_delete(temp_path)
        raise Exception(("Split/Upload", str(e)))

    safe_delete(temp_path)
    return 1, subtitle_warnings  # one episode handled


def process_zip_row(jwt, tmdb_id, series_name, season, quality, link):
    """
    One ZIP link may contain many episodes. Extract ONE video entry at a
    time (never the whole archive at once), process it, delete it, move on.
    Episode numbers are read from each entry's filename via regex — this is
    reliable because zip contents are properly named (unlike raw CDN URLs).
    """
    zip_path = os.path.join(TEMP_FOLDER, f"{tmdb_id}_S{season}_{quality}.zip")

    if not download_file(link, zip_path):
        safe_delete(zip_path)
        raise Exception(("Download", "ZIP download failed after retries"))

    if not zipfile.is_zipfile(zip_path):
        safe_delete(zip_path)
        raise Exception(("Download", "File is not a valid ZIP"))

    episodes_done = 0
    processed = {}  # episode_num -> set(languages already uploaded, this zip only)
    subtitle_warnings = []

    try:
        with zipfile.ZipFile(zip_path, 'r') as archive:
            video_entries = [
                f for f in archive.infolist()
                if f.filename.lower().endswith(VIDEO_EXTENSIONS)
                and not os.path.basename(f.filename).startswith('.')
            ]
            video_entries.sort(key=lambda f: natural_sort_key(f.filename))

            for pos, entry in enumerate(video_entries, start=1):
                entry_basename = os.path.basename(entry.filename)
                extract_target = os.path.join(TEMP_FOLDER, f"zipentry_{tmdb_id}_{quality}_{pos}_{entry_basename}")

                with archive.open(entry) as src, open(extract_target, "wb") as dst:
                    shutil.copyfileobj(src, dst)

                ep_num = get_episode_number_from_filename(entry_basename, fallback_ep=pos)
                done_langs = processed.setdefault(ep_num, set())

                try:
                    process_episode_file(jwt, tmdb_id, series_name, season, ep_num, quality, extract_target, done_langs, subtitle_warnings)
                except Exception as e:
                    safe_delete(extract_target)
                    safe_delete(zip_path)
                    raise Exception(("Split/Upload", str(e)))

                safe_delete(extract_target)
                episodes_done = len(processed)
    except Exception as e:
        safe_delete(zip_path)
        if isinstance(e.args[0], tuple):
            raise
        raise Exception(("ZIP Read", str(e)))

    safe_delete(zip_path)
    return episodes_done, subtitle_warnings


# ============================================================================
# MAIN
# ============================================================================

def main():
    print("=" * 60)
    print("BEAM SERIES DOWNLOADER v3 — STARTING")
    print("=" * 60)

    raw_json_str = os.environ.get("GOOGLE_SHEETS_JSON")
    if not raw_json_str:
        raise ValueError("GOOGLE_SHEETS_JSON is missing.")
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

    # ---- MASTER column map ----
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
    if missing:
        raise Exception(f"Series_Master missing columns: {missing}")

    mcols = {name: master_headers.index(name) + 1 for name in MASTER_REQUIRED}

    QUALITY_STATUS_COL = {
        "1080p": "DOWNLOAD_STATUS_1080p",
        "720p": "DOWNLOAD_STATUS_720p",
        "480p": "DOWNLOAD_STATUS_480p",
    }
    QUALITY_LINK_COL = {
        "1080p": "Link_1080p",
        "720p": "Link_720p",
        "480p": "Link_480p",
    }

    master_rows_by_key = {}
    for i, row_cells in enumerate(master_values[1:], start=2):
        padded = row_cells + [""] * (len(master_headers) - len(row_cells))
        row = {master_headers[j]: padded[j] for j in range(len(master_headers))}
        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        season = str(row.get("Season", "")).strip()
        if tmdb_id and season:
            master_rows_by_key[(tmdb_id, season)] = i

    # ---- PIPELINE column map ----
    pipeline_values = pipeline_sheet.get_all_values()
    if not pipeline_values:
        raise Exception("Series_Pipeline is empty (no header row).")
    pipeline_headers = [h.strip() for h in pipeline_values[0]]

    PIPELINE_REQUIRED = ["TMDB_ID", "TMDB_NAME", "Season", "Episode", "Quality",
                          "Input_Link", "Link_Type", "DOWNLOAD_STATUS", "Error"]
    missing_p = [h for h in PIPELINE_REQUIRED if h not in pipeline_headers]
    if missing_p:
        raise Exception(f"Series_Pipeline missing columns: {missing_p}. Expected: {PIPELINE_REQUIRED}")

    pcol = {name: pipeline_headers.index(name) + 1 for name in PIPELINE_REQUIRED}

    pipeline_rows = []
    for i, row_cells in enumerate(pipeline_values[1:], start=2):
        padded = row_cells + [""] * (len(pipeline_headers) - len(row_cells))
        row = {pipeline_headers[j]: padded[j] for j in range(len(pipeline_headers))}
        row["_row_idx"] = i
        pipeline_rows.append(row)

    print(f"Loaded {len(pipeline_rows)} Pipeline rows.\n")

    touched_groups = set()  # (tmdb_id, season, quality)

    # ---- Process every row not yet Done ----
    for row in pipeline_rows:
        row_idx = row["_row_idx"]
        status = str(row.get("DOWNLOAD_STATUS", "")).strip().lower()
        if status == "done":
            continue

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

        pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Running")

        try:
            if link_type == "SINGLE":
                _count, subtitle_warnings = process_single_row(jwt, tmdb_id, series_name, season, episode, quality, link)
            else:
                _count, subtitle_warnings = process_zip_row(jwt, tmdb_id, series_name, season, quality, link)

            pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Done")
            if subtitle_warnings:
                # Row still counts as Done (video uploaded fine) — Error just
                # notes which captions need manual attaching, with the direct
                # download link for each one.
                note = "DONE — but some subtitles need manual attach:\n\n" + "\n\n".join(subtitle_warnings)
                pipeline_sheet.update_cell(row_idx, pcol["Error"], note[:1500])
                print(f"    [DONE with subtitle warnings] Row {row_idx}")
            else:
                pipeline_sheet.update_cell(row_idx, pcol["Error"], "")
                print(f"    [DONE] Row {row_idx}")

        except Exception as e:
            stage, reason = e.args[0] if e.args and isinstance(e.args[0], tuple) else ("Unknown", str(e))
            error_text = format_error(episode if link_type == "SINGLE" else None, None, stage, reason)
            pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Failed")
            pipeline_sheet.update_cell(row_idx, pcol["Error"], error_text)
            print(f"    [FAILED] Row {row_idx}:\n{error_text}")

    # ---- Recompute Master quality status from ALL matching Pipeline rows ----
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
        if not matching:
            continue

        statuses = [str(r.get("DOWNLOAD_STATUS", "")).strip().lower() for r in matching]
        if any(s == "failed" for s in statuses):
            agg = "Failed"
        elif all(s == "done" for s in statuses):
            agg = "Done"
        else:
            agg = "Running"

        master_row_idx = master_rows_by_key.get((tmdb_id, season))
        if master_row_idx:
            status_col = mcols[QUALITY_STATUS_COL[quality]]
            master_sheet.update_cell(master_row_idx, status_col, agg)
            print(f"  Master ({tmdb_id}, S{season}, {quality}) -> {agg}")

    # ---- Archive rollup ----
    print(f"\n{'='*60}\nChecking rows for archiving...\n{'='*60}")

    master_values_fresh = master_sheet.get_all_values()
    to_archive = []

    for i, row_cells in enumerate(master_values_fresh[1:], start=2):
        padded = row_cells + [""] * (len(master_headers) - len(row_cells))
        row = {master_headers[j]: padded[j] for j in range(len(master_headers))}

        tmdb_id = str(row.get("TMDB_ID", "")).strip()
        season = str(row.get("Season", "")).strip()
        if not tmdb_id or not season:
            continue
        if (tmdb_id, season) not in touched_series_seasons:
            continue

        present_qualities = [q for q in ("1080p", "720p", "480p")
                              if str(row.get(QUALITY_LINK_COL[q], "")).strip()]
        if not present_qualities:
            continue

        all_done = all(
            str(row.get(QUALITY_STATUS_COL[q], "")).strip().lower() == "done"
            for q in present_qualities
        )

        if all_done:
            to_archive.append((i, row, tmdb_id, season))

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
                    # Row is Done, but still has an unresolved note (e.g. a
                    # subtitle that needs manual attaching). Keep it around
                    # instead of silently discarding that info — it just
                    # becomes a leftover reference row you can clear once
                    # you've handled it.
                    kept_for_warnings += 1
                    continue
                pipeline_sheet.delete_rows(j + 1)

        if kept_for_warnings:
            print(f"[OK] Archived. Kept {kept_for_warnings} Pipeline row(s) with unresolved warnings — check their Error column.")
        else:
            print(f"[OK] Archived and cleaned up.")

    try:
        shutil.rmtree(BASE_DIR, ignore_errors=True)
    except Exception:
        pass

    print(f"\n{'='*60}\nSERIES PIPELINE COMPLETE\n{'='*60}")


if __name__ == "__main__":
    main()
