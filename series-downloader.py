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

Subtitles are EMBEDDED directly into each output video (stream-copy, same
source file, no re-encode) — this is the actual delivery path. Vidara's
subtitle-attach API is not used (it was unreliable). English subtitle
tracks are also hosted on Archive.org + Litterbox purely as backup/manual
links, dropped into the Pipeline row's Error cell for reference.

Vidara folders are per (series, season, quality, language), e.g.
"Breaking Bad Season 1 1080p English" / "Breaking Bad Season 1 720p Hindi".
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
# subtitles so they have a shareable backup URL.
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

# Comprehensive ISO 639-1 language list (name + ISO 639-2/B code) so audio
# tracks in less-common languages (Indonesian, Thai, Hebrew, etc.) get
# properly identified instead of falling through to "Unknown".
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

def remux_single_audio(source_path, output_path, audio_track, subtitle_tracks, subtitle_srt_overrides=None):
    """
    Produces exactly one output file containing:
      - the original video stream
      - ONE specific audio stream (by its audio-only index)
      - all subtitle streams from this same source file (if any) - EMBEDDED
    All streams are stream-copied (-c copy) -> no quality loss, no re-encoding,
    EXCEPT subtitle streams present in `subtitle_srt_overrides` (a dict of
    subtitle stream_index -> path to a converted .srt file): those are read
    from a second input (the OCR'd SRT) and encoded as real text `srt`
    instead of copying the original bitmap (e.g. PGS/HDMV) stream. Later
    -c:s options override earlier global ones for that specific stream,
    so this is valid ffmpeg -map/-c syntax.
    """
    subtitle_srt_overrides = subtitle_srt_overrides or {}
    audio_stream_index = audio_track["stream_index"]
    audio_iso3 = iso3_for_language(audio_track["language"])

    cmd = ["ffmpeg", "-y", "-i", str(source_path)]

    # Extra -i inputs for any OCR'd SRT overrides, in subtitle_tracks order.
    override_input_idx = {}  # stream_index -> input index (1, 2, ...)
    next_input = 1
    for sub in subtitle_tracks:
        override_path = subtitle_srt_overrides.get(sub["stream_index"])
        if override_path and os.path.exists(override_path):
            cmd += ["-i", str(override_path)]
            override_input_idx[sub["stream_index"]] = next_input
            next_input += 1

    cmd += ["-map", "0:v:0", "-map", f"0:a:{audio_stream_index}"]

    for sub in subtitle_tracks:
        if sub["stream_index"] in override_input_idx:
            cmd += ["-map", f"{override_input_idx[sub['stream_index']]}:0"]
        else:
            cmd += ["-map", f"0:s:{sub['stream_index']}"]

    cmd += ["-c", "copy", "-map_chapters", "-1"]
    cmd += ["-metadata:s:a:0", f"language={audio_iso3}"]
    for out_idx, sub in enumerate(subtitle_tracks):
        sub_iso3 = iso3_for_language(sub["language"])
        if sub["stream_index"] in override_input_idx:
            # This stream comes from the OCR'd .srt input — encode as
            # text subtitle rather than stream-copying the bitmap codec.
            cmd += [f"-c:s:{out_idx}", "srt"]
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


def get_or_create_vidara_folder(series_name, season_num, quality, languages):
    """
    ONE folder per (series, season, quality) — e.g.
    'Game of Thrones Season 1 1080p Tam Tel Hin Eng'. `languages` is the
    full list of audio languages found for this episode; they're all
    joined (abbreviated) into a single folder name. Cache key deliberately
    does NOT include language, so every language for this quality reuses
    the exact same folder instead of creating a new one per language.
    """
    clean_name = clean_string_for_vidara(series_name)
    cache_key = (clean_name, int(season_num), quality)
    if cache_key in _folder_id_cache:
        return _folder_id_cache[cache_key]

    lang_str = " ".join(l[:3] for l in languages)
    folder_name = f"{clean_name} Season {int(season_num):02d} {quality} {lang_str}"
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
# SUBTITLES — extract English tracks, host them on Archive.org + Litterbox
# purely as backup/manual-reference copies. They are ALSO embedded directly
# into the video via remux_single_audio, so hosting them is not the
# delivery path anymore — just a convenience link dropped into the Error
# cell. PGS/other image-based subtitle codecs can't convert to SRT, so
# those fall back to a raw stream-copy (.sup) instead of being skipped.
# ============================================================================

def extract_subtitle_to_srt(source_path, subtitle_stream_index, output_srt_path):
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


def extract_subtitle_raw_copy(source_path, subtitle_stream_index, output_path):
    """
    Fallback for image-based subtitle codecs (PGS/HDMV, VobSub, etc.) that
    ffmpeg cannot convert to text-based SRT ("Subtitle encoding currently
    only possible from text to text or bitmap to bitmap"). These still
    stream-copy fine, so we pull the raw track out as-is (no conversion)
    into a .sup container — not human-readable directly, but still a usable
    backup (e.g. via SubtitleEdit/PgsToSrt locally). The same track is
    already embedded in the video regardless of whether this succeeds.
    """
    cmd = [
        "ffmpeg", "-y",
        "-i", str(source_path),
        "-map", f"0:s:{subtitle_stream_index}",
        "-c:s", "copy",
        str(output_path)
    ]
    result = subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    if result.returncode != 0 or not os.path.exists(output_path) or os.path.getsize(output_path) < 10:
        raise Exception(f"ffmpeg raw subtitle copy failed: {result.stderr[-300:] if result.stderr else 'unknown error'}")
    return True


def fix_common_ocr_errors(text):
    """
    Tesseract very commonly misreads a capital "I" as a pipe character —
    e.g. "| wanna catch him" instead of "I wanna catch him". This is a
    well-known failure mode (worse on lower-accuracy trained data), not
    random corruption. Movie/TV dialogue essentially never contains a
    literal "|", so a blanket replace here is safe and high-precision —
    used as a safety net on top of using the more accurate tessdata_best
    model (the real fix; this just catches whatever still slips through).
    """
    return text.replace("|", "I")


def ocr_pgs_from_source(source_path, language_code="en", timeout_seconds=600):
    """
    Runs pgsrip directly on the ORIGINAL media file — NOT a pre-extracted
    standalone .sup. pgsrip uses mkvextract internally to pull PGS tracks
    straight out of the container, which produces output its own
    type-detection recognizes. A manually ffmpeg-extracted .sup gets
    silently rejected by pgsrip's detector ("0 PGS subtitle collected"),
    even though the track itself is perfectly fine — the detection step,
    not the OCR step, was the actual problem with that approach.

    pgsrip names its output next to the source file as
    "<basename-without-ext>.<language_code>.srt" (e.g. "episode.en.srt"
    for "episode.mkv" with language_code="en"). Raises if that file never
    appears, or looks empty/corrupt, or the process times out.

    Kept strictly sequential (one file at a time, never run in parallel
    with other OCR jobs) — tesseract is CPU-heavy, so parallelizing this
    on a shared CI runner just causes contention and slows everything
    down rather than speeding it up.
    """
    cmd = ["pgsrip", "-l", language_code, str(source_path)]
    try:
        result = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, timeout=timeout_seconds
        )
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
    """
    Internet Archive item/bucket identifiers and S3 keys only allow
    alphanumerics, -, _, . — anything else gets collapsed to a dash.
    """
    text = re.sub(r'[^a-zA-Z0-9\-_.]', '-', text or "")
    text = re.sub(r'-+', '-', text).strip('-_.')
    return (text.lower() or "item")[:max_len]


def upload_to_archive_org(file_path, bucket_hint, key_hint, content_type="application/x-subrip", extension="srt", wait_seconds=60):
    """
    Uploads via Internet Archive's S3-compatible endpoint. `bucket_hint`
    should be something stable per show+season so multiple subtitle files
    land in the same IA "item" instead of creating a new one per file.
    x-amz-auto-make-bucket creates that item automatically if it doesn't
    exist yet. Storage is free and permanent — no expiry to manage.
    """
    bucket = slugify_for_ia(f"beamplay-subs-{bucket_hint}")
    key = slugify_for_ia(key_hint) + f".{extension}"
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

    print(f"         [WARN] Archive.org file not confirmed reachable after {wait_seconds}s, proceeding anyway: {direct_url}")
    return direct_url


LITTERBOX_API = "https://litterbox.catbox.moe/resources/internals/api.php"


def upload_to_litterbox(file_path, expire="72h"):
    """
    Second hosting copy, uploaded alongside Archive.org (not just as a
    fallback-on-exception). Free, no-signup, temporary (72h).
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


def host_subtitle_everywhere(sub_path, bucket_hint, key_hint, content_type="application/x-subrip", extension="srt"):
    """
    Hosts the same file on BOTH Archive.org and Litterbox (not one-then-
    fallback) — whichever succeed get returned as a list of (url, host)
    pairs. Raises only if BOTH hosts fail.
    """
    hosted = []
    errors = []

    try:
        url = upload_to_archive_org(sub_path, bucket_hint, key_hint, content_type=content_type, extension=extension)
        hosted.append((url, "Archive.org"))
    except Exception as e:
        errors.append(f"Archive.org: {e}")

    try:
        url = upload_to_litterbox(sub_path)
        hosted.append((url, "Litterbox"))
    except Exception as e:
        errors.append(f"Litterbox: {e}")

    if not hosted:
        raise Exception(" | ".join(errors))

    return hosted


def prepare_english_subtitle_urls(source_path, subtitle_tracks, bucket_hint, tmp_prefix):
    """
    Extracts every subtitle track normalized to 'English', converts it to a
    real text .srt (running it through pgsrip OCR first if the source
    codec is image-based like PGS/HDMV), and hosts each resulting .srt on
    BOTH Archive.org and Litterbox for a shareable backup link. The exact
    same .srt file is also handed back via `srt_overrides` so the caller
    can embed the readable text subtitle into the video itself instead of
    the original bitmap stream.

    OCR is run strictly one track at a time (never in parallel) to avoid
    piling up tesseract/mkvtoolnix processes on a shared CI runner.

    Returns (candidates, failure_reasons, srt_overrides):
      - candidates: [{"hosts": [(url, host_name), ...], "format": "srt" | "srt (OCR)"}]
      - failure_reasons: ["track #N: reason", ...] — only when hosting AND
        every conversion path (native srt, OCR, raw fallback) failed.
      - srt_overrides: {subtitle_stream_index: path_to_srt_on_disk, ...} —
        caller is responsible for deleting these paths when done (see
        process_episode_file's `finally` cleanup).
    """
    candidates = []
    failures = []
    srt_overrides = {}
    english_tracks = [s for s in subtitle_tracks if s["language"] == "English"]
    if not english_tracks:
        return candidates, failures, srt_overrides

    whole_file_ocr_tried = False
    whole_file_ocr_srt = None
    whole_file_ocr_error = None

    for idx, sub in enumerate(english_tracks):
        srt_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.srt")
        sup_path = os.path.join(TEMP_FOLDER, f"{tmp_prefix}_sub{idx}.sup")

        try:
            # Fast path: source codec is already text-based (SRT/ASS/etc.)
            extract_subtitle_to_srt(source_path, sub["stream_index"], srt_path)
            hosted = host_subtitle_everywhere(srt_path, bucket_hint, f"{tmp_prefix}_sub{idx}")
            candidates.append({"hosts": hosted, "format": "srt"})
            srt_overrides[sub["stream_index"]] = srt_path  # kept, not deleted here
            for url, host in hosted:
                print(f"         [SUB] English subtitle #{idx+1} (srt) hosted via {host} -> {url}")
            continue
        except Exception as srt_err:
            safe_delete(srt_path)

        # Image-based track — OCR the whole episode file once, reuse the
        # result for any further English PGS tracks in this same episode.
        if not whole_file_ocr_tried:
            whole_file_ocr_tried = True
            try:
                produced_path = ocr_pgs_from_source(source_path, language_code="en")
                shutil.copy(produced_path, srt_path)
                safe_delete(produced_path)  # pgsrip's own output, next to source — clean it up
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
                srt_overrides[sub["stream_index"]] = srt_path  # kept, not deleted here
                for url, host in hosted:
                    print(f"         [SUB] English subtitle #{idx+1} (OCR'd from PGS) hosted via {host} -> {url}")
                continue
            except Exception as host_err:
                safe_delete(srt_path)
                failures.append(f"track #{idx+1}: OCR succeeded but hosting failed ({host_err})")
                continue

        # OCR unavailable/failed — last resort: raw, unconverted backup.
        try:
            extract_subtitle_raw_copy(source_path, sub["stream_index"], sup_path)
            hosted = host_subtitle_everywhere(
                sup_path, bucket_hint, f"{tmp_prefix}_sub{idx}",
                content_type="application/octet-stream", extension="sup"
            )
            candidates.append({"hosts": hosted, "format": "sup (OCR failed, raw)"})
            for url, host in hosted:
                print(f"         [SUB] English subtitle #{idx+1} (raw .sup, OCR failed) hosted via {host} -> {url}")
        except Exception as raw_err:
            failures.append(f"track #{idx+1}: srt failed ({srt_err}); OCR failed ({whole_file_ocr_error}); raw backup failed ({raw_err})")
            print(f"         [WARN] Could not prepare English subtitle #{idx+1} via any method")
        finally:
            safe_delete(sup_path)

    return candidates, failures, srt_overrides


def beam_login():
    res = requests.post(f"{BEAM_WORKER_URL}/auth/login", json={
        "email": ADMIN_EMAIL,
        "password": ADMIN_PASSWORD
    }, timeout=30)
    res.raise_for_status()
    return res.json()["token"]


def beam_upsert(jwt, tmdb_id, season, episode, quality, language, url,
                 max_attempts=5, base_delay=2):
    """
    Registers the uploaded episode with BEAM. The worker intermittently
    throws transient 500s under load, so this retries with exponential
    backoff (2s, 4s, 8s, 16s, 32s) on 5xx / network errors before giving
    up. 4xx errors (bad request, auth, etc.) are not retried — those won't
    fix themselves by waiting.

    Raises only after all attempts are exhausted; the caller decides
    whether that should be fatal (it no longer is — see
    process_episode_file, which treats this as non-fatal since the video
    is already uploaded and live regardless of DB registration).
    """
    last_err = None
    for attempt in range(1, max_attempts + 1):
        try:
            res = requests.post(f"{BEAM_WORKER_URL}/admin/vidara/upsert", json={
                "content_type": "episode",
                "tmdb_id": int(tmdb_id),
                "season": int(season),
                "episode": int(episode),
                "url": url,
                "quality": quality,
                "audio_languages": [language]
            }, headers={"Authorization": f"Bearer {jwt}"}, timeout=30)

            if res.status_code >= 500:
                raise Exception(f"{res.status_code} Server Error: {res.text[:200]}")
            res.raise_for_status()
            return res.json()

        except requests.exceptions.HTTPError as e:
            # 4xx — won't fix itself by retrying.
            raise Exception(f"BEAM upsert rejected (not retrying): {e}")

        except Exception as e:
            last_err = e
            if attempt == max_attempts:
                break
            delay = base_delay * (2 ** (attempt - 1))
            print(f"         [BEAM] upsert attempt {attempt}/{max_attempts} failed ({e}); retrying in {delay}s...")
            time.sleep(delay)

    raise Exception(f"BEAM upsert failed after {max_attempts} attempts: {last_err}")


def download_file(url, dest_path):
    cmd = [
        "aria2c",
        "-x", "16",              # max connections PER SERVER (was 8) — most
                                  # slowdowns on hosts like this are per-
                                  # connection throttling, not actual
                                  # runner bandwidth, so more parallel
                                  # connections is what speeds this up.
        "-s", "16",               # split file into 16 pieces (was 8)
        "-j", "16",               # max concurrent downloads (keeps aria2c
                                  # from capping itself below -x/-s)
        "-k", "1M",               # smaller min split size (was 5M) so 16
                                  # connections can actually be used
        "--file-allocation=none",
        "--summary-interval=0",
        "--retry-wait=5",        # was 10 — retry faster on transient drops
        "--max-tries=8",
        "--timeout=45",
        "--connect-timeout=15",
        "--auto-file-renaming=false",
        "--disable-ipv6=true",
        "--max-connection-per-server=16",
        "--min-split-size=1M",
        "--user-agent=Mozilla/5.0",
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


# Google Sheets' real per-cell character limit is 50,000. The old 1500
# limit was truncating episode/subtitle links mid-URL. Keep a small
# safety margin under the real ceiling rather than the old cap.
SHEET_CELL_CHAR_LIMIT = 49000


def format_error(episode, language, stage, reason):
    """
    Compact single-line format so multiple episodes' worth of notes/links
    can share a cell without one bulky entry eating the whole 50k budget.
    e.g. "E7 [English] FAILED @ Split/Upload: 500 Server Error ..."
    """
    ep_part = f"E{episode} " if episode is not None else ""
    lang_part = f"[{language}] " if language else ""
    return f"{ep_part}{lang_part}FAILED @ {stage}: {reason}"[:SHEET_CELL_CHAR_LIMIT]


# ============================================================================
# CORE: split one already-on-disk video file into per-language uploads
# ============================================================================

def process_episode_file(jwt, tmdb_id, series_name, season_num, episode_num,
                          quality, source_path, already_done_langs, subtitle_notes):
    """
    `already_done_langs` is a set (mutated in place) of languages already
    uploaded for THIS episode within THIS row's scope. Raises on failure.
    `subtitle_notes` is a list (mutated in place) of human-readable notes
    with the hosted Archive.org / Litterbox backup links for the English
    subtitle(s) found in this episode — for manual reference. Subtitles are
    already embedded in the uploaded video itself, so this is not a failure
    indicator, just a convenience record.
    """
    audio_tracks, subtitle_tracks = inspect_tracks(source_path)
    print(f"         Audio: {[a['language'] for a in audio_tracks]}"
          + (f" | Subs: {[s['language'] for s in subtitle_tracks]}" if subtitle_tracks else ""))

    # Host every English subtitle track from this episode as backup copies
    # (Archive.org + Litterbox), converting image-based (PGS/HDMV) tracks
    # to real text SRT via OCR first. `subtitle_srt_overrides` maps
    # subtitle stream_index -> path of a converted .srt file, so the remux
    # step below can embed the readable SRT instead of the original
    # bitmap subtitle for those specific streams. Compact per-episode
    # note format keeps the Error/notes cell from being flooded with text
    # and having links truncated.
    subtitle_candidates, prep_failures, subtitle_srt_overrides = prepare_english_subtitle_urls(
        source_path, subtitle_tracks, f"{tmdb_id}-s{season_num}", f"{tmdb_id}_S{season_num}E{episode_num}_{quality}"
    )
    # One folder for this whole quality — built from every language found
    # in THIS episode's audio tracks. Only the first episode to reach this
    # for a given (series, season, quality) actually creates the folder;
    # every later episode/language reuses that same folder_id via the cache.
    all_langs_this_episode = []
    for t in audio_tracks:
        if t["language"] not in all_langs_this_episode:
            all_langs_this_episode.append(t["language"])
    folder_id = get_or_create_vidara_folder(series_name, season_num, quality, all_langs_this_episode)

    try:
        if subtitle_candidates:
            links_all = []
            for candidate in subtitle_candidates:
                links_all.extend(url for url, _host in candidate["hosts"])
            if links_all:
                subtitle_notes.append(f"E{episode_num}: " + " | ".join(links_all))
        for fail_reason in prep_failures:
            subtitle_notes.append(f"E{episode_num}: subtitle prep failed — {fail_reason}")

        for track in audio_tracks:
            lang = track["language"]
            if lang in already_done_langs:
                print(f"         Skipping duplicate language for E{episode_num}: {lang}")
                continue

            output_name = build_filename(series_name, season_num, episode_num, quality, lang)
            output_path = os.path.join(OUTPUT_FOLDER, output_name)

            try:
                # Embed ALL subtitle tracks from this same source file
                # directly into the output (stream-copy, no re-encode).
                # This is the actual delivery mechanism for captions now.
                # Any OCR'd (PGS -> SRT) tracks get swapped in here
                # instead of the raw bitmap stream.
                remux_single_audio(source_path, output_path, track, subtitle_tracks, subtitle_srt_overrides)
                video_url, filecode = upload_to_vidara(output_path, output_name, folder_id)
            except Exception as e:
                # Fatal: the video itself never made it up. Nothing to
                # register with BEAM, nothing to keep on disk.
                safe_delete(output_path)
                raise Exception(f"[E{episode_num} / {lang}] {e}")

            # The video is uploaded and live on Vidara at this point —
            # that's the part that matters. BEAM registration (the
            # metadata/DB sync) is handled separately and is NOT allowed
            # to roll back or delete the already-uploaded video. If it
            # fails even after retries, we just log a note so it can be
            # registered manually later; we don't abort the rest of the
            # episode/row over it.
            try:
                beam_upsert(jwt, tmdb_id, season_num, episode_num, quality, lang, video_url)
            except Exception as beam_err:
                subtitle_notes.append(
                    f"E{episode_num} {lang}: live ({video_url}) DB-pending — {beam_err}"
                )
                print(f"         [WARN] BEAM registration failed for E{episode_num} {lang}, "
                      f"video stays live: {beam_err}")

            safe_delete(output_path)
            already_done_langs.add(lang)
            print(f"         [OK] S{int(season_num):02d}E{int(episode_num):02d} {lang} uploaded (with embedded subtitles) ({video_url}).")
    finally:
        # Clean up any temporary OCR'd .srt files now that both the
        # hosting step and every per-language remux/embed has finished
        # with them.
        for _idx, override_path in (subtitle_srt_overrides or {}).items():
            safe_delete(override_path)


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

    subtitle_notes = []
    try:
        process_episode_file(jwt, tmdb_id, series_name, season, episode, quality, temp_path, set(), subtitle_notes)
    except Exception as e:
        safe_delete(temp_path)
        raise Exception(("Split/Upload", str(e)))

    safe_delete(temp_path)
    return 1, subtitle_notes  # one episode handled


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
    subtitle_notes = []

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
                    process_episode_file(jwt, tmdb_id, series_name, season, ep_num, quality, extract_target, done_langs, subtitle_notes)
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
    return episodes_done, subtitle_notes


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
                _count, subtitle_notes = process_single_row(jwt, tmdb_id, series_name, season, episode, quality, link)
            else:
                _count, subtitle_notes = process_zip_row(jwt, tmdb_id, series_name, season, quality, link)

            pipeline_sheet.update_cell(row_idx, pcol["DOWNLOAD_STATUS"], "Done")
            if subtitle_notes:
                # Row is Done either way (subtitles are embedded). These
                # notes are just the backup Archive.org/Litterbox links
                # (compact "E<n>: url | url" per episode) plus any
                # DB-registration warnings, one per line — no filler text,
                # so far more episodes/links fit before hitting the cell
                # character limit.
                note = "\n".join(subtitle_notes)
                pipeline_sheet.update_cell(row_idx, pcol["Error"], note[:SHEET_CELL_CHAR_LIMIT])
                print(f"    [DONE with notes] Row {row_idx}")
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
                    # subtitle backup link worth keeping visible). Keep it
                    # around instead of silently discarding that info.
                    kept_for_warnings += 1
                    continue
                pipeline_sheet.delete_rows(j + 1)

        if kept_for_warnings:
            print(f"[OK] Archived. Kept {kept_for_warnings} Pipeline row(s) with unresolved notes — check their Error column.")
        else:
            print(f"[OK] Archived and cleaned up.")

    try:
        shutil.rmtree(BASE_DIR, ignore_errors=True)
    except Exception:
        pass

    print(f"\n{'='*60}\nSERIES PIPELINE COMPLETE\n{'='*60}")


if __name__ == "__main__":
    main()
