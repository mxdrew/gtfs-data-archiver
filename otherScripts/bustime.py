#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 30, 2026 (Last updated June 30, 2026)
#
# Description:
# Dedicated ingestion engine for Clever Devices BusTime® Developer API v3.
# Uses stateful session mapping to pull independent streams (Routes, Stops, Vehicles, ETAs)
# for configured multi-feed agencies and maps them to standard Data Lake GTFS formats.
#
# Data Integrity & Storage:
# Implements an append-only JSONL write path with deterministic SHA-256 hashes.
# A background compactor rotates completed day files into Parquet archives under data/archive/.
# Live polling workers remain isolated from file rotation and compaction work.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   event   — add | update | remove | reset | snapshot | error
#   data    — full API payload (JSON string in Parquet, raw dict in JSONL)

import io
import json
import hashlib
import os
import random
import select
import threading
import time
import sys
try:
    import termios
except ImportError:
    termios = None
try:
    import tty
except ImportError:
    tty = None
import zipfile
try:
    import msvcrt
except ImportError:
    msvcrt = None
from datetime import datetime, timedelta
from pathlib import Path
from queue import Empty, Queue
from zoneinfo import ZoneInfo
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import pandas as pd
from rich.live import Live
from rich.table import Table
from rich.console import Console, Group
from rich.panel import Panel
from rich.text import Text
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

load_dotenv(Path(__file__).with_name(".env"))

#### CONFIGURATION ####

# Environment helpers.
def env_text(name, default=""):
    value = os.environ.get(name)
    if value is None:
        value = default
    return str(value).strip()


def env_required(name):
    value = env_text(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

def env_int(name, default):
    raw_value = env_text(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default

# 1. BusTime agencies.
BUSTIME_AGENCY = env_required("BUSTIME_AGENCY").upper()
TARGET_AGENCIES = {
    BUSTIME_AGENCY: {
        "base_url": env_required("BUSTIME_BASE_URL"),
        "api_key": env_required("BUSTIME_API_KEY"),
    }
}

# 2. BusTime feed names.
BUSTIME_FEEDS = (
    "Agencies",
    "Locales",
    "RealTimeDataFeeds",
    "Routes",
    "Directions",
    "Stops",
    "Patterns",
    "Vehicles",
    "Predictions",
    "ServiceBulletins",
    "Detours",
    "EnhancedDetours",
    "BusBridges"
)

# Maps BusTime endpoints directly to GTFS structural equivalents.
GTFS_MAP = {
    "Agencies": "agency",
    "Locales": "localized_strings",
    "RealTimeDataFeeds": "feed_info",
    "Routes": "routes",
    "Directions": "routes",
    "Stops": "stops",
    "Patterns": "shapes",
    "Vehicles": "vehicle_positions",
    "Predictions": "trip_updates",
    "ServiceBulletins": "service_alerts",
    "Detours": "service_alerts",
    "EnhancedDetours": "service_alerts",
    "BusBridges": "service_alerts"
}

SCRIPT_NAME = "BusTime"

def safe_prefix(value):
    cleaned_value = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return cleaned_value.strip("_") or "stream"

def gtfs_file_stem(stream):
    return GTFS_MAP.get(stream, safe_prefix(stream)).lower()

def default_data_dir():
    def is_wsl():
        try:
            if os.name != "posix":
                return False
            with open("/proc/version", "r", encoding="utf-8") as f:
                v = f.read()
            return "microsoft" in v.lower() or "wsl" in v.lower()
        except Exception:
            return bool(os.environ.get("WSL_INTEROP") or os.environ.get("WSLENV"))

    if is_wsl():
        return Path(env_required("DATA_DIR_WSL"))
    return Path(env_required("DATA_DIR"))

ARGS = SimpleNamespace(
    no_compaction=False,
    global_concurrency=12,
    poll_jitter=1.25,
)

LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE"))
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
CACHE_DIR = DATA_DIR / "cache"
LOG_FILE = LOG_DIR / "bustime_ingest.log"

for directory in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR, CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
# Polling intervals are governed by the GTFS state machine.
POLL_INTERVAL_ACTIVE = env_int("POLL_INTERVAL_ACTIVE", 300)       # 5 min during service
POLL_INTERVAL_PREFLIGHT = env_int("POLL_INTERVAL_PREFLIGHT", 300)  # 5 min pre-flight
POLL_INTERVAL_EOD = env_int("POLL_INTERVAL_EOD", 600)            # 10 min end-of-service
# Request timeout and route batch size are fixed, but can be updated here.
REQUEST_TIMEOUT_SECONDS = 12
BATCH_SIZE = 10
MAX_WORKERS = max(1, ARGS.global_concurrency)

# GTFS static feed used to derive the operational window.
GTFS_FEED_URL = env_required("GTFS_FEED_URL")

# Clever Devices developer limits.
API_LIMIT_MAX_REQUESTS_PER_DAY = 10000
VERBOSE_LOGGING = env_int("VERBOSE_LOGGING", 1)

_API_CALLS_TRACKER_LOCK = threading.Lock()
_API_CALLS_TODAY = {}
UNSUPPORTED_ENDPOINTS = {}

#### GTFS STATE MACHINE ####
# States: STARTUP -> (OFF_HOURS | PRE_FLIGHT | ACTIVE | END_OF_SERVICE)
_SYSTEM_STATE = "STARTUP"
_STATE_LOCK = threading.Lock()
_T_MIN = None   # seconds since midnight – earliest stop time
_T_MAX = None   # seconds since midnight – latest stop time
_GTFS_READY = threading.Event()

def get_system_state():
    with _STATE_LOCK:
        return _SYSTEM_STATE, _T_MIN, _T_MAX

def set_system_state(new_state):
    global _SYSTEM_STATE
    with _STATE_LOCK:
        if _SYSTEM_STATE == new_state:
            return
        _SYSTEM_STATE = new_state
    log(f"State transition → {new_state}", level="info", agency="SYSTEM")

def get_poll_interval():
    with _STATE_LOCK:
        state = _SYSTEM_STATE
    return {
        "STARTUP":        60,
        "PRE_FLIGHT":     POLL_INTERVAL_PREFLIGHT,
        "ACTIVE":         POLL_INTERVAL_ACTIVE,
        "END_OF_SERVICE": POLL_INTERVAL_EOD,
        "OFF_HOURS":      None,
    }.get(state, POLL_INTERVAL_ACTIVE)

def time_to_seconds(t_str):
    """Parse HH:MM:SS → seconds since midnight. Handles GTFS times > 24:00:00."""
    parts = str(t_str).strip().split(":")
    h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
    return h * 3600 + m * 60 + s

def seconds_since_midnight():
    now = datetime.now(LOCAL_TZ)
    return now.hour * 3600 + now.minute * 60 + now.second

def secs_to_hhmmss(secs):
    secs = int(secs) % 86400
    h, rem = divmod(secs, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def fetch_gtfs_operational_window():
    """Download the GTFS zip and parse stop_times.txt for T_min / T_max."""
    global _T_MIN, _T_MAX
    try:
        log(f"Downloading GTFS feed to determine operational window: {GTFS_FEED_URL}", level="info", agency="GTFS")
        resp = requests.get(GTFS_FEED_URL, timeout=30)
        resp.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
            names = zf.namelist()
            if "stop_times.txt" not in names:
                log("stop_times.txt not found in GTFS zip", level="error", agency="GTFS")
                return False
            t_min = float("inf")
            t_max = 0
            with zf.open("stop_times.txt") as raw:
                for chunk in pd.read_csv(raw, usecols=["arrival_time", "departure_time"],
                                         chunksize=10000, dtype=str):
                    for col in ("arrival_time", "departure_time"):
                        for val in chunk[col].dropna():
                            try:
                                secs = time_to_seconds(val)
                                if secs < t_min: t_min = secs
                                if secs > t_max: t_max = secs
                            except Exception:
                                pass
        if t_min == float("inf") or t_max == 0:
            log("Could not determine operational window from stop_times.txt", level="error", agency="GTFS")
            return False
        with _STATE_LOCK:
            _T_MIN = int(t_min)
            _T_MAX = int(t_max)
        log(f"Parsed stop_times.txt. Earliest departure: {secs_to_hhmmss(t_min)} | Latest arrival: {secs_to_hhmmss(t_max)}", level="info", agency="GTFS")
        _GTFS_READY.set()
        return True
    except Exception as exc:
        log(f"Failed to fetch GTFS feed: {exc}", level="error", agency="GTFS")
        return False

def update_state_from_time():
    """Update the GTFS state machine from the configured service window."""
    with _STATE_LOCK:
        t_min = _T_MIN
        t_max = _T_MAX

    if t_min is not None and t_max is not None:
        now_secs = seconds_since_midnight()
        preflight_start = t_min - 3600   # 1 h before service
        eod_end        = t_max + 3600    # 1 h after service

        if preflight_start <= now_secs < t_min:
            set_system_state("PRE_FLIGHT")
        elif t_min <= now_secs <= t_max:
            set_system_state("ACTIVE")
        elif t_max < now_secs <= eod_end:
            set_system_state("END_OF_SERVICE")
        else:
            set_system_state("OFF_HOURS")

def gtfs_state_machine_loop():
    """Background thread: evaluates current time vs T_min/T_max and transitions state."""
    last_fetch_day = datetime.now(LOCAL_TZ).day

    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)

        # Re-fetch GTFS once per day at midnight.
        if now.day != last_fetch_day and now.hour == 0:
            fetch_gtfs_operational_window()
            last_fetch_day = now.day

        update_state_from_time()
        sleep_with_stop(30)

def track_api_call(agency):
    """
    Tracks API calls to ensure compliance with Clever Devices' 10,000/day limit.
    Returns False if the daily limit has been breached to safely block the request.
    Persists the running count to the cache file every 10 calls.
    """
    today_str = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
    should_save = False
    with _API_CALLS_TRACKER_LOCK:
        if agency not in _API_CALLS_TODAY:
            _API_CALLS_TODAY[agency] = {"date": today_str, "count": 0}
            
        if _API_CALLS_TODAY[agency]["date"] != today_str:
            _API_CALLS_TODAY[agency] = {"date": today_str, "count": 0}
            
        _API_CALLS_TODAY[agency]["count"] += 1
        count = _API_CALLS_TODAY[agency]["count"]

        if count % 10 == 0:
            should_save = True
        
        if count > API_LIMIT_MAX_REQUESTS_PER_DAY:
            if count == API_LIMIT_MAX_REQUESTS_PER_DAY + 1:
                log(f"HARD LIMIT BREACHED: Over {API_LIMIT_MAX_REQUESTS_PER_DAY} calls today. Polling suspended until midnight.", level="error", agency=agency)
            return False
        
    if should_save:
        threading.Thread(target=save_cache, daemon=True).start()
    return True

write_queue = Queue(maxsize=50000)
stop_event = threading.Event()
TUI_READY = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []
HOURLY_REFRESH_FLAG = threading.Event()

AGENCY_SESSIONS = {}
SESSION_LOCK = threading.Lock()

#### CACHE MANAGEMENT ####
BUSTIME_CACHE_FILE = CACHE_DIR / "bustime_cache.json"

BUSTIME_CACHE = {
    "route_ids": {},
    "vid_ids": {},
    "active_detours": {},
    "static_last_fetch": {}
}

_CACHE_LOCK = threading.Lock()

def load_cache():
    try:
        if not BUSTIME_CACHE_FILE.exists():
            log(f"No cache file found at {BUSTIME_CACHE_FILE}; initializing fresh.", level="info", agency="SYSTEM")
            save_cache()
            return
        with open(BUSTIME_CACHE_FILE, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        with _CACHE_LOCK:
            if "route_ids" in saved:
                for aid, rids in saved["route_ids"].items():
                    BUSTIME_CACHE.setdefault("route_ids", {})[aid] = set(str(r) for r in rids)
            if "vid_ids" in saved:
                for aid, vids in saved["vid_ids"].items():
                    BUSTIME_CACHE.setdefault("vid_ids", {})[aid] = set(str(v) for v in vids)
            if "active_detours" in saved:
                for aid, dtrs in saved["active_detours"].items():
                    BUSTIME_CACHE.setdefault("active_detours", {})[aid] = {str(rt): set(str(d) for d in ds) for rt, ds in dtrs.items()}
            if "static_last_fetch" in saved:
                for aid, fetches in saved["static_last_fetch"].items():
                    BUSTIME_CACHE.setdefault("static_last_fetch", {})[aid] = fetches

        # Restore daily API call counts and unsupported lockouts.
        with _API_CALLS_TRACKER_LOCK:
            if "unsupported" in saved:
                UNSUPPORTED_ENDPOINTS.update(saved["unsupported"])
                
            if "daily_api_calls" in saved:
                today_str = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
                for agency, data in saved["daily_api_calls"].items():
                    if data.get("date") == today_str:
                        _API_CALLS_TODAY[agency] = {
                            "date": data["date"],
                            "count": int(data.get("count", 0)),
                        }

        log(f"Cache loaded successfully.", level="info", agency="SYSTEM")
    except Exception as exc:
        log(f"Could not load cache: {exc}", level="error", agency="SYSTEM")
        save_cache()

def save_cache():
    try:
        with _CACHE_LOCK:
            payload = {"route_ids": {}, "vid_ids": {}, "active_detours": {}}
            for aid, rids in BUSTIME_CACHE.get("route_ids", {}).items():
                payload["route_ids"][aid] = sorted(list(rids))
            for aid, vids in BUSTIME_CACHE.get("vid_ids", {}).items():
                payload["vid_ids"][aid] = sorted(list(vids))
            for aid, dtrs in BUSTIME_CACHE.get("active_detours", {}).items():
                payload["active_detours"][aid] = {rt: sorted(list(ds)) for rt, ds in dtrs.items()}
            payload["static_last_fetch"] = BUSTIME_CACHE.get("static_last_fetch", {})

        with _API_CALLS_TRACKER_LOCK:
            payload["daily_api_calls"] = {
                agency: {"date": data["date"], "count": data["count"]}
                for agency, data in _API_CALLS_TODAY.items()
            }
            payload["unsupported"] = UNSUPPORTED_ENDPOINTS
        temp_path = BUSTIME_CACHE_FILE.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        temp_path.replace(BUSTIME_CACHE_FILE)
    except Exception as exc:
        log(f"Could not save cache: {exc}", level="error", agency="SYSTEM")


#### LOGGING ####
_log_lock = threading.Lock()

def log(message, level="info", agency="SYSTEM"):
    now = datetime.now(LOCAL_TZ)
    ts_full = now.strftime("%Y-%m-%d %H:%M:%S")
    ts_short = now.strftime("%H:%M:%S")
    
    if level == "error":
        try:
            with _log_lock:
                with open(LOG_FILE, "a", encoding="utf-8") as handle:
                    handle.write(f"{ts_full} [{level.upper():7s}] [{agency}] {message}\n")
        except Exception:
            pass

    if level in ("error", "warning"):
        with UI_STATE_LOCK:
            RECENT_ERRORS.append({"dt": now, "ts_str": ts_short, "message": f"[{agency}] {message}"})
            if len(RECENT_ERRORS) > 50:
                RECENT_ERRORS.pop(0)

    if level == "error":
        try:
            err_record = {
                "hash_id": hashlib.sha256(f"{agency}|error|{ts_full}|{message}".encode("utf-8")).hexdigest(),
                "ts": ts_full,
                "agency": agency,
                "stream": "Errors",
                "event": "error",
                "endpoint": "",
                "data": {"message": str(message)},
            }
            write_queue.put(err_record, timeout=0.1)
        except Exception:
            pass

    # No-TTY / Docker mode: mirror all log messages to stderr (skip verbose)
    if not sys.stdout.isatty() and level not in ("verbose", "VERBOSE"):
        print(f"{ts_full} [{level.upper():7s}] [{agency}] {message}", file=sys.stderr)

#### NETWORK CONSUMERS ####
def get_agency_session(agency):
    with SESSION_LOCK:
        if agency not in AGENCY_SESSIONS:
            session = requests.Session()
            retry = Retry(total=3, connect=3, read=3, status_forcelist=(429, 500, 502, 503, 504))
            adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:151.0) Gecko/20100101 Firefox/151.0",
                "Accept": "application/json",
            })
            AGENCY_SESSIONS[agency] = session
        return AGENCY_SESSIONS[agency]

def sleep_with_stop(seconds):
    deadline = time.time() + max(0, seconds)
    while not stop_event.is_set() and time.time() < deadline:
        time.sleep(0.25)

def normalize_jsonable(value):
    if isinstance(value, dict):
        return {k: normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, list):
        return [normalize_jsonable(i) for i in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value

def chunked(values, size):
    size = max(1, int(size))
    for index in range(0, len(values), size):
        yield values[index:index + size]

def check_unsupported(agency, endpoint):
    """Returns True if the endpoint was historically unsupported within the last 12 hours."""
    with _API_CALLS_TRACKER_LOCK:
        if agency in UNSUPPORTED_ENDPOINTS and endpoint in UNSUPPORTED_ENDPOINTS[agency]:
            if time.time() < UNSUPPORTED_ENDPOINTS[agency][endpoint]:
                return True
    return False

def mark_unsupported(agency, endpoint, msg, res_url):
    """Silences logs for unsupported endpoints for 12 hours to prevent log flooding."""
    with _API_CALLS_TRACKER_LOCK:
        agency_unsup = UNSUPPORTED_ENDPOINTS.setdefault(agency, {})
        if endpoint not in agency_unsup or time.time() >= agency_unsup[endpoint]:
            # Log only when it is newly marked or its 12-hour timeout expired.
            log(f"API Error (Unsupported Endpoint) on {res_url}: {msg}", level="warning", agency=agency)
        agency_unsup[endpoint] = time.time() + (12 * 3600)

def bustime_request(agency, endpoint, **params):
    """Makes a request to the Clever Devices API v3 and evaluates standardized errors."""
    if check_unsupported(agency, endpoint):
        return "UNSUPPORTED"

    config = TARGET_AGENCIES.get(agency)
    if not config: return None
    
    if not track_api_call(agency):
        return None
    
    session = get_agency_session(agency)
    url = f"{config['base_url'].rstrip('/')}/{endpoint}"
    
    req_params = {"key": config["api_key"], "format": "json"}
    req_params.update(params)
    
    try:
        res = session.get(url, params=req_params, timeout=REQUEST_TIMEOUT_SECONDS)
        
        if VERBOSE_LOGGING:
            log(f"GET {res.url}", level="verbose", agency=agency)
            
        if res.status_code != 200:
            log(f"HTTP {res.status_code} for {res.url}", level="error", agency=agency)
            return None
        
        try:
            data = res.json()
        except ValueError:
            log(f"Invalid JSON returned for {res.url}", level="error", agency=agency)
            return None
            
        if "bustime-response" in data:
            br = data["bustime-response"]
            if "error" in br:
                err = br["error"]
                if isinstance(err, list): err = err[0]
                msg = str(err.get("msg", "Unknown API error")).strip()
                
                # Unsupported endpoints are locked out for 12 hours.
                if "Unsupported" in msg or "does not support" in msg:
                    mark_unsupported(agency, endpoint, msg, res.url)
                    return "UNSUPPORTED"
                
                # Triggers graceful fallback to route-batch logic.
                if msg in ("No parameter provided", "No RTPI Data Feed parameter provided"):
                    if VERBOSE_LOGGING:
                        log(f"API Fallback Triggered on {res.url}: {msg}", level="verbose", agency=agency)
                    return "NEEDS_PARAM"
                
                # Treat expected empty responses as successful no-data polls.
                empty_msgs = ["No data found", "No vehicles found", "No service", "No arrival times", "No agencies"]
                if any(s in msg for s in empty_msgs):
                    if VERBOSE_LOGGING:
                        log(f"API Empty Response on {res.url}: {msg}", level="verbose", agency=agency)
                    return "EMPTY"
                
                # All other unhandled errors must be logged fully.
                log(f"API Error on {res.url}: {msg}", level="error", agency=agency)
                return None
            return br
        return data
    except Exception as e:
        log(f"Error fetching {url}: {e}", level="error", agency=agency)
        return None

def extract_list(response, keys):
    """
    Safely extracts a list of items from a standard BusTime JSON response.
    Handles direct lists, direct dictionaries, and Clever Devices nested wrapper format
    (e.g., {"routes": {"route": [...]}}).
    """
    if not response or isinstance(response, str):
        return []
    if isinstance(keys, str): keys = [keys]

    if isinstance(response, list):
        return response

    if isinstance(response, dict):
        for k in keys:
            if k in response:
                items = response[k]
                if isinstance(items, list):
                    return items
                if isinstance(items, dict):
                    # Handle nested wrappers such as {"routes": {"route": [...]}}.
                    if len(items) == 1:
                        inner_val = list(items.values())[0]
                        if isinstance(inner_val, list):
                            return inner_val
                        elif isinstance(inner_val, dict):
                            return [inner_val]
                    return [items]
                return [items]
    return []

#### WRITER & COMPACTION ####
seen_hashes = {}
seen_lock = threading.Lock()

def history_file_path(agency, stream, date_str):
    return EVENTS_DIR / f"{agency}_{gtfs_file_stem(stream)}_{date_str}.jsonl"

def load_history_metadata(file_path):
    hashes = {}
    if not file_path.exists():
        return hashes
    with open(file_path, "r", encoding="utf-8") as handle:
        for line in handle:
            try:
                entry = json.loads(line)
                hashes[entry["hash_id"]] = True
            except Exception:
                continue
    return hashes

def ensure_history_loaded(agency, stream):
    stream_key = f"{agency}::{gtfs_file_stem(stream)}"
    with seen_lock:
        if stream_key in seen_hashes:
            return stream_key

    today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    history = load_history_metadata(history_file_path(agency, stream, today_str))

    with seen_lock:
        if stream_key not in seen_hashes:
            seen_hashes[stream_key] = history
    return stream_key

def touch_ui_sync(agency, stream, status=None):
    now_time = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    val = status if status else now_time
    ui_key = f"{agency}_{stream}"
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            UI_STREAM_STATE[ui_key]["last_sync_time"] = val

def update_ui_poll_count(agency, stream, count, accumulate=False):
    ui_key = f"{agency}_{stream}"
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            if accumulate:
                UI_STREAM_STATE[ui_key]["new_this_poll"] += count
            else:
                UI_STREAM_STATE[ui_key]["new_this_poll"] = count

def emit_record(agency, stream, event, data, endpoint="", metadata=None):
    now = datetime.now(LOCAL_TZ)
    now_ts = now.isoformat(timespec="seconds")
    stream_key = ensure_history_loaded(agency, stream)

    normalized_data = normalize_jsonable(data)
    normalized_metadata = normalize_jsonable(metadata) if metadata is not None else None

    hash_payload = {
        "agency": agency,
        "stream": stream,
        "event": event,
        "endpoint": endpoint,
        "data": normalized_data,
        "metadata": normalized_metadata,
    }
    payload = json.dumps(hash_payload, sort_keys=True, separators=(",", ":"), default=str)
    hash_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    # Track uniquely inside our daily metrics panel UI environment.
    with UI_STATE_LOCK:
        state = UI_STREAM_STATE.setdefault(
            f"{agency}_{stream}",
            {
                "agency": agency,
                "stream": stream,
                "type": infer_stream_type(stream),
                "new_this_poll": 0,
                "total_today": 0,
                "last_sync_time": "Loading...",
            },
        )

    with seen_lock:
        history = seen_hashes.setdefault(stream_key, {})
        if hash_id in history:
            return False
        history[hash_id] = True

    record = {
        "hash_id": hash_id,
        "ts": now_ts,
        "agency": agency,
        "stream": stream,
        "event": event,
        "endpoint": endpoint,
        "data": normalized_data,
    }
    if normalized_metadata is not None:
        record["metadata"] = normalized_metadata

    try:
        write_queue.put(record, timeout=2)
        with UI_STATE_LOCK:
            UI_STREAM_STATE[f"{agency}_{stream}"]["total_today"] += 1
        return True
    except Exception:
        log(f"Write queue is full for {stream}; dropping record", level="error", agency=agency)
        return False

def infer_stream_type(stream):
    if stream in {"Agencies", "Locales", "RealTimeDataFeeds"}:
        return "Bi-Weekly"
    hourly_like = {"Routes", "Directions", "Stops", "Patterns", "ServiceBulletins", "BusBridges"}
    if stream in hourly_like:
        return "Hourly"
    return "Streamed"

def init_ui_state(load_history=True):
    with UI_STATE_LOCK:
        preserved_times = {}
        for ui_key, state in UI_STREAM_STATE.items():
            if state["last_sync_time"] and state["last_sync_time"] not in ("Loading...", "Waiting...", "Waiting for Routes...", "Waiting for Vehicles...", "Fetching...", "Failed", "Unsupported", "No Routes", "Off Hours"):
                preserved_times[ui_key] = state["last_sync_time"]

        for agency in TARGET_AGENCIES.keys():
            for stream in BUSTIME_FEEDS:
                ui_key = f"{agency}_{stream}"
                UI_STREAM_STATE.setdefault(
                    ui_key,
                    {
                        "agency": agency,
                        "stream": stream,
                        "type": infer_stream_type(stream),
                        "new_this_poll": 0,
                        "total_today": 0,
                        "last_sync_time": "Loading...",
                    },
                )

    temp_state = {}
    if load_history:
        today = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
        for file_path in EVENTS_DIR.glob("*_*.jsonl"):
            if today not in file_path.name:
                continue
            try:
                with open(file_path, "r", encoding="utf-8") as handle:
                    for line in handle:
                        line = line.strip()
                        if not line: continue
                        try:
                            entry = json.loads(line)
                        except Exception: continue

                        agency = entry.get("agency")
                        stream = entry.get("stream")
                        if not agency or stream == "Errors" or not stream:
                            continue

                        ui_key = f"{agency}_{stream}"
                        state = temp_state.setdefault(
                            ui_key,
                            {
                                "agency": agency,
                                "stream": stream,
                                "type": infer_stream_type(stream),
                                "new_this_poll": 0,
                                "total_today": 0,
                                "last_sync_time": "Loading...",
                            },
                        )
                        state["total_today"] += 1

                        ts_value = entry.get("ts")
                        if ts_value:
                            try:
                                state["last_sync_time"] = ts_value[11:19]
                            except Exception:
                                state["last_sync_time"] = ts_value
            except Exception:
                continue

    with UI_STATE_LOCK:
        for agency in TARGET_AGENCIES.keys():
            for stream in BUSTIME_FEEDS:
                ui_key = f"{agency}_{stream}"
                if ui_key in temp_state:
                    UI_STREAM_STATE[ui_key]["total_today"] = temp_state[ui_key]["total_today"]
                    if temp_state[ui_key]["last_sync_time"] != "Loading...":
                        UI_STREAM_STATE[ui_key]["last_sync_time"] = temp_state[ui_key]["last_sync_time"]

                if ui_key in preserved_times:
                    old_ts = preserved_times[ui_key]
                    new_ts = UI_STREAM_STATE[ui_key]["last_sync_time"]
                    if new_ts == "Loading..." or old_ts > new_ts:
                        UI_STREAM_STATE[ui_key]["last_sync_time"] = old_ts

def mark_off_hours_ui(streams):
    """Safely transitions active streams into visual sleep states instead of 'Loading...'"""
    with UI_STATE_LOCK:
        for agency in TARGET_AGENCIES.keys():
            for stream in streams:
                ui_key = f"{agency}_{stream}"
                if ui_key in UI_STREAM_STATE:
                    if UI_STREAM_STATE[ui_key]["last_sync_time"] != "Unsupported":
                        UI_STREAM_STATE[ui_key]["last_sync_time"] = "Off Hours"

_UI_REFRESHING = False
_UI_REFRESH_LOCK = threading.Lock()

def trigger_ui_refresh():
    global _UI_REFRESHING
    with _UI_REFRESH_LOCK:
        if _UI_REFRESHING:
            return
        _UI_REFRESHING = True

    def _refresh():
        try:
            init_ui_state(load_history=True)
        finally:
            global _UI_REFRESHING
            with _UI_REFRESH_LOCK:
                _UI_REFRESHING = False

    threading.Thread(target=_refresh, daemon=True).start()

def writer_worker():
    """Drains the queue and appends JSONL records to daily event files."""
    handles = {}
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                record = write_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                agency = record.get("agency", "UNKNOWN")
                stream = record.get("stream", "stream")
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{agency}_{gtfs_file_stem(stream)}_{date_str}.jsonl"
                filepath = EVENTS_DIR / filename

                if filename not in handles:
                    handles[filename] = open(filepath, "a", encoding="utf-8")
                handles[filename].write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                handles[filename].flush()
            except Exception as exc:
                log(f"Writer exception: {exc}", level="error", agency="SYSTEM")
            finally:
                write_queue.task_done()
    finally:
        for h in handles.values():
            try:
                h.close()
            except Exception:
                pass

def _align_table_to_schema(table, schema):
    """Coerce a PyArrow table to the target schema, filling missing columns with nulls."""
    arrays = []
    for field in schema:
        if field.name in table.column_names:
            col = table.column(field.name)
            if col.type != field.type:
                try:
                    col = col.cast(field.type, safe=False)
                except Exception:
                    col = pa.array([None] * len(table), type=field.type)
        else:
            col = pa.array([None] * len(table), type=field.type)
        arrays.append(col)
    return pa.Table.from_arrays(arrays, schema=schema)


def stringify_chunk(chunk):
    for col in ["data", "metadata"]:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False)
                if isinstance(x, (dict, list))
                else (None if x is None else str(x))
            )
    return chunk

def compact_worker():
    if not PARQUET_AVAILABLE or ARGS.no_compaction:
        return
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            for file_path in EVENTS_DIR.glob("*_*.jsonl"):
                if today_str not in file_path.name:
                    parquet_path = ARCHIVE_DIR / file_path.with_suffix(".parquet").name
                    temp_path = Path(str(parquet_path) + ".tmp")
                    writer = None
                    target_schema = None
                    row_count = 0
                    try:
                        for chunk in pd.read_json(file_path, lines=True, chunksize=50000):
                            chunk = stringify_chunk(chunk)
                            table = pa.Table.from_pandas(chunk, preserve_index=False)
                            if writer is None:
                                target_schema = table.schema
                                writer = pq.ParquetWriter(
                                    str(temp_path),
                                    target_schema,
                                    compression="zstd",
                                    compression_level=ARCHIVE_ZSTD_LEVEL,
                                )
                            else:
                                table = _align_table_to_schema(table, target_schema)
                            writer.write_table(table)
                            row_count += len(chunk)
                        if writer is not None:
                            writer.close()
                            temp_path.replace(parquet_path)
                            file_path.unlink()
                            log(f"Compacted {file_path.name} into {parquet_path.name} ({row_count} rows).", level="info", agency="SYSTEM")
                    except Exception as exc:
                        log(f"Failed to compact {file_path.name}: {exc}", level="error", agency="SYSTEM")
                    finally:
                        if writer:
                            try:
                                writer.close()
                            except Exception:
                                pass
                        if temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                pass
        except Exception:
            pass
        sleep_with_stop(60)


#### NETWORK CONSUMERS ####

def bustime_poll_generic(agency, stream, endpoint, root_keys, event="snapshot", metadata=None, reset_ui=True, finalize_ui=True, **params):
    if reset_ui: 
        update_ui_poll_count(agency, stream, 0, accumulate=False)
        
    if check_unsupported(agency, endpoint):
        if finalize_ui: touch_ui_sync(agency, stream, "Unsupported")
        return "UNSUPPORTED"
        
    if finalize_ui: touch_ui_sync(agency, stream, "Fetching...")
    res = bustime_request(agency, endpoint, **params)
    
    if res == "UNSUPPORTED":
        if finalize_ui: touch_ui_sync(agency, stream, "Unsupported")
        return "UNSUPPORTED"
        
    if res == "NEEDS_PARAM":
        return "NEEDS_PARAM"
        
    if res == "EMPTY":
        if finalize_ui: touch_ui_sync(agency, stream)
        return []
        
    if res is None:
        if finalize_ui: touch_ui_sync(agency, stream, "Failed")
        return []
    
    items = extract_list(res, root_keys)
    fetched_count = len(items)
    
    for item in items:
        emit_record(agency, stream, event, item, endpoint=endpoint, metadata=metadata)
            
    # Display the number of records fetched in the most recent poll
    update_ui_poll_count(agency, stream, fetched_count, accumulate=True)
    
    if finalize_ui:
        touch_ui_sync(agency, stream)
    return items

def fetch_route_structure(agency, rid):
    """Fetches Directions, Patterns, and conditionally Stops for a specific route."""
    if stop_event.is_set():
        return
    dirs = bustime_poll_generic(agency, "Directions", "getdirections", ["dir", "directions"], rt=rid, metadata={"route_id": rid})
    bustime_poll_generic(agency, "Patterns", "getpatterns", ["ptr", "pattern", "patterns"], rt=rid, metadata={"route_id": rid})
    
    if dirs and isinstance(dirs, list):
        for direction in dirs:
            dir_val = direction.get("dir", direction.get("id"))
            if dir_val:
                bustime_poll_generic(agency, "Stops", "getstops", ["stop", "stops"], rt=rid, dir=dir_val, metadata={"route_id": rid, "direction": dir_val})

def bustime_run_hourly_for_agency(agency):
    """Run the hourly BusTime discovery pass for a single agency."""
    log("Starting hourly initialization", level="info", agency=agency)
    
    now_ts = time.time()
    with _CACHE_LOCK:
        static_cache = BUSTIME_CACHE.get("static_last_fetch", {}).get(agency, {})
        
    # 1. Base configuration feeds.
    STATIC_REFRESH_DAYS = 14
    for stream, endpoint, root_keys in [
        ("Agencies", "getagencies", ["agency", "agencies"]),
        ("Locales", "getlocalelist", ["locale", "locales"]),
        ("RealTimeDataFeeds", "getrtpidatafeeds", ["feed", "feeds"])
    ]:
        last_fetch = static_cache.get(stream, 0)
        days_passed = (now_ts - last_fetch) / 86400.0
        
        if days_passed >= STATIC_REFRESH_DAYS or last_fetch == 0:
            res = bustime_poll_generic(agency, stream, endpoint, root_keys)
            
            # Cache the fetch attempt as long as it didn't strictly fail on missing params or timeout completely
            if res is not None and res != "NEEDS_PARAM":
                with _CACHE_LOCK:
                    BUSTIME_CACHE.setdefault("static_last_fetch", {}).setdefault(agency, {})[stream] = now_ts
                save_cache()
                touch_ui_sync(agency, stream, f"Cached ({STATIC_REFRESH_DAYS}d)")
        else:
            days_left = max(0, int(STATIC_REFRESH_DAYS - days_passed))
            touch_ui_sync(agency, stream, f"Cached ({days_left}d)")
            
    # 2. Routes are required before downstream fetches.
    routes = bustime_poll_generic(agency, "Routes", "getroutes", ["route", "routes"])
    route_ids = set()
    if isinstance(routes, list):
        for rt in routes:
            rid = rt.get("rt")
            if rid: route_ids.add(str(rid))
        
    if route_ids:
        with _CACHE_LOCK:
            if "route_ids" not in BUSTIME_CACHE: BUSTIME_CACHE["route_ids"] = {}
            BUSTIME_CACHE["route_ids"][agency] = route_ids
        save_cache()
    else:
        # Cascade the no-route state to dependent UI elements.
        for stream in ["Directions", "Patterns", "Stops"]:
            touch_ui_sync(agency, stream, "No Routes")
        with _CACHE_LOCK:
            if "route_ids" in BUSTIME_CACHE and agency in BUSTIME_CACHE["route_ids"]:
                BUSTIME_CACHE["route_ids"][agency] = set()
        save_cache()
    
    # 3. Handle service bulletins and bus bridges.
    for stream, endpoint, rkeys in [
        ("ServiceBulletins", "getservicebulletins", ["sb", "sbs", "serviceBulletins"]),
        ("BusBridges", "getbusbridges", ["bb", "bbs", "busBridges"])
    ]:
        if check_unsupported(agency, endpoint):
            touch_ui_sync(agency, stream, "Unsupported")
            continue
            
        res = bustime_poll_generic(agency, stream, endpoint, rkeys)
        if res == "NEEDS_PARAM" and route_ids:
            update_ui_poll_count(agency, stream, 0, accumulate=False)
            for batch in chunked(list(route_ids), BATCH_SIZE):
                if stop_event.is_set():
                    break
                rt_str = ",".join(str(r) for r in batch)
                items = bustime_poll_generic(agency, stream, endpoint, rkeys, reset_ui=False, finalize_ui=False, rt=rt_str)
                if items == "UNSUPPORTED":
                    touch_ui_sync(agency, stream, "Unsupported")
                    break
            else:
                touch_ui_sync(agency, stream)
    
    # 4. Fetch route structure concurrently.
    if route_ids:
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = [executor.submit(fetch_route_structure, agency, rid) for rid in sorted(route_ids)]
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                try: future.result()
                except Exception: pass

def bustime_hourly_loop():
    """Runs hourly discovery for static and route-structure feeds."""
    last_hour = -1
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        state, _, _ = get_system_state()
        force = HOURLY_REFRESH_FLAG.is_set()

        # Run hourly feeds during startup, pre-flight, and active service.
        if state == "OFF_HOURS":
            mark_off_hours_ui(["Agencies", "Locales", "RealTimeDataFeeds", "Routes", "Directions", "Stops", "Patterns", "ServiceBulletins", "BusBridges"])
            if force:
                HOURLY_REFRESH_FLAG.clear()
            sleep_with_stop(5)
            continue

        if force or (now.minute == 0 and now.hour != last_hour) or last_hour == -1:
            if force:
                HOURLY_REFRESH_FLAG.clear()
            last_hour = now.hour
            
            with ThreadPoolExecutor(max_workers=min(len(TARGET_AGENCIES), MAX_WORKERS)) as executor:
                futures = [executor.submit(bustime_run_hourly_for_agency, agency) for agency in TARGET_AGENCIES.keys()]
                for future in as_completed(futures):
                    if stop_event.is_set():
                        break
                    try: future.result()
                    except Exception as exc: log(f"Hourly init failed: {exc}", level="warning", agency="SYSTEM")
                
        sleep_with_stop(5)

def bustime_detour_loop():
    """Detours are streamed independently as per the API guide to frequently catch alerts."""
    sleep_with_stop(10)
    while not stop_event.is_set():
        state, _, _ = get_system_state()
        if state == "OFF_HOURS":
            mark_off_hours_ui(["Detours", "EnhancedDetours"])
            sleep_with_stop(60)
            continue

        start_time = time.time()
        
        for agency in TARGET_AGENCIES.keys():
            if stop_event.is_set():
                break
            
            new_active_detours = {}
            
            for stream, endpoint, rkeys in [
                ("Detours", "getdetours", ["dtr", "detour", "detours"]),
                ("EnhancedDetours", "getenhanceddetours", ["dtr", "detour", "detours"])
            ]:
                if check_unsupported(agency, endpoint):
                    touch_ui_sync(agency, stream, "Unsupported")
                    continue
                
                res = bustime_poll_generic(agency, stream, endpoint, rkeys)
                
                if res == "NEEDS_PARAM":
                    # Fall back to route batching if parameterless queries are denied.
                    update_ui_poll_count(agency, stream, 0, accumulate=False)
                    with _CACHE_LOCK:
                        route_ids = list(BUSTIME_CACHE.get("route_ids", {}).get(agency, set()))
                    
                    if not route_ids:
                        touch_ui_sync(agency, stream, "No Routes")
                        continue
                        
                    for batch in chunked(route_ids, BATCH_SIZE):
                        if stop_event.is_set():
                            break
                        rt_str = ",".join(str(r) for r in batch)
                        items = bustime_poll_generic(agency, stream, endpoint, rkeys, reset_ui=False, finalize_ui=False, rt=rt_str)
                        if items == "UNSUPPORTED":
                            touch_ui_sync(agency, stream, "Unsupported")
                            break
                        if items and isinstance(items, list):
                            # Track route detours dynamically.
                            for item in items:
                                dtr_id = str(item.get("id", item.get("dtrid", "")))
                                rts = item.get("rt", [])
                                if isinstance(rts, str): rts = [rts]
                                for r_obj in rts:
                                    rt = str(r_obj.get("rt", r_obj)) if isinstance(r_obj, dict) else str(r_obj)
                                    if rt: new_active_detours.setdefault(rt, set()).add(dtr_id)
                    else:
                        touch_ui_sync(agency, stream)
                elif isinstance(res, list):
                    # Aggregate active detours from system-wide response.
                    for item in res:
                        dtr_id = str(item.get("id", item.get("dtrid", "")))
                        rts = item.get("rt", [])
                        if isinstance(rts, str): rts = [rts]
                        for r_obj in rts:
                            rt = str(r_obj.get("rt", r_obj)) if isinstance(r_obj, dict) else str(r_obj)
                            if rt: new_active_detours.setdefault(rt, set()).add(dtr_id)

            # Trigger route refreshes when detours change.
            with _CACHE_LOCK:
                active_detour_ids = BUSTIME_CACHE.get("active_detours", {}).get(agency, {})
            
            impacted_routes = []
            for rt, dtrs in new_active_detours.items():
                if rt not in active_detour_ids or dtrs != active_detour_ids[rt]:
                    impacted_routes.append(rt)
                    
            if impacted_routes:
                log(f"Detours changed for routes {impacted_routes}. Triggering condition network map updates.", level="info", agency=agency)
                with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                    futures = [executor.submit(fetch_route_structure, agency, rid) for rid in impacted_routes]
                    for future in as_completed(futures):
                        if stop_event.is_set():
                            break
                        try: future.result()
                        except Exception: pass
            
            with _CACHE_LOCK:
                BUSTIME_CACHE.setdefault("active_detours", {})[agency] = new_active_detours
            
        elapsed = time.time() - start_time
        interval = get_poll_interval() or POLL_INTERVAL_ACTIVE
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)

def bustime_vehicle_loop():
    """Polls vehicle positions using route and vehicle caches."""
    sleep_with_stop(5)
    while not stop_event.is_set():
        state, _, _ = get_system_state()
        if state == "OFF_HOURS":
            mark_off_hours_ui(["Vehicles"])
            sleep_with_stop(60)
            continue

        start_time = time.time()
        
        for agency in TARGET_AGENCIES.keys():
            if stop_event.is_set():
                break
            
            with _CACHE_LOCK:
                route_ids = list(BUSTIME_CACHE.get("route_ids", {}).get(agency, set()))
            
            if not route_ids:
                touch_ui_sync(agency, "Vehicles", "No Routes")
                continue
                
            touch_ui_sync(agency, "Vehicles", "Fetching...")
            update_ui_poll_count(agency, "Vehicles", 0, accumulate=False)
            total_fetched = 0
            live_vids = set()
            local_err = False
            
            # Phase 1: chunk active routes natively.
            for batch in chunked(route_ids, BATCH_SIZE):
                if stop_event.is_set():
                    break
                
                rt_str = ",".join(str(r) for r in batch)
                res = bustime_request(agency, "getvehicles", rt=rt_str)
                if res is None:
                    local_err = True
                    continue
                elif res == "EMPTY":
                    continue
                
                items = extract_list(res, ["vehicle", "vehicles"])
                total_fetched += len(items)
                
                for item in items:
                    vid = item.get("vid")
                    if vid: live_vids.add(str(vid))
                    emit_record(agency, "Vehicles", "update", item, endpoint="getvehicles")
                
                sleep_with_stop(0.1 + random.random() * 0.2)
                
            # Phase 2: poll cached vehicles that may be off-route.
            with _CACHE_LOCK:
                cached_vids = set(BUSTIME_CACHE.get("vid_ids", {}).get(agency, set()))
            
            missing_vids = cached_vids - live_vids
            if missing_vids:
                for batch in chunked(list(missing_vids), BATCH_SIZE):
                    if stop_event.is_set(): break
                    vid_str = ",".join(str(v) for v in batch)
                    res = bustime_request(agency, "getvehicles", vid=vid_str)
                    
                    if res and res not in ("EMPTY", "NEEDS_PARAM", "UNSUPPORTED"):
                        items = extract_list(res, ["vehicle", "vehicles"])
                        total_fetched += len(items)
                        for item in items:
                            vid = item.get("vid")
                            if vid: live_vids.add(str(vid))
                            emit_record(agency, "Vehicles", "update", item, endpoint="getvehicles")
                    
                    sleep_with_stop(0.1 + random.random() * 0.2)

            update_ui_poll_count(agency, "Vehicles", total_fetched, accumulate=False)
            touch_ui_sync(agency, "Vehicles", "Failed" if local_err and not total_fetched else None)
            log(f"Vehicles: {total_fetched} fetched, {len(live_vids)} live IDs tracked", level="info", agency=agency)
            
            if live_vids:
                with _CACHE_LOCK:
                    BUSTIME_CACHE.setdefault("vid_ids", {})[agency] = live_vids
                save_cache()

        elapsed = time.time() - start_time
        interval = get_poll_interval() or POLL_INTERVAL_ACTIVE
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)

def bustime_prediction_loop():
    """Polls ETA predictions using cached live vehicle IDs."""
    sleep_with_stop(15)
    while not stop_event.is_set():
        state, _, _ = get_system_state()
        if state == "OFF_HOURS":
            mark_off_hours_ui(["Predictions"])
            sleep_with_stop(60)
            continue

        start_time = time.time()
        
        for agency in TARGET_AGENCIES.keys():
            if stop_event.is_set():
                break
            
            # Chunk live vehicle IDs to keep prediction requests low.
            with _CACHE_LOCK:
                vid_ids = list(BUSTIME_CACHE.get("vid_ids", {}).get(agency, set()))
                
            if not vid_ids:
                touch_ui_sync(agency, "Predictions", "Waiting for Vehicles...")
                continue

            touch_ui_sync(agency, "Predictions", "Fetching...")
            update_ui_poll_count(agency, "Predictions", 0, accumulate=False)
            total_fetched = 0
            local_err = False
            
            for batch in chunked(vid_ids, BATCH_SIZE):
                if stop_event.is_set():
                    break
                
                vid_str = ",".join(str(v) for v in batch)
                res = bustime_request(agency, "getpredictions", vid=vid_str)
                if res is None:
                    local_err = True
                    continue
                elif res == "EMPTY":
                    continue
                
                items = extract_list(res, ["prd", "pre", "predictions"])
                total_fetched += len(items)
                
                for item in items:
                    emit_record(agency, "Predictions", "update", item, endpoint="getpredictions")
                        
                sleep_with_stop(0.1 + random.random() * 0.2)
                
            update_ui_poll_count(agency, "Predictions", total_fetched, accumulate=False)
            touch_ui_sync(agency, "Predictions", "Failed" if local_err and not total_fetched else None)

        elapsed = time.time() - start_time
        interval = get_poll_interval() or POLL_INTERVAL_ACTIVE
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)


def start_workers():
    """Start all background workers after the cache and GTFS window are ready."""
    load_cache()
    
    # Run the GTFS evaluation synchronously before starting any loops.
    # to guarantee we don't spam start-up requests during off-hours.
    log("Running initial GTFS operational window check...", agency="SYSTEM")
    fetch_gtfs_operational_window()
    update_state_from_time()
    
    threading.Thread(target=writer_worker, daemon=True).start()
    if PARQUET_AVAILABLE:
        threading.Thread(target=compact_worker, daemon=True).start()

    log("Starting GTFS state machine", agency="SYSTEM")
    threading.Thread(target=gtfs_state_machine_loop, daemon=True).start()

    log("Starting Clever Devices BusTime loops", agency="SYSTEM")
    threading.Thread(target=bustime_hourly_loop, daemon=True).start()
    threading.Thread(target=bustime_vehicle_loop, daemon=True).start()
    threading.Thread(target=bustime_detour_loop, daemon=True).start()
    threading.Thread(target=bustime_prediction_loop, daemon=True).start()


#### UI ####
def format_duration(seconds):
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes}m {secs}s" if hours else f"{minutes}m {secs}s"

def next_hourly_timestamp():
    return (datetime.now(LOCAL_TZ) + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

def get_layout(refresh_flash=False):
    with UI_STATE_LOCK:
        # Sort alphabetically by GTFS/Data Lake standard, then stream name.
        rows = sorted(
            UI_STREAM_STATE.values(),
            key=lambda s: (
                s["agency"],
                GTFS_MAP.get(s["stream"], "z"),
                s["stream"]
            ),
        )
        
        now = datetime.now(LOCAL_TZ)
        RECENT_ERRORS[:] = [e for e in RECENT_ERRORS if (now - e["dt"]).total_seconds() < 180]
        errors = list(RECENT_ERRORS)

        # API usage stats from the in-memory tracker.
        total_count = 0
        with _API_CALLS_TRACKER_LOCK:
            for agency, data in _API_CALLS_TODAY.items():
                total_count += data["count"]

    # State machine status.
    state, t_min, t_max = get_system_state()
    state_colors = {
        "STARTUP": "yellow", "PRE_FLIGHT": "cyan",
        "ACTIVE": "green", "END_OF_SERVICE": "yellow", "OFF_HOURS": "dim",
    }
    state_str = f"[{state_colors.get(state, 'white')}]{state}[/{state_colors.get(state, 'white')}]"
    if t_min is not None and t_max is not None:
        window_str = f"  Window: {secs_to_hhmmss(t_min)} → {secs_to_hhmmss(t_max)}"
    else:
        window_str = "  Window: parsing GTFS..."

    table = Table(expand=True, header_style="bold magenta", border_style="dim", highlight=True, show_lines=False)
    table.add_column("Agency", justify="left", style="bold")
    table.add_column("Stream (Type)", justify="left", vertical="middle")
    table.add_column("Data Lake Standard", justify="center", vertical="middle", style="dim")
    table.add_column("Last Sync", justify="center", vertical="middle")
    table.add_column("Fetched (Latest)", justify="center", vertical="middle")
    table.add_column("Unique (Today)", justify="center", vertical="middle")

    if not rows:
        table.add_row(Text("Loading...", style="yellow"), "", "", "", "0", "0")
    else:
        for state_row in rows:
            stream_name = state_row['stream']
            gtfs_std = GTFS_MAP.get(stream_name, "unknown")
            stream_label = f"{stream_name} ({state_row['type']})"
            
            color = "green" if state_row["type"] == "Hourly" else ("magenta" if state_row["type"] == "Bi-Weekly" else "cyan")

            sync_time = state_row["last_sync_time"]
            
            if "Cached" in sync_time:
                sync_style = "cyan"
            elif sync_time == "Off Hours":
                sync_style = "dim"
            elif sync_time == "Failed":
                sync_style = "red"
            elif sync_time in ("Loading...", "Waiting...", "Waiting for Routes...", "Waiting for Vehicles...", "Fetching...", "No Routes"):
                sync_style = "yellow"
            elif sync_time == "Unsupported":
                sync_style = "red"
            else:
                sync_style = ""

            new_poll_val = str(state_row.get("new_this_poll", 0))
            new_poll_style = "dim" if new_poll_val == "0" else ""

            # Compute the total length of today's in-memory hash store directly for true distinct count integrity
            stream_key = f"{state_row['agency']}::{gtfs_file_stem(state_row['stream'])}"
            with seen_lock:
                true_unique_today = len(seen_hashes.get(stream_key, {}))

            table.add_row(
                Text(state_row["agency"], style="bold white"),
                Text(stream_label, style=color),
                Text(gtfs_std, style="dim", justify="center"),
                Text(sync_time, style=sync_style, justify="center"),
                Text(new_poll_val, style=new_poll_style, justify="center"),
                Text(str(true_unique_today), justify="center"),
            )

    if errors:
        error_text = Text()
        for entry in errors[-5:]:
            error_text.append(f"{entry['message']}\n", style="bold red")
    else:
        error_text = Text(f"No errors in the last 3 minutes. Written safely to {LOG_FILE.name}.", style="dim")

    interval = get_poll_interval() or POLL_INTERVAL_ACTIVE
    poll_countdown = interval - int(time.time() % interval)
    poll_countdown_str = format_duration(poll_countdown)
    
    next_h = next_hourly_timestamp()
    time_to_hourly = format_duration((next_h - datetime.now(LOCAL_TZ)).total_seconds())
    next_h_str = next_h.strftime('%I:%M %p').lstrip('0')

    # Time until midnight reset.
    now_local = datetime.now(LOCAL_TZ)
    midnight = (now_local + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
    reset_in = format_duration((midnight - now_local).total_seconds())

    if refresh_flash:
        refresh_note = "[bold green]REFRESHING HOURLY FEEDS...[/bold green]"
    else:
        refresh_note = "[bold yellow]Press R[/bold yellow] to force refresh hourly feeds  |  [bold yellow]Press Q[/bold yellow] to quit"

    api_counter_text = f"Today's Calls: {total_count:,} / {API_LIMIT_MAX_REQUESTS_PER_DAY:,} | Headroom Remaining: {max(0, API_LIMIT_MAX_REQUESTS_PER_DAY - total_count):,}"
    
    footer_body = (
        f"State: {state_str}{window_str}  |  Next poll: ~{poll_countdown_str}  |  "
        f"Next hourly: {time_to_hourly} (@ {next_h_str})  |  Reset: {reset_in}\n"
        f"{refresh_note}"
    )
    footer = Panel(footer_body, title="Controls", border_style="blue")
    api_panel = Panel(api_counter_text, border_style="cyan")
    errors_panel = Panel(error_text, title="Recent Logs", border_style="red")

    return Group(table, errors_panel, footer, api_panel)

_refresh_flash_until = 0.0

def trigger_full_refresh():
    state, _, _ = get_system_state()
    if state == "OFF_HOURS":
        log("Manual refresh requested, but system is in OFF_HOURS. Ignored.", level="warning", agency="SYSTEM")
        return
        
    global _refresh_flash_until
    _refresh_flash_until = time.time() + 3.0
    log("Manual refresh requested (R key)", level="warning", agency="SYSTEM")
    HOURLY_REFRESH_FLAG.set()
    trigger_ui_refresh()

def run_live_gui():
    """Render the live TUI and handle keyboard controls."""
    
    try:
        fd = sys.stdin.fileno()
    except Exception:
        fd = None
        
    use_unix_keys = termios is not None and tty is not None and fd is not None and sys.stdin.isatty()
    old_settings = termios.tcgetattr(fd) if use_unix_keys else None
    running = True

    def poll_keys():
        if msvcrt is not None and os.name == "nt":
            while msvcrt.kbhit():
                try:
                    key = msvcrt.getwch().lower()
                except Exception:
                    return None
                if key == "q": return "quit"
                if key == "r": return "refresh"
            return None

        if fd is not None:
            if select.select([sys.stdin], [], [], 0)[0]:
                try:
                    key = os.read(fd, 1).decode(errors="ignore").lower()
                except Exception:
                    return None
                if key == "q": return "quit"
                if key == "r": return "refresh"
        return None

    if use_unix_keys:
        tty.setcbreak(fd)

    def delayed_worker_start():
        TUI_READY.wait(timeout=10)
        start_workers()

    threading.Thread(target=delayed_worker_start, daemon=True).start()

    console = Console()
    try:
        with Live(get_layout(), console=console, refresh_per_second=4, screen=True) as live:
            live.update(get_layout())
            if not TUI_READY.is_set():
                TUI_READY.set()
            
            # Loads long history in daemon thread instead of blocking at startup
            trigger_ui_refresh()

            while running and not stop_event.is_set():
                key_action = poll_keys()
                if key_action == "quit":
                    stop_event.set()
                    running = False
                    break
                elif key_action == "refresh":
                    trigger_full_refresh()

                flash = time.time() < _refresh_flash_until
                live.update(get_layout(refresh_flash=flash))
                time.sleep(0.25)
    finally:
        if use_unix_keys and old_settings is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def main():
    """Entry point for the BusTime ingestor."""
    log(f"BusTime ingestor starting. Data dir: {DATA_DIR}  Log: {LOG_FILE}", level="info", agency="SYSTEM")
    
    # Very fast, synchronous load logic so the GUI renders immediately
    init_ui_state(load_history=False)
    try:
        run_live_gui()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        log("BusTime ingestor stopped.", level="info", agency="SYSTEM")

if __name__ == "__main__":
    main()