#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 28, 2026 (Last updated June 11, 2026)
#
# Description:
# Dedicated ingestion engine for PASSIOGO Transit Systems.
# Uses stateful session mapping to pull independent streams (Routes, Stops, Vehicles, ETAs)
# for configured agencies (BAT, FRTA, MART) and map them to the Data Lake.
#
# Data Integrity & Storage:
# Implements append-only JSONL event capture with deterministic deduplication hashes.
# A background compactor rotates completed files into Parquet archives under data/archive/.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   event   — update | snapshot | error
#   data    — full API payload (JSON string in Parquet, raw dict in JSONL)

import json
import hashlib
import os
import random
import select
import tempfile
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
    import websocket
    from websocket import WebSocketTimeoutException
    WEBSOCKET_AVAILABLE = True
except ImportError:
    websocket = None
    WebSocketTimeoutException = Exception
    WEBSOCKET_AVAILABLE = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    PARQUET_AVAILABLE = True
except ImportError:
    PARQUET_AVAILABLE = False

load_dotenv(Path(__file__).with_name(".env"))

#### CONFIGURATION ####

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


def env_list(name, default=""):
    raw_value = env_text(name, default)
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def env_map(name):
    raw_value = env_required(name)
    mapping = {}
    for pair in [value.strip() for value in raw_value.split(",") if value.strip()]:
        if ":" not in pair:
            continue
        key, value = pair.split(":", 1)
        key = key.strip()
        value = value.strip().upper()
        if key and value:
            mapping[key] = value
    return mapping

# Define the PASSIOGO agencies to track from .env: ID -> Abbreviation.
PASSIOGO_AGENCY_CATALOG = env_map(
    "PASSIOGO_AGENCY_CATALOG",
)

PASSIOGO_TARGET_AGENCY_IDS = env_list(
    "PASSIOGO_TARGET_AGENCY_IDS",
)
if not PASSIOGO_TARGET_AGENCY_IDS:
    PASSIOGO_TARGET_AGENCY_IDS = list(PASSIOGO_AGENCY_CATALOG.keys())

TARGET_AGENCIES = {
    agency_id: PASSIOGO_AGENCY_CATALOG[agency_id]
    for agency_id in PASSIOGO_TARGET_AGENCY_IDS
    if agency_id in PASSIOGO_AGENCY_CATALOG
}

if not TARGET_AGENCIES:
    TARGET_AGENCIES = dict(PASSIOGO_AGENCY_CATALOG)

PASSIOGO_STREAM_AGENCY = "PASSIOGO"

PASSIOGO_FEEDS = (
    "FeedInfo",
    "Routes",
    "Stops",
    "Vehicles",
    "EnhancedVehicles",
    "TripUpdates",
    "Alerts"
)

# Maps PASSIOGO streams to standard Data Lake GTFS formats.
GTFS_MAP = {
    "FeedInfo": "feed_info",
    "Routes": "routes",
    "Stops": "stops",
    "Vehicles": "VehiclePositions",
    "EnhancedVehicles": "VehiclePositions_enhanced",
    "vehicle_stream": "unknown",
    "TripUpdates": "TripUpdates",
    "Alerts": "Alerts"
}

def safe_prefix(value):
    cleaned_value = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return cleaned_value.strip("_") or "stream"


def stream_filename_label(stream):
    return safe_prefix(stream).lower()

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

LOCAL_TZ = ZoneInfo(env_required("SYNC_TIMEZONE"))
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
CACHE_DIR = DATA_DIR / "cache"
LOG_FILE = LOG_DIR / "passigo_ingest.log"

for directory in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR, CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
# Pipeline runtime knobs are fixed, but can be updated here.
POLL_INTERVAL_FAST = env_int("POLL_INTERVAL_FAST", 20)
POLL_INTERVAL_SLOW = env_int("POLL_INTERVAL_SLOW", 15)
PASSIOGO_REQUEST_TIMEOUT_SECONDS = 10
PASSIOGO_TRIPUPDATE_START_DELAY_SECONDS = 15
TRIPUPDATE_ROUTE_BATCH_SIZE = 8
TRIPUPDATE_ROTATE_SESSION_PER_BATCH = 1
TRIPUPDATE_403_COUNTS = {}
TRIPUPDATE_LAST_403_TS = {}
PASSIOGO_ACTIVE_REFRESH_SECONDS = 10
PASSIOGO_LAST_ACTIVE_REFRESH = 0
PASSIOGO_SAVE_ACTIVE_JSON = env_int("PASSIOGO_SAVE_ACTIVE_JSON", 1)
PASSIOGO_MULTI_AGENCY_BUS_POLL = 1
MAX_WORKERS = max(1, ARGS.global_concurrency)

write_queue = Queue(maxsize=50000)
stop_event = threading.Event()

TUI_READY = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []
HOURLY_REFRESH_FLAG = threading.Event()

# Store agency-specific requests.Session objects.
AGENCY_SESSIONS = {}
AGENCY_DEVICE_IDS = {}
SESSION_LOCK = threading.Lock()

PASSIOGO_BASE_URL = "https://passiogo.com"

TRIPUPDATE_403_COOLDOWN_SECONDS = 30
TRIPUPDATE_403_MAX_COOLDOWN_SECONDS = 300
TRIPUPDATE_403_BACKOFF_UNTIL = {}
TRIPUPDATE_403_FAILURE_STREAK = {}
TRIPUPDATE_403_LOCK = threading.Lock()
API_CALL_STATS = {
    "current_date": datetime.now(LOCAL_TZ).strftime("%Y%m%d"),
    "current_total": 0,
    "previous_date": None,
    "previous_total": 0,
}


def _roll_api_call_stats_if_needed():
    today_str = datetime.now(LOCAL_TZ).strftime("%Y%m%d")
    if API_CALL_STATS["current_date"] != today_str:
        if API_CALL_STATS["current_total"] > 0:
            API_CALL_STATS["previous_date"] = API_CALL_STATS["current_date"]
            API_CALL_STATS["previous_total"] = API_CALL_STATS["current_total"]
        API_CALL_STATS["current_date"] = today_str
        API_CALL_STATS["current_total"] = 0


def record_api_call():
    _roll_api_call_stats_if_needed()
    API_CALL_STATS["current_total"] += 1


def log_api_call_summary(final=False):
    today_total = API_CALL_STATS["current_total"]
    today_date = API_CALL_STATS["current_date"]
    previous_total = API_CALL_STATS["previous_total"]
    previous_date = API_CALL_STATS["previous_date"]
    if today_total <= 0 and not final:
        return
    if previous_date is None:
        log(f"API calls for {today_date}: {today_total}", level="info")
    else:
        delta = today_total - previous_total
        log(f"API calls for {today_date}: {today_total} ({delta:+d} vs {previous_date})", level="info")

#### PASSIOGO CACHE ####
PASSIOGO_CACHE_FILE = CACHE_DIR / "PASSIOGO_cache.json"

PASSIOGO_CACHE = {
    "route_ids": {},
    "live_bus_ids": {},
    "seen_bus_ids": {}
}

_CACHE_LOCK = threading.Lock()

def load_cache():
    try:
        if not PASSIOGO_CACHE_FILE.exists():
            log(f"No cache file found at {PASSIOGO_CACHE_FILE}; initializing fresh.", level="info")
            save_cache()
            return
        with open(PASSIOGO_CACHE_FILE, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        with _CACHE_LOCK:
            for key, values in saved.items():
                if key in PASSIOGO_CACHE:
                    if isinstance(values, dict):
                        PASSIOGO_CACHE[key] = {aid: set(str(v) for v in aid_values) for aid, aid_values in values.items()}
                    else:
                        PASSIOGO_CACHE[key] = values
            stats = saved.get("api_calls") if isinstance(saved, dict) else None
            if isinstance(stats, dict):
                API_CALL_STATS["current_date"] = str(stats.get("current_date", API_CALL_STATS["current_date"]))
                API_CALL_STATS["current_total"] = int(stats.get("current_total", 0))
                API_CALL_STATS["previous_date"] = stats.get("previous_date")
                API_CALL_STATS["previous_total"] = int(stats.get("previous_total", 0))
        log(f"Cache loaded from {PASSIOGO_CACHE_FILE}", level="info")
    except Exception as exc:
        log(f"Could not load cache: {exc}", level="error")
        save_cache()

def save_cache():
    try:
        with _CACHE_LOCK:
            payload = {}
            for key, value in PASSIOGO_CACHE.items():
                if isinstance(value, dict):
                    payload[key] = {aid: sorted(list(aid_values)) for aid, aid_values in value.items()}
                else:
                    payload[key] = value
            payload["api_calls"] = dict(API_CALL_STATS)

        temp_path = PASSIOGO_CACHE_FILE.with_suffix(".tmp")
        temp_path.parent.mkdir(parents=True, exist_ok=True)
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        temp_path.replace(PASSIOGO_CACHE_FILE)
    except Exception as exc:
        log(f"Could not save cache: {exc}", level="error", agency="SYSTEM")


#### LOGGING ####
_log_lock = threading.Lock()

def log(message, level="info", agency="PASSIOGO"):
    now = datetime.now(LOCAL_TZ)
    ts_full = now.strftime("%Y-%m-%d %H:%M:%S")
    ts_short = now.strftime("%H:%M:%S")
    
    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as handle:
                handle.write(f"{ts_full} [{level.upper()}] [{agency}] {message}\n")
    except Exception: pass

    if level in ("error", "warning"):
        with UI_STATE_LOCK:
            RECENT_ERRORS.append({"dt": now, "ts_str": ts_short, "message": str(message)})
            if len(RECENT_ERRORS) > 50: RECENT_ERRORS.pop(0)

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


class TrackedSession(requests.Session):
    def request(self, method, url, *args, **kwargs):
        record_api_call()
        return super().request(method, url, *args, **kwargs)


#### NETWORK & STATE MANAGEMENT ####
def get_agency_session(agency_id, force_new=False):
    """Returns a requests Session tied to a specific agency."""
    with SESSION_LOCK:
        if force_new or agency_id not in AGENCY_SESSIONS:
            session = TrackedSession()
            retry = Retry(total=3, connect=3, read=3, status_forcelist=(429, 500, 502, 503, 504))
            adapter = HTTPAdapter(pool_connections=10, pool_maxsize=10, max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            # Exact headers mapped from your curl commands
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:151.0) Gecko/20100101 Firefox/151.0",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": PASSIOGO_BASE_URL,
                "Referer": f"{PASSIOGO_BASE_URL}/?agency={agency_id}",
                "DNT": "1",
                "Connection": "keep-alive",
                "Sec-Fetch-Dest": "empty",
                "Sec-Fetch-Mode": "cors",
                "Sec-Fetch-Site": "same-origin"
            })
            AGENCY_SESSIONS[agency_id] = session
            # Expanded range to randomly bypass 403s
            AGENCY_DEVICE_IDS[agency_id] = str(random.randint(10000000, 999999999))
        return AGENCY_SESSIONS[agency_id]

def sleep_with_stop(seconds):
    deadline = time.time() + max(0, seconds)
    while not stop_event.is_set() and time.time() < deadline:
        time.sleep(0.25)

def normalize_jsonable(value):
    if isinstance(value, dict): return {k: normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, list): return [normalize_jsonable(i) for i in value]
    if isinstance(value, bytes): return value.decode("utf-8", errors="replace")
    return value

def tripupdate_in_cooldown(agency_id):
    with TRIPUPDATE_403_LOCK:
        until = TRIPUPDATE_403_BACKOFF_UNTIL.get(agency_id)
    return until is not None and time.time() < until

def tripupdate_mark_403(agency_id, abbrev):
    now = time.time()
    with TRIPUPDATE_403_LOCK:
        until = TRIPUPDATE_403_BACKOFF_UNTIL.get(agency_id)
        if until is not None and now < until:
            return False
        streak = TRIPUPDATE_403_FAILURE_STREAK.get(agency_id, 0) + 1
        TRIPUPDATE_403_FAILURE_STREAK[agency_id] = streak
        cooldown = min(TRIPUPDATE_403_COOLDOWN_SECONDS * (2 ** (streak - 1)), TRIPUPDATE_403_MAX_COOLDOWN_SECONDS)
        TRIPUPDATE_403_BACKOFF_UNTIL[agency_id] = now + cooldown
    # Track counts and last timestamp for diagnostics
    TRIPUPDATE_403_COUNTS[agency_id] = TRIPUPDATE_403_COUNTS.get(agency_id, 0) + 1
    TRIPUPDATE_LAST_403_TS[agency_id] = now
    log(
        f"[{abbrev}] TripUpdates hit a 403 WAF block; pausing retries for {int(cooldown)}s. (count={TRIPUPDATE_403_COUNTS[agency_id]})",
        level="warning",
        agency=abbrev,
    )
    # Rotate session to vary deviceId/connection state
    get_agency_session(agency_id, force_new=True)
    return True

def tripupdate_clear_backoff(agency_id=None):
    with TRIPUPDATE_403_LOCK:
        if agency_id is None:
            TRIPUPDATE_403_BACKOFF_UNTIL.clear()
            TRIPUPDATE_403_FAILURE_STREAK.clear()
        else:
            TRIPUPDATE_403_BACKOFF_UNTIL.pop(agency_id, None)
            TRIPUPDATE_403_FAILURE_STREAK.pop(agency_id, None)

def tripupdate_clear_failure_state(agency_id):
    with TRIPUPDATE_403_LOCK:
        TRIPUPDATE_403_BACKOFF_UNTIL.pop(agency_id, None)
        TRIPUPDATE_403_FAILURE_STREAK.pop(agency_id, None)

def chunked(values, size):
    size = max(1, int(size))
    for index in range(0, len(values), size):
        yield values[index:index + size]


def build_multi_agency_payload(field_prefix="s"):
    payload = {}
    agency_ids = list(TARGET_AGENCIES.keys())
    for index, agency_id in enumerate(agency_ids):
        payload[f"{field_prefix}{index}"] = str(agency_id)
    payload["sA" if field_prefix == "s" else "amount"] = len(agency_ids)
    return payload


def fetch_active_routes_all(session=None):
    """Fetch active buses for all TARGET_AGENCIES in a single request and
    update PASSIOGO_CACHE['active_route_ids'] with per-agency sets.
    This uses the mapGetData.php?getBuses=1 endpoint with a form field
    `json` containing s0/s1/... and sA=count as observed in curl samples.
    """
    global PASSIOGO_LAST_ACTIVE_REFRESH
    try:
        agency_ids = list(TARGET_AGENCIES.keys())
        if not agency_ids:
            return

        # Use provided session or a lightweight temporary Session
        own_session = False
        if session is None:
            session = requests.Session()
            session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:151.0) Gecko/20100101 Firefox/151.0",
                "Accept": "application/json, text/javascript, */*; q=0.01",
                "Accept-Language": "en-US,en;q=0.9",
                "X-Requested-With": "XMLHttpRequest",
                "Origin": PASSIOGO_BASE_URL,
                "Referer": f"{PASSIOGO_BASE_URL}/",
                "DNT": "1",
                "Connection": "keep-alive",
            })
            own_session = True

        payload = build_multi_agency_payload("s")

        # deviceId can help; use a fresh random id to vary server state
        dev_id = str(random.randint(10000000, 999999999))
        url = f"{PASSIOGO_BASE_URL}/mapGetData.php?getBuses=1&deviceId={dev_id}"
        res = session.post(url, data={"json": json.dumps(payload)}, timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS)
        if res.status_code != 200:
            log(f"ActiveRoutes fetch returned {res.status_code}", level="debug")
            if own_session:
                session.close()
            return

        temp_json_path = None
        raw_text = res.text
        if PASSIOGO_SAVE_ACTIVE_JSON:
            # Use a temporary file for local processing and always clean it up.
            tmp = tempfile.NamedTemporaryFile(
                mode="w",
                encoding="utf-8",
                suffix=".json",
                prefix="active_routes_",
                delete=False,
                dir=str(DATA_DIR),
            )
            try:
                tmp.write(raw_text)
                temp_json_path = Path(tmp.name)
            finally:
                tmp.close()

        if temp_json_path is not None:
            with open(temp_json_path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        else:
            data = res.json()
        active = {aid: set() for aid in TARGET_AGENCIES.keys()}

        buses_obj = data.get("buses", {}) if isinstance(data, dict) else {}
        if isinstance(buses_obj, dict):
            for bus_list in buses_obj.values():
                if not isinstance(bus_list, list):
                    continue
                for bus in bus_list:
                    if not isinstance(bus, dict):
                        continue
                    agency_id = str(bus.get("userId", "")).strip()
                    route_id = str(bus.get("routeId", "")).strip()
                    if agency_id in active and route_id:
                        active[agency_id].add(route_id)

        # Fallback: if we couldn't parse buses, don't clobber existing active sets
        if any(active.values()):
            with _CACHE_LOCK:
                PASSIOGO_CACHE.setdefault("active_route_ids", {})
                for aid, routes in active.items():
                    PASSIOGO_CACHE["active_route_ids"][aid] = set(routes)
            PASSIOGO_LAST_ACTIVE_REFRESH = time.time()

    except Exception as e:
        log(f"ActiveRoutes fetch exception: {e}", level="debug")
    finally:
        if 'temp_json_path' in locals() and temp_json_path is not None:
            try:
                temp_json_path.unlink(missing_ok=True)
            except Exception:
                pass
        if own_session:
            try:
                session.close()
            except: pass


def PASSIOGO_fetch_feed_info_all_agencies():
    """Fetch FeedInfo once and fan the same snapshot out to every agency."""
    first_agency_id = next(iter(TARGET_AGENCIES.keys()), None)
    if first_agency_id is None:
        return

    try:
        log("[PASSIOGO] Fetching FeedInfo snapshot", level="info")
        record_api_call()
        res_ver = requests.get(f"{PASSIOGO_BASE_URL}/goServices.php?goWebVer=1", timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS)
        res_ver.raise_for_status()
        data_ver = res_ver.json()
        log("[PASSIOGO] FeedInfo snapshot received", level="info")
    except Exception as exc:
        log(f"[PASSIOGO] FeedInfo fetch failed: {exc}", level="warning")
        for abbrev in TARGET_AGENCIES.values():
            touch_ui_sync(abbrev, "FeedInfo", "Failed")
        return

    for agency_id, abbrev in TARGET_AGENCIES.items():
        touch_ui_sync(abbrev, "FeedInfo", "Fetching...")
        update_ui_poll_count(abbrev, "FeedInfo", 0, accumulate=False)
        nc = 1 if emit_record(abbrev, "FeedInfo", "snapshot", data_ver, endpoint="goServices.php?goWebVer=1") else 0
        update_ui_poll_count(abbrev, "FeedInfo", nc, accumulate=True)
        touch_ui_sync(abbrev, "FeedInfo")


def PASSIOGO_poll_vehicle_stream():
    """Consumes the PASSIOGO websocket location feed as a standalone stream."""
    if not WEBSOCKET_AVAILABLE:
        log("[PASSIOGO] websocket client is unavailable; vehicle_stream disabled.", level="warning")
        return

    stream_agency = PASSIOGO_STREAM_AGENCY
    stream_name = "vehicle_stream"
    payload = {
        "subscribe": "location",
        "userId": [int(agency_id) for agency_id in TARGET_AGENCIES.keys()],
        "field": ["busId", "latitude", "longitude", "course", "paxLoad", "more"],
    }

    while not stop_event.is_set():
        ws = None
        try:
            with _CACHE_LOCK:
                seen_bus_ids = sorted({
                    int(bus_id)
                    for bus_ids in PASSIOGO_CACHE.get("seen_bus_ids", {}).values()
                    for bus_id in bus_ids
                    if str(bus_id).isdigit()
                })

            if seen_bus_ids:
                payload["filter"] = {"outOfService": 0, "busId": seen_bus_ids}

            touch_ui_sync(stream_agency, stream_name, "Connecting...")
            ws = websocket.create_connection(
                "wss://PASSIOGO3.com/",
                origin="https://PASSIOGO.com",
                timeout=15,
            )
            touch_ui_sync(stream_agency, stream_name, "Connected")
            ws.settimeout(10)
            ws.send(json.dumps(payload))
            touch_ui_sync(stream_agency, stream_name, "Subscribed")
            
            update_ui_poll_count(stream_agency, stream_name, 0, accumulate=False)

            while not stop_event.is_set():
                try:
                    message = ws.recv()
                except WebSocketTimeoutException:
                    continue

                if not message:
                    continue

                try:
                    event = json.loads(message)
                except Exception:
                    continue

                if not isinstance(event, dict) or "busId" not in event:
                    continue

                if emit_record(stream_agency, stream_name, "update", event, endpoint="wss://PASSIOGO3.com/"):
                    update_ui_poll_count(stream_agency, stream_name, 1, accumulate=True)
                touch_ui_sync(stream_agency, stream_name)

        except Exception as exc:
            log(f"[PASSIOGO] vehicle_stream websocket error: {exc}", level="warning")
            touch_ui_sync(stream_agency, stream_name, "Failed")
            sleep_with_stop(5)
        finally:
            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass


def tripupdate_fetch_batch(session, agency_id, abbrev, dev_id, route_ids):
    if not route_ids:
        return 0, False

    params = [
        ("eta", "8"),
        ("deviceId", str(dev_id)),
    ]
    params.extend(("routeId", str(route_id)) for route_id in route_ids)
    params.append(("userId", str(agency_id)))

    try:
        response = session.get(
            f"{PASSIOGO_BASE_URL}/mapGetData.php",
            headers=session.headers,
            params=params,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 403:
            return 0, True
        if response.status_code != 200:
            return 0, False

        data = response.json()
        etas_dict = data.get("ETAs", {}) if isinstance(data, dict) else {}
        if not isinstance(etas_dict, dict):
            return 0, False

        new_count = 0
        for stop_id, eta_list in etas_dict.items():
            if not isinstance(eta_list, list):
                continue
            for eta_obj in eta_list:
                if isinstance(eta_obj, dict):
                    cleaned_eta = dict(eta_obj)
                    cleaned_eta.pop("error", None)
                    cleaned_eta["_parent_stop_id"] = stop_id
                    if emit_record(abbrev, "TripUpdates", "update", cleaned_eta, endpoint="mapGetData.php?eta=8"):
                        new_count += 1
        return new_count, False
    except Exception:
        return 0, False


#### DATA LAKE WRITING ####
seen_hashes = {}
seen_lock = threading.Lock()

def history_file_path(abbrev, stream, date_str):
    return EVENTS_DIR / f"{abbrev}_{stream_filename_label(stream)}_{date_str}.jsonl"

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

def ensure_history_loaded(abbrev, stream):
    stream_key = f"{abbrev}::{stream_filename_label(stream)}"
    with seen_lock:
        if stream_key in seen_hashes:
            return stream_key
            
    today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    history = load_history_metadata(history_file_path(abbrev, stream, today_str))
    
    with seen_lock:
        if stream_key not in seen_hashes:
            seen_hashes[stream_key] = history
    return stream_key

def touch_ui_sync(abbrev, stream, status=None):
    now_time = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    val = status if status else now_time
    ui_key = f"{abbrev}_{stream}"
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            UI_STREAM_STATE[ui_key]["last_sync_time"] = val

def update_ui_poll_count(abbrev, stream, count, accumulate=False):
    ui_key = f"{abbrev}_{stream}"
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            if accumulate:
                UI_STREAM_STATE[ui_key]["new_this_poll"] += count
            else:
                UI_STREAM_STATE[ui_key]["new_this_poll"] = count

def emit_record(abbrev, stream, event, data, endpoint="", metadata=None):
    now = datetime.now(LOCAL_TZ)
    now_ts = now.isoformat(timespec="seconds")
    stream_key = ensure_history_loaded(abbrev, stream)

    normalized_data = normalize_jsonable(data)
    normalized_metadata = normalize_jsonable(metadata) if metadata is not None else None
    
    hash_payload = {
        "agency": abbrev,
        "stream": stream,
        "event": event,
        "endpoint": endpoint,
        "data": normalized_data,
        "metadata": normalized_metadata,
    }
    payload = json.dumps(hash_payload, sort_keys=True, separators=(",", ":"), default=str)
    hash_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()

    with UI_STATE_LOCK:
        state = UI_STREAM_STATE.setdefault(
            f"{abbrev}_{stream}",
            {
                "agency": abbrev,
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
        "agency": abbrev,
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
            UI_STREAM_STATE[f"{abbrev}_{stream}"]["total_today"] += 1
        return True
    except Exception:
        log(f"[{abbrev}] Write queue is full for {stream}; dropping record", level="error", agency=abbrev)
        return False

def infer_stream_type(stream):
    hourly_like = {"FeedInfo", "Routes", "Stops"}
    if stream in hourly_like:
        return "Hourly"
    return "Streamed"

def init_ui_state(load_history=True):
    with UI_STATE_LOCK:
        preserved_times = {}
        for ui_key, state in UI_STREAM_STATE.items():
            if state["last_sync_time"] and state["last_sync_time"] not in ("Loading...", "Waiting...", "Waiting for Routes...", "Fetching...", "Failed"):
                preserved_times[ui_key] = state["last_sync_time"]

        for aid, abbrev in TARGET_AGENCIES.items():
            for stream in PASSIOGO_FEEDS:
                ui_key = f"{abbrev}_{stream}"
                UI_STREAM_STATE.setdefault(
                    ui_key,
                    {
                        "agency": abbrev,
                        "stream": stream,
                        "type": infer_stream_type(stream),
                        "new_this_poll": 0,
                        "total_today": 0,
                        "last_sync_time": "Loading...",
                    },
                )

        ui_key = f"{PASSIOGO_STREAM_AGENCY}_vehicle_stream"
        UI_STREAM_STATE.setdefault(
            ui_key,
            {
                "agency": PASSIOGO_STREAM_AGENCY,
                "stream": "vehicle_stream",
                "type": infer_stream_type("vehicle_stream"),
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
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except Exception:
                            continue

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
        for aid, abbrev in TARGET_AGENCIES.items():
            for stream in PASSIOGO_FEEDS:
                ui_key = f"{abbrev}_{stream}"
                UI_STREAM_STATE[ui_key] = temp_state.get(ui_key, {
                    "agency": abbrev,
                    "stream": stream,
                    "type": infer_stream_type(stream),
                    "new_this_poll": 0,
                    "total_today": 0,
                    "last_sync_time": "Loading...",
                })
                if ui_key in preserved_times:
                    old_ts = preserved_times[ui_key]
                    new_ts = UI_STREAM_STATE[ui_key]["last_sync_time"]
                    if new_ts == "Loading..." or old_ts > new_ts:
                        UI_STREAM_STATE[ui_key]["last_sync_time"] = old_ts

        ui_key = f"{PASSIOGO_STREAM_AGENCY}_vehicle_stream"
        UI_STREAM_STATE[ui_key] = temp_state.get(
            ui_key,
            {
                "agency": PASSIOGO_STREAM_AGENCY,
                "stream": "vehicle_stream",
                "type": infer_stream_type("vehicle_stream"),
                "new_this_poll": 0,
                "total_today": 0,
                "last_sync_time": "Loading...",
            },
        )
        if ui_key in preserved_times:
            old_ts = preserved_times[ui_key]
            new_ts = UI_STREAM_STATE[ui_key]["last_sync_time"]
            if new_ts == "Loading..." or old_ts > new_ts:
                UI_STREAM_STATE[ui_key]["last_sync_time"] = old_ts


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
    handles = {}
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try: record = write_queue.get(timeout=0.5)
            except Empty: continue
            try:
                abbrev = record.get("agency", "UNKNOWN")
                stream = record.get("stream", "stream")
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{abbrev}_{stream_filename_label(stream)}_{date_str}.jsonl"
                filepath = EVENTS_DIR / filename

                if filename not in handles:
                    handles[filename] = open(filepath, "a", encoding="utf-8")
                handles[filename].write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                handles[filename].flush()
            except Exception as exc:
                log(f"Writer exception: {exc}", level="error")
            finally:
                write_queue.task_done()
    finally:
        for h in handles.values():
            try: h.close()
            except: pass

def stringify_chunk(chunk):
    for col in ["data", "metadata"]:
        if col in chunk.columns:
            chunk[col] = chunk[col].apply(lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else (None if x is None else str(x)))
    return chunk

def compact_worker():
    if not PARQUET_AVAILABLE or ARGS.no_compaction: return
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            for file_path in EVENTS_DIR.glob("*_*.jsonl"):
                if today_str not in file_path.name:
                    parquet_path = ARCHIVE_DIR / file_path.with_suffix(".parquet").name
                    temp_path = Path(str(parquet_path) + ".tmp")
                    writer = None
                    row_count = 0
                    try:
                        for chunk in pd.read_json(file_path, lines=True, chunksize=50000):
                            chunk = stringify_chunk(chunk)
                            table = pa.Table.from_pandas(chunk, preserve_index=False)
                            if writer is None:
                                writer = pq.ParquetWriter(str(temp_path), table.schema, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                            writer.write_table(table)
                            row_count += len(chunk)
                        if writer is not None:
                            writer.close()
                            temp_path.replace(parquet_path)
                            file_path.unlink()
                            log(f"Compacted {file_path.name} into {parquet_path.name} ({row_count} rows).", level="info")
                    except Exception as exc:
                        log(f"Failed to compact {file_path.name}: {exc}", level="error")
                    finally:
                        if writer:
                            try: writer.close()
                            except: pass
                        if temp_path.exists():
                            try: temp_path.unlink()
                            except: pass
        except Exception: pass
        sleep_with_stop(60)


#### TRANSFORMATION & NORMALIZATION ####

def process_bus_detail(bus_data):
    """Maps custom values to standard GTFS-Realtime occupancy enums."""
    if not isinstance(bus_data, dict): return bus_data
    if 'gtfsOccupancyStatus' not in bus_data and 'paxLoad' in bus_data:
        try:
            pax = float(bus_data['paxLoad'])
            if pax == 0.0: bus_data['gtfsOccupancyStatus'] = 'EMPTY'
            elif pax < 0.3: bus_data['gtfsOccupancyStatus'] = 'MANY_SEATS_AVAILABLE'
            elif pax < 0.7: bus_data['gtfsOccupancyStatus'] = 'FEW_SEATS_AVAILABLE'
            elif pax < 0.9: bus_data['gtfsOccupancyStatus'] = 'STANDING_ROOM_ONLY'
            else: bus_data['gtfsOccupancyStatus'] = 'FULL'
        except (ValueError, TypeError):
            bus_data['gtfsOccupancyStatus'] = 'NO_DATA_AVAILABLE'
    return bus_data

def extract_items(data, preferred_key):
    """Extracts an iterable list of items from PASSIOGO's JSON structures."""
    if not data:
        return []
        
    target = data
    if isinstance(data, dict):
        if preferred_key in data:
            target = data[preferred_key]
        else:
            # Sometime PASSIOGO wraps it in the Agency ID first
            first_val = next(iter(data.values())) if data else None
            if isinstance(first_val, dict) and preferred_key in first_val:
                target = first_val[preferred_key]
        
    if isinstance(target, dict):
        items = []
        for k, v in target.items():
            if isinstance(v, dict):
                # PASSIOGO often uses the ID as the dictionary key; inject it safely just in case
                v_copy = dict(v)
                if "id" not in v_copy and "Id" not in str(v_copy.keys()):
                    v_copy["_PASSIOGO_dict_key"] = k
                items.append(v_copy)
            else:
                items.append({"_key": k, "value": v})
        return items
    elif isinstance(target, list):
        return target
    else:
        return [target]

#### PASSIOGO POLLING LOGIC ####

def PASSIOGO_run_hourly_for_agency(agency_id, abbrev):
    """
    Triggers independent hourly endpoints mapping to Data Lake standard feeds.
    """
    # Force a totally new session and new random deviceId every hour to stop 403 blocks
    session = get_agency_session(agency_id, force_new=True)
    dev_id = AGENCY_DEVICE_IDS[agency_id]
    log(f"[{abbrev}] Starting hourly initialization", level="info", agency=abbrev)
    
    # 1. Establish PHP Session (The Handshake)
    try:
        log(f"[{abbrev}] Handshake starting", level="info", agency=abbrev)
        session.get(f"{PASSIOGO_BASE_URL}/?agency={agency_id}", timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS)
        log(f"[{abbrev}] Handshake complete", level="info", agency=abbrev)
    except Exception as e:
        log(f"[{abbrev}] Handshake error: {e}", level="warning", agency=abbrev)

    # 2. Fetch Routes & Extract Route IDs for ETA polling
    touch_ui_sync(abbrev, "Routes", "Fetching...")
    update_ui_poll_count(abbrev, "Routes", 0, accumulate=False)
    try:
        payload = {"systemSelected0": str(agency_id), "amount": 1}
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        log(f"[{abbrev}] Routes request starting", level="info", agency=abbrev)
        res = session.post(
            f"{PASSIOGO_BASE_URL}/mapGetData.php?getRoutes=1&deviceId={dev_id}",
            data={"json": json.dumps(payload)},
            headers=headers,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS
        )
        res.raise_for_status()
        data = json.loads(res.text.strip())
        log(f"[{abbrev}] Routes request complete", level="info", agency=abbrev)
        
        nc = 0
        route_ids = set()
        seen_route_ids = set()
        for item in extract_items(data, "routes"):
            # Keep only the canonical route id; myid is a duplicate variant.
            rid = str(item.get("id") or item.get("myid") or "").strip()
            if not rid or rid in seen_route_ids:
                continue
            seen_route_ids.add(rid)
            route_ids.add(rid)

            route_item = dict(item)
            route_item.pop("myid", None)

            if emit_record(abbrev, "Routes", "snapshot", route_item, endpoint="mapGetData.php?getRoutes=1"):
                nc += 1
        
        if not route_ids:
            log(f"[{abbrev}] WARNING: No route IDs found in getRoutes response.", level="warning", agency=abbrev)
        else:
            log(f"[{abbrev}] Extracted {len(route_ids)} route IDs.", level="info", agency=abbrev)

        # Cache Route IDs globally for the TripUpdates loop
        if route_ids:
            with _CACHE_LOCK:
                if "route_ids" not in PASSIOGO_CACHE:
                    PASSIOGO_CACHE["route_ids"] = {}
                if agency_id not in PASSIOGO_CACHE["route_ids"]:
                    PASSIOGO_CACHE["route_ids"][agency_id] = set()
                PASSIOGO_CACHE["route_ids"][agency_id].update(route_ids)
            save_cache()

        update_ui_poll_count(abbrev, "Routes", len(route_ids), accumulate=True)
        touch_ui_sync(abbrev, "Routes")
    except Exception as e:
        log(f"[{abbrev}] Routes fetch failed: {e}", level="warning", agency=abbrev)
        if "403" in str(e):
            get_agency_session(agency_id, force_new=True)
        touch_ui_sync(abbrev, "Routes", "Failed")

    # 3. Fetch Stops
    touch_ui_sync(abbrev, "Stops", "Fetching...")
    update_ui_poll_count(abbrev, "Stops", 0, accumulate=False)
    try:
        payload = {"s0": str(agency_id), "sA": 1}
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        # Cleared out extra unneeded url parameters here
        log(f"[{abbrev}] Stops request starting", level="info", agency=abbrev)
        res = session.post(
            f"{PASSIOGO_BASE_URL}/mapGetData.php?getStops=2&deviceId={dev_id}&withOutdated=1",
            data={"json": json.dumps(payload)},
            headers=headers,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS
        )
        res.raise_for_status()
        data = json.loads(res.text.strip())
        log(f"[{abbrev}] Stops request complete", level="info", agency=abbrev)
        
        nc = 0
        items = extract_items(data, "stops")
        for item in items:
            if emit_record(abbrev, "Stops", "snapshot", item, endpoint="mapGetData.php?getStops=2"):
                nc += 1
                
        update_ui_poll_count(abbrev, "Stops", len(items), accumulate=True)
        touch_ui_sync(abbrev, "Stops")
    except Exception as e:
        log(f"[{abbrev}] Stops fetch failed: {e}", level="warning", agency=abbrev)
        if "403" in str(e):
            get_agency_session(agency_id, force_new=True)
        touch_ui_sync(abbrev, "Stops", "Failed")


def PASSIOGO_poll_vehicles(agency_id, abbrev):
    """
    Executes live vehicle data requests for both generic positions and enhanced telemetry.
    """
    session = get_agency_session(agency_id)
    dev_id = AGENCY_DEVICE_IDS[agency_id]
    touch_ui_sync(abbrev, "Vehicles", "Fetching...")
    touch_ui_sync(abbrev, "EnhancedVehicles", "Waiting...")
    update_ui_poll_count(abbrev, "Vehicles", 0, accumulate=False)
    update_ui_poll_count(abbrev, "EnhancedVehicles", 0, accumulate=False)

    try:
        # Build dynamic payload passing route IDs as required by PASSIOGO
        payload = {"s0": str(agency_id), "sA": 1}
        with _CACHE_LOCK:
            route_ids = list(PASSIOGO_CACHE.get("route_ids", {}).get(agency_id, set()))
            
        if route_ids:
            payload["rA"] = len(route_ids)
            for i, rid in enumerate(route_ids):
                payload[f"r{i}"] = str(rid)

        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        
        res = session.post(
            f"{PASSIOGO_BASE_URL}/mapGetData.php?getBuses=2&deviceId={dev_id}",
            data={"json": json.dumps(payload)},
            headers=headers,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS
        )
        res.raise_for_status()
        text_resp = res.text.strip()
        data = json.loads(text_resp)

        nc = 0
        buses_obj = data.get("buses", {})
        bus_ids = set()
        
        # Iterate over the agency buckets and then the buses inside
        for key, bus_list in buses_obj.items():
            for bus in bus_list:
                if "busId" in bus:
                    bus_ids.add(bus["busId"])
                
                # Emit the core bus record here!
                if emit_record(abbrev, "Vehicles", "update", bus, endpoint="mapGetData.php?getBuses=2"):
                    nc += 1
        
        update_ui_poll_count(abbrev, "Vehicles", len(bus_ids), accumulate=True)
        touch_ui_sync(abbrev, "Vehicles")

        with _CACHE_LOCK:
            PASSIOGO_CACHE.setdefault("live_bus_ids", {})[agency_id] = set(str(bus_id) for bus_id in bus_ids)
            PASSIOGO_CACHE.setdefault("seen_bus_ids", {}).setdefault(agency_id, set()).update(str(bus_id) for bus_id in bus_ids)

        # Fetch detailed telemetry + occupancy
        touch_ui_sync(abbrev, "EnhancedVehicles", "Fetching...")
        detailed_buses = []
        local_403_event = threading.Event()
        
        def fetch_bus_detail(b_id):
            if stop_event.is_set() or local_403_event.is_set(): return None
            
            # Add a human jitter to prevent rapid-fire 403 blocks
            time.sleep(random.uniform(0.1, 0.4))
            
            try:
                url = f"{PASSIOGO_BASE_URL}/mapGetData.php?bus=1&busId={b_id}&deviceId={dev_id}"
                r = session.get(url, headers=session.headers, timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS)
                if r.status_code == 403:
                    if not local_403_event.is_set():
                        local_403_event.set()
                        log(f"[{abbrev}] 403 WAF Block on EnhancedVehicles. Aborting loop.", level="warning", agency=abbrev)
                        get_agency_session(agency_id, force_new=True)
                    return None
                if r.status_code == 200:
                    return r.json()
            except: pass
            return None

        if bus_ids:
            # Keep this sequential so we do not burst the WAF with parallel detail requests.
            for bus_id in bus_ids:
                res_detail = fetch_bus_detail(bus_id)
                if res_detail and "theBus" in res_detail:
                    detailed_buses.append(res_detail)
        
        nc_enhanced = 0
        for dbus in detailed_buses:
            processed_dbus = process_bus_detail(dbus)
            if emit_record(abbrev, "EnhancedVehicles", "update", processed_dbus, endpoint="mapGetData.php?bus=1"):
                nc_enhanced += 1

        update_ui_poll_count(abbrev, "EnhancedVehicles", len(detailed_buses), accumulate=True)
        touch_ui_sync(abbrev, "EnhancedVehicles", "Failed" if local_403_event.is_set() else None)

    except Exception as e:
        log(f"[{abbrev}] Vehicle fetch failed: {e}", level="warning", agency=abbrev)
        if "403" in str(e):
            get_agency_session(agency_id, force_new=True)
        touch_ui_sync(abbrev, "Vehicles", "Failed")
        touch_ui_sync(abbrev, "EnhancedVehicles", "Failed")


def PASSIOGO_poll_vehicles_all_agencies():
    """Fetch buses once for all agencies and split locally by userId."""
    for agency_id, abbrev in TARGET_AGENCIES.items():
        touch_ui_sync(abbrev, "Vehicles", "Fetching...")
        touch_ui_sync(abbrev, "EnhancedVehicles", "Fetching...")
        update_ui_poll_count(abbrev, "Vehicles", 0, accumulate=False)
        update_ui_poll_count(abbrev, "EnhancedVehicles", 0, accumulate=False)

    # Reuse one session to reduce connection churn and request volume
    first_agency_id = next(iter(TARGET_AGENCIES.keys()), None)
    if not first_agency_id:
        return
    session = get_agency_session(first_agency_id)
    dev_id = AGENCY_DEVICE_IDS.get(first_agency_id, str(random.randint(10000000, 999999999)))

    try:
        payload = build_multi_agency_payload("s")
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        response = session.post(
            f"{PASSIOGO_BASE_URL}/mapGetData.php?getBuses=2&deviceId={dev_id}",
            data={"json": json.dumps(payload)},
            headers=headers,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS,
        )
        response.raise_for_status()
        data = json.loads(response.text.strip())

        vehicle_counts = {abbrev: 0 for abbrev in TARGET_AGENCIES.values()}
        enhanced_counts = {abbrev: 0 for abbrev in TARGET_AGENCIES.values()}
        buses_obj = data.get("buses", {}) if isinstance(data, dict) else {}

        if isinstance(buses_obj, dict):
            for bus_list in buses_obj.values():
                if not isinstance(bus_list, list):
                    continue
                for bus in bus_list:
                    if not isinstance(bus, dict):
                        continue
                    agency_id = str(bus.get("userId", "")).strip()
                    abbrev = TARGET_AGENCIES.get(agency_id)
                    if not abbrev:
                        continue

                    # Keep route cache warm using live bus payloads
                    route_id = str(bus.get("routeId", "")).strip()
                    if route_id:
                        with _CACHE_LOCK:
                            PASSIOGO_CACHE.setdefault("route_ids", {}).setdefault(agency_id, set()).add(route_id)

                    vehicle_counts[abbrev] += 1
                    emit_record(abbrev, "Vehicles", "update", bus, endpoint="mapGetData.php?getBuses=2")

                    # Local enhancement path: derive occupancy from same payload instead of extra bus=1 calls
                    enhanced_bus = process_bus_detail(dict(bus))
                    enhanced_counts[abbrev] += 1
                    emit_record(abbrev, "EnhancedVehicles", "update", enhanced_bus, endpoint="mapGetData.php?getBuses=2")

                    bus_id = str(bus.get("busId", "")).strip()
                    if bus_id:
                        with _CACHE_LOCK:
                            PASSIOGO_CACHE.setdefault("live_bus_ids", {}).setdefault(agency_id, set()).add(bus_id)
                            PASSIOGO_CACHE.setdefault("seen_bus_ids", {}).setdefault(agency_id, set()).add(bus_id)

        save_cache()

        for agency_id, abbrev in TARGET_AGENCIES.items():
            vc = vehicle_counts.get(abbrev, 0)
            ec = enhanced_counts.get(abbrev, 0)
            update_ui_poll_count(abbrev, "Vehicles", vc, accumulate=True)
            update_ui_poll_count(abbrev, "EnhancedVehicles", ec, accumulate=True)
            touch_ui_sync(abbrev, "Vehicles")
            touch_ui_sync(abbrev, "EnhancedVehicles")
            if vc or ec:
                log(f"Vehicles: {vc} active, {ec} enhanced", level="info", agency=abbrev)

    except Exception as exc:
        for agency_id, abbrev in TARGET_AGENCIES.items():
            log(f"[{abbrev}] Multi-agency vehicle fetch failed: {exc}", level="warning", agency=abbrev)
            touch_ui_sync(abbrev, "Vehicles", "Failed")
            touch_ui_sync(abbrev, "EnhancedVehicles", "Failed")


def PASSIOGO_poll_trip_updates(agency_id, abbrev):
    """
    Polls ETAs in route batches based on the cached Route IDs per agency.
    """
    if tripupdate_in_cooldown(agency_id):
        touch_ui_sync(abbrev, "TripUpdates", "403 Backoff")
        return

    session = get_agency_session(agency_id)
    dev_id = AGENCY_DEVICE_IDS[agency_id]
    touch_ui_sync(abbrev, "TripUpdates", "Fetching...")
    update_ui_poll_count(abbrev, "TripUpdates", 0, accumulate=False)

    with _CACHE_LOCK:
        route_ids = list(PASSIOGO_CACHE.get("route_ids", {}).get(agency_id, set()))
        active_routes = PASSIOGO_CACHE.get("active_route_ids", {}).get(agency_id, set())

    if active_routes:
        route_ids = [route_id for route_id in route_ids if str(route_id) in active_routes]

    if not route_ids:
        touch_ui_sync(abbrev, "TripUpdates", "Waiting for Routes...")
        return

    nc_fetched = 0
    local_403_event = threading.Event()

    for batch_route_ids in chunked(route_ids, TRIPUPDATE_ROUTE_BATCH_SIZE):
        if stop_event.is_set() or local_403_event.is_set():
            break
        batch_count, batch_403 = tripupdate_fetch_batch(session, agency_id, abbrev, dev_id, batch_route_ids)
        if batch_403:
            local_403_event.set()
            tripupdate_mark_403(agency_id, abbrev)
            break
        if batch_count:
            nc_fetched += batch_count

        # Respectfully avoid bursts: small pause between batches
        sleep_with_stop(random.uniform(0.15, 0.5))

        # Optionally rotate per-batch session (safe: creates a new session/deviceId)
        if TRIPUPDATE_ROTATE_SESSION_PER_BATCH:
            get_agency_session(agency_id, force_new=True)
            session = get_agency_session(agency_id)
            dev_id = AGENCY_DEVICE_IDS[agency_id]

    if nc_fetched > 0:
        tripupdate_clear_failure_state(agency_id)

    update_ui_poll_count(abbrev, "TripUpdates", nc_fetched, accumulate=True)
    touch_ui_sync(abbrev, "TripUpdates", "Failed" if local_403_event.is_set() else None)
    if nc_fetched > 0:
        log(f"TripUpdates: {nc_fetched} ETAs fetched", level="info", agency=abbrev)


def PASSIOGO_poll_alerts(agency_id, abbrev):
    """Requests active service alerts and notifications for the agency."""
    session = get_agency_session(agency_id)
    dev_id = AGENCY_DEVICE_IDS[agency_id]
    touch_ui_sync(abbrev, "Alerts", "Fetching...")
    update_ui_poll_count(abbrev, "Alerts", 0, accumulate=False)

    try:
        payload = {"systemSelected0": str(agency_id), "amount": 1, "routesAmount": 0}
        headers = {"Content-Type": "application/x-www-form-urlencoded; charset=UTF-8"}
        res = session.post(
            f"{PASSIOGO_BASE_URL}/goServices.php?getAlertMessages=1&deviceId={dev_id}&alertCRC=19ed0c5f&buildNo=110&noOptions=1",
            data={"json": json.dumps(payload)},
            headers=headers,
            timeout=PASSIOGO_REQUEST_TIMEOUT_SECONDS,
        )
        res.raise_for_status()
        data = json.loads(res.text.strip())

        nc = 0
        msgs = data.get("msgs", []) if isinstance(data, dict) else []
        if not isinstance(msgs, list):
            msgs = []
        for item in msgs:
            if emit_record(abbrev, "Alerts", "update", item, endpoint="goServices.php?getAlertMessages=1"):
                pass
            nc += 1

        update_ui_poll_count(abbrev, "Alerts", nc, accumulate=True)
        touch_ui_sync(abbrev, "Alerts")

    except Exception as e:
        log(f"[{abbrev}] Alert fetch exception: {e}", level="warning", agency=abbrev)
        if "403" in str(e):
            get_agency_session(agency_id, force_new=True)
        touch_ui_sync(abbrev, "Alerts", "Failed")


#### BACKGROUND LOOPS ####
def PASSIOGO_hourly_loop():
    """Runs graph initialization for all agencies periodically."""
    last_hour = -1
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        force = HOURLY_REFRESH_FLAG.is_set()
        
        if force or (now.minute == 0 and now.hour != last_hour) or last_hour == -1:
            if force: HOURLY_REFRESH_FLAG.clear()
            last_hour = now.hour
            
            log("Running PASSIOGO Hourly Feeds", level="info")
            PASSIOGO_fetch_feed_info_all_agencies()
            with ThreadPoolExecutor(max_workers=min(len(TARGET_AGENCIES), MAX_WORKERS)) as executor:
                futures = [executor.submit(PASSIOGO_run_hourly_for_agency, agency_id, abbrev) for agency_id, abbrev in TARGET_AGENCIES.items()]
                for future in as_completed(futures):
                    if stop_event.is_set():
                        break
                    try:
                        future.result()
                    except Exception as exc:
                        log(f"[PASSIOGO] Hourly agency init failed: {exc}", level="warning")
                
        sleep_with_stop(5)

def PASSIOGO_vehicle_loop():
    """Polls live vehicles for all configured agencies."""
    sleep_with_stop(5) # Give init a head start
    while not stop_event.is_set():
        start_time = time.time()
        if PASSIOGO_MULTI_AGENCY_BUS_POLL:
            PASSIOGO_poll_vehicles_all_agencies()
        else:
            for agency_id, abbrev in TARGET_AGENCIES.items():
                if stop_event.is_set():
                    break
                PASSIOGO_poll_vehicles(agency_id, abbrev)
            
        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)

def PASSIOGO_tripupdate_loop():
    """Polls ETAs (Trip Updates) based on cached Route IDs."""
    sleep_with_stop(PASSIOGO_TRIPUPDATE_START_DELAY_SECONDS) # Offset start
    while not stop_event.is_set():
        start_time = time.time()
        # Refresh active routes periodically to limit TripUpdates to active trips
        if time.time() - PASSIOGO_LAST_ACTIVE_REFRESH > PASSIOGO_ACTIVE_REFRESH_SECONDS:
            try:
                fetch_active_routes_all()
            except Exception:
                pass

        for agency_id, abbrev in TARGET_AGENCIES.items():
            if stop_event.is_set(): break
            PASSIOGO_poll_trip_updates(agency_id, abbrev)
            
        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)

def PASSIOGO_alert_loop():
    """Polls service alerts for all configured agencies."""
    sleep_with_stop(10)
    while not stop_event.is_set():
        start_time = time.time()
        for agency_id, abbrev in TARGET_AGENCIES.items():
            if stop_event.is_set(): break
            PASSIOGO_poll_alerts(agency_id, abbrev)
            
        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_SLOW - elapsed) + random.random() * ARGS.poll_jitter)

def start_workers():
    load_cache()
    threading.Thread(target=writer_worker, daemon=True).start()
    if PARQUET_AVAILABLE:
        threading.Thread(target=compact_worker, daemon=True).start()

    log("Starting PASSIOGO loops")
    threading.Thread(target=PASSIOGO_hourly_loop, daemon=True).start()
    threading.Thread(target=PASSIOGO_vehicle_loop, daemon=True).start()
    threading.Thread(target=PASSIOGO_poll_vehicle_stream, daemon=True).start()
    threading.Thread(target=PASSIOGO_tripupdate_loop, daemon=True).start()
    threading.Thread(target=PASSIOGO_alert_loop, daemon=True).start()


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
        # Match standard sort: Agency alphabetically, then Stream predefined order
        order_map = {k: i for i, k in enumerate(PASSIOGO_FEEDS)}
        rows = sorted(
            UI_STREAM_STATE.values(),
            key=lambda s: (
                0 if s["agency"] == PASSIOGO_STREAM_AGENCY else 1,
                s["agency"],
                order_map.get(s["stream"], 99),
            ),
        )
        
        now = datetime.now(LOCAL_TZ)
        RECENT_ERRORS[:] = [e for e in RECENT_ERRORS if (now - e["dt"]).total_seconds() < 180]
        errors = list(RECENT_ERRORS)

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
        for state in rows:
            stream_name = state['stream']
            gtfs_std = GTFS_MAP.get(stream_name, "unknown")
            stream_label = f"{stream_name} ({state['type']})"
            if state["agency"] == PASSIOGO_STREAM_AGENCY and stream_name == "vehicle_stream":
                color = "bright_magenta"
            else:
                color = "green" if state["type"] == "Hourly" else "cyan"

            sync_time = state["last_sync_time"]
            sync_style = "yellow" if sync_time in ("Loading...", "Waiting...", "Waiting for Routes...", "Fetching...", "Failed", "Subscribed") else ""

            new_poll_val = str(state.get("new_this_poll", 0))
            new_poll_style = "dim" if new_poll_val == "0" else ""

            # True unique metric computed directly via our stateful lock map
            stream_key = f"{state['agency']}::{stream_filename_label(state['stream'])}"
            with seen_lock:
                true_unique_today = len(seen_hashes.get(stream_key, {}))

            table.add_row(
                Text(state["agency"], style="bold white"),
                Text(stream_label, style=color),
                Text(gtfs_std, style="dim", justify="center"),
                Text(sync_time, style=sync_style, justify="center"),
                Text(new_poll_val, style=new_poll_style, justify="center"),
                Text(str(true_unique_today), justify="center"),
            )

    if errors:
        error_text = Text()
        for entry in errors[-5:]:
            error_text.append(f"[{entry['ts_str']}] {entry['message']}\n", style="bold red")
    else:
        error_text = Text(f"No errors in the last 3 minutes. Written safely to {LOG_FILE.name}.", style="dim")

    poll_countdown = POLL_INTERVAL_FAST - int(time.time() % POLL_INTERVAL_FAST)
    next_h = next_hourly_timestamp()
    time_to_hourly = format_duration((next_h - datetime.now(LOCAL_TZ)).total_seconds())
    next_h_str = next_h.strftime('%I:%M %p').lstrip('0')

    if refresh_flash:
        refresh_note = "[bold green]REFRESHING HOURLY FEEDS...[/bold green]"
    else:
        refresh_note = "[bold yellow]Press R[/bold yellow] to force refresh hourly feeds  |  [bold yellow]Press Q[/bold yellow] to quit"

    footer_body = f"Next auto-poll: ~{poll_countdown}s  |  Next hourly auto-run: {time_to_hourly} (@ {next_h_str})\n{refresh_note}"
    footer = Panel(footer_body, title="Controls", border_style="blue")
    errors_panel = Panel(error_text, title="Recent Logs", border_style="red")

    return Group(table, errors_panel, footer)

_refresh_flash_until = 0.0

def trigger_full_refresh():
    global _refresh_flash_until
    _refresh_flash_until = time.time() + 3.0
    log("Manual refresh requested (R key)", level="warning")
    tripupdate_clear_backoff()
    HOURLY_REFRESH_FLAG.set()
    trigger_ui_refresh()

def run_live_gui():
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
                try: key = os.read(fd, 1).decode(errors="ignore").lower()
                except Exception: return None
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
                
            # History log parsing is pushed back to unblock initialization UI render
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
    log(f"PASSIOGO ingestor starting. Data dir: {DATA_DIR}  Log: {LOG_FILE}", level="info")
    
    # Very fast, synchronous load logic so the GUI renders immediately 
    init_ui_state(load_history=False)
    try:
        run_live_gui()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        log_api_call_summary(final=True)
        log("PASSIOGO ingestor stopped.", level="info")

if __name__ == "__main__":
    main()