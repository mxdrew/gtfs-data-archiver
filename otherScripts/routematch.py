#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 28, 2026 (Last updated June 30, 2026)
#
# Description:
# Dedicated ingestion engine for RouteMatch Transit Systems.
# Uses stateful session mapping to pull independent streams (Vehicles, Trips, Routes, Stops, Departures)
# for configured agencies and maps them to standard Data Lake GTFS formats.
#
# Data Integrity & Storage:
# Implements an append-only JSONL write path with deterministic SHA-256 hashes.
# A background compactor rotates completed day files into Parquet archives under data/archive/.
# Live polling workers remain isolated from file rotation and compaction work.
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
import threading
import time
import urllib.parse
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

def env_int(name, default):
    raw_value = env_text(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default

def env_float(name, default):
    raw_value = env_text(name, str(default))
    try:
        return float(raw_value)
    except ValueError:
        return default

def safe_prefix(value):
    cleaned_value = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return cleaned_value.strip("_") or "agency"

def default_data_dir():
    # Prefer DATA_DIR_WSL (cross-platform, used by all other scripts)
    raw = os.environ.get("DATA_DIR_WSL", "").strip()
    if not raw:
        raw = os.environ.get("DATA_DIR", "").strip()
    if raw:
        is_windows = os.name == "nt"
        if is_windows and raw.startswith("/mnt/"):
            parts = raw.split("/")
            if len(parts) >= 3:
                drive = parts[2].upper()
                rest = "\\".join(parts[3:])
                raw = f"{drive}:\\{rest}"
        elif not is_windows and len(raw) >= 2 and raw[1] == ":":
            drive = raw[0].lower()
            rest = raw[2:].replace("\\", "/").lstrip("/")
            raw = f"/mnt/{drive}/{rest}"
        return Path(raw)

    # Fallback: next to this script
    return Path(__file__).parent / "data"

ARGS = SimpleNamespace(
    no_compaction=False,
    global_concurrency=12,
    poll_jitter=1.25,
)

# Set to True to skip polling /departures/byStop/{id} entirely.
# BRTA's firewall blocks this endpoint from server IPs — it works in a browser
# but returns "Request Rejected" from any non-residential IP. Vehicles and trips
# still collect fine; departure predictions can be inferred from TripByID data.
SKIP_DEPARTURES_BY_STOP = True

ROUTEMATCH_AGENCY = env_text("ROUTEMATCH_AGENCY").upper()
ROUTEMATCH_BASE_URL = env_text("ROUTEMATCH_BASE_URL").rstrip("/")
ROUTEMATCH_REFERER = env_text("ROUTEMATCH_REFERER").rstrip("/") + "/"
ROUTEMATCH_AGENCIES = (
    {
        "agency": ROUTEMATCH_AGENCY,
        "feed_url": ROUTEMATCH_BASE_URL,
    },
)
ROUTEMATCH_LABEL = "RouteMatch"
ROUTEMATCH_OUTPUT_LABEL = safe_prefix(ROUTEMATCH_AGENCY)

LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE", "America/New_York") or "America/New_York")
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
CACHE_DIR = DATA_DIR / "cache"
LOG_FILE = LOG_DIR / f"{ROUTEMATCH_LABEL}_ingest.log"

for directory in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR, CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
# Polling cadence is fixed, but can be updated here.
POLL_INTERVAL_FAST = 10
POLL_INTERVAL_SLOW = 30
MAX_WORKERS = max(1, ARGS.global_concurrency)

REQUEST_SEMAPHORE = threading.Semaphore(MAX_WORKERS)
write_queue = Queue(maxsize=50000)
stop_event = threading.Event()

TUI_READY = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []

HOURLY_REFRESH_FLAG = threading.Event()

ROUTEMATCH_FEEDS = (
    "Vehicles",
    "VehicleByID",
    "ParaVehicle",
    "ParaVehicleByID",
    "TripByID",
    "Version",
    "Timezone",
    "MasterRoute",
    "MasterRouteNames",
    "LandRouteByRoute",
    "StopsByRoute",
    "StopKeywords",
    "StopByKeyword",
    "DeparturesByStop",
)

RouteMatch_FEEDS = ROUTEMATCH_FEEDS

# Maps RouteMatch endpoints to GTFS file-name stems used for JSONL and Parquet outputs.
GTFS_MAP = {
    "Version": "feed_info",
    "Timezone": "feed_info",
    "MasterRoute": "routes",
    "MasterRouteNames": "routes",
    "LandRouteByRoute": "shapes",
    "StopsByRoute": "stops",
    "StopKeywords": "custom_keywords",
    "StopByKeyword": "stops",
    "Vehicles": "vehicle_positions",
    "VehicleByID": "vehicle_positions_enhanced",
    "ParaVehicle": "vehicle_positions",
    "ParaVehicleByID": "vehicle_positions_enhanced",
    "TripByID": "trip_updates",
    "DeparturesByStop": "trip_updates",
}

#### DEPENDENCY EVENTS ####
ROUTEMATCH_ROUTES_READY = threading.Event()
ROUTEMATCH_VEHICLES_READY = threading.Event()

RouteMatch_ROUTES_READY = ROUTEMATCH_ROUTES_READY
RouteMatch_VEHICLES_READY = ROUTEMATCH_VEHICLES_READY

#### CACHE MANAGEMENT ####
ROUTEMATCH_CACHE_FILE = CACHE_DIR / "routematch_cache.json"

ROUTEMATCH_CACHE = {
    "vehicle_ids": set(),
    "trip_ids": set(),
    "route_ids": set(),
    "stop_ids": set(),
    "paravehicle_ids": set(),
    "keywords": set(),
}

API_CALL_STATS = {
    "current_date": datetime.now(LOCAL_TZ).strftime("%Y%m%d"),
    "current_total": 0,
    "previous_date": None,
    "previous_total": 0,
}

RouteMatch_CACHE_FILE = ROUTEMATCH_CACHE_FILE
RouteMatch_CACHE = ROUTEMATCH_CACHE

_CACHE_LOCK = threading.Lock()


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

def load_cache():
    try:
        if not ROUTEMATCH_CACHE_FILE.exists():
            log(f"No cache file found at {ROUTEMATCH_CACHE_FILE}; starting fresh.", level="info")
            return
        with open(ROUTEMATCH_CACHE_FILE, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        with _CACHE_LOCK:
            for key, values in saved.items():
                if key in ROUTEMATCH_CACHE:
                    ROUTEMATCH_CACHE[key] = set(str(v) for v in values)
            stats = saved.get("api_calls") if isinstance(saved, dict) else None
            if isinstance(stats, dict):
                API_CALL_STATS["current_date"] = str(stats.get("current_date", API_CALL_STATS["current_date"]))
                API_CALL_STATS["current_total"] = int(stats.get("current_total", 0))
                API_CALL_STATS["previous_date"] = stats.get("previous_date")
                API_CALL_STATS["previous_total"] = int(stats.get("previous_total", 0))
                    
        if ROUTEMATCH_CACHE["route_ids"] and ROUTEMATCH_CACHE["stop_ids"]:
            ROUTEMATCH_ROUTES_READY.set()
        if ROUTEMATCH_CACHE["vehicle_ids"]:
            ROUTEMATCH_VEHICLES_READY.set()
            
        log(f"Cache loaded from {ROUTEMATCH_CACHE_FILE}", level="info")
    except Exception as exc:
        log(f"Could not load cache: {exc}", level="error")

def save_cache():
    try:
        with _CACHE_LOCK:
            payload = {k: sorted(list(v)) for k, v in ROUTEMATCH_CACHE.items()}
            payload["api_calls"] = dict(API_CALL_STATS)
            
            temp_path = ROUTEMATCH_CACHE_FILE.with_suffix(".tmp")
            temp_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(temp_path, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)
                
            temp_path.replace(ROUTEMATCH_CACHE_FILE)
    except Exception as exc:
        log(f"Could not save cache: {exc}", level="error")

#### LOGGING ####
_log_lock = threading.Lock()

def log(message, level="info", agency=ROUTEMATCH_AGENCY):
    now = datetime.now(LOCAL_TZ)
    ts_full = now.strftime("%Y-%m-%d %H:%M:%S")
    ts_short = now.strftime("%H:%M:%S")
    line = f"{ts_full} [{level.upper()}] [{agency}] {message}\n"

    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as handle:
                handle.write(line)
    except Exception:
        pass

    if level == "error" or level == "warning":
        with UI_STATE_LOCK:
            RECENT_ERRORS.append({
                "dt": now, 
                "ts_str": ts_short, 
                "message": str(message)
            })
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


#### SHARED HTTP ####
_http_session = None

def get_http_session():
    global _http_session
    if _http_session is None:
        session = requests.Session()
        retry = Retry(total=3, connect=3, read=3, status_forcelist=(429, 500, 502, 503, 504))
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _http_session = session
    return _http_session

def guarded_get(url, **kwargs):
    session = get_http_session()
    with REQUEST_SEMAPHORE:
        return session.get(url, **kwargs)

def sleep_with_stop(seconds):
    deadline = time.time() + max(0, seconds)
    while not stop_event.is_set() and time.time() < deadline:
        time.sleep(0.25)

def normalize_jsonable(value):
    if isinstance(value, dict):
        return {key: normalize_jsonable(inner) for key, inner in value.items()}
    if isinstance(value, list):
        return [normalize_jsonable(item) for item in value]
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    return value

def request_json(url, headers=None, params=None, timeout=10):
    try:
        record_api_call()
        response = guarded_get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if response.status_code != 200:
            log(f"HTTP {response.status_code} for {url}", level="warning")
            return None
        try:
            return response.json()
        except Exception:
            text = response.text.strip()
            # Discard HTML responses (WAF rejections, login redirects, etc.) — never store as data
            text_lower = text.lower()
            if text_lower.startswith("<html") or text_lower.startswith("<!doctype") or "<title>request rejected</title>" in text_lower:
                log(f"Received HTML response (WAF/firewall rejection?) from {url} — discarding", level="warning")
                return None
            return {"data": text} if text else None
    except Exception as exc:
        log(f"Request failed for {url}: {exc}", level="warning")
        return None

#### SHARED OUTPUT ####

def history_file_path(stream, date_str):
    return EVENTS_DIR / f"{ROUTEMATCH_OUTPUT_LABEL}_{gtfs_file_stem(stream)}_{date_str}.jsonl"

def gtfs_file_stem(stream):
    return GTFS_MAP.get(stream, safe_prefix(stream))

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

seen_hashes = {}
seen_lock = threading.Lock()

def ensure_history_loaded(stream):
    stream_key = f"{ROUTEMATCH_AGENCY}::{safe_prefix(stream)}"
    with seen_lock:
        if stream_key in seen_hashes:
            return stream_key
            
    today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    history = load_history_metadata(history_file_path(stream, today_str))
    
    with seen_lock:
        if stream_key not in seen_hashes:
            seen_hashes[stream_key] = history
    return stream_key

def touch_ui_sync(stream, status=None):
    now_time = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    val = status if status else now_time
    with UI_STATE_LOCK:
        if stream in UI_STREAM_STATE:
            UI_STREAM_STATE[stream]["last_sync_time"] = val

def update_ui_poll_count(stream, count):
    with UI_STATE_LOCK:
        if stream in UI_STREAM_STATE:
            UI_STREAM_STATE[stream]["new_this_poll"] = count

def emit_record(stream, event, data, endpoint="", metadata=None):
    now = datetime.now(LOCAL_TZ)
    now_ts = now.isoformat(timespec="seconds")
    stream_key = ensure_history_loaded(stream)

    normalized_data = normalize_jsonable(data)
    normalized_metadata = normalize_jsonable(metadata) if metadata is not None else None
    hash_payload = {
        "agency": ROUTEMATCH_AGENCY,
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
            stream,
            {
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
        "agency": ROUTEMATCH_AGENCY,
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
            UI_STREAM_STATE[stream]["total_today"] += 1
        return True
    except Exception:
        log(f"Write queue is full for {ROUTEMATCH_AGENCY}::{stream}; dropping record", level="error")
        return False

def infer_stream_type(stream):
    hourly_like = {
        "Version", "Timezone", "MasterRoute", "MasterRouteNames", "StopKeywords"
    }
    if stream in hourly_like:
        return "Hourly"
    return "Streamed"

def init_ui_state(load_history=True):
    preserved_times = {}
    with UI_STATE_LOCK:
        for stream, state in UI_STREAM_STATE.items():
            if state["last_sync_time"] and state["last_sync_time"] != "Loading...":
                preserved_times[stream] = state["last_sync_time"]
                
        for stream in RouteMatch_FEEDS:
            UI_STREAM_STATE.setdefault(
                stream,
                {
                    "stream": stream,
                    "type": infer_stream_type(stream),
                    "new_this_poll": 0,
                    "total_today": 0,
                    "last_sync_time": "Loading...",
                },
            )

    if not load_history:
        return

    today = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    temp_state = {}

    for file_path in EVENTS_DIR.glob(f"{ROUTEMATCH_OUTPUT_LABEL}_*.jsonl"):
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
                    if agency != ROUTEMATCH_AGENCY or stream == "Errors" or not stream:
                        continue

                    state = temp_state.setdefault(
                        stream,
                        {
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
        for stream in RouteMatch_FEEDS:
            if stream in temp_state:
                UI_STREAM_STATE[stream]["total_today"] = temp_state[stream]["total_today"]
                if temp_state[stream]["last_sync_time"] != "Loading...":
                    UI_STREAM_STATE[stream]["last_sync_time"] = temp_state[stream]["last_sync_time"]
            if stream in preserved_times:
                old_ts = preserved_times[stream]
                new_ts = UI_STREAM_STATE[stream]["last_sync_time"]
                if new_ts == "Loading..." or old_ts > new_ts:
                    UI_STREAM_STATE[stream]["last_sync_time"] = old_ts

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
        with _log_lock:
            pass
        while not stop_event.is_set() or not write_queue.empty():
            try:
                record = write_queue.get(timeout=0.5)
            except Empty:
                continue
            try:
                stream = record.get("stream", "stream")
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{ROUTEMATCH_OUTPUT_LABEL}_{gtfs_file_stem(stream)}_{date_str}.jsonl"
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
        for handle in handles.values():
            try:
                handle.close()
            except Exception:
                pass

def stringify_chunk(chunk):
    for column in ["data", "metadata"]:
        if column in chunk.columns:
            chunk[column] = chunk[column].apply(
                lambda value: json.dumps(value, default=str, ensure_ascii=False)
                if isinstance(value, (dict, list))
                else (None if value is None else str(value))
            )
    return chunk

def compact_worker():
    if not PARQUET_AVAILABLE or ARGS.no_compaction:
        return
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            for file_path in EVENTS_DIR.glob(f"{ROUTEMATCH_OUTPUT_LABEL}_*.jsonl"):
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
                                writer = pq.ParquetWriter(
                                    str(temp_path),
                                    table.schema,
                                    compression="zstd",
                                    compression_level=ARCHIVE_ZSTD_LEVEL,
                                )
                            writer.write_table(table)
                            row_count += len(chunk)
                            
                        if writer is not None:
                            writer.close()
                            temp_path.replace(parquet_path)
                            file_path.unlink()
                            log(f"Compacted {file_path.name} into {parquet_path.name} with {row_count} rows.", level="info")
                    except Exception as exc:
                        log(f"Failed to compact {file_path.name}: {exc}", level="error")
                    finally:
                        if writer is not None:
                            try:
                                writer.close()
                            except Exception:
                                pass
                        if temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                pass
                                
        except Exception as exc:
            log(f"Compaction thread error: {exc}", level="error")
        sleep_with_stop(60)


#### RouteMatch POLLING LOGIC ####

RouteMatch_BASE_URL = ROUTEMATCH_BASE_URL
ROUTEMATCH_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:151.0) Gecko/20100101 Firefox/151.0",
    "Referer": ROUTEMATCH_REFERER,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "application/json, text/javascript, */*; q=0.01",
}

RouteMatch_HEADERS = ROUTEMATCH_HEADERS

def RouteMatch_extract_items(data):
    if not data:
        return []
    if isinstance(data, (str, int, float, bool)):
        return [{"data": str(data)}]
    if isinstance(data, dict):
        if "data" in data:
            data = data["data"]
        elif len(data) == 1:
            data = next(iter(data.values()))
    return data if isinstance(data, list) else [data]

def RouteMatch_extract_ids(items, keys):
    identifiers = set()
    for item in items:
        if not isinstance(item, dict):
            if isinstance(item, (str, int)):
                identifiers.add(str(item))
            continue
        for key in keys:
            value = item.get(key)
            if value not in (None, ""):
                identifiers.add(str(value))
                break
    return identifiers

def RouteMatch_validate_route_ids(route_ids):
    valid = set()
    for rid in route_ids:
        try:
            if int(rid) != 0:
                valid.add(str(rid))
        except (ValueError, TypeError):
            if rid and str(rid).strip():
                valid.add(str(rid))
    return valid

def RouteMatch_poll_json(path, stream, event="update", params=None, timeout=8, metadata=None, return_data=False, skip_ui_update=False):
    if not skip_ui_update:
        touch_ui_sync(stream, "Fetching...")
        
    update_ui_poll_count(stream, 0)
    url = f"{ROUTEMATCH_BASE_URL}{path}"
    data = request_json(url, headers=ROUTEMATCH_HEADERS, params=params, timeout=timeout)
    if data is None:
        log(f"Request failed: {path}", level="warning")
        if not skip_ui_update:
            touch_ui_sync(stream, "Failed")
        return (0, None) if return_data else 0

    items = RouteMatch_extract_items(data)
    fetched_count = len(items)
    for item in items:
        emit_record(stream, event, item, endpoint=path, metadata=metadata)
            
    if not skip_ui_update:
        update_ui_poll_count(stream, fetched_count)
        touch_ui_sync(stream)
        
    return (fetched_count, data) if return_data else fetched_count

def RouteMatch_concurrent_fetch(path_template, ids, stream, event="update", timeout=8, max_workers=None, inter_request_delay=0.0):
    if not ids:
        return 0

    workers = max_workers if max_workers is not None else MAX_WORKERS
    touch_ui_sync(stream, "Fetching...")
    update_ui_poll_count(stream, 0)
    total_fetched = 0

    def fetch_id(item_id):
        if stop_event.is_set(): return 0
        if inter_request_delay > 0:
            time.sleep(inter_request_delay)
        encoded_id = urllib.parse.quote(str(item_id), safe="")
        path = path_template.format(id=encoded_id)
        return RouteMatch_poll_json(path, stream, event=event, timeout=timeout, skip_ui_update=True)

    with ThreadPoolExecutor(max_workers=workers) as executor:
        try:
            futures = [executor.submit(fetch_id, i) for i in ids]
            for future in as_completed(futures):
                if stop_event.is_set():
                    break
                try:
                    total_fetched += future.result()
                except Exception as e:
                    log(f"Concurrent fetch failed for {stream}: {e}", level="warning")
        finally:
            executor.shutdown(wait=False, cancel_futures=True)

    update_ui_poll_count(stream, total_fetched)
    touch_ui_sync(stream)
    return total_fetched


def RouteMatch_vehicle_loop():
    while not stop_event.is_set():
        start_time = time.time()
        
        touch_ui_sync("Vehicles", "Fetching...")
        v_data = request_json(f"{ROUTEMATCH_BASE_URL}/vehicle", headers=ROUTEMATCH_HEADERS, timeout=10)
        if v_data is not None:
            items = RouteMatch_extract_items(v_data)
            fetched_count = len(items)
            if items:
                vehicle_ids = RouteMatch_extract_ids(items, ("vehicleId", "id"))
                with _CACHE_LOCK:
                    RouteMatch_CACHE["vehicle_ids"].update(vehicle_ids)
                    RouteMatch_CACHE["trip_ids"].update(RouteMatch_extract_ids(items, ("tripId",)))
                save_cache()

            emit_record("Vehicles", "update", v_data, endpoint="/vehicle")
            update_ui_poll_count("Vehicles", fetched_count)
            touch_ui_sync("Vehicles")
            log(f"Vehicles: {fetched_count} fetched", level="info")
            RouteMatch_VEHICLES_READY.set()

            vehicle_ids = sorted(RouteMatch_CACHE["vehicle_ids"])
            if vehicle_ids:
                n = RouteMatch_concurrent_fetch("/vehicle/{id}", vehicle_ids, "VehicleByID")
                log(f"VehicleByID: {n}/{len(vehicle_ids)} fetched", level="info")
            trip_ids = sorted(RouteMatch_CACHE["trip_ids"])
            log(f"  Cache: {len(vehicle_ids)} vehicles, {len(trip_ids)} trips, {len(RouteMatch_CACHE['stop_ids'])} stops", level="info")
        else:
            touch_ui_sync("Vehicles", "Failed")
            log("Vehicles: fetch failed", level="warning")

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)


def RouteMatch_paravehicle_loop():
    while not stop_event.is_set():
        start_time = time.time()
        
        touch_ui_sync("ParaVehicle", "Fetching...")
        pv_data = request_json(f"{ROUTEMATCH_BASE_URL}/paraVehicle", headers=ROUTEMATCH_HEADERS, timeout=10)
        if pv_data is not None:
            items = RouteMatch_extract_items(pv_data)
            fetched_count = len(items)
            if items:
                paravehicle_ids = RouteMatch_extract_ids(items, ("vehicleId", "id"))
                if paravehicle_ids:
                    with _CACHE_LOCK:
                        RouteMatch_CACHE["paravehicle_ids"].update(paravehicle_ids)
                    save_cache()

            emit_record("ParaVehicle", "update", pv_data, endpoint="/paraVehicle")
            update_ui_poll_count("ParaVehicle", fetched_count)
            touch_ui_sync("ParaVehicle")
            log(f"ParaVehicle: {fetched_count} fetched", level="info")

            paravehicle_ids = sorted(RouteMatch_CACHE["paravehicle_ids"])
            if paravehicle_ids:
                n = RouteMatch_concurrent_fetch("/paraVehicle/{id}", paravehicle_ids, "ParaVehicleByID")
                log(f"ParaVehicleByID: {n}/{len(paravehicle_ids)} fetched", level="info")
        else:
            touch_ui_sync("ParaVehicle", "Failed")
            log("ParaVehicle: fetch failed", level="warning")

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)


def RouteMatch_trip_loop():
    RouteMatch_VEHICLES_READY.wait(timeout=15)
    while not stop_event.is_set():
        start_time = time.time()

        trip_ids = sorted(RouteMatch_CACHE["trip_ids"])
        if trip_ids:
            n = RouteMatch_concurrent_fetch("/trip/byId/{id}", trip_ids, "TripByID")
            log(f"TripByID: {n}/{len(trip_ids)} fetched", level="info")
        else:
            touch_ui_sync("TripByID", "Waiting...")
            log("TripByID: no trip IDs in cache yet", level="warning")

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)


def RouteMatch_run_hourly():
    log("Running RouteMatch hourly feeds", level="info")

    try:
        RouteMatch_poll_json("/version", "Version", event="snapshot", timeout=10)
        RouteMatch_poll_json("/timezone", "Timezone", event="snapshot", timeout=10)
        
        touch_ui_sync("MasterRoute", "Fetching...")
        mr_data = request_json(f"{ROUTEMATCH_BASE_URL}/masterRoute", headers=ROUTEMATCH_HEADERS, timeout=10)
        if mr_data is not None:
            items = RouteMatch_extract_items(mr_data)
            emit_record("MasterRoute", "snapshot", mr_data, endpoint="/masterRoute")
            update_ui_poll_count("MasterRoute", len(items))
            touch_ui_sync("MasterRoute")

            route_ids_raw = RouteMatch_extract_ids(items, ("masterRouteId", "routeId", "id", "shortName", "name"))
            route_ids = RouteMatch_validate_route_ids(route_ids_raw)
            if route_ids:
                with _CACHE_LOCK:
                    RouteMatch_CACHE["route_ids"].update(route_ids)
                save_cache()
        else:
            touch_ui_sync("MasterRoute", "Failed")
            route_ids = RouteMatch_validate_route_ids(RouteMatch_CACHE.get("route_ids", set()))

        RouteMatch_poll_json("/masterRouteNames", "MasterRouteNames", event="snapshot", timeout=10)

        if route_ids:
            RouteMatch_concurrent_fetch("/landRoute/byRoute/{id}", route_ids, "LandRouteByRoute")
            
            touch_ui_sync("StopsByRoute", "Fetching...")
            update_ui_poll_count("StopsByRoute", 0)
            total_s_fetched = 0
            
            def extract_stops(rid):
                if stop_event.is_set(): return 0
                enc_rid = urllib.parse.quote(str(rid), safe="")
                nc, s_data = RouteMatch_poll_json(f"/stops/{enc_rid}", "StopsByRoute", event="snapshot", timeout=10, return_data=True, skip_ui_update=True)
                if s_data:
                    s_ids = RouteMatch_extract_ids(RouteMatch_extract_items(s_data), ("stopId", "id"))
                    if s_ids:
                        with _CACHE_LOCK:
                            RouteMatch_CACHE["stop_ids"].update(s_ids)
                return nc
                                    
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                try:
                    futures = [executor.submit(extract_stops, rid) for rid in sorted(route_ids)]
                    for future in as_completed(futures):
                        if stop_event.is_set(): break
                        try:
                            total_s_fetched += future.result()
                        except Exception:
                            pass
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
            
            save_cache()
            update_ui_poll_count("StopsByRoute", total_s_fetched)
            touch_ui_sync("StopsByRoute")

        sk_count, sk_data = RouteMatch_poll_json("/stopKeywords", "StopKeywords", event="snapshot", timeout=10, return_data=True)
        if sk_data is not None:
            keyword_items = RouteMatch_extract_items(sk_data)
            keywords = set()
            for k in keyword_items:
                if isinstance(k, str) and k.strip():
                    keywords.add(k.strip())
                elif isinstance(k, dict):
                    v = k.get("string") or k.get("keyword") or k.get("stopKeyword") or k.get("id")
                    if v and str(v).strip():
                        keywords.add(str(v).strip())
            if keywords:
                with _CACHE_LOCK:
                    RouteMatch_CACHE["keywords"].update(keywords)
                save_cache()
                
            enc_kws = [urllib.parse.quote(kw, safe="") for kw in sorted(RouteMatch_CACHE["keywords"])]
            
            touch_ui_sync("StopByKeyword", "Fetching...")
            update_ui_poll_count("StopByKeyword", 0)
            total_kw_fetched = 0
            
            def extract_kw_stops(kw):
                if stop_event.is_set(): return 0
                nc, kw_data = RouteMatch_poll_json(f"/keyword/{kw}", "StopByKeyword", event="snapshot", timeout=10, return_data=True, skip_ui_update=True)
                if kw_data is not None:
                    s_ids = RouteMatch_extract_ids(RouteMatch_extract_items(kw_data), ("stopId", "id"))
                    if s_ids:
                        with _CACHE_LOCK:
                            RouteMatch_CACHE["stop_ids"].update(s_ids)
                return nc

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                try:
                    futures = [executor.submit(extract_kw_stops, keyword) for keyword in enc_kws]
                    for future in as_completed(futures):
                        if stop_event.is_set(): break
                        try:
                            total_kw_fetched += future.result()
                        except Exception:
                            pass
                finally:
                    executor.shutdown(wait=False, cancel_futures=True)
            
            save_cache()
            update_ui_poll_count("StopByKeyword", total_kw_fetched)
            touch_ui_sync("StopByKeyword")

        RouteMatch_ROUTES_READY.set()
        log(
            f"Hourly feeds complete — routes={len(RouteMatch_CACHE['route_ids'])}, "
            f"stops={len(RouteMatch_CACHE['stop_ids'])}, "
            f"keywords={len(RouteMatch_CACHE['keywords'])}",
            level="info",
        )
    except Exception as e:
        log(f"Error in {ROUTEMATCH_AGENCY} hourly loop: {e}", level="error")


def RouteMatch_hourly_loop():
    last_hour = -1
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        force = HOURLY_REFRESH_FLAG.is_set()

        if force or (now.minute == 0 and now.hour != last_hour) or last_hour == -1:
            if force:
                HOURLY_REFRESH_FLAG.clear()
            last_hour = now.hour
            RouteMatch_run_hourly()

        sleep_with_stop(5)


def RouteMatch_departure_loop():
    if SKIP_DEPARTURES_BY_STOP:
        log("DeparturesByStop: disabled (SKIP_DEPARTURES_BY_STOP=True)", level="info")
        return
    RouteMatch_ROUTES_READY.wait(timeout=30)
    while not stop_event.is_set():
        start_time = time.time()

        stop_ids = sorted(RouteMatch_CACHE["stop_ids"])
        if stop_ids:
            n = RouteMatch_concurrent_fetch(
                "/departures/byStop/{id}", stop_ids, "DeparturesByStop",
                timeout=10, max_workers=2, inter_request_delay=0.5,
            )
            log(f"DeparturesByStop: {n}/{len(stop_ids)} fetched", level="info")
        else:
            touch_ui_sync("DeparturesByStop", "Waiting...")
            log("DeparturesByStop: no stop IDs in cache yet", level="warning")

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_SLOW - elapsed) + random.random() * ARGS.poll_jitter)


def start_workers():
    load_cache()
    threading.Thread(target=writer_worker, daemon=True).start()
    if not ARGS.no_compaction:
        threading.Thread(target=compact_worker, daemon=True).start()

    log("Starting RouteMatch loops")
    threading.Thread(target=RouteMatch_hourly_loop, daemon=True).start()
    threading.Thread(target=RouteMatch_vehicle_loop, daemon=True).start()
    threading.Thread(target=RouteMatch_paravehicle_loop, daemon=True).start()
    threading.Thread(target=RouteMatch_trip_loop, daemon=True).start()
    threading.Thread(target=RouteMatch_departure_loop, daemon=True).start()


#### UI ####

def format_duration(seconds):
    seconds = int(seconds)
    hours, remainder = divmod(seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours}h {minutes}m {secs}s" if hours else f"{minutes}m {secs}s"

def next_hourly_timestamp():
    now = datetime.now(LOCAL_TZ)
    # FIX: Corrected typo `microsecond=Microsecond=0` to `microsecond=0`
    return (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)

def get_layout(refresh_flash=False):
    with UI_STATE_LOCK:
        rows = sorted(UI_STREAM_STATE.values(), key=lambda s: s["stream"])
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
        table.add_row(Text(ROUTEMATCH_AGENCY, style="bold white"), Text("Loading...", style="yellow"), "", "", "0", "0")
    else:
        for state in rows:
            stream_name = state['stream']
            gtfs_std = GTFS_MAP.get(stream_name, "unknown")
            stream_label = f"{stream_name} ({state['type']})"
            color = "green" if state["type"] == "Hourly" else "cyan"

            sync_time = state["last_sync_time"]
            sync_style = "yellow" if sync_time in ("Loading...", "Waiting...", "Fetching...", "Failed", "") else ""

            new_poll_val = str(state.get("new_this_poll", 0))
            new_poll_style = "dim" if new_poll_val == "0" else ""

            stream_key = f"{ROUTEMATCH_AGENCY}::{safe_prefix(stream_name)}"
            with seen_lock:
                true_unique_today = len(seen_hashes.get(stream_key, {}))

            table.add_row(
                Text(ROUTEMATCH_AGENCY, style="bold white"),
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

            trigger_ui_refresh()

            while running and not stop_event.is_set():
                key_action = poll_keys()
                if key_action == "quit":
                    running = False
                    stop_event.set()
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
    log(f"RouteMatch ingestor starting. Data dir: {DATA_DIR}  Log: {LOG_FILE}", level="info")
    init_ui_state(load_history=False)
    try:
        run_live_gui()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        log_api_call_summary(final=True)
        log("RouteMatch ingestor stopped.", level="info")

if __name__ == "__main__":
    main()