#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 30, 2026 (Last updated June 11, 2026)
#
# Description:
# Dedicated ingestion engine for Massport and Logan Express realtime feeds.
# Uses a stateful session mapping to pull hourly snapshots and prediction polls
# for the configured Massport agency and maps them to standard Data Lake formats.
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

import hashlib
import json
import os
import random
import select
import sys
import tempfile
import threading
import time
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
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import requests
from rich.console import Console, Group
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
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


def safe_prefix(value):
    cleaned_value = "".join(character if character.isalnum() or character in {"-", "_"} else "_" for character in value.strip())
    return cleaned_value.strip("_") or "agency"


# Massport integration is fixed to one agency endpoint.
MASSPORT_AGENCY = "MASSPORT"
TARGET_AGENCIES = {
    MASSPORT_AGENCY: {
        "base_url": "https://gtfs.bos.aocadp.com",
    }
}
MASSPORT_BASE_URL = TARGET_AGENCIES[MASSPORT_AGENCY]["base_url"]
MASSPORT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}
MASSPORT_FEEDS = (
    "Routes",
    "Stops",
    "Trips",
    "Collections",
    "Predictions",
)

# Maps Massport streams to standard Data Lake GTFS formats.
GTFS_MAP = {
    "Routes": "routes",
    "Stops": "stops",
    "Trips": "trips",
    "Collections": "collections",
    "Predictions": "trip_updates",
}


def gtfs_file_stem(stream):
    return GTFS_MAP.get(stream, safe_prefix(stream)).lower()


def default_data_dir():
    configured = os.environ.get("DATA_DIR")
    if configured:
        return Path(configured)

    def is_wsl():
        try:
            if os.name != "posix":
                return False
            with open("/proc/version", "r", encoding="utf-8") as handle:
                version_text = handle.read()
            return "microsoft" in version_text.lower() or "wsl" in version_text.lower()
        except Exception:
            return bool(os.environ.get("WSL_INTEROP") or os.environ.get("WSLENV"))

    if is_wsl():
        return Path("/mnt/c/Users/drewm/GitHub/gtfs-data-archiver/data")
    return Path("./data")


ARGS = SimpleNamespace(
    no_compaction=False,
    global_concurrency=12,
    poll_jitter=1.25,
)

LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE", "America/New_York") or "America/New_York")
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
CACHE_DIR = DATA_DIR / "cache"
LOG_FILE = LOG_DIR / "massport_ingest.log"

for directory in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR, CACHE_DIR]:
    directory.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
# Polling cadence and request behavior are fixed, but can be updated here.
POLL_INTERVAL_FAST = 20
POLL_INTERVAL_SLOW = 15
MASSPORT_REQUEST_TIMEOUT_SECONDS = 12
MASSPORT_COLLECTION_POLL_DELAY_SECONDS = 15
MAX_WORKERS = max(1, ARGS.global_concurrency)

write_queue = Queue(maxsize=50000)
stop_event = threading.Event()
TUI_READY = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []
HOURLY_REFRESH_FLAG = threading.Event()

AGENCY_SESSIONS = {}
SESSION_LOCK = threading.Lock()

MASSPORT_COLLECTIONS_READY = threading.Event()

#### CACHE MANAGEMENT ####
MASSPORT_CACHE_FILE = CACHE_DIR / "massport_cache.json"
MASSPORT_CACHE = {
    "collection_ids": set(),
}
_CACHE_LOCK = threading.Lock()
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
            previous_total = API_CALL_STATS["current_total"]
            previous_date = API_CALL_STATS["current_date"]
            if API_CALL_STATS["previous_date"] is not None:
                API_CALL_STATS["previous_date"] = previous_date
                API_CALL_STATS["previous_total"] = previous_total
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
        log(f"API calls for {today_date}: {today_total}", level="info", agency=MASSPORT_AGENCY)
    else:
        delta = today_total - previous_total
        log(f"API calls for {today_date}: {today_total} ({delta:+d} vs {previous_date})", level="info", agency=MASSPORT_AGENCY)


def load_cache():
    try:
        if not MASSPORT_CACHE_FILE.exists():
            log(f"No cache file found at {MASSPORT_CACHE_FILE}; initializing fresh.", level="info", agency=MASSPORT_AGENCY)
            save_cache()
            return
        with open(MASSPORT_CACHE_FILE, "r", encoding="utf-8") as handle:
            saved = json.load(handle)
        with _CACHE_LOCK:
            for key in MASSPORT_CACHE.keys():
                if key in saved:
                    MASSPORT_CACHE[key] = set(str(value) for value in saved[key])
            stats = saved.get("api_calls") if isinstance(saved, dict) else None
            if isinstance(stats, dict):
                API_CALL_STATS["current_date"] = str(stats.get("current_date", API_CALL_STATS["current_date"]))
                API_CALL_STATS["current_total"] = int(stats.get("current_total", 0))
                API_CALL_STATS["previous_date"] = stats.get("previous_date")
                API_CALL_STATS["previous_total"] = int(stats.get("previous_total", 0))

        if MASSPORT_CACHE["collection_ids"]:
            MASSPORT_COLLECTIONS_READY.set()

        log(f"Cache loaded from {MASSPORT_CACHE_FILE}", level="info", agency=MASSPORT_AGENCY)
    except Exception as exc:
        log(f"Could not load cache: {exc}", level="error", agency=MASSPORT_AGENCY)
        save_cache()


def save_cache():
    try:
        with _CACHE_LOCK:
            payload = {key: sorted(list(values)) for key, values in MASSPORT_CACHE.items()}
            payload["api_calls"] = dict(API_CALL_STATS)
        temp_path = MASSPORT_CACHE_FILE.with_suffix(".tmp")
        with open(temp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, indent=2)
        temp_path.replace(MASSPORT_CACHE_FILE)
    except Exception as exc:
        log(f"Could not save cache: {exc}", level="error", agency=MASSPORT_AGENCY)


#### LOGGING ####
_log_lock = threading.Lock()


def log(message, level="info", agency=MASSPORT_AGENCY):
    now = datetime.now(LOCAL_TZ)
    ts_full = now.strftime("%Y-%m-%d %H:%M:%S")
    ts_short = now.strftime("%H:%M:%S")

    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as handle:
                handle.write(f"{ts_full} [{level.upper():7s}] [{agency}] {message}\n")
    except Exception:
        pass

    if level in ("error", "warning"):
        with UI_STATE_LOCK:
            RECENT_ERRORS.append({"dt": now, "ts_str": ts_short, "agency": agency, "message": str(message)})
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


#### SHARED HTTP ####
_http_session = None


def get_http_session():
    global _http_session
    if _http_session is None:
        session = requests.Session()
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            status=3,
            backoff_factor=0.75,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(pool_connections=32, pool_maxsize=32, max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _http_session = session
    return _http_session


def guarded_get(url, **kwargs):
    session = get_http_session()
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


def request_json(url, headers=None, params=None, timeout=15, agency=""):
    try:
        record_api_call()
        response = guarded_get(url, headers=headers or {}, params=params or {}, timeout=timeout)
        if response.status_code != 200:
            log(f"HTTP {response.status_code} for {url}", level="warning", agency=agency)
            return None

        try:
            return response.json()
        except Exception:
            text = response.text.strip()
            return {"data": text} if text else None
    except Exception as exc:
        log(f"Request failed for {url}: {exc}", level="warning", agency=agency)
        return None


#### SHARED OUTPUT ####
def history_file_path(agency, stream, date_str):
    return EVENTS_DIR / f"{safe_prefix(agency)}_{gtfs_file_stem(stream)}_{date_str}.jsonl"


def today_history_file(agency, stream):
    return history_file_path(agency, stream, datetime.now(LOCAL_TZ).strftime("%m%d%Y"))


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


def ensure_history_loaded(agency, stream):
    stream_key = f"{safe_prefix(agency)}::{safe_prefix(stream)}"
    with seen_lock:
        if stream_key in seen_hashes:
            return stream_key

    history = load_history_metadata(today_history_file(agency, stream))
    with seen_lock:
        if stream_key not in seen_hashes:
            seen_hashes[stream_key] = history
    return stream_key


def touch_ui_sync(agency, stream, status=None):
    now_time = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    value = status if status else now_time
    ui_key = (agency, stream)
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            UI_STREAM_STATE[ui_key]["last_sync_time"] = value


def update_ui_poll_count(agency, stream, count):
    ui_key = (agency, stream)
    with UI_STATE_LOCK:
        if ui_key in UI_STREAM_STATE:
            UI_STREAM_STATE[ui_key]["new_batch"] = count


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

    # Verify stream setup mapping inside UI structure context
    with UI_STATE_LOCK:
        state = UI_STREAM_STATE.setdefault(
            (agency, stream),
            {
                "agency": agency,
                "stream": stream,
                "type": infer_stream_type(stream),
                "new_batch": 0,
                "total_today": 0,
                "total_yesterday": 0,
                "last_sync": "Loading...",
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
        return True
    except Exception:
        log(f"Write queue is full for {agency}::{stream}; dropping record", level="error", agency=agency)
        return False


def infer_stream_type(stream):
    hourly_like = {"Routes", "Stops", "Trips", "Collections"}
    if stream in hourly_like:
        return "Hourly"
    if stream == "Predictions":
        return "Live"
    return "Streamed"


def init_ui_state(load_history=True):
    with UI_STATE_LOCK:
        for agency in TARGET_AGENCIES.keys():
            for stream in MASSPORT_FEEDS:
                UI_STREAM_STATE.setdefault(
                    (agency, stream),
                    {
                        "agency": agency,
                        "stream": stream,
                        "type": infer_stream_type(stream),
                        "new_batch": 0,
                        "total_today": 0,
                        "total_yesterday": 0,
                        "last_sync": "Loading...",
                        "last_sync_time": "Loading...",
                    },
                )

    if not load_history:
        return

    today = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    yesterday = (datetime.now(LOCAL_TZ) - timedelta(days=1)).strftime("%m%d%Y")
    temp_state = {}

    for file_path in EVENTS_DIR.glob("*.jsonl"):
        date_str = file_path.stem.rsplit("_", 1)[-1] if "_" in file_path.stem else ""
        if date_str not in {today, yesterday}:
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

                    agency = entry.get("agency", MASSPORT_AGENCY)
                    stream = entry.get("stream", "Errors")
                    if agency not in TARGET_AGENCIES or stream == "Errors":
                        continue

                    key = (agency, stream)
                    state = temp_state.setdefault(
                        key,
                        {
                            "agency": agency,
                            "stream": stream,
                            "type": infer_stream_type(stream),
                            "new_batch": 0,
                            "total_today": 0,
                            "total_yesterday": 0,
                            "last_sync": "Loading...",
                            "last_sync_time": "Loading...",
                        },
                    )
                    if date_str == today:
                        state["total_today"] += 1
                    elif date_str == yesterday:
                        state["total_yesterday"] += 1

                    ts_value = entry.get("ts")
                    if ts_value:
                        state["last_sync"] = ts_value
                        state["last_sync_time"] = ts_value[11:19] if len(ts_value) >= 19 else ts_value
        except Exception:
            continue

    with UI_STATE_LOCK:
        for agency in TARGET_AGENCIES.keys():
            for stream in MASSPORT_FEEDS:
                key = (agency, stream)
                if key in temp_state:
                    if temp_state[key]["last_sync_time"] != "Loading...":
                        UI_STREAM_STATE[key]["last_sync"] = temp_state[key]["last_sync"]
                        UI_STREAM_STATE[key]["last_sync_time"] = temp_state[key]["last_sync_time"]


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
    log("Writer thread engaged. Streaming to JSONL.")
    handles = {}
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                record = write_queue.get(timeout=0.5)
            except Empty:
                continue

            try:
                agency = record.get("agency", MASSPORT_AGENCY)
                stream = record.get("stream", "stream")
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{safe_prefix(agency)}_{gtfs_file_stem(stream)}_{date_str}.jsonl"
                filepath = EVENTS_DIR / filename

                if filename not in handles:
                    handles[filename] = open(filepath, "a", encoding="utf-8")

                handles[filename].write(json.dumps(record, default=str, ensure_ascii=False) + "\n")
                handles[filename].flush()
            except Exception as exc:
                log(f"Writer exception: {exc}", level="error", agency=agency)
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
                lambda value: json.dumps(value, default=str, ensure_ascii=False) if isinstance(value, (dict, list)) else (None if value is None else str(value))
            )
    return chunk


def compact_jsonl_file(file_path):
    if not PARQUET_AVAILABLE:
        return

    parquet_path = ARCHIVE_DIR / file_path.with_suffix(".parquet").name
    temp_handle = tempfile.NamedTemporaryFile(
        delete=False,
        dir=parquet_path.parent,
        prefix=f"{parquet_path.stem}.",
        suffix=".tmp",
    )
    temp_handle.close()
    temp_path = Path(temp_handle.name)
    writer = None

    try:
        row_count = 0
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

        if writer is None:
            return

        writer.close()
        writer = None
        temp_path.replace(parquet_path)
        file_path.unlink()
        log(f"Compacted {file_path.name} into {parquet_path.name} with {row_count} rows.", agency=MASSPORT_AGENCY)
    except Exception as exc:
        log(f"Failed to compact {file_path.name}: {exc}", level="error", agency=MASSPORT_AGENCY)
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


def compact_worker():
    if not PARQUET_AVAILABLE or ARGS.no_compaction:
        return

    log("Parquet compaction watchdog active.", agency=MASSPORT_AGENCY)
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            for file_path in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file_path.name:
                    compact_jsonl_file(file_path)
        except Exception as exc:
            log(f"Compaction thread error: {exc}", level="error", agency=MASSPORT_AGENCY)

        sleep_with_stop(60)


#### NETWORK CONSUMERS ####

def massport_poll(path, stream, event="update", timeout=12, return_data=False):
    url = f"{MASSPORT_BASE_URL}{path}"
    touch_ui_sync(MASSPORT_AGENCY, stream, "Fetching...")
    
    # Initialize the latest poll counter explicitly to 0 before fetching
    update_ui_poll_count(MASSPORT_AGENCY, stream, 0)
    
    data = request_json(url, headers=MASSPORT_HEADERS, timeout=timeout, agency=MASSPORT_AGENCY)
    if data is None:
        return (0, None) if return_data else 0

    if isinstance(data, dict):
        items = data.get("data", [data]) if "data" in data else [data]
    elif isinstance(data, list):
        items = data
    else:
        items = [data]

    fetched_count = len(items)
    for item in items:
        emit_record(MASSPORT_AGENCY, stream, event, item, endpoint=path)

    # Reflect transient payload length transactionally
    update_ui_poll_count(MASSPORT_AGENCY, stream, fetched_count)
    touch_ui_sync(MASSPORT_AGENCY, stream)

    return (fetched_count, items) if return_data else fetched_count


def massport_hourly_loop():
    last_hour = -1
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        force = HOURLY_REFRESH_FLAG.is_set()

        if force or now.hour != last_hour or last_hour == -1:
            if force:
                HOURLY_REFRESH_FLAG.clear()
            last_hour = now.hour

            try:
                touch_ui_sync(MASSPORT_AGENCY, "Routes", "Fetching...")
                massport_poll("/routes", "Routes", event="snapshot")

                touch_ui_sync(MASSPORT_AGENCY, "Stops", "Fetching...")
                massport_poll("/stops", "Stops", event="snapshot")

                touch_ui_sync(MASSPORT_AGENCY, "Trips", "Fetching...")
                massport_poll("/trips", "Trips", event="snapshot")

                touch_ui_sync(MASSPORT_AGENCY, "Collections", "Fetching...")
                _, collections = massport_poll("/collections", "Collections", event="snapshot", return_data=True)
                if collections:
                    with _CACHE_LOCK:
                        for collection in collections:
                            if isinstance(collection, dict):
                                collection_name = collection.get("name") or collection.get("id")
                                if collection_name:
                                    MASSPORT_CACHE["collection_ids"].add(str(collection_name))
                            elif isinstance(collection, str):
                                MASSPORT_CACHE["collection_ids"].add(str(collection))

                    if MASSPORT_CACHE["collection_ids"]:
                        save_cache()
                        MASSPORT_COLLECTIONS_READY.set()
            except Exception as exc:
                log(f"Massport Hourly Loop Error: {exc}", level="error", agency=MASSPORT_AGENCY)

        sleep_with_stop(30)


def massport_predictions_loop():
    MASSPORT_COLLECTIONS_READY.wait(timeout=15)

    while not stop_event.is_set():
        start_time = time.time()
        try:
            touch_ui_sync(MASSPORT_AGENCY, "Predictions", "Fetching...")
            collection_ids = sorted(MASSPORT_CACHE["collection_ids"])
            if collection_ids:
                # Predictions update transient totals independently per collection chunk iteration
                total_fetched_this_poll = 0
                for collection_id in collection_ids:
                    if stop_event.is_set():
                        break
                    path = f"/collections/{collection_id}/prediction"
                    cnt = massport_poll(path, "Predictions", timeout=10)
                    total_fetched_this_poll += cnt
                    sleep_with_stop(0.1)
                update_ui_poll_count(MASSPORT_AGENCY, "Predictions", total_fetched_this_poll)
            else:
                log("Predictions waiting for collection IDs", level="warning", agency=MASSPORT_AGENCY)
        except Exception as exc:
            log(f"Massport Predictions Loop Error: {exc}", level="error", agency=MASSPORT_AGENCY)

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)


def run_massport():
    log("Starting MASSPORT runners", agency=MASSPORT_AGENCY)
    threading.Thread(target=massport_hourly_loop, daemon=True).start()
    threading.Thread(target=massport_predictions_loop, daemon=True).start()


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
        rows = sorted(UI_STREAM_STATE.values(), key=lambda state: state["stream"])
        now = datetime.now(LOCAL_TZ)
        RECENT_ERRORS[:] = [entry for entry in RECENT_ERRORS if (now - entry["dt"]).total_seconds() < 180]
        errors = list(RECENT_ERRORS)

    table = Table(expand=True, header_style="bold magenta", border_style="dim", highlight=True, show_lines=False)
    table.add_column("Agency", justify="left", style="bold")
    table.add_column("Stream (Type)", justify="left", vertical="middle")
    table.add_column("Data Lake Standard", justify="center", vertical="middle", style="dim")
    table.add_column("Last Sync", justify="center", vertical="middle")
    table.add_column("Fetched (Latest)", justify="center", vertical="middle")
    table.add_column("Unique (Today)", justify="center", vertical="middle")

    if not rows:
        table.add_row(Text(MASSPORT_AGENCY, style="bold white"), Text("Loading...", style="yellow"), "", "", "0", "0")
    else:
        for state in rows:
            stream_style = "green" if state["type"] == "Hourly" else "cyan"
            sync_time = state["last_sync_time"]
            sync_style = (
                "dim"
                if sync_time == "Off Hours"
                else (
                    "red"
                    if sync_time == "Failed"
                    else (
                        "yellow"
                        if sync_time in ("Loading...", "Waiting...", "Fetching...", "Syncing...") or str(sync_time).startswith("Cached (")
                        else ("red" if sync_time == "Unsupported" else "")
                    )
                )
            )
            new_batch_style = "dim" if state["new_batch"] == 0 else "green"

            # Compute the total length of today's in-memory hash store directly for true distinct count integrity
            stream_key = f"{safe_prefix(state['agency'])}::{safe_prefix(state['stream'])}"
            with seen_lock:
                true_unique_today = len(seen_hashes.get(stream_key, {}))

            table.add_row(
                Text(state["agency"], style="bold white"),
                Text(f"{state['stream']} ({state['type']})", style=stream_style),
                Text(gtfs_file_stem(state["stream"]), style="dim"),
                Text(sync_time, style=sync_style),
                Text(str(state["new_batch"]), style=new_batch_style),
                Text(str(true_unique_today)),
            )

    if errors:
        error_text = Text()
        for entry in errors[-5:]:
            error_text.append(
                f"[{entry.get('ts_str', '')}] [{entry.get('agency', MASSPORT_AGENCY)}] {entry.get('message', '')}\n",
                style="bold red",
            )
    else:
        error_text = Text(f"No errors in the last 3 minutes. Written safely to {LOG_FILE.name}.", style="dim")

    poll_countdown = POLL_INTERVAL_FAST - int(time.time() % POLL_INTERVAL_FAST)
    next_h = next_hourly_timestamp()
    time_to_hourly = format_duration((next_h - datetime.now(LOCAL_TZ)).total_seconds())

    if refresh_flash:
        refresh_note = "[bold green]REFRESHING hourly feeds...[/bold green]"
    else:
        refresh_note = "[bold yellow]Press R[/bold yellow] to force refresh all feeds  |  [bold yellow]Press Q[/bold yellow] to quit"

    footer_body = (
        f"Next auto-poll: ~{poll_countdown}s  |  Next hourly auto-run: {time_to_hourly} (@ {next_h.strftime('%I:%M:%S %p %m/%d/%Y')})\n"
        f"{refresh_note}\n"
    )
    footer = Panel(footer_body, title="Controls", border_style="blue")
    errors_panel = Panel(error_text, title="Recent Logs", border_style="red")

    return Group(table, errors_panel, footer)


_refresh_flash_until = 0.0


def trigger_full_refresh():
    global _refresh_flash_until
    _refresh_flash_until = time.time() + 4.0
    log("Manual refresh requested (R key)", level="warning", agency=MASSPORT_AGENCY)
    HOURLY_REFRESH_FLAG.set()
    trigger_ui_refresh()


def start_workers():
    load_cache()
    threading.Thread(target=writer_worker, daemon=True).start()
    if PARQUET_AVAILABLE and not ARGS.no_compaction:
        threading.Thread(target=compact_worker, daemon=True).start()
    log("Starting Massport loops", agency=MASSPORT_AGENCY)
    threading.Thread(target=massport_hourly_loop, daemon=True).start()
    threading.Thread(target=massport_predictions_loop, daemon=True).start()


def run_live_gui():
    try:
        fd = sys.stdin.fileno()
    except Exception:
        fd = None
        
    use_unix_keys = termios is not None and tty is not None and fd is not None
    old_settings = termios.tcgetattr(fd) if use_unix_keys else None
    running = True

    def poll_keys():
        if msvcrt is not None and os.name == "nt":
            while msvcrt.kbhit():
                try:
                    key = msvcrt.getwch().lower()
                except Exception:
                    return None
                if key == "q":
                    return "quit"
                if key == "r":
                    return "refresh"
            return None

        if fd is not None:
            if select.select([sys.stdin], [], [], 0)[0]:
                try:
                    key = os.read(fd, 1).decode(errors="ignore").lower()
                except Exception:
                    return None
                if key == "q":
                    return "quit"
                if key == "r":
                    return "refresh"
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
            
            # Instantly fire history file log line checks inside background thread context
            trigger_ui_refresh()

            while running and not stop_event.is_set():
                key_action = poll_keys()
                if key_action == "quit":
                    running = False
                    stop_event.set()
                    break
                if key_action == "refresh":
                    trigger_full_refresh()

                live.update(get_layout(refresh_flash=time.time() < _refresh_flash_until))
                time.sleep(0.25)
    finally:
        if use_unix_keys and old_settings is not None:
            termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)


def main():
    log(f"Massport ingestor starting. Data dir: {DATA_DIR}  Log: {LOG_FILE}", agency=MASSPORT_AGENCY)
    
    # Render layout scaffolding immediately to avoid blank screens
    init_ui_state(load_history=False)
    
    try:
        run_live_gui()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        log_api_call_summary(final=True)
        log("Massport ingestor stopped.", agency=MASSPORT_AGENCY)


if __name__ == "__main__":
    main()