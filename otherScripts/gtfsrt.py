#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# June 11, 2026
#
# Description:
# Dedicated ingestion engine for GTFS-RT protobuf agencies.
# Polls VehiclePosition, TripUpdate, and Alert feeds for configured agencies
# (PVTA, MVRTA) and writes records to the Data Lake.
#
# Data Integrity & Storage:
# Implements append-only JSONL event capture with deterministic deduplication hashes.
# A background compactor rotates completed files into Parquet archives under data/archive/.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   agency  — agency label such as PVTA or MVRTA
#   stream  — logical stream name (VehiclePosition, TripUpdate, Alert)
#   event   — update | error
#   data    — full protobuf payload decoded to dict

import json
import hashlib
import os
import random
import re
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
from datetime import datetime
from pathlib import Path
from queue import Empty, Queue
from zoneinfo import ZoneInfo
from types import SimpleNamespace

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

try:
    from google.transit import gtfs_realtime_pb2
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
except ImportError:
    PROTOBUF_AVAILABLE = False

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


def env_float(name, default):
    raw_value = env_text(name, str(default))
    try:
        return float(raw_value)
    except ValueError:
        return default


def env_list(name, default=""):
    raw_value = env_text(name, default)
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def safe_prefix(value):
    cleaned_value = "".join(c if c.isalnum() or c in {"-", "_"} else "_" for c in value.strip())
    return cleaned_value.strip("_") or "stream"


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


# GTFS-RT agency configurations (protobuf-based). PVTA uses InfoPoint REST (see below).
GTFSRT_AGENCIES = {
    "MVRTA": {
        "base_url": "https://meva.syncromatics.com/gtfs-rt",
        "url_style": "path",      # URL style: {base_url}/{endpoint}
        "endpoints": ["vehiclepositions", "tripupdates", "alerts"],
        "headers": {"Accept": "application/x-protobuf"},
    },
}

# Derive target agencies from .env; fall back to running all configured ones.
_target_env = env_list("GTFSRT_TARGET_AGENCIES")
TARGET_AGENCIES = {
    k: v for k, v in GTFSRT_AGENCIES.items()
    if not _target_env or k in _target_env
}

# PVTA uses InfoPoint REST API (bustracker.pvta.com), not GTFS-RT protobuf.
PVTA_AGENCY_KEY = "PVTA"
PVTA_INFOPOINT_BASE = env_text("PVTA_INFOPOINT_BASE", "https://bustracker.pvta.com/InfoPoint/rest")
PVTA_INFOPOINT_STREAMS = ("VehiclePositions", "Alerts")

# Combined supported agencies: GTFS-RT (MVRTA) + InfoPoint (PVTA)
SUPPORTED_AGENCIES = tuple(TARGET_AGENCIES.keys()) + (PVTA_AGENCY_KEY,)

ARGS = SimpleNamespace(
    no_compaction=False,
    poll_jitter=1.25,
)

LOCAL_TZ = ZoneInfo(env_required("SYNC_TIMEZONE"))
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
LOG_FILE = LOG_DIR / "gtfsrt_ingest.log"

for _dir in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
POLL_INTERVAL_FAST = env_int("POLL_INTERVAL_FAST", 10)
POLL_INTERVAL_MEDIUM = env_int("POLL_INTERVAL_MEDIUM", 15)
POLL_INTERVAL_SLOW = env_int("POLL_INTERVAL_SLOW", 30)

write_queue = Queue(maxsize=50000)
stop_event = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []
REFRESH_REQUESTED = threading.Event()
_log_lock = threading.Lock()

FEEDS_PER_AGENCY = {k: tuple(v["endpoints"]) for k, v in TARGET_AGENCIES.items()}
FEEDS_PER_AGENCY[PVTA_AGENCY_KEY] = PVTA_INFOPOINT_STREAMS


#### LOGGING ####

def log(message, level="info", agency=""):
    now = datetime.now(LOCAL_TZ)
    ts_full = now.strftime("%Y-%m-%d %H:%M:%S")
    ts_short = now.strftime("%H:%M:%S")
    prefix = f"[{agency}] " if agency else ""

    try:
        with _log_lock:
            with open(LOG_FILE, "a", encoding="utf-8") as handle:
                handle.write(f"{ts_full} [{level.upper()}] {prefix}{message}\n")
    except Exception:
        pass

    if level in ("error", "warning"):
        with UI_STATE_LOCK:
            RECENT_ERRORS.append({"ts": ts_short, "agency": agency or "SYSTEM", "message": str(message)})
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
_http_lock = threading.Lock()


def get_http_session():
    global _http_session
    with _http_lock:
        if _http_session is None:
            session = requests.Session()
            retry = Retry(
                total=3,
                connect=3,
                read=3,
                status=3,
                backoff_factor=1.0,
                status_forcelist=(429, 500, 502, 503, 504),
                allowed_methods=frozenset(["GET"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry, pool_connections=20, pool_maxsize=20)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            _http_session = session
    return _http_session


def sleep_with_stop(seconds):
    deadline = time.time() + seconds
    while not stop_event.is_set() and time.time() < deadline:
        time.sleep(min(0.25, deadline - time.time()))


def stable_stagger(label, span_seconds=4.0):
    """Deterministic startup offset so threads don't all fire at once."""
    return (hash(label) % 100) / 100.0 * span_seconds


def normalize_jsonable(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {k: normalize_jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [normalize_jsonable(item) for item in value]
    return str(value)


#### SHARED OUTPUT ####

def history_file_path(agency, stream, date_str):
    return EVENTS_DIR / f"{safe_prefix(agency)}_{safe_prefix(stream)}_{date_str}.jsonl"


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


def touch_ui_sync(agency, stream):
    now_time = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    with UI_STATE_LOCK:
        key = (agency, stream)
        if key in UI_STREAM_STATE:
            UI_STREAM_STATE[key]["last_sync_time"] = now_time


def emit_record(agency, stream, event, data, endpoint=""):
    now = datetime.now(LOCAL_TZ)
    now_ts = now.isoformat(timespec="seconds")
    stream_key = ensure_history_loaded(agency, stream)

    normalized_data = normalize_jsonable(data)
    hash_payload = {
        "agency": agency,
        "stream": stream,
        "event": event,
        "endpoint": endpoint,
        "data": normalized_data,
    }
    payload = json.dumps(hash_payload, sort_keys=True, separators=(",", ":"), default=str)
    hash_id = hashlib.sha256(payload.encode("utf-8")).hexdigest()

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

    try:
        write_queue.put(record, timeout=2)
        with UI_STATE_LOCK:
            state = UI_STREAM_STATE.get((agency, stream))
            if state:
                state["total_today"] += 1
        return True
    except Exception:
        log(f"Write queue full for {agency}::{stream}; dropping record", level="error", agency=agency)
        return False


def infer_stream_type(stream):
    """Label a stream as vehicle, trip, alert, or general for the TUI type column."""
    sl = stream.lower()
    if "vehicle" in sl:
        return "vehicle"
    if "trip" in sl:
        return "trip"
    if "alert" in sl:
        return "alert"
    return "general"


#### WRITER & COMPACTION ####

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
                agency = record.get("agency", "agency")
                stream = record.get("stream", "stream")
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{safe_prefix(agency)}_{safe_prefix(stream)}_{date_str}.jsonl"
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
    if "data" in chunk.columns:
        chunk["data"] = chunk["data"].apply(
            lambda v: json.dumps(v, default=str, ensure_ascii=False) if isinstance(v, (dict, list)) else (None if v is None else str(v))
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

        if writer is None:
            return

        writer.close()
        writer = None
        temp_path.replace(parquet_path)
        file_path.unlink()
        log(f"Compacted {file_path.name} into {parquet_path.name} with {row_count} rows.")
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


def compact_worker():
    if not PARQUET_AVAILABLE or ARGS.no_compaction:
        return

    log("Parquet compaction watchdog active.")
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            for file_path in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file_path.name:
                    compact_jsonl_file(file_path)
        except Exception as exc:
            log(f"Compaction thread error: {exc}", level="error")

        sleep_with_stop(60)


#### GTFS-RT POLLING ####

def build_endpoint_url(agency_key, endpoint):
    config = TARGET_AGENCIES[agency_key]
    base = config["base_url"]
    if config["url_style"] == "query":
        return f"{base}?Type={endpoint}"
    return f"{base}/{endpoint}"


def request_proto(url, headers, agency):
    """Fetch a GTFS-RT protobuf feed and return a parsed FeedMessage, or None on error."""
    if not PROTOBUF_AVAILABLE:
        log("google-transit-realtime is not installed; install gtfs-realtime-bindings", level="error", agency=agency)
        return None

    try:
        session = get_http_session()
        response = session.get(url, headers=headers, timeout=15)
        if response.status_code != 200:
            log(f"HTTP {response.status_code} fetching {url}", level="warning", agency=agency)
            return None

        feed = gtfs_realtime_pb2.FeedMessage()
        feed.ParseFromString(response.content)
        return feed
    except Exception as exc:
        log(f"Protobuf request failed for {url}: {exc}", level="warning", agency=agency)
        return None


def sync_poller(agency_key, endpoint, interval):
    """Polls one GTFS-RT feed endpoint in a tight loop, emitting one record per entity."""
    config = TARGET_AGENCIES[agency_key]
    url = build_endpoint_url(agency_key, endpoint)
    headers = config["headers"]

    sleep_with_stop(stable_stagger(f"GTFSRT:{agency_key}:{endpoint}", 5))

    while not stop_event.is_set():
        start_time = time.time()
        try:
            feed = request_proto(url, headers, agency_key)
            if feed is not None:
                touch_ui_sync(agency_key, endpoint)
                new_count = 0
                for entity in feed.entity:
                    record = MessageToDict(entity, preserving_proto_field_name=True)
                    if emit_record(agency_key, endpoint, "update", record, endpoint=url):
                        new_count += 1

                log(f"{endpoint}: {new_count} entities", level="info", agency=agency_key)
                with UI_STATE_LOCK:
                    state = UI_STREAM_STATE.get((agency_key, endpoint))
                    if state:
                        state["new_this_poll"] = new_count
        except Exception as exc:
            log(f"GTFS-RT Poller Error for {agency_key} {endpoint}: {exc}", level="error", agency=agency_key)

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)


def run_agency(agency_key):
    """Spawn one polling thread per endpoint for the given agency."""
    if not PROTOBUF_AVAILABLE:
        log("gtfs-realtime-bindings is not installed; GTFS-RT polling unavailable", level="error", agency=agency_key)
        return

    config = TARGET_AGENCIES[agency_key]
    log(f"Starting GTFS-RT runner for {agency_key}", agency=agency_key)

    for endpoint in config["endpoints"]:
        endpoint_lower = endpoint.lower()
        if "vehicle" in endpoint_lower:
            interval = POLL_INTERVAL_FAST
        elif "alert" in endpoint_lower:
            interval = POLL_INTERVAL_SLOW
        else:
            interval = POLL_INTERVAL_MEDIUM

        threading.Thread(
            target=sync_poller,
            args=(agency_key, endpoint, interval),
            daemon=True,
        ).start()


#### PVTA INFOPOINT REST POLLING ####

def parse_infopoint_date(raw):
    """Parse .NET JSON date /Date(ms±offset)/ → ISO-8601 string in LOCAL_TZ."""
    if not raw:
        return None
    m = re.search(r'/Date\((\d+)([+-]\d{4})?\)/', str(raw))
    if not m:
        return str(raw)
    try:
        dt = datetime.fromtimestamp(int(m.group(1)) / 1000.0, tz=LOCAL_TZ)
        return dt.isoformat(timespec="seconds")
    except Exception:
        return str(raw)


def pvta_vehicles_poller(interval):
    """
    Polls PVTA InfoPoint GetVisibleRoutes and emits one VehiclePositions record
    per active vehicle (deduplicated by hash — unchanged positions are dropped).
    """
    session = get_http_session()
    url = f"{PVTA_INFOPOINT_BASE}/Routes/GetVisibleRoutes"
    sleep_with_stop(stable_stagger("PVTA:VehiclePositions", 5))

    while not stop_event.is_set():
        start_time = time.time()
        try:
            resp = session.get(url, timeout=15, headers={"Accept": "application/json"})
            if resp.status_code == 200:
                routes = resp.json()
                seen_vids = set()
                new_count = 0

                for route in routes:
                    route_id = route.get("RouteId")
                    route_abbr = route.get("RouteAbbreviation") or route.get("ShortName")
                    route_name = route.get("LongName") or route.get("GoogleDescription")
                    for vehicle in route.get("Vehicles", []):
                        vid = vehicle.get("VehicleId")
                        if vid in seen_vids:
                            continue
                        seen_vids.add(vid)

                        record_data = {
                            "vehicle_id": vid,
                            "vehicle_name": vehicle.get("Name"),
                            "vehicle_farebox_id": vehicle.get("VehicleFareboxId"),
                            "route_id": route_id,
                            "route_abbr": route_abbr,
                            "route_name": route_name,
                            "run_id": vehicle.get("RunId"),
                            "trip_id": vehicle.get("TripId"),
                            "block_farebox_id": vehicle.get("BlockFareboxId"),
                            "latitude": vehicle.get("Latitude"),
                            "longitude": vehicle.get("Longitude"),
                            "heading": vehicle.get("Heading"),
                            "speed": vehicle.get("Speed"),
                            "direction": vehicle.get("Direction"),
                            "direction_long": vehicle.get("DirectionLong"),
                            "destination": vehicle.get("Destination"),
                            "last_stop": vehicle.get("LastStop"),
                            "stop_id": vehicle.get("StopId"),
                            "on_board": vehicle.get("OnBoard"),
                            "occupancy_status": vehicle.get("OccupancyStatus"),
                            "occupancy_label": vehicle.get("OccupancyStatusReportLabel"),
                            "deviation": vehicle.get("Deviation"),
                            "op_status": vehicle.get("OpStatus"),
                            "display_status": vehicle.get("DisplayStatus"),
                            "comm_status": vehicle.get("CommStatus"),
                            "gps_status": vehicle.get("GPSStatus"),
                            "seating_capacity": vehicle.get("SeatingCapacity"),
                            "total_capacity": vehicle.get("TotalCapacity"),
                            "property_name": vehicle.get("PropertyName"),
                            "last_updated": parse_infopoint_date(vehicle.get("LastUpdated", "")),
                        }

                        if emit_record(PVTA_AGENCY_KEY, "VehiclePositions", "update", record_data):
                            new_count += 1

                touch_ui_sync(PVTA_AGENCY_KEY, "VehiclePositions")
                log(
                    f"VehiclePositions: {new_count} new from {len(seen_vids)} active vehicles",
                    level="info",
                    agency=PVTA_AGENCY_KEY,
                )
                with UI_STATE_LOCK:
                    state = UI_STREAM_STATE.get((PVTA_AGENCY_KEY, "VehiclePositions"))
                    if state:
                        state["new_this_poll"] = new_count
            else:
                log(f"HTTP {resp.status_code} from GetVisibleRoutes", level="warning", agency=PVTA_AGENCY_KEY)
        except Exception as exc:
            log(f"Vehicles poller error: {exc}", level="error", agency=PVTA_AGENCY_KEY)

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)


def pvta_alerts_poller(interval):
    """
    Polls PVTA InfoPoint GetCurrentMessages and emits one Alerts record per message.
    """
    session = get_http_session()
    url = f"{PVTA_INFOPOINT_BASE}/PublicMessages/GetCurrentMessages"
    sleep_with_stop(stable_stagger("PVTA:Alerts", 5))

    while not stop_event.is_set():
        start_time = time.time()
        try:
            resp = session.get(url, timeout=15, headers={"Accept": "application/json"})
            if resp.status_code == 200:
                messages = resp.json()
                new_count = 0
                for msg in messages:
                    record_data = {
                        "message_id": msg.get("MessageId"),
                        "header": msg.get("Header"),
                        "message": msg.get("Message"),
                        "routes": msg.get("Routes", []),
                        "priority": msg.get("Priority"),
                        "cause": msg.get("CauseReportLabel"),
                        "effect": msg.get("EffectReportLabel"),
                        "days_of_week": msg.get("DaysOfWeek"),
                        "from_date": parse_infopoint_date(msg.get("FromDate", "")),
                        "to_date": parse_infopoint_date(msg.get("ToDate", "")),
                        "url": msg.get("URL"),
                        "published": msg.get("Published"),
                        "is_primary_record": msg.get("IsPrimaryRecord"),
                    }
                    if emit_record(PVTA_AGENCY_KEY, "Alerts", "update", record_data):
                        new_count += 1

                touch_ui_sync(PVTA_AGENCY_KEY, "Alerts")
                log(f"Alerts: {new_count} new from {len(messages)} active", level="info", agency=PVTA_AGENCY_KEY)
                with UI_STATE_LOCK:
                    state = UI_STREAM_STATE.get((PVTA_AGENCY_KEY, "Alerts"))
                    if state:
                        state["new_this_poll"] = new_count
            else:
                log(f"HTTP {resp.status_code} from GetCurrentMessages", level="warning", agency=PVTA_AGENCY_KEY)
        except Exception as exc:
            log(f"Alerts poller error: {exc}", level="error", agency=PVTA_AGENCY_KEY)

        elapsed = time.time() - start_time
        sleep_with_stop(max(0, interval - elapsed) + random.random() * ARGS.poll_jitter)


def run_pvta_infopoint():
    """Spawn PVTA InfoPoint polling threads (VehiclePositions + Alerts)."""
    log("Starting PVTA InfoPoint REST runner", agency=PVTA_AGENCY_KEY)
    threading.Thread(
        target=pvta_vehicles_poller,
        args=(POLL_INTERVAL_FAST,),
        daemon=True,
    ).start()
    threading.Thread(
        target=pvta_alerts_poller,
        args=(POLL_INTERVAL_SLOW,),
        daemon=True,
    ).start()


#### TUI ####

def init_ui_state():
    with UI_STATE_LOCK:
        for agency in SUPPORTED_AGENCIES:
            for stream in FEEDS_PER_AGENCY.get(agency, ()):
                UI_STREAM_STATE[(agency, stream)] = {
                    "agency": agency,
                    "stream": stream,
                    "type": infer_stream_type(stream),
                    "new_this_poll": 0,
                    "total_today": 0,
                    "last_sync_time": "Loading...",
                }


def trigger_ui_refresh():
    REFRESH_REQUESTED.set()


def build_ui_table():
    table = Table(show_header=True, header_style="bold cyan", expand=True, box=None)
    table.add_column("Agency", style="bold", width=10)
    table.add_column("Stream", width=20)
    table.add_column("Type", width=10)
    table.add_column("New/Poll", justify="right", width=10)
    table.add_column("Total Today", justify="right", width=12)
    table.add_column("Last Sync", justify="right", width=12)

    with UI_STATE_LOCK:
        rows = sorted(UI_STREAM_STATE.values(), key=lambda r: (r["agency"], r["stream"]))
        for row in rows:
            table.add_row(
                row["agency"],
                row["stream"],
                row["type"],
                str(row["new_this_poll"]),
                str(row["total_today"]),
                row.get("last_sync_time", "—"),
            )

    return table


def build_error_panel():
    with UI_STATE_LOCK:
        errors = list(RECENT_ERRORS[-8:])
    if not errors:
        return Panel("[dim]No recent errors[/dim]", title="Recent Errors", border_style="dim")
    lines = "\n".join(
        f"[dim]{e['ts']}[/dim] [{e['agency']}] {e['message'][:100]}"
        for e in reversed(errors)
    )
    return Panel(lines, title="Recent Errors", border_style="red")


def run_live_gui():
    console = Console()
    init_ui_state()

    def poll_keys():
        # Graceful keyboard exit: q or Ctrl-C.
        if termios and tty:
            try:
                fd = sys.stdin.fileno()
                old_settings = termios.tcgetattr(fd)
                tty.setcbreak(fd)
                try:
                    while not stop_event.is_set():
                        import select as _select
                        readable, _, _ = _select.select([sys.stdin], [], [], 0.25)
                        if readable:
                            ch = sys.stdin.read(1)
                            if ch in ("q", "Q"):
                                stop_event.set()
                                break
                finally:
                    termios.tcsetattr(fd, termios.TCSADRAIN, old_settings)
            except Exception:
                pass
        elif msvcrt:
            while not stop_event.is_set():
                if msvcrt.kbhit():
                    ch = msvcrt.getwch()
                    if ch in ("q", "Q"):
                        stop_event.set()
                        break
                time.sleep(0.25)

    threading.Thread(target=poll_keys, daemon=True).start()

    def delayed_worker_start():
        time.sleep(0.5)
        threading.Thread(target=writer_worker, daemon=True).start()
        if not ARGS.no_compaction:
            threading.Thread(target=compact_worker, daemon=True).start()

        for agency_key in TARGET_AGENCIES:
            run_agency(agency_key)
            sleep_with_stop(1.5)
        run_pvta_infopoint()

    threading.Thread(target=delayed_worker_start, daemon=True).start()

    try:
        with Live(console=console, refresh_per_second=1, screen=True) as live:
            while not stop_event.is_set():
                streams_table = build_ui_table()
                error_panel = build_error_panel()
                header = Text(
                    f"GTFS-RT Ingestor  |  {', '.join(SUPPORTED_AGENCIES)}  |  {datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}  |  q to quit",
                    style="bold white on dark_blue",
                    justify="center",
                )
                live.update(Group(header, streams_table, error_panel))
                sleep_with_stop(1)
    except KeyboardInterrupt:
        stop_event.set()


#### MAIN ORCHESTRATOR ####

if __name__ == "__main__":
    try:
        run_live_gui()
    except KeyboardInterrupt:
        stop_event.set()
    finally:
        stop_event.set()
        log("GTFS-RT ingestor stopped.", level="info", agency="SYSTEM")
