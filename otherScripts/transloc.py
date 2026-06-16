#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# June 30, 2026
#
# Description:
# Dedicated ingestion engine for TransLoc/RideSystems CAD AVL agencies.
# Crawls Routes+Stops (hourly) and Vehicles, StopArrivalTimes, VehicleCapacities
# (poll-loop) for configured agencies (NRTA) via the TransLoc JSON relay API
# and writes records to the Data Lake.
#
# Data Integrity & Storage:
# Implements append-only JSONL event capture with deterministic deduplication hashes.
# A background compactor rotates completed files into Parquet archives under data/archive/.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   agency  — agency label such as NRTA
#   stream  — logical stream name (Routes, Vehicles, StopArrivalTimes, VehicleCapacities)
#   event   — update | snapshot | error
#   data    — full API payload (JSON string in Parquet, raw dict in JSONL)

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


# TransLoc/RideSystems base URL and credentials.
TRANSLOC_BASE_URL = env_text(
    "TRANSLOC_BASE_URL",
    "https://nrtawave.transloc.com/Services/JSONPRelay.svc",
).rstrip("/")
TRANSLOC_API_KEY = env_text("TRANSLOC_API_KEY", "8882812681")
TRANSLOC_AGENCY = env_text("TRANSLOC_AGENCY", "NRTA")

ARGS = SimpleNamespace(
    no_compaction=False,
    poll_jitter=1.25,
)

LOCAL_TZ = ZoneInfo(env_required("SYNC_TIMEZONE"))
DATA_DIR = default_data_dir()
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
LOG_DIR = DATA_DIR / "logs"
LOG_FILE = LOG_DIR / "transloc_ingest.log"

for _dir in [EVENTS_DIR, ARCHIVE_DIR, LOG_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
POLL_INTERVAL_FAST = env_int("POLL_INTERVAL_FAST", 10)    # Vehicles
POLL_INTERVAL_MEDIUM = env_int("POLL_INTERVAL_MEDIUM", 15) # StopArrivalTimes
POLL_INTERVAL_SLOW = env_int("POLL_INTERVAL_SLOW", 30)    # VehicleCapacities

AGENCY_FEEDS = ("Routes", "Vehicles", "StopArrivalTimes", "VehicleCapacities")

# Force hourly refresh flags — set via TUI keypress.
HOURLY_REFRESH_FLAG = threading.Event()

write_queue = Queue(maxsize=50000)
stop_event = threading.Event()
UI_STATE_LOCK = threading.Lock()
UI_STREAM_STATE = {}
RECENT_ERRORS = []
REFRESH_REQUESTED = threading.Event()
_log_lock = threading.Lock()


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


def parse_transloc_date(value):
    """Convert a /Date(ms)/ timestamp to an ISO string, or return as-is."""
    if isinstance(value, str):
        match = re.match(r"/Date\((\d+)\)/", value)
        if match:
            ms = int(match.group(1))
            return datetime.fromtimestamp(ms / 1000, tz=LOCAL_TZ).isoformat(timespec="seconds")
    return value


def request_json(url, params=None, timeout=15, agency=""):
    try:
        session = get_http_session()
        response = session.get(url, params=params, timeout=timeout)
        if response.status_code != 200:
            log(f"HTTP {response.status_code} for {url}", level="warning", agency=agency)
            return None
        return response.json()
    except Exception as exc:
        log(f"Request failed for {url}: {exc}", level="warning", agency=agency)
        return None


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
    sl = stream.lower()
    if "vehicle" in sl:
        return "vehicle"
    if "arrival" in sl or "stop" in sl:
        return "prediction"
    if "route" in sl:
        return "config"
    if "capacit" in sl:
        return "occupancy"
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


#### TRANSLOC CRAWLER ####

class TransLocCrawlerRunner:
    """Polls a TransLoc/RideSystems API relay for one agency.

    Hourly streams (Routes) run once per hour and on startup.
    Poll streams (Vehicles, StopArrivalTimes, VehicleCapacities) run continuously.
    The poll loop gates on routes_ready so route IDs are available before predictions.
    """

    def __init__(self, agency, base_url, api_key):
        self.agency = agency.upper()
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.last_hourly = -1
        self.routes_ready = threading.Event()

        # Cache of route IDs for per-route prediction fetches.
        # Populated by the hourly Routes fetch.
        self._route_ids = []
        self._route_lock = threading.Lock()

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _get(self, endpoint, params=None, timeout=15):
        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        return request_json(url, params=params, timeout=timeout, agency=self.agency)

    def _base_params(self):
        return {"apiKey": self.api_key, "isPublicMap": "true"}

    def _update_route_ids(self, routes):
        """Extract and cache route IDs from a Routes payload."""
        ids = []
        if isinstance(routes, list):
            for route in routes:
                rid = route.get("RouteId") or route.get("routeId") or route.get("id")
                if rid is not None:
                    ids.append(str(rid))
        with self._route_lock:
            if ids:
                self._route_ids = ids
                log(f"Routes: cached {len(ids)} route IDs", agency=self.agency)

    def _get_route_ids(self):
        with self._route_lock:
            return list(self._route_ids)

    # ── Hourly: Routes ────────────────────────────────────────────────────────

    def fetch_routes(self):
        """GET GetRoutesForMapWithScheduleWithEncodedLine — full route + stop definitions."""
        touch_ui_sync(self.agency, "Routes")
        params = {"apiKey": self.api_key, "isDispatch": "false"}
        data = self._get("GetRoutesForMapWithScheduleWithEncodedLine", params=params)
        if data is None:
            log("Routes: request failed", level="warning", agency=self.agency)
            return 0

        routes = data if isinstance(data, list) else (data.get("d") or data.get("routes") or [])
        self._update_route_ids(routes)

        new_count = 0
        for route in routes:
            if emit_record(self.agency, "Routes", "snapshot", route, endpoint="GetRoutesForMapWithScheduleWithEncodedLine"):
                new_count += 1

        with UI_STATE_LOCK:
            state = UI_STREAM_STATE.get((self.agency, "Routes"))
            if state:
                state["new_this_poll"] = new_count

        if new_count > 0:
            log(f"Routes: {new_count} new records", agency=self.agency)
        return new_count

    # ── Fast poll: Vehicles ───────────────────────────────────────────────────

    def fetch_vehicles(self):
        """GET GetMapVehiclePoints — live vehicle positions."""
        touch_ui_sync(self.agency, "Vehicles")
        data = self._get("GetMapVehiclePoints", params=self._base_params())
        if data is None:
            log("Vehicles: request failed", level="warning", agency=self.agency)
            return 0

        vehicles = data if isinstance(data, list) else (data.get("d") or data.get("vehicles") or [])
        new_count = 0
        for vehicle in vehicles:
            if emit_record(self.agency, "Vehicles", "update", vehicle, endpoint="GetMapVehiclePoints"):
                new_count += 1

        with UI_STATE_LOCK:
            state = UI_STREAM_STATE.get((self.agency, "Vehicles"))
            if state:
                state["new_this_poll"] = new_count

        if new_count > 0:
            log(f"Vehicles: {new_count} new records", agency=self.agency)
        return new_count

    # ── Medium poll: StopArrivalTimes (per route) ─────────────────────────────

    def fetch_stop_arrival_times(self):
        """GET GetStopArrivalTimes per route — real-time predictions."""
        touch_ui_sync(self.agency, "StopArrivalTimes")
        route_ids = self._get_route_ids()
        if not route_ids:
            log("StopArrivalTimes: no route IDs yet; skipping", level="verbose", agency=self.agency)
            return 0

        new_count = 0
        for route_id in route_ids:
            if stop_event.is_set():
                break
            params = {"routeIds": route_id, "version": "2"}
            data = self._get("GetStopArrivalTimes", params=params, timeout=10)
            if data is None:
                continue

            arrivals = data if isinstance(data, list) else (data.get("d") or data.get("arrivals") or [])
            for arrival in arrivals:
                # Attach route context so downstream consumers can join easily.
                arrival_with_context = dict(arrival) if isinstance(arrival, dict) else {"raw": arrival}
                arrival_with_context.setdefault("_routeId", route_id)
                if emit_record(self.agency, "StopArrivalTimes", "update", arrival_with_context,
                               endpoint=f"GetStopArrivalTimes?routeIds={route_id}"):
                    new_count += 1

            sleep_with_stop(0.1)  # gentle between routes

        with UI_STATE_LOCK:
            state = UI_STREAM_STATE.get((self.agency, "StopArrivalTimes"))
            if state:
                state["new_this_poll"] = new_count

        if new_count > 0:
            log(f"StopArrivalTimes: {new_count} new records across {len(route_ids)} routes", agency=self.agency)
        return new_count

    # ── Slow poll: VehicleCapacities ──────────────────────────────────────────

    def fetch_vehicle_capacities(self):
        """GET GetVehicleCapacities — occupancy data."""
        touch_ui_sync(self.agency, "VehicleCapacities")
        data = self._get("GetVehicleCapacities", params=self._base_params())
        if data is None:
            log("VehicleCapacities: request failed", level="warning", agency=self.agency)
            return 0

        capacities = data if isinstance(data, list) else (data.get("d") or data.get("capacities") or [])
        new_count = 0
        for cap in capacities:
            if emit_record(self.agency, "VehicleCapacities", "update", cap, endpoint="GetVehicleCapacities"):
                new_count += 1

        with UI_STATE_LOCK:
            state = UI_STREAM_STATE.get((self.agency, "VehicleCapacities"))
            if state:
                state["new_this_poll"] = new_count

        if new_count > 0:
            log(f"VehicleCapacities: {new_count} new records", agency=self.agency)
        return new_count

    # ── Loop orchestration ────────────────────────────────────────────────────

    def run_hourly(self):
        """Execute one full cycle of hourly feeds."""
        self.fetch_routes()
        self.routes_ready.set()

    def hourly_loop(self):
        while not stop_event.is_set():
            now = datetime.now(LOCAL_TZ)
            force = HOURLY_REFRESH_FLAG.is_set()
            if force or (now.minute == 0 and now.hour != self.last_hourly) or self.last_hourly == -1:
                if force:
                    HOURLY_REFRESH_FLAG.clear()
                self.last_hourly = now.hour
                try:
                    self.run_hourly()
                except Exception as exc:
                    log(f"Hourly error: {exc}", level="error", agency=self.agency)
            sleep_with_stop(30)

    def vehicles_loop(self):
        """Fast poll: Vehicles every POLL_INTERVAL_FAST seconds."""
        self.routes_ready.wait(timeout=30)
        sleep_with_stop(stable_stagger("vehicles"))
        while not stop_event.is_set():
            start = time.time()
            try:
                self.fetch_vehicles()
            except Exception as exc:
                log(f"Vehicles poll error: {exc}", level="error", agency=self.agency)
            elapsed = time.time() - start
            sleep_with_stop(max(0, POLL_INTERVAL_FAST - elapsed) + random.random() * ARGS.poll_jitter)

    def arrivals_loop(self):
        """Medium poll: StopArrivalTimes every POLL_INTERVAL_MEDIUM seconds."""
        self.routes_ready.wait(timeout=30)
        sleep_with_stop(stable_stagger("arrivals"))
        while not stop_event.is_set():
            start = time.time()
            try:
                self.fetch_stop_arrival_times()
            except Exception as exc:
                log(f"StopArrivalTimes poll error: {exc}", level="error", agency=self.agency)
            elapsed = time.time() - start
            sleep_with_stop(max(0, POLL_INTERVAL_MEDIUM - elapsed) + random.random() * ARGS.poll_jitter)

    def capacities_loop(self):
        """Slow poll: VehicleCapacities every POLL_INTERVAL_SLOW seconds."""
        self.routes_ready.wait(timeout=30)
        sleep_with_stop(stable_stagger("capacities"))
        while not stop_event.is_set():
            start = time.time()
            try:
                self.fetch_vehicle_capacities()
            except Exception as exc:
                log(f"VehicleCapacities poll error: {exc}", level="error", agency=self.agency)
            elapsed = time.time() - start
            sleep_with_stop(max(0, POLL_INTERVAL_SLOW - elapsed) + random.random() * ARGS.poll_jitter)

    def run(self):
        log(f"Starting TransLoc crawler for {self.agency}", agency=self.agency)
        threading.Thread(target=self.hourly_loop, daemon=True, name=f"hourly-{self.agency}").start()
        threading.Thread(target=self.vehicles_loop, daemon=True, name=f"vehicles-{self.agency}").start()
        threading.Thread(target=self.arrivals_loop, daemon=True, name=f"arrivals-{self.agency}").start()
        threading.Thread(target=self.capacities_loop, daemon=True, name=f"capacities-{self.agency}").start()


#### TUI ####

def init_ui_state():
    with UI_STATE_LOCK:
        for stream in AGENCY_FEEDS:
            UI_STREAM_STATE[(TRANSLOC_AGENCY, stream)] = {
                "agency": TRANSLOC_AGENCY,
                "stream": stream,
                "type": infer_stream_type(stream),
                "new_this_poll": 0,
                "total_today": 0,
                "last_sync_time": "Loading...",
            }


def build_ui_table():
    table = Table(show_header=True, header_style="bold cyan", expand=True, box=None)
    table.add_column("Agency", style="bold", width=10)
    table.add_column("Stream", width=20)
    table.add_column("Type", width=12)
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
                            elif ch in ("r", "R"):
                                HOURLY_REFRESH_FLAG.set()
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
                    elif ch in ("r", "R"):
                        HOURLY_REFRESH_FLAG.set()
                time.sleep(0.25)

    threading.Thread(target=poll_keys, daemon=True).start()

    def delayed_worker_start():
        time.sleep(0.5)
        threading.Thread(target=writer_worker, daemon=True).start()
        if not ARGS.no_compaction:
            threading.Thread(target=compact_worker, daemon=True).start()

        crawler = TransLocCrawlerRunner(
            agency=TRANSLOC_AGENCY,
            base_url=TRANSLOC_BASE_URL,
            api_key=TRANSLOC_API_KEY,
        )
        crawler.run()

    threading.Thread(target=delayed_worker_start, daemon=True).start()

    try:
        with Live(console=console, refresh_per_second=1, screen=True) as live:
            while not stop_event.is_set():
                streams_table = build_ui_table()
                error_panel = build_error_panel()
                header = Text(
                    f"TransLoc Ingestor  |  {TRANSLOC_AGENCY}  |  {datetime.now(LOCAL_TZ).strftime('%Y-%m-%d %H:%M:%S %Z')}  |  q quit  r refresh",
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
        log("TransLoc ingestor stopped.", level="info", agency="SYSTEM")
