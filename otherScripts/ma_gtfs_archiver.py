#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 25, 2026 (Last updated June 16, 2026)
#
# Description:
# Multi-agency runner for static GTFS and optional MBTA live ingestion.
# Pulls Massachusetts regional transit authority static GTFS feeds and can
# also ingest MBTA live data streams in the same process.
#
# Data Integrity & Storage:
# Implements a crash-proof Write-Ahead Log (WAL) pattern. All network records are instantly 
# streamed to append-only, daily-tagged JSONL files under data/events/. An automated background 
# thread rotates files at the configured timezone's midnight boundary and safely compresses 
# the previous day's JSONL logs into highly efficient Parquet archives under data/archive/. 
# Active stream consumers are never blocked during rotation.
#
# If a Parquet archive already exists for a given day (due to container restarts), the worker 
# merges the JSONL data into the existing Parquet file and deduplicates records using a SHA-256 hash.
# Static GTFS snapshots are merged into stable per-table Parquet files under data/archive/gtfs/.
# Identical rows are stored once using SHA-256 deduplication; `first_logged` and `last_logged`
# metadata columns are added exclusively to static GTFS tables to track when each unique record
# first appeared and was most recently seen. Live event records do not carry these columns.
#
# Startup routines include an orphaned JSONL recovery sweep and automated scheduling for 
# twice-daily GTFS static package downloads (03:00 and 15:00 local time) routed to data/archive/gtfs/.
# All timestamps and rotation boundaries strictly adhere to the configured timezone.
#
# Output Schema:
#   hash_id — SHA-256 deterministic deduplication fingerprint
#   ts      — ISO 8601 timestamp in the configured timezone
#   event   — add | update | remove | reset | snapshot | error
#   id      — entity ID string from the API
#   data    — full API payload (JSON string in Parquet, raw dict in JSONL)
#
# License:
# BSD 3-Clause License
# Find the full agreement at https://github.com/mxdrew/gtfs-data-logger/blob/main/LICENSE

import os
import io
import json
import time
import queue
import zipfile
import threading
import hashlib
import tempfile
import logging
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from concurrent.futures import ThreadPoolExecutor

import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv
from urllib3.util.retry import Retry

# Load environment variables
load_dotenv(Path(__file__).with_name(".env"))

# Setup logging
logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("TransitArchiver")


#### GLOBAL CONFIGURATION ####

def env_text(name, default=""):
    val = os.environ.get(name)
    if val is None:
        val = default
    return str(val).strip()

def env_int(name, default):
    raw_value = env_text(name, str(default))
    try:
        return int(raw_value)
    except ValueError:
        return default


def env_required(name):
    value = env_text(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def env_flag(name):
    return env_required(name).lower() == "true"


def env_list(name):
    raw_value = env_required(name)
    return [value.strip() for value in raw_value.split(",") if value.strip()]

LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE"))
LOG_LEVEL = env_text("LOG_LEVEL", "info").lower() or "info"

ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
DEFAULT_BATCH_SIZE = env_int("DEFAULT_BATCH_SIZE", 25)
PREDICTIONS_BATCH_SIZE = env_int("PREDICTIONS_BATCH_SIZE", max(DEFAULT_BATCH_SIZE, 75))

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "archive"
ARCHIVE_DIR = DATA_DIR / "archive"
GTFS_DIR = ARCHIVE_DIR / "gtfs"

for d in [EVENTS_DIR, ARCHIVE_DIR, GTFS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

GTFS_DOWNLOAD_HEADERS = {
    "Accept": "application/zip,application/octet-stream,*/*",
    "User-Agent": env_required("GTFS_DOWNLOAD_USER_AGENT"),
}


#### MULTI-AGENCY CONFIGURATION DICTIONARY ####

AGENCIES = [
    {
        "name": "MBTA",
        "prefix": "MBTA",
        "base_url": "https://api-v3.mbta.com",
        "gtfs_url": "https://cdn.mbta.com/MBTA_GTFS.zip",
        "api_key": env_text("MBTA_API_KEY"),
        "enable_base_streams": True,
        "enable_route_streams": True,
        "enable_snapshot_pulls": True,
        "enable_enhanced_streams": True,
        "enable_gtfs_static": True,
        "base_streams": ["vehicles", "alerts"],
        "route_streams": ["predictions", "stop_events"],
        "snapshot_eps": [
            "schedules", "services", "shapes", "trips", "lines", "routes",
            "route_patterns", "facilities", "stops", "live_facilities"
        ],
        "enhanced_poll_interval": 7,
        "enhanced_streams": {
            "vehicles_enhanced": "https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json",
            "alerts_enhanced": "https://cdn.mbta.com/realtime/Alerts_enhanced.json",
            "trips_enhanced": "https://cdn.mbta.com/realtime/TripUpdates_enhanced.json"
        }
    },
    {"name": "BRTA", "prefix": "BRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/berkshire-ma-us/berkshire-ma-us.zip", "enable_gtfs_static": True},
    {"name": "BAT", "prefix": "BAT", "gtfs_url": "https://data.trilliumtransit.com/gtfs/brockton-ma-us/brockton-ma-us.zip", "enable_gtfs_static": True},
    {"name": "CATA", "prefix": "CATA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/capeann-ma-us/capeann-ma-us.zip", "enable_gtfs_static": True},
    {"name": "CCRTA", "prefix": "CCRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/capecod-ma-us/capecod-ma-us.zip", "enable_gtfs_static": True},
    {"name": "FRTA", "prefix": "FRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/frta-ma-us/frta-ma-us.zip", "enable_gtfs_static": True},
    {"name": "GATRA", "prefix": "GATRA", "gtfs_url": "http://data.trilliumtransit.com/gtfs/gatra-ma-us/gatra-ma-us.zip", "enable_gtfs_static": True},
    {"name": "LRTA", "prefix": "LRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/lowell-ma-us/lowell-ma-us.zip", "enable_gtfs_static": True},
    {"name": "MVRTA", "prefix": "MVRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/merrimackvalley-ma-us/merrimackvalley-ma-us.zip", "enable_gtfs_static": True},
    {"name": "MWRTA", "prefix": "MWRTA", "gtfs_url": "http://vc.mwrta.com/gtfs/google_transit.zip", "enable_gtfs_static": True},
    {"name": "MART", "prefix": "MART", "gtfs_url": "https://data.trilliumtransit.com/gtfs/montachusett-ma-us/montachusett-ma-us.zip", "enable_gtfs_static": True},
    {"name": "NRTA", "prefix": "NRTA", "gtfs_url": "https://nrtawave.transloc.com/Secure/Admin/Reports/GTFSDownload.aspx", "enable_gtfs_static": True},
    {"name": "PVTA", "prefix": "PVTA", "gtfs_url": "http://www.pvta.com/g_trans/google_transit.zip", "enable_gtfs_static": True},
    {"name": "VTA", "prefix": "VTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/marthasvineyard-ma-us/marthasvineyard-ma-us.zip", "enable_gtfs_static": True},
    {"name": "WRTA", "prefix": "WRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/wrta-ma-us/wrta-ma-us.zip", "enable_gtfs_static": True},
    {"name": "SRTA", "prefix": "SRTA", "gtfs_url": "https://data.trilliumtransit.com/gtfs/srta-ma-us/srta-ma-us.zip", "enable_gtfs_static": True},
    {"name": "YANKEE", "prefix": "YANKEE", "gtfs_url": "https://data.trilliumtransit.com/gtfs/yankeeline-ma-us/yankeeline-ma-us.zip", "enable_gtfs_static": True},
    {"name": "MASSPORT", "prefix": "MASSPORT", "gtfs_url": "https://data.trilliumtransit.com/gtfs/massport-ma-us/massport-ma-us.zip", "enable_gtfs_static": True},
    {"name": "LEXPRESS", "prefix": "LEXPRESS", "gtfs_url": "http://rtaalerts.com/gtfs/lexpress/google_transit.zip", "enable_gtfs_static": True},
    {"name": "SSA", "prefix": "SSA", "gtfs_url": "https://www-steamship-assets.s3.amazonaws.com/versioned_downloadable_forms/path/2025_gtfs_copy1.zip", "enable_gtfs_static": True},
    {"name": "HYLINE", "prefix": "HYLINE", "gtfs_url": "https://data.trilliumtransit.com/gtfs/hylinecruises-ma-us/hylinecruises-ma-us.zip", "enable_gtfs_static": True}
]


#### SHARED RESOURCES ####

write_queue = queue.Queue(maxsize=1000000)
stop_event = threading.Event()
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
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry, pool_connections=50, pool_maxsize=50)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _http_session = session

    return _http_session

def filename_date_key(filename):
    try:
        return filename.rsplit("_", 1)[-1].split(".", 1)[0]
    except Exception:
        return ""

def close_stale_event_handles(handles, current_date_key):
    stale_filenames = [name for name in handles if filename_date_key(name) != current_date_key]
    for filename in stale_filenames:
        try:
            handles[filename].close()
        finally:
            del handles[filename]

def gtfs_row_hash(table_name, row_data):
    normalized_row = {}
    for key, value in row_data.items():
        normalized_row[key] = None if pd.isna(value) else value

    payload = json.dumps(
        {"table": table_name, "row": normalized_row},
        sort_keys=True,
        separators=(",", ":"),
        default=str,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()

def prepare_gtfs_snapshot(df, table_name, logged_date):
    frame = df.copy()
    records = [dict(zip(frame.columns, row)) for row in frame.itertuples(index=False, name=None)]
    frame["hash_id"] = [gtfs_row_hash(table_name, record) for record in records]
    frame["first_logged"] = logged_date
    frame["last_logged"] = logged_date
    return frame

def normalize_gtfs_logged_dates(series):
    normalized_values = []
    for value in series:
        if pd.isna(value):
            normalized_values.append(pd.NaT)
            continue

        parsed = pd.to_datetime(value, errors="coerce")
        if pd.isna(parsed):
            parsed = pd.to_datetime(value, format="%m%d%Y", errors="coerce")

        if pd.isna(parsed):
            normalized_values.append(pd.NaT)
            continue

        if getattr(parsed, "tzinfo", None) is None:
            parsed = parsed.tz_localize(LOCAL_TZ)
        else:
            parsed = parsed.tz_convert(LOCAL_TZ)

        normalized_values.append(parsed)

    return pd.Series(normalized_values, index=series.index)

def merge_gtfs_snapshots(existing_df, new_df, table_name, logged_date):
    frames = []

    for frame in (existing_df, new_df):
        if frame is not None and not frame.empty:
            frames.append(frame.copy())

    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True, sort=False)

    if "hash_id" not in combined.columns:
        data_columns = [column for column in combined.columns if column not in {"first_logged", "last_logged"}]
        combined["hash_id"] = [gtfs_row_hash(table_name, record) for record in combined[data_columns].to_dict(orient="records")]

    if "first_logged" not in combined.columns:
        combined["first_logged"] = logged_date
    if "last_logged" not in combined.columns:
        combined["last_logged"] = combined["first_logged"]

    combined["first_logged"] = normalize_gtfs_logged_dates(combined["first_logged"])
    combined["last_logged"] = normalize_gtfs_logged_dates(combined["last_logged"])

    if combined["first_logged"].isna().all():
        combined["first_logged"] = pd.to_datetime(logged_date, format="%Y-%m-%d", errors="coerce")
    if combined["last_logged"].isna().all():
        combined["last_logged"] = pd.to_datetime(logged_date, format="%Y-%m-%d", errors="coerce")

    agg_map = {}
    for column in combined.columns:
        if column == "hash_id":
            continue
        if column == "first_logged":
            agg_map[column] = "min"
        elif column == "last_logged":
            agg_map[column] = "max"
        else:
            agg_map[column] = "first"

    merged = combined.groupby("hash_id", as_index=False).agg(agg_map)
    merged["first_logged"] = normalize_gtfs_logged_dates(merged["first_logged"]).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    merged["last_logged"] = normalize_gtfs_logged_dates(merged["last_logged"]).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return merged


#### LOGGING ####

def log(message, agency_log_label="SYSTEM", is_error=False, is_warning=False):
    if LOG_LEVEL == "errors" and not is_error and not is_warning:
        return
        
    ts = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    agency_prefix = f"[{agency_log_label}] " if agency_log_label else ""
    if is_error:
        print(f"[{ts}] 🔴 ERROR {agency_prefix}{message}".rstrip())
    elif is_warning:
        print(f"[{ts}] 🟡 WARNING {agency_prefix}{message}".rstrip())
    else:
        print(f"[{ts}] 🟢 {agency_prefix}{message}".rstrip())


#### WRITER & COMPACTION ####

def writer_worker():
    # Drains the shared queue, computes deduplication hashes, and appends to today's agency JSONL files.
    log("Unified Writer thread engaged. Streaming to JSONL.")
    handles = {}
    
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                current_date_key = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                close_stale_event_handles(handles, current_date_key)

                agency_prefix, endpoint, evt_type, record_data = write_queue.get(timeout=1.0)
                
                # Deterministic hash excludes timestamp so deduplication works across days.
                payload_str = f"{agency_prefix}|{endpoint}|{evt_type}|{record_data.get('id', '')}|{json.dumps(record_data, sort_keys=True)}"
                hash_id = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
                
                record = {
                    "hash_id": hash_id,
                    "ts": datetime.now(LOCAL_TZ).isoformat(),
                    "event": evt_type,
                    "id": str(record_data.get("id", "")),
                    "data": record_data
                }
                
                filename = f"{agency_prefix}_{endpoint}_{current_date_key}.jsonl"
                filepath = EVENTS_DIR / filename
                
                if filename not in handles:
                    handles[filename] = open(filepath, "a", encoding="utf-8")
                
                handles[filename].write(json.dumps(record) + "\n")
                handles[filename].flush()
                write_queue.task_done()
                
            except queue.Empty:
                pass
            except Exception as e:
                log(f"Writer exception: {e}", is_error=True)
    finally:
        for f in handles.values():
            f.close()

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


def parquet_compaction_worker():
    # Streams ALL orphaned/yesterday's JSONL files into Parquet and removes the source after success.
    log("Parquet compaction watchdog and recovery sweep active.")
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            
            for file in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file.name:
                    log(f"Compacting orphaned/rotated {file.name} to Parquet archive...")
                    temp_path = None
                    writer = None
                    target_schema = None
                    try:
                        parquet_filename = file.with_suffix(".parquet").name
                        parquet_path = ARCHIVE_DIR / parquet_filename

                        temp_file = tempfile.NamedTemporaryFile(
                            delete=False,
                            dir=parquet_path.parent,
                            prefix=f"{parquet_path.stem}.",
                            suffix=".tmp",
                        )
                        temp_file.close()
                        temp_path = Path(temp_file.name)

                        row_count = 0

                        for chunk in pd.read_json(file, lines=True, chunksize=100000):
                            if "data" in chunk.columns:
                                chunk["data"] = chunk["data"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))

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

                        if writer is None:
                            log(f"Skipping empty JSONL file: {file.name}", is_warning=True)
                            continue

                        writer.close()
                        writer = None
                        temp_path.replace(parquet_path)
                        file.unlink()
                        log(f"Success: {file.name} compacted into {parquet_path.name} with {row_count} rows.")
                    except Exception as e:
                        log(f"Failed to compact {file.name}: {e}", is_error=True)
                    finally:
                        if writer is not None:
                            writer.close()
                        if temp_path is not None and temp_path.exists():
                            try:
                                temp_path.unlink()
                            except Exception:
                                pass
        except Exception as e:
            log(f"Compaction thread error: {e}", is_error=True)
            
        for _ in range(60):
            if stop_event.is_set(): break
            time.sleep(1)


#### NETWORK CONSUMERS ####

def fetch_api_ids(agency, endpoint):
    # Helper to fetch active route IDs for batching.
    if not agency.get("base_url"): return []
    headers = {"Accept": "application/json"}
    if agency.get("api_key"): headers["X-API-Key"] = agency["api_key"]
    try:
        res = requests.get(f"{agency['base_url']}/{endpoint}", headers=headers, timeout=15)
        if res.status_code == 200:
            return [x.get("id") for x in res.json().get("data", []) if x.get("id")]
    except Exception as e:
        log(f"Failed to fetch {endpoint} IDs: {e}", agency_log_label=agency["prefix"], is_warning=True)
    return []

def start_batched_streams(agency, endpoint, routes):
    batch_size = PREDICTIONS_BATCH_SIZE if endpoint == "predictions" else DEFAULT_BATCH_SIZE
    for i in range(0, len(routes), batch_size):
        batch = routes[i:i + batch_size]
        threading.Thread(
            target=sse_consumer,
            args=(agency, endpoint, {"filter[route]": ",".join(batch)}),
            daemon=True,
        ).start()
        time.sleep(0.25)

def sse_consumer(agency, endpoint, params=None):
    # Maintains a persistent live connection to transit agency SSE endpoints.
    if not agency.get("base_url"): return
    url = f"{agency['base_url']}/{endpoint}"
    headers = {
        "Accept": "text/event-stream",
        "Accept-Encoding": "identity",
        "Cache-Control": "no-cache",
    }
    if agency.get("api_key"): headers["X-API-Key"] = agency["api_key"]
    req_params = params or {}
    session = get_http_session()
    reconnect_delay = 5

    while not stop_event.is_set():
        log(f"Connecting to live stream: /{endpoint} {req_params}", agency_log_label=agency["prefix"])
        response = None
        try:
            response = session.get(url, headers=headers, params=req_params, stream=True, timeout=(10, 120))
            current_event = "message"
            data_buffer = []
            received_any_data = False

            for line in response.iter_lines(decode_unicode=True):
                if stop_event.is_set(): break
                if not line:
                    if data_buffer:
                        payload = json.loads("\n".join(data_buffer))
                        records = payload if isinstance(payload, list) else [payload]
                        for r in records:
                            write_queue.put((agency["prefix"], endpoint, current_event, r))
                        received_any_data = True
                    current_event = "message"
                    data_buffer = []
                    continue

                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data_buffer.append(line.split(":", 1)[1].strip())

            if not stop_event.is_set():
                reconnect_delay = 5 if received_any_data else min(60, reconnect_delay * 2)
                time.sleep(reconnect_delay)
                    
        except (requests.exceptions.ChunkedEncodingError, requests.exceptions.ConnectionError, requests.exceptions.ReadTimeout) as e:
            log(f"Stream /{endpoint} disconnected: {e}. Reconnecting in {reconnect_delay}s...", agency_log_label=agency["prefix"], is_warning=True)
            time.sleep(reconnect_delay)
            reconnect_delay = min(60, reconnect_delay * 2)
        except Exception as e:
            log(f"Stream /{endpoint} disconnected: {e}. Reconnecting in {reconnect_delay}s...", agency_log_label=agency["prefix"], is_warning=True)
            time.sleep(reconnect_delay)
            reconnect_delay = min(60, reconnect_delay * 2)
        finally:
            if response is not None:
                response.close()

def enhanced_poller(agency, endpoint, url):
    # Polls the heavy bulk JSON files based on configured interval.
    if not url: return
    headers = {"Accept": "application/json"}
    if agency.get("api_key"): headers["X-API-Key"] = agency["api_key"]
    
    while not stop_event.is_set():
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                records = data.get("entity", []) if "entity" in data else data
                records = records if isinstance(records, list) else [records]
                for r in records:
                    write_queue.put((agency["prefix"], endpoint, "update", r))
        except Exception as e:
            log(f"Enhanced poll failed for {endpoint}: {e}", agency_log_label=agency["prefix"], is_warning=True)
            
        sleep_ticks = int(agency.get("enhanced_poll_interval", 10) * 10)
        for _ in range(max(10, sleep_ticks)):
            if stop_event.is_set(): break
            time.sleep(0.1)


#### SCHEDULER TASKS ####

def fetch_snapshots(agency):
    # Pulls contextual snapshots for specific agency endpoints.
    # NOTE (Task #29 fix): The MBTA V3 API rejects unfiltered GET /trips requests
    # (HTTP 400 — requires filter[route], filter[id], filter[route_pattern], or filter[name]).
    # We special-case "trips" here: fetch all route IDs first, then pull trips in batches.
    if not agency.get("base_url") or not agency.get("snapshot_eps"):
        return
    log("Pulling hourly snapshots...", agency_log_label=agency["prefix"])
    headers = {"Accept": "application/json"}
    if agency.get("api_key"): headers["X-API-Key"] = agency["api_key"]

    # Endpoints that require a filter parameter — maps endpoint name → filter key.
    # "trips" is auto-detected; override via agency["snapshot_eps_filtered"] if needed.
    REQUIRE_ROUTE_FILTER = set(agency.get("snapshot_eps_require_route_filter", ["trips"]))

    for endpoint in agency.get("snapshot_eps", []):
        if stop_event.is_set():
            break
        try:
            if endpoint in REQUIRE_ROUTE_FILTER:
                # Fetch route IDs then pull per-route batch to satisfy the API filter requirement.
                route_ids = fetch_api_ids(agency, "routes")
                if not route_ids:
                    log(
                        f"Skipping {endpoint} snapshot: could not retrieve route IDs",
                        agency_log_label=agency["prefix"], is_warning=True,
                    )
                    continue
                batch_size = DEFAULT_BATCH_SIZE
                for i in range(0, len(route_ids), batch_size):
                    if stop_event.is_set():
                        break
                    batch = route_ids[i : i + batch_size]
                    try:
                        res = requests.get(
                            f"{agency['base_url']}/{endpoint}",
                            headers=headers,
                            params={"filter[route]": ",".join(batch)},
                            timeout=30,
                        )
                        if res.status_code == 200:
                            for r in res.json().get("data", []):
                                write_queue.put((agency["prefix"], endpoint, "snapshot", r))
                        else:
                            log(
                                f"Snapshot {endpoint} returned HTTP {res.status_code}",
                                agency_log_label=agency["prefix"], is_warning=True,
                            )
                    except Exception as e:
                        log(
                            f"Snapshot batch failed for {endpoint}: {e}",
                            agency_log_label=agency["prefix"], is_warning=True,
                        )
            else:
                res = requests.get(f"{agency['base_url']}/{endpoint}", headers=headers, timeout=30)
                if res.status_code == 200:
                    for r in res.json().get("data", []):
                        write_queue.put((agency["prefix"], endpoint, "snapshot", r))
        except Exception: pass

def fetch_gtfs_static(agency):
    # Downloads GTFS zip, deduplicates rows, and updates first/last seen dates.
    if not agency.get("gtfs_url"):
        return
    
    prefix = agency["prefix"]
    log("Pulling static GTFS assets...", agency_log_label=prefix)
    try:
        res = requests.get(agency["gtfs_url"], headers=GTFS_DOWNLOAD_HEADERS, timeout=120)
        res.raise_for_status()
        
        logged_at = datetime.now(LOCAL_TZ).isoformat(timespec="seconds")
        z = zipfile.ZipFile(io.BytesIO(res.content))
        
        for file_name in z.namelist():
            if file_name.endswith(".txt"):
                table_name = file_name.replace(".txt", "")
                
                df = pd.read_csv(z.open(file_name), dtype=str, low_memory=False)
                out_path = GTFS_DIR / f"{prefix}_gtfs_{table_name}.parquet"

                current_df = prepare_gtfs_snapshot(df, table_name, logged_at)
                existing_df = pd.read_parquet(out_path) if out_path.exists() else None
                merged_df = merge_gtfs_snapshots(existing_df, current_df, table_name, logged_at)

                if merged_df is None:
                    log(f"Skipping empty GTFS table: {table_name}", agency_log_label=prefix, is_warning=True)
                    continue

                temp_path = out_path.with_name(f"{out_path.name}.tmp")
                if temp_path.exists():
                    temp_path.unlink()

                table = pa.Table.from_pandas(merged_df, preserve_index=False)
                pq.write_table(table, temp_path, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                temp_path.replace(out_path)
                
        log("Static GTFS update complete.", agency_log_label=prefix)
    except Exception as e:
        log(f"GTFS pull failed: {e}", agency_log_label=prefix, is_error=True)

def scheduler_thread():
    # Triggers hourly and daily automated tasks for all agencies using ThreadPoolExecutor.
    last_snapshot_hour = -1
    last_gtfs_pull = -1
    executor = ThreadPoolExecutor(max_workers=4)
    
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        
        # Hourly REST Snapshots
        if now.hour != last_snapshot_hour and now.minute == 0:
            for agency in AGENCIES:
                if agency.get("enable_snapshot_pulls"):
                    executor.submit(fetch_snapshots, agency)
            last_snapshot_hour = now.hour
            
        # Static GTFS Pulls at 3AM and 3PM
        if now.hour in [3, 15] and now.hour != last_gtfs_pull:
            for agency in AGENCIES:
                if agency.get("enable_gtfs_static"):
                    executor.submit(fetch_gtfs_static, agency)
            last_gtfs_pull = now.hour
            
        time.sleep(10)


#### MAIN ORCHESTRATOR ####

if __name__ == "__main__":
    try:
        log("Starting Unified Transit Ingestion Engine...")
        
        threading.Thread(target=writer_worker, daemon=True).start()
        threading.Thread(target=parquet_compaction_worker, daemon=True).start()
        threading.Thread(target=scheduler_thread, daemon=True).start()
        
        for agency in AGENCIES:
            # Initial baseline pulls
            if agency.get("enable_snapshot_pulls"):
                threading.Thread(target=fetch_snapshots, args=(agency,), daemon=True).start()
            if agency.get("enable_gtfs_static"):
                threading.Thread(target=fetch_gtfs_static, args=(agency,), daemon=True).start()
            
            # Deploy Base Streams
            if agency.get("enable_base_streams"):
                for endpoint in agency.get("base_streams", []):
                    threading.Thread(target=sse_consumer, args=(agency, endpoint), daemon=True).start()
                    
                # Deploy Route-Batched Streams
                if agency.get("enable_route_streams"):
                    routes = fetch_api_ids(agency, "routes")
                    for endpoint in agency.get("route_streams", []):
                        start_batched_streams(agency, endpoint, routes)
                        
            # Deploy Enhanced Pollers
            if agency.get("enable_enhanced_streams"):
                for endpoint, url in agency.get("enhanced_streams", {}).items():
                    threading.Thread(target=enhanced_poller, args=(agency, endpoint, url), daemon=True).start()
            
        while not stop_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        log("Shutdown signal received. Closing pipelines cleanly...")
        stop_event.set()
        time.sleep(2)