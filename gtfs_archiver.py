#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 25, 2026 (Last updated May 27, 2026)
#
# Description:
# Lightweight, single-process data ingestion engine for real-time and static transit feeds. 
# Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk 
# JSON endpoints, executes hourly contextual REST snapshots, and manages cumulative static GTFS 
# history for each agency.
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
# Identical rows are stored once using SHA-256 deduplication, with `first_logged` and 
# `last_logged` metadata preserving when each unique record first appeared and was most recently seen.
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
import re
import time
import queue
import zipfile
import threading
import hashlib
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

#### CONFIGURATION ####

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


def env_float(name, default):
    raw_value = env_text(name, str(default))
    try:
        return float(raw_value)
    except ValueError:
        return default


LOCAL_TZ = ZoneInfo(env_text("SYNC_TIMEZONE", "America/New_York") or "America/New_York")
API_KEY = env_text("API_KEY")
BASE_URL = env_text("BASE_URL")
GTFS_URL = env_text("GTFS_URL")
AGENCY_NAME = env_text("AGENCY_NAME", "agency") or "agency"


def env_flag(name, default="false"):
    val = os.environ.get(name)
    if val is None:
        val = default
    return str(val).strip().lower() == "true"


def env_list(name, default=""):
    raw_value = env_text(name, default)
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def safe_prefix(value):
    cleaned_value = re.sub(r"[^A-Za-z0-9_-]+", "_", value.strip())
    return cleaned_value.strip("_") or "agency"


AGENCY_PREFIX = safe_prefix(AGENCY_NAME)
AGENCY_LOG_LABEL = AGENCY_NAME.strip() or AGENCY_PREFIX

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
GTFS_DIR = ARCHIVE_DIR / "gtfs"

for d in [EVENTS_DIR, ARCHIVE_DIR, GTFS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

ENABLE_BASE_STREAMS = env_flag("ENABLE_BASE_STREAMS", "true")
ENABLE_ROUTE_STREAMS = env_flag("ENABLE_ROUTE_STREAMS", "true")
ENABLE_SNAPSHOT_PULLS = env_flag("ENABLE_SNAPSHOT_PULLS", "true")
ENABLE_ENHANCED_STREAMS = env_flag("ENABLE_ENHANCED_STREAMS", "true")
ENABLE_GTFS_STATIC = env_flag("ENABLE_GTFS_STATIC", "true")

GTFS_DOWNLOAD_HEADERS = {
    "Accept": "application/zip,application/octet-stream,*/*",
    "User-Agent": "gtfs-data-archiver/1.0 (+https://github.com/mxdrew/gtfs-data-archiver)",
}

# 1. Base Streams
BASE_STREAMS = env_list("BASE_STREAMS")

# 2. Batched Streams
ROUTE_STREAMS = env_list("ROUTE_STREAMS")

# 3. Snapshot Endpoints
SNAPSHOT_EPS = env_list("SNAPSHOT_EPS")

# 4. Enhanced Bulk Feeds
ENHANCED_BULK_STREAMS = {}
if ENABLE_ENHANCED_STREAMS:
    if env_text("VEHICLES_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["vehicles_enhanced"] = env_text("VEHICLES_ENHANCED_URL")
    if env_text("ALERTS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["alerts_enhanced"] = env_text("ALERTS_ENHANCED_URL")
    if env_text("TRIPS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["trips_enhanced"] = env_text("TRIPS_ENHANCED_URL")

# Tuning Parameters
DEFAULT_BATCH_SIZE = env_int("DEFAULT_BATCH_SIZE", 25)
ENHANCED_POLL_INTERVAL_SECONDS = env_float("ENHANCED_POLL_INTERVAL_SECONDS", 7)
ARCHIVE_ZSTD_LEVEL = env_int("ARCHIVE_ZSTD_LEVEL", 10)
LOG_LEVEL = env_text("LOG_LEVEL", "info").lower() or "info"

write_queue = queue.Queue(maxsize=500000)
stop_event = threading.Event()


def dated_output_filename(stem, extension):
    date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
    return f"{AGENCY_PREFIX}_{stem}_{date_str}.{extension}"


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

def log(message, is_error=False, is_warning=False):
    if LOG_LEVEL == "errors" and not is_error and not is_warning:
        return
        
    ts = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    agency_prefix = f"[{AGENCY_LOG_LABEL}] " if AGENCY_LOG_LABEL else ""
    if is_error:
        print(f"[{ts}] 🔴 ERROR {agency_prefix}{message}".rstrip())
    elif is_warning:
        print(f"[{ts}] 🟡 WARNING {agency_prefix}{message}".rstrip())
    else:
        print(f"[{ts}] 🟢 {agency_prefix}{message}".rstrip())

#### WRITER & COMPACTION ####

def writer_worker():
    """Drains the queue, computes deduplication hashes, and appends to today's JSONL file."""
    log("Writer thread engaged. Streaming to JSONL.")
    handles = {}
    
    # Capture the current date once for the `first_logged` field.
    first_logged_date = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d")
    
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                current_date_key = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                close_stale_event_handles(handles, current_date_key)

                endpoint, evt_type, record_data = write_queue.get(timeout=1.0)
                
                # Deterministic hash excludes timestamp and log date so deduplication works across days.
                payload_str = f"{endpoint}|{evt_type}|{record_data.get('id', '')}|{json.dumps(record_data, sort_keys=True)}"
                hash_id = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
                
                # Format the record to schema with historical tracking.
                record = {
                    "hash_id": hash_id,
                    "first_logged": first_logged_date,  # This date is stable for the life of the record.
                    "ts": datetime.now(LOCAL_TZ).isoformat(),
                    "event": evt_type,
                    "id": str(record_data.get("id", "")),
                    "data": record_data
                }
                
                filename = dated_output_filename(endpoint, "jsonl")
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

def parquet_compaction_worker():
    """Streams orphaned/yesterday's JSONL files into Parquet and removes the source after success."""
    log("Parquet compaction watchdog and recovery sweep active.")
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            
            for file in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file.name:
                    log(f"Compacting orphaned/rotated {file.name} to Parquet archive...")
                    temp_path = None
                    writer = None
                    try:
                        parquet_filename = file.with_suffix(".parquet").name
                        parquet_path = ARCHIVE_DIR / parquet_filename

                        temp_path = parquet_path.with_name(f"{parquet_path.name}.tmp")
                        if temp_path.exists():
                            temp_path.unlink()

                        row_count = 0

                        for chunk in pd.read_json(file, lines=True, chunksize=100000):
                            if "data" in chunk.columns:
                                chunk["data"] = chunk["data"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))

                            table = pa.Table.from_pandas(chunk, preserve_index=False)

                            if writer is None:
                                writer = pq.ParquetWriter(
                                    temp_path,
                                    table.schema,
                                    compression="zstd",
                                    compression_level=ARCHIVE_ZSTD_LEVEL,
                                )

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
                        if temp_path is not None and temp_path.exists() and file.exists():
                            temp_path.unlink()
        except Exception as e:
            log(f"Compaction thread error: {e}", is_error=True)
            
        for _ in range(60):
            if stop_event.is_set(): break
            time.sleep(1)

#### NETWORK CONSUMERS ####

def fetch_api_ids(endpoint):
    """Helper to fetch active route IDs for batching."""
    if not BASE_URL: return []
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    try:
        res = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, timeout=15)
        if res.status_code == 200:
            return [x.get("id") for x in res.json().get("data", []) if x.get("id")]
    except Exception as e:
        log(f"Failed to fetch {endpoint} IDs: {e}", is_warning=True)
    return []

def sse_consumer(endpoint, params=None):
    """Maintains a persistent live connection to transit agency SSE endpoints."""
    if not BASE_URL: return
    url = f"{BASE_URL}/{endpoint}"
    headers = {"Accept": "text/event-stream"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    req_params = params or {}

    while not stop_event.is_set():
        log(f"Connecting to live stream: /{endpoint} {req_params}")
        try:
            response = requests.get(url, headers=headers, params=req_params, stream=True, timeout=(10, None))
            current_event = "message"
            data_buffer = []

            for line in response.iter_lines(decode_unicode=True):
                if stop_event.is_set(): break
                if not line:
                    if data_buffer:
                        payload = json.loads("\n".join(data_buffer))
                        records = payload if isinstance(payload, list) else [payload]
                        for r in records:
                            write_queue.put((endpoint, current_event, r))
                    current_event = "message"
                    data_buffer = []
                    continue

                if line.startswith("event:"):
                    current_event = line.split(":", 1)[1].strip()
                elif line.startswith("data:"):
                    data_buffer.append(line.split(":", 1)[1].strip())
                    
        except Exception as e:
            log(f"Stream /{endpoint} disconnected: {e}. Reconnecting in 5s...", is_warning=True)
            time.sleep(5)

def enhanced_poller(endpoint, url):
    """Polls the heavy bulk JSON files based on configured interval."""
    if not url: return
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    
    while not stop_event.is_set():
        try:
            res = requests.get(url, headers=headers, timeout=15)
            if res.status_code == 200:
                data = res.json()
                records = data.get("entity", []) if "entity" in data else data
                records = records if isinstance(records, list) else [records]
                for r in records:
                    write_queue.put((endpoint, "update", r))
        except Exception as e:
            log(f"Enhanced poll failed for {endpoint}: {e}", is_warning=True)
            
        sleep_ticks = int(ENHANCED_POLL_INTERVAL_SECONDS * 10)
        for _ in range(max(10, sleep_ticks)):
            if stop_event.is_set(): break
            time.sleep(0.1)

#### SCHEDULER TASKS ####

def fetch_snapshots():
    """Runs hourly. Pulls contextual snapshots for all endpoints."""
    if not BASE_URL or not ENABLE_SNAPSHOT_PULLS:
        return
    log("Pulling hourly snapshots...")
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY

    # Snapshot endpoints are pulled once each so the stream toggles remain independent.
    for endpoint in SNAPSHOT_EPS:
        if stop_event.is_set():
            break
        try:
            res = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, timeout=30)
            if res.status_code == 200:
                for r in res.json().get("data", []):
                    write_queue.put((endpoint, "snapshot", r))
        except Exception: pass

def fetch_gtfs_static():
    """Runs twice a day. Downloads GTFS zip, deduplicates rows, and updates first/last seen dates."""
    if not GTFS_URL or not ENABLE_GTFS_STATIC:
        return
    log("Pulling static GTFS assets...")
    try:
        res = requests.get(GTFS_URL, headers=GTFS_DOWNLOAD_HEADERS, timeout=120)
        res.raise_for_status()
        
        logged_at = datetime.now(LOCAL_TZ).isoformat(timespec="seconds")
        z = zipfile.ZipFile(io.BytesIO(res.content))
        
        for file_name in z.namelist():
            if file_name.endswith(".txt"):
                table_name = file_name.replace(".txt", "")
                
                df = pd.read_csv(z.open(file_name), dtype=str, low_memory=False)
                out_path = GTFS_DIR / f"{AGENCY_PREFIX}_gtfs_{table_name}.parquet"

                current_df = prepare_gtfs_snapshot(df, table_name, logged_at)
                existing_df = pd.read_parquet(out_path) if out_path.exists() else None
                merged_df = merge_gtfs_snapshots(existing_df, current_df, table_name, logged_at)

                if merged_df is None:
                    log(f"Skipping empty GTFS table: {table_name}", is_warning=True)
                    continue

                temp_path = out_path.with_name(f"{out_path.name}.tmp")
                if temp_path.exists():
                    temp_path.unlink()

                table = pa.Table.from_pandas(merged_df, preserve_index=False)
                pq.write_table(table, temp_path, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                temp_path.replace(out_path)
                
        log("Static GTFS update complete.")
    except Exception as e:
        log(f"GTFS pull failed: {e}", is_error=True)

def scheduler_thread():
    """Triggers hourly and daily automated tasks."""
    last_snapshot_hour = -1
    last_gtfs_pull = -1
    
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        
        if ENABLE_SNAPSHOT_PULLS and now.hour != last_snapshot_hour and now.minute == 0:
            threading.Thread(target=fetch_snapshots, daemon=True).start()
            last_snapshot_hour = now.hour
            
        if ENABLE_GTFS_STATIC and now.hour in [3, 15] and now.hour != last_gtfs_pull:
            threading.Thread(target=fetch_gtfs_static, daemon=True).start()
            last_gtfs_pull = now.hour
            
        time.sleep(10)

#### MAIN ORCHESTRATOR ####

if __name__ == "__main__":
    try:
        log("Starting transit ingestion engine...")
        
        threading.Thread(target=writer_worker, daemon=True).start()
        threading.Thread(target=parquet_compaction_worker, daemon=True).start()
        if ENABLE_SNAPSHOT_PULLS or ENABLE_GTFS_STATIC:
            threading.Thread(target=scheduler_thread, daemon=True).start()
        
        # Initial baseline pulls
        if BASE_URL and ENABLE_SNAPSHOT_PULLS:
            threading.Thread(target=fetch_snapshots, daemon=True).start()
        if GTFS_URL and ENABLE_GTFS_STATIC:
            threading.Thread(target=fetch_gtfs_static, daemon=True).start()
        
        # Deploy Base Streams
        if BASE_URL and ENABLE_BASE_STREAMS:
            for endpoint in BASE_STREAMS:
                threading.Thread(target=sse_consumer, args=(endpoint,), daemon=True).start()
                
            # Deploy Route-Batched Streams
            if ENABLE_ROUTE_STREAMS:
                routes = fetch_api_ids("routes")
                for endpoint in ROUTE_STREAMS:
                    for i in range(0, len(routes), DEFAULT_BATCH_SIZE):
                        batch = routes[i:i + DEFAULT_BATCH_SIZE]
                        threading.Thread(target=sse_consumer, args=(endpoint, {"filter[route]": ",".join(batch)}), daemon=True).start()
                        time.sleep(0.1)
                
        # Deploy Enhanced Pollers
        for endpoint, url in ENHANCED_BULK_STREAMS.items():
            threading.Thread(target=enhanced_poller, args=(endpoint, url), daemon=True).start()
            
        while not stop_event.is_set():
            time.sleep(1)
            
    except KeyboardInterrupt:
        log("Shutdown signal received. Closing pipelines cleanly...")
        stop_event.set()
        time.sleep(2)