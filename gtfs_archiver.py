#!/usr/bin/env python3
# Author Information:
# Drew Mulcare
# github@mxdrew.com
# May 25, 2026 (Last updated May 25, 2026)
#
# Description:
# Lightweight, single-file data ingestion engine for real-time and static transit feeds. 
# Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk 
# JSON endpoints, and executes hourly contextual REST snapshots across the system.
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
#
# Startup routines include an orphaned JSONL recovery sweep and automated scheduling for 
# twice-daily GTFS static package downloads (03:00 and 15:00 local time) routed to data/gtfs/.
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

LOCAL_TZ = ZoneInfo(os.environ.get("SYNC_TIMEZONE").strip())
API_KEY = os.environ.get("API_KEY").strip()
BASE_URL = os.environ.get("BASE_URL").strip()
GTFS_URL = os.environ.get("GTFS_URL").strip()

DATA_DIR = Path("data")
EVENTS_DIR = DATA_DIR / "events"
ARCHIVE_DIR = DATA_DIR / "archive"
GTFS_DIR = ARCHIVE_DIR / "gtfs"

for d in [EVENTS_DIR, ARCHIVE_DIR, GTFS_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# 1. Base Streams
_base_streams_env = os.environ.get("BASE_STREAMS")
BASE_STREAMS = [x.strip() for x in _base_streams_env.split(",") if x.strip()]

# 2. Batched Streams
_route_streams_env = os.environ.get("ROUTE_STREAMS")
ROUTE_STREAMS = [x.strip() for x in _route_streams_env.split(",") if x.strip()]
ENABLE_ROUTE_STREAMS = os.environ.get("ENABLE_ROUTE_STREAMS", "true").strip().lower() == "true"

# 3. Snapshot Endpoints
_snapshot_eps_env = os.environ.get("SNAPSHOT_EPS")
SNAPSHOT_EPS = [x.strip() for x in _snapshot_eps_env.split(",") if x.strip()]

# 4. Enhanced Bulk Feeds
ENABLE_ENHANCED_STREAMS = os.environ.get("ENABLE_ENHANCED_STREAMS", "true").strip().lower() == "true"
ENHANCED_BULK_STREAMS = {}
if ENABLE_ENHANCED_STREAMS:
    if os.environ.get("VEHICLES_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["vehicles_enhanced"] = os.environ["VEHICLES_ENHANCED_URL"].strip()
    if os.environ.get("ALERTS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["alerts_enhanced"] = os.environ["ALERTS_ENHANCED_URL"].strip()
    if os.environ.get("TRIPS_ENHANCED_URL"):
        ENHANCED_BULK_STREAMS["trips_enhanced"] = os.environ["TRIPS_ENHANCED_URL"].strip()

# Tuning Parameters
DEFAULT_BATCH_SIZE = int(os.environ.get("DEFAULT_BATCH_SIZE"))
ENHANCED_POLL_INTERVAL_SECONDS = float(os.environ.get("ENHANCED_POLL_INTERVAL_SECONDS"))
ARCHIVE_ZSTD_LEVEL = int(os.environ.get("ARCHIVE_ZSTD_LEVEL"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "info").strip().lower()

write_queue = queue.Queue(maxsize=500000)
stop_event = threading.Event()

#### LOGGING ####

def log(message, is_error=False, is_warning=False):
    if LOG_LEVEL == "errors" and not is_error and not is_warning:
        return # Suppress standard info logs
        
    ts = datetime.now(LOCAL_TZ).strftime("%H:%M:%S")
    if is_error:
        print(f"[{ts}] 🔴 ERROR: {message}")
    elif is_warning:
        print(f"[{ts}] 🟡 WARNING: {message}")
    else:
        print(f"[{ts}] 🟢 {message}")

#### WRITER & COMPACTION ####

def writer_worker():
    """Drains the queue, computes deduplication hashes, and appends to today's JSONL file."""
    log("Writer thread engaged. Streaming to JSONL.")
    handles = {}
    
    try:
        while not stop_event.is_set() or not write_queue.empty():
            try:
                endpoint, evt_type, record_data = write_queue.get(timeout=1.0)
                
                # Deterministic deduplication hash
                payload_str = f"{endpoint}|{evt_type}|{record_data.get('id', '')}|{json.dumps(record_data, sort_keys=True)}"
                hash_id = hashlib.sha256(payload_str.encode("utf-8")).hexdigest()
                
                # Format to exact schema specification
                record = {
                    "hash_id": hash_id,
                    "ts": datetime.now(LOCAL_TZ).isoformat(),
                    "event": evt_type,
                    "id": str(record_data.get("id", "")),
                    "data": record_data
                }
                
                date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
                filename = f"{endpoint}_{date_str}.jsonl"
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
    """Compresses orphaned/yesterday's JSONL files, merges if Parquet exists, and archives."""
    log("Parquet compaction watchdog and recovery sweep active.")
    while not stop_event.is_set():
        try:
            today_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
            
            for file in EVENTS_DIR.glob("*.jsonl"):
                if today_str not in file.name:
                    log(f"Compacting orphaned/rotated {file.name} to Parquet archive...")
                    try:
                        df = pd.read_json(file, lines=True)
                        
                        if "data" in df.columns:
                            df["data"] = df["data"].apply(lambda x: json.dumps(x) if isinstance(x, dict) else str(x))
                            
                        parquet_filename = file.with_suffix(".parquet").name
                        parquet_path = ARCHIVE_DIR / parquet_filename
                        
                        # Merge and deduplicate if the archive file already exists
                        if parquet_path.exists():
                            existing_df = pd.read_parquet(parquet_path)
                            combined_df = pd.concat([existing_df, df], ignore_index=True)
                            
                            if "hash_id" in combined_df.columns:
                                combined_df = combined_df.drop_duplicates(subset=["hash_id"], keep="last")
                            
                            table = pa.Table.from_pandas(combined_df)
                        else:
                            table = pa.Table.from_pandas(df)
                        
                        pq.write_table(table, parquet_path, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                        
                        file.unlink()
                        log(f"Success: {file.name} compacted and merged into {parquet_path.name}.")
                    except Exception as e:
                        log(f"Failed to compact {file.name}: {e}", is_error=True)
        except Exception as e:
            log(f"Compaction thread error: {e}", is_error=True)
            
        for _ in range(3600):
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
    if not BASE_URL: return
    log("Pulling hourly snapshots...")
    headers = {"Accept": "application/json"}
    if API_KEY: headers["X-API-Key"] = API_KEY
    
    routes = fetch_api_ids("routes")
    
    # 1. Base Streams (No batching)
    for endpoint in BASE_STREAMS:
        try:
            res = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, timeout=30)
            if res.status_code == 200:
                for r in res.json().get("data", []):
                    write_queue.put((endpoint, "snapshot", r))
        except Exception: pass

    # 2. Batched Snapshot Endpoints
    for endpoint in SNAPSHOT_EPS:
        if stop_event.is_set(): break
        for i in range(0, len(routes), DEFAULT_BATCH_SIZE):
            batch = routes[i:i + DEFAULT_BATCH_SIZE]
            try:
                res = requests.get(f"{BASE_URL}/{endpoint}", headers=headers, params={"filter[route]": ",".join(batch)}, timeout=30)
                if res.status_code == 200:
                    for r in res.json().get("data", []):
                        write_queue.put((endpoint, "snapshot", r))
            except Exception: pass
            time.sleep(0.1)

def fetch_gtfs_static():
    """Runs twice a day. Downloads GTFS zip, converts to Parquet."""
    if not GTFS_URL: return
    log("Pulling static GTFS assets...")
    try:
        res = requests.get(GTFS_URL, timeout=120)
        res.raise_for_status()
        
        date_str = datetime.now(LOCAL_TZ).strftime("%m%d%Y")
        z = zipfile.ZipFile(io.BytesIO(res.content))
        
        for file_name in z.namelist():
            if file_name.endswith(".txt"):
                table_name = file_name.replace(".txt", "")
                
                df = pd.read_csv(z.open(file_name), dtype=str, low_memory=False)
                
                out_path = GTFS_DIR / f"gtfs_{table_name}_{date_str}.parquet"
                table = pa.Table.from_pandas(df)
                pq.write_table(table, out_path, compression="zstd", compression_level=ARCHIVE_ZSTD_LEVEL)
                
        log("Static GTFS update complete.")
    except Exception as e:
        log(f"GTFS pull failed: {e}", is_error=True)

def scheduler_thread():
    """Triggers hourly and daily automated tasks."""
    last_snapshot_hour = -1
    last_gtfs_pull = -1
    
    while not stop_event.is_set():
        now = datetime.now(LOCAL_TZ)
        
        if now.hour != last_snapshot_hour and now.minute == 0:
            threading.Thread(target=fetch_snapshots, daemon=True).start()
            last_snapshot_hour = now.hour
            
        if now.hour in [3, 15] and now.hour != last_gtfs_pull:
            threading.Thread(target=fetch_gtfs_static, daemon=True).start()
            last_gtfs_pull = now.hour
            
        time.sleep(10)

#### MAIN ORCHESTRATOR ####

if __name__ == "__main__":
    try:
        log("Starting transit ingestion engine...")
        
        threading.Thread(target=writer_worker, daemon=True).start()
        threading.Thread(target=parquet_compaction_worker, daemon=True).start()
        threading.Thread(target=scheduler_thread, daemon=True).start()
        
        # Initial baseline pulls
        if BASE_URL: threading.Thread(target=fetch_snapshots, daemon=True).start()
        if GTFS_URL: threading.Thread(target=fetch_gtfs_static, daemon=True).start()
        
        # Deploy Base Streams
        if BASE_URL:
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