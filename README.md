# GTFS Archiver

## Overview

Lightweight ingestion engine for real-time and static GTFS transit feeds.  
Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk JSON feeds, and executes scheduled contextual REST snapshots.

Built with the MBTA in mind, but designed to be adaptable to any transit agency publishing [GTFS](https://gtfs.org/) data.

Data is archived to JSONL (real-time write-ahead log) and Parquet (compressed analytics format) using queue-based asynchronous writers optimized for continuous ingestion.

Configuration lives directly in the compose files. `docker-compose.yaml` contains MBTA values as a default.

This project aims to adhere to the [MassDOT Developers License Agreement](https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf). Any data collected is owned by the provider (MBTA & MassDOT) and is not claimed by this project.

---

## System Architecture

### High-Level Data Flow

```
    SSE / REST / Enhanced APIs
    │
    ▼
    write_queue
    │
    ▼
    writer_worker
    │
    ├──► AGENCY_NAME_<endpoint>_<MMDDYYYY>.jsonl (Write-Ahead Log)
    │
    ▼
    parquet_compaction_worker
    │
    ├──► AGENCY_NAME_<endpoint>_<MMDDYYYY>.parquet
    └──► AGENCY_NAME_gtfs_<table>_<MMDDYYYY>.parquet
```

### Core Components

- **SSE Consumers** → real-time event ingestion
- **Enhanced Pollers** → bulk snapshot ingestion
- **Writer Worker** → append-only write-ahead log (JSONL)
- **Compaction Worker** → JSONL → Parquet + deduplication
- **Scheduler** → snapshot + GTFS orchestration
- **Queue System** → backpressure-safe buffering

---

## Data Flow Summary

1. Data is ingested from SSE, REST snapshots, and enhanced feeds  
2. Events are pushed into a bounded in-memory queue  
3. Writer thread persists events into daily JSONL logs  
4. Background worker compacts older logs into Parquet archives  
5. Static GTFS datasets are periodically downloaded and converted  

---

## Data Schema

All schema definitions are intentionally centralized in [**DATA.md**](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md) to avoid duplication and drift.

All generated files use the same `AGENCY_NAME_<source>_<MMDDYYYY>` prefix so archives from different agencies stay isolated and easy to scan.

See:
- `data/events/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#1-dataevents-active-write-ahead-logs) (Active ingestion schema)
- `data/archive/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#2-dataarchive-compacted-event-logs) (Compacted analytical schema)
- `data/archive/gtfs/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#3-dataarchivegtfs-static-schedule-data) (Static GTFS schema)

---

## Directory Structure

```
    data/
    ├── events/   (Active daily JSONL files)
    └── archive/  (Compacted Parquet files + Static GTFS Parquet files)
        └── gtfs/ (Static GTFS Parquet files)
```

---

## Key Guarantees

- At-least-once delivery semantics  
- Crash-safe ingestion via write-ahead log (JSONL)  
- Deduplication via SHA-256 `hash_id`  
- Automatic recovery of orphaned logs  
- No ordering guarantees across streams or threads  

---

## Failure Handling

- SSE streams automatically reconnect with backoff  
- Partial writes are recovered on startup  
- Parquet archives are merged and deduplicated on re-runs  
- Queue buffers absorb temporary upstream outages  

---

## Runtime Model

### Threads

- Writer thread (JSONL WAL)
- Compaction worker (Parquet archiver)
- Scheduler thread (snapshot + GTFS jobs)
- N SSE consumer threads
- N enhanced poller threads

### Scheduled Tasks

- Hourly snapshot ingestion
- GTFS refresh at 03:00 and 15:00 (local timezone)

### Runtime Logs

- Container stdout and stderr are the live operational log for this project.
- It includes startup messages, stream connect/reconnect notices, warnings, and errors.
- View it with `docker logs -f <container_name>` or Docker Desktop's Logs tab.
- Keep `LOG_LEVEL` at `warning` or `errors` for normal runs. At `info`, output can grow very quickly during high-throughput stream activity and thus will be hard to keep track of.

---

## Configuration

Configuration is defined directly in the compose file. `docker-compose.yaml` is the sample setup. It uses MBTA defaults for the API endpoints, feed URLs, stream lists, and tuning values, with `API_KEY` left blank.

The application reads only the values in the table below. Missing values are handled safely, and stream-only, snapshot-only, or GTFS-only runs are all supported by the toggles.

| Variable | Purpose | Default in `docker-compose.yaml` |
|----------|---------|----------------------------------|
| `API_KEY` | API authentication key | YOUR_API_KEY |
| `SYNC_TIMEZONE` | [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) used for scheduling and filenames | `America/New_York` |
| `AGENCY_NAME` | Filename prefix for generated outputs | `MBTA` |
| `BASE_URL` | Transit Agency's API base URL | `https://api-v3.mbta.com` |
| `GTFS_URL` | Static GTFS ZIP source static schedule imports | `https://cdn.mbta.com/MBTA_GTFS.zip` |
| `ENABLE_BASE_STREAMS` | Enables live base SSE consumers | `true` |
| `ENABLE_ROUTE_STREAMS` | Enables route-batched SSE consumers | `true` |
| `ENABLE_SNAPSHOT_PULLS` | Enables scheduled snapshot pulls | `true` |
| `ENABLE_ENHANCED_STREAMS` | Enables enhanced bulk pollers | `true` |
| `ENABLE_GTFS_STATIC` | Enables twice-daily static GTFS downloads | `true` |
| `BASE_STREAMS` | Base SSE endpoints | `vehicles,alerts` |
| `ROUTE_STREAMS` | Route-batched SSE endpoints | `predictions,stop_events` |
| `SNAPSHOT_EPS` | Snapshot endpoints | `schedules,services,shapes,trips,lines,routes,route_patterns,facilities,stops,live_facilities` |
| `ENHANCED_POLL_INTERVAL_SECONDS` | Delay between enhanced feed polls | `7` |
| `VEHICLES_ENHANCED_URL` | Enhanced vehicle feed URL - Potentially MBTA Specific | `https://cdn.mbta.com/realtime/VehiclePositions_enhanced.json` |
| `ALERTS_ENHANCED_URL` | Enhanced alerts feed URL - Potentially MBTA Specific | `https://cdn.mbta.com/realtime/Alerts_enhanced.json` |
| `TRIPS_ENHANCED_URL` | Enhanced trip updates feed URL - Potentially MBTA Specific| `https://cdn.mbta.com/realtime/TripUpdates_enhanced.json` |
| `DEFAULT_BATCH_SIZE` | Batch size for route grouping | `25` |
| `LOG_LEVEL` | Logging verbosity | `warning` |
| `ARCHIVE_ZSTD_LEVEL` | Parquet compression level used for archive files | `10` |

### Required For Any Run

| Variable | Description |
|----------|-------------|
| `SYNC_TIMEZONE` | [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) string used for scheduling and timestamp alignment |
| `ARCHIVE_ZSTD_LEVEL` | Compression level for Parquet archives |
| `LOG_LEVEL` | Logging verbosity (e.g., `info`, `warnings`, `errors`) |

### Recommended For Any Run

| Variable | Description |
|----------|-------------|
| `AGENCY_NAME` | Prefix used in generated file names; defaults to `agency` if omitted |

### Required For Static GTFS Mode

| Variable | Description |
|----------|-------------|
| `GTFS_URL` | Static GTFS ZIP source |

### Required Only For API-Backed Modes

| Variable | Description |
|----------|-------------|
| `API_KEY` | API authentication key |
| `BASE_URL` | Transit API base URL |

### Mode-Specific Variables

| Variable | Description |
|----------|-------------|
| `BASE_STREAMS` | SSE endpoints |
| `ROUTE_STREAMS` | Route-batched SSE endpoints |
| `SNAPSHOT_EPS` | Snapshot endpoints |
| `ENABLE_BASE_STREAMS` | Enables live base SSE consumers |
| `ENABLE_ROUTE_STREAMS` | Enables route-batched SSE consumers |
| `ENABLE_SNAPSHOT_PULLS` | Enables scheduled snapshot pulls |
| `ENABLE_ENHANCED_STREAMS` | Enables enhanced bulk pollers |
| `ENABLE_GTFS_STATIC` | Enables twice-daily static GTFS downloads |
| `DEFAULT_BATCH_SIZE` | Batch size for route grouping |
| `ENHANCED_POLL_INTERVAL_SECONDS` | Poll interval for enhanced bulk feeds |

Static-only runs can disable every API-backed mode and leave only GTFS enabled:

```yaml
ENABLE_BASE_STREAMS=false
ENABLE_ROUTE_STREAMS=false
ENABLE_SNAPSHOT_PULLS=false
ENABLE_ENHANCED_STREAMS=false
ENABLE_GTFS_STATIC=true
```

---

## Installation

### Docker (recommended)

```bash
git clone https://github.com/mxdrew/gtfs-data-archiver.git
cd gtfs-data-archiver
docker compose up -d --build
```

### Manual

```bash
git clone https://github.com/mxdrew/gtfs-data-archiver.git
cd gtfs-data-archiver
pip install -r requirements.txt
python gtfs_archiver.py
```

---


## Operational Notes

This system prioritizes durability and recoverability over strict consistency or ordering guarantees. It is designed for continuous ingestion rather than transactional processing.

- Designed for continuous, long-running ingestion workloads
- Uses append-only disk writes for durability
- Queue-based buffering prevents ingestion blocking
- Optimized for high-frequency transit data streams

## Security Notes

- API keys are only read from environment variables
- No secrets are persisted to disk
- All upstream requests use header-based authentication

## Design Rationale

- JSONL provides durable write-ahead logging and recovery
- Parquet enables efficient analytics and compression
- Separation of WAL and archive layers improves reliability and performance

## Data Archive

The data archiver began continuous operation on May 26, 2026 - focused on MBTA data. Any archived datasets available from this system begin as of that date. The archived data generated by this system is not stored in this repository.

More info, such as schema definitions, file formats, period of time captured, and downtime, will be available [here](https://github.com/mxdrew/gtfs-data-logger/blob/main/DATA.md).

---

## Future Plans

- Run analytics on the collected data.
- Build an MBTA system map or line map that shows train positions on a given line in real time.
- Explore MBTA's historical performance data.
- Explore other agencies through the [Mobility Database](https://mobilitydatabase.org/) or [Transitland](https://www.transit.land/).
- Allow for logging multiple agencies at once.

---

## Other Cool Links

Other useful places to look are:

- [Official GTFS Website](https://gtfs.org/): Official specification and documentation for the General Transit Feed Specification (GTFS), including static schedules and realtime extensions.

- [The Mobility Database](https://mobilitydatabase.org/): An open data catalog with over 6000 GTFS, GTFS Realtime, and GBFS feeds in over 99 countries.

- [Transitland](https://www.transit.land/): A transit data platform for finding and working with agency feeds.

- [MBTA API developer docs](https://www.mbta.com/developers/v3-api): MBTA's public API overview and entry point for the v3 endpoints.

- [MBTA API Swagger docs](https://api-v3.mbta.com/docs/swagger/index.html): Interactive reference for the MBTA v3 API endpoints and response shapes.

- [MBTA GitHub](https://github.com/mbta): Source repositories and related MBTA projects.

- [MBTA Historical Performance Data](https://www.mbta.com/developers/historical-performance-data): Historical performance data and related developer resources.

---

## Questions

If you have interest in accessing the archived data, or if you have any questions, comments, or concerns, email [github@mxdrew.com](mailto:github@mxdrew.com).