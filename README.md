# GTFS Archiver

## Overview

Lightweight ingestion engine for real-time and static GTFS transit feeds.  
Captures live Server-Sent Events (SSE) with dynamic route batching, polls enhanced bulk JSON feeds, and executes scheduled contextual REST snapshots.

Built with the MBTA in mind, but designed to be adaptable to any transit agency publishing [GTFS](https://gtfs.org/) data.

Data is archived to JSONL (real-time write-ahead log) and Parquet (compressed analytics format) using queue-based asynchronous writers optimized for continuous ingestion.

Configuration lives directly in the compose files. [`docker-compose.yaml`](https://github.com/mxdrew/gtfs-data-archiver/blob/main/docker-compose.yaml) contains MBTA values as a default.

This project aims to adhere to the [MassDOT Developers License Agreement](https://cdn.mbta.com/sites/default/files/2023-08/mbta-massdot-develop-license-agreement.pdf). Any data collected is owned by the provider (MBTA & MassDOT) and is not claimed by this project.

## Table of Contents

- [Overview](#overview)
- [System Architecture](#system-architecture)
  - [High-Level Data Flow](#high-level-data-flow)
  - [Core Components](#core-components)
- [Data Flow Summary](#data-flow-summary)
- [Data Schema](#data-schema)
- [Directory Structure](#directory-structure)
- [Key Guarantees](#key-guarantees)
- [Failure Handling](#failure-handling)
- [Runtime Model](#runtime-model)
  - [Threads](#threads)
  - [Scheduled Tasks](#scheduled-tasks)
  - [Runtime Logs](#runtime-logs)
- [Configuration](#configuration)
  - [Required For Any Run](#required-for-any-run)
  - [Recommended For Any Run](#recommended-for-any-run)
  - [Required For Static GTFS Mode](#required-for-static-gtfs-mode)
  - [Required Only For API-Backed Modes](#required-only-for-api-backed-modes)
  - [Mode-Specific Variables](#mode-specific-variables)
  - [Multiple Agencies](#multiple-agencies)
- [Extra Scripts](#extra-scripts)
- [Installation](#installation)
  - [Docker (Recommended)](#docker-recommended)
  - [Manual](#manual)
- [Operational Notes](#operational-notes)
- [Security Notes](#security-notes)
- [Design Rationale](#design-rationale)
- [Data Archive](#data-archive)
- [Future Plans](#future-plans)
- [Other Cool Links](#other-cool-links)
- [Questions](#questions)

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
    └──► AGENCY_NAME_gtfs_<table>.parquet
```

### Core Components

- **SSE Consumers** → real-time event ingestion
- **Enhanced Pollers** → bulk snapshot ingestion
- **Writer Worker** → append-only write-ahead log (JSONL)
- **Compaction Worker** → JSONL → Parquet + deduplication
- **Scheduler** → snapshot + GTFS orchestration
- **Queue System** → backpressure-safe buffering

## Data Flow Summary

1. Data is ingested from SSE, REST snapshots, and enhanced feeds  
2. Events are pushed into a bounded in-memory queue  
3. Writer thread persists events into daily JSONL logs  
4. Background worker compacts older logs into Parquet archives  
5. Static GTFS datasets are periodically downloaded and converted  

## Data Schema

All schema definitions are intentionally centralized in [**DATA.md**](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md) to avoid duplication and drift.

Event archives use the `AGENCY_NAME_<source>_<MMDDYYYY>` prefix so logs from different agencies stay isolated and easy to scan. Static GTFS uses stable per-table files named `AGENCY_NAME_gtfs_<table>.parquet`, with `first_logged` and `last_logged` metadata tracking the exact timestamp when each unique row was first and most recently seen.

See:
- `data/events/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#1-dataevents-active-write-ahead-logs) (Active ingestion schema)
- `data/archive/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#2-dataarchive-compacted-event-logs) (Compacted analytical schema)
- `data/archive/gtfs/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#3-dataarchivegtfs-static-schedule-data) (Static GTFS schema)
- `data/archive/combinedEvents/` → [DATA.md](https://github.com/mxdrew/gtfs-data-archiver/blob/main/DATA.md#4-dataarchivecombinedevents-merged-historical-outputs) (Merged historical outputs)

## Directory Structure

```
    data/
  ├── events/    (Active daily JSONL files)
  └── archive/   (Compacted Parquet files + static GTFS + merged historical outputs)
    ├── gtfs/  (Static GTFS Parquet files)
    └── combinedEvents/  (Merged historical parquet outputs)
```

## Key Guarantees

- At-least-once delivery semantics  
- Crash-safe ingestion via write-ahead log (JSONL)  
- Deduplication via SHA-256 `hash_id`  
- Automatic recovery of orphaned logs  
- No ordering guarantees across streams or threads  

## Failure Handling

- SSE streams automatically reconnect with backoff  
- Partial writes are recovered on startup  
- Parquet archives are merged and deduplicated on re-runs  
- Queue buffers absorb temporary upstream outages  

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

## Configuration

Configuration is defined directly in the compose file. `docker-compose.yaml` is the sample setup. It uses MBTA defaults for the API endpoints, feed URLs, stream lists, and tuning values, with `API_KEY` left blank.

The application reads only the values in the table below. Missing values are handled safely, and stream-only, snapshot-only, or GTFS-only runs are all supported by the toggles.

| Variable | Purpose | Default in `docker-compose.yaml` |
|-||-|
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
|-|-|
| `SYNC_TIMEZONE` | [IANA timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones#List) string used for scheduling and timestamp alignment |
| `ARCHIVE_ZSTD_LEVEL` | Compression level for Parquet archives |
| `LOG_LEVEL` | Logging verbosity (e.g., `info`, `warnings`, `errors`) |

### Recommended For Any Run

| Variable | Description |
|-|-|
| `AGENCY_NAME` | Prefix used in generated file names; defaults to `agency` if omitted |

### Required For Static GTFS Mode

| Variable | Description |
|-|-|
| `GTFS_URL` | Static GTFS ZIP source |

### Required Only For API-Backed Modes

| Variable | Description |
|-|-|
| `API_KEY` | API authentication key |
| `BASE_URL` | Transit API base URL |

### Mode-Specific Variables

| Variable | Description |
|-|-|
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

### Multiple Agencies

For static GTFS only, it is fine to keep multiple agencies in one compose file as separate service blocks. For live streams, separate compose files are the recommended production setup.

Separate containers in the same Compose stack are still independent; splitting compose files is about operational isolation, not a hard technical requirement.

- Static-only: one compose file with multiple service blocks is usually fine and easy to manage.
- Live streams: separate compose files are recommended so restarts, logs, and resource limits stay isolated per agency.
- In either case, give each service or container a unique `container_name` and, if needed, a unique Compose project name with `-p`.
- Point each agency at its own upstream URLs and GTFS ZIP source.
- Keep the same `./data` mount if you want all output in one place, because the archive naming already prefixes files by agency.

Why this is the recommendation:

- Live streams are easier to restart, debug, and tune when each agency is isolated in its own compose file.
- Shared live stacks can entangle logs, restarts, and backpressure across agencies.
- Static-only downloads are much less sensitive, so multiple agencies in one file are usually fine there.

#### Example Compose Files

Example for separate compose files:

```bash
cp docker-compose.yaml docker-compose-lrta.yaml
docker compose -f docker-compose.yaml -p mbta up -d --build
docker compose -f docker-compose-lrta.yaml -p lrta up -d --build
```

In the copied compose file, update the values that make the agency unique: `container_name`, `AGENCY_NAME`, `BASE_URL`, `GTFS_URL`, `BASE_STREAMS`, `ROUTE_STREAMS`, `SNAPSHOT_EPS`, `VEHICLES_ENHANCED_URL`, `ALERTS_ENHANCED_URL`, `TRIPS_ENHANCED_URL`, and the `ENABLE_*` flags.

Example for static GTFS only in one compose file:

```yaml
services:
  mbta-archiver:
    build: .
    container_name: mbta-data-archiver
    restart: unless-stopped
    environment:
      - SYNC_TIMEZONE=America/New_York
      - AGENCY_NAME=MBTA
      - GTFS_URL=https://cdn.mbta.com/MBTA_GTFS.zip
      - ENABLE_BASE_STREAMS=false
      - ENABLE_ROUTE_STREAMS=false
      - ENABLE_SNAPSHOT_PULLS=false
      - ENABLE_ENHANCED_STREAMS=false
      - ENABLE_GTFS_STATIC=true
    volumes:
      - ./data:/app/data
  bat-archiver:
    build: .
    container_name: bat-data-archiver
    restart: unless-stopped
    environment:
      - SYNC_TIMEZONE=America/New_York
      - AGENCY_NAME=BAT
      - GTFS_URL=https://data.trilliumtransit.com/gtfs/brockton-ma-us/brockton-ma-us.zip
      - ENABLE_BASE_STREAMS=false
      - ENABLE_ROUTE_STREAMS=false
      - ENABLE_SNAPSHOT_PULLS=false
      - ENABLE_ENHANCED_STREAMS=false
      - ENABLE_GTFS_STATIC=true
    volumes:
      - ./data:/app/data
```
Which would be built and launched with:
```bash
docker compose -f docker-compose.yaml up -d --build
```

## Extra Scripts

The `otherScripts/` folder contains dedicated ingestion runners for agency-specific platforms and one multi-agency orchestrator. 

_**DISCLAIMER**: <u>These should be used at your own discretion and risk</u>. They are not maintained. They are also not tested to the same degree as the main `gtfs_archiver.py` script, so they may contain bugs or issues that could cause data loss or other problems. Especially as not all of the APIs or endpoints queried are necessarily supported by their providers (i.e., they are meant to feed their own realtime maps, not necessairly to be queried by the public). If you choose to use anything in `otherScripts/`, please review the code carefully, consider if you _<u>realllllyyyyy</u>_ want to run them, and also consider running them in a test environment first. These are really only included for informational purposes._

- `otherScripts/bustime.py`: Clever Devices BusTime ingestion for the configured agency feed.
- `otherScripts/massport.py`: Massport and Logan Express polling workflow.
- `otherScripts/passigo.py`: PassioGo ingestion for configured agency IDs.
- `otherScripts/routematch.py`: RouteMatch ingestion for configured agency/feed endpoints.
- `otherScripts/ma_gtfs_archiver.py`: multi-agency runner for static GTFS pulls (MBTA, 14 Massachusetts Regional Transit Agencies, Yankee Line, and Massport) with optional MBTA live ingestion.

Everything in `otherScripts` reads environment-based configuration and writes outputs under the configured data directory.

### Root `.env` Variables `otherScripts`
The root `.env` includes variables for scripts tracked in git (non-ignored scripts):

- `gtfs_archiver.py`:
`API_KEY`, `SYNC_TIMEZONE`, `AGENCY_NAME`, `BASE_URL`, `GTFS_URL`, `ENABLE_BASE_STREAMS`, `ENABLE_ROUTE_STREAMS`, `ENABLE_SNAPSHOT_PULLS`, `ENABLE_ENHANCED_STREAMS`, `ENABLE_GTFS_STATIC`, `BASE_STREAMS`, `ROUTE_STREAMS`, `SNAPSHOT_EPS`, `ENHANCED_POLL_INTERVAL_SECONDS`, `VEHICLES_ENHANCED_URL`, `ALERTS_ENHANCED_URL`, `TRIPS_ENHANCED_URL`, `DEFAULT_BATCH_SIZE`, `LOG_LEVEL`, `ARCHIVE_ZSTD_LEVEL`

- `otherScripts/ma_gtfs_archiver.py`:
`MBTA_API_KEY`, `GTFS_DOWNLOAD_USER_AGENT`, plus shared runtime variables `SYNC_TIMEZONE`, `ARCHIVE_ZSTD_LEVEL`, `LOG_LEVEL`

- `otherScripts/bustime.py`:
`BUSTIME_AGENCY`, `BUSTIME_BASE_URL`, `BUSTIME_API_KEY`, `GTFS_FEED_URL`, `VERBOSE_LOGGING`, plus shared runtime variables `SYNC_TIMEZONE`, `ARCHIVE_ZSTD_LEVEL`, `DATA_DIR`, `DATA_DIR_WSL`

- `otherScripts/routematch.py`:
`ROUTEMATCH_AGENCY`, `ROUTEMATCH_BASE_URL`, `ROUTEMATCH_REFERER`, plus shared runtime variables `SYNC_TIMEZONE`, `ARCHIVE_ZSTD_LEVEL`, `DATA_DIR`, `DATA_DIR_WSL`

- `otherScripts/passigo.py`:
`PASSIOGO_AGENCY_CATALOG`, `PASSIOGO_TARGET_AGENCY_IDS`, `PASSIOGO_SAVE_ACTIVE_JSON`, plus shared runtime variables `SYNC_TIMEZONE`, `ARCHIVE_ZSTD_LEVEL`, `DATA_DIR`, `DATA_DIR_WSL`

- `otherScripts/massport.py`:
uses fixed in-script agency and polling values; only shared runtime variables apply (`SYNC_TIMEZONE`, `ARCHIVE_ZSTD_LEVEL`, `DATA_DIR`, `DATA_DIR_WSL`).

## Installation

### Docker (Recommended)

```bash
git clone https://github.com/mxdrew/gtfs-data-archiver.git
cd gtfs-data-archiver
docker compose up -d --build
```

If you add another agency later, use the copy pattern shown above in the Multiple Agencies section, then update the agency-specific values there.

### Manual

```bash
git clone https://github.com/mxdrew/gtfs-data-archiver.git
cd gtfs-data-archiver
pip install -r requirements.txt
python gtfs_archiver.py
```

For manual runs, use the included `.env` sample as the starting point: [`.env`](https://github.com/mxdrew/gtfs-data-archiver/blob/main/.env). Update the values that make the agency unique, especially `AGENCY_NAME`, `SYNC_TIMEZONE`, `API_KEY`, `BASE_URL`, `GTFS_URL`, `BASE_STREAMS`, `ROUTE_STREAMS`, `SNAPSHOT_EPS`, the `VEHICLES_ENHANCED_URL`/`ALERTS_ENHANCED_URL`/`TRIPS_ENHANCED_URL` entries, and the `ENABLE_*` flags. I did not try multiple agencies with this, but I'd imagine in order to do so you'd need to clone `gtfs_archiver.py` and `.env`, rename them, update the new `.env` file, and then launch the cloned script.

## Operational Notes

This system prioritizes durability and recoverability over strict consistency or ordering guarantees. It is designed for continuous ingestion rather than transactional processing.

- Designed for continuous, long-running ingestion workloads
- Uses append-only disk writes for durability
- Queue-based buffering prevents ingestion blocking
- Optimized for high-frequency transit data streams
- JSONL provides durable write-ahead logging and recovery
- Parquet enables efficient analytics and compression
- Separation of WAL and archive layers improves reliability and performance

## Data Archive

The data archiver began continuous operation on May 26, 2026 - focused on MBTA data. Any archived datasets available from this system begin as of that date. The archived data generated by this system is not stored in this repository.

More info, such as schema definitions, file formats, period of time captured, and downtime, will be available [here](https://github.com/mxdrew/gtfs-data-logger/blob/main/DATA.md).

## Future Plans

- Run analytics on the collected data.
- Build an MBTA system map or line map that shows train positions on a given line in real time.
- Explore the MBTA's [MBTA Historical Performance Data](https://www.mbta.com/developers/historical-performance-data).
- Explore other agencies through the [Mobility Database](https://mobilitydatabase.org/) or [Transitland](https://www.transit.land/).
- Explore data from [opentransportdata.swiss](https://opentransportdata.swiss/).

## Other Cool Links

Other useful places to look are:

- [Official GTFS Website](https://gtfs.org/): Official specification and documentation for the General Transit Feed Specification (GTFS), including static schedules and realtime extensions.

- [The Mobility Database](https://mobilitydatabase.org/): An open data catalog with over 6000 GTFS, GTFS Realtime, and GBFS feeds in over 99 countries.

- [Transitland](https://www.transit.land/): A transit data platform for finding and working with agency feeds.

- [MBTA API developer docs](https://www.mbta.com/developers/v3-api): MBTA's public API overview and entry point for the v3 endpoints.

- [MBTA API Swagger docs](https://api-v3.mbta.com/docs/swagger/index.html): Interactive reference for the MBTA v3 API endpoints and response shapes.

- [MBTA GitHub](https://github.com/mbta): Source repositories and related MBTA projects.

- [MBTA Historical Performance Data](https://www.mbta.com/developers/historical-performance-data): Historical performance data and related developer resources.

- [Public transportation in Massachusetts](https://www.mass.gov/info-details/public-transportation-in-massachusetts#regional-transit-authorities): Overview of the MBTA and the 15 Regional Transit Authorities, including a map of service areas, member towns/cities/areas, and contact information.

- [opentransportdata.swiss](https://opentransportdata.swiss/): The open data platform for customer information on Swiss public transport, operated on behalf of the Swiss Federal Office of Transport (FOT). Contains everything from Static and Realtime GTFS data to road traffic counters, traffic light data, and train composititon data.

## Questions

If you have interest in accessing the archived data, or if you have any questions, comments, or concerns, email [github@mxdrew.com](mailto:github@mxdrew.com).