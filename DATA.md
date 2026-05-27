# Archived Data Overview

## Archive Dates

| Date | Event | Agency | Base Streams | Route Streams | Snapshot Streams | Enhanced Streams | Static GTFS | Notes |
| :--: | :---: | :----: | :----------: | :-----------: | :--------------: | :--------------: | :---------: | :---: |
| May 28, 2026 | Archiving Started | [MBTA](https://www.mbta.com/) | `vehicles`, `alerts` | `predictions`, `stop_events` | `schedules`, `services`, `shapes`, `trips`, `lines`, `routes`, `route_patterns`, `facilities`, `stops`, `live_facilities` | `vehicles_enhanced`, `alerts_enhanced`, `trips_enhanced` | [MBTA_GTFS.zip](https://cdn.mbta.com/MBTA_GTFS.zip) | There may be a few records from May 25-27, 2026 due to building and testing of the script.|
| May 28, 2026 | Archiving Started | [BAT](https://www.ridebat.com/) | — | — | — | — | [brockton-ma-us.zip](https://data.trilliumtransit.com/gtfs/brockton-ma-us/brockton-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [BRTA](https://berkshirerta.com/) | — | — | — | — | [berkshire-ma-us.zip](https://data.trilliumtransit.com/gtfs/berkshire-ma-us/berkshire-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [CATA](https://www.canntran.com/) | — | — | — | — | [capeann-ma-us.zip](https://data.trilliumtransit.com/gtfs/capeann-ma-us/capeann-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [CCRTA](https://www.capecodrta.org/) | — | — | — | — | [capecod-ma-us.zip](https://data.trilliumtransit.com/gtfs/capecod-ma-us/capecod-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [FRTA](https://www.frta.org/) | — | — | — | — | [frta-ma-us.zip](https://data.trilliumtransit.com/gtfs/frta-ma-us/frta-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [LRTA](https://www.lrta.com/) | — | — | — | — | [lowell-ma-us.zip](https://data.trilliumtransit.com/gtfs/lowell-ma-us/lowell-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [MART](https://www.mrta.us/) | — | — | — | — | [montachusett-ma-us.zip](https://data.trilliumtransit.com/gtfs/montachusett-ma-us/montachusett-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [Massport](https://www.massport.com/) | — | — | — | — | [massport-ma-us.zip](https://data.trilliumtransit.com/gtfs/massport-ma-us/massport-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [MeVa](https://www.mvrta.com/) | — | — | — | — | [merrimackvalley-ma-us.zip](https://data.trilliumtransit.com/gtfs/merrimackvalley-ma-us/merrimackvalley-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [MWRTA](https://www.mwrta.com/) | — | — | — | — | [google_transit.zip](http://vc.mwrta.com/gtfs/google_transit.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [NRTA](https://www.nrtawave.com/) | — | — | — | — | [GTFSDownload.aspx](https://nrtawave.transloc.com/Secure/Admin/Reports/GTFSDownload.aspx) | Static GTFS only |
| May 28, 2026 | Archiving Started | [PVTA](https://www.pvta.com/) | — | — | — | — | [google_transit.zip](http://www.pvta.com/g_trans/google_transit.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [SRTA](https://www.srtabus.com/) | TBD | TBD | TBD | TBD | TBD | TBD |
| May 28, 2026 | Archiving Started | [VTA](https://www.vineyardtransit.com/) | — | — | — | — | [marthasvineyard-ma-us.zip](https://data.trilliumtransit.com/gtfs/marthasvineyard-ma-us/marthasvineyard-ma-us.zip) | Static GTFS only |
| May 28, 2026 | Archiving Started | [WRTA](https://therta.com/) | — | — | — | — | [wrta-ma-us.zip](https://data.trilliumtransit.com/gtfs/wrta-ma-us/wrta-ma-us.zip) | Static GTFS only - for now, intresting in seeing how their [Bus Tracker](https://swiv.wrta.cadavl.com/SWIV/WRTA) gets its data. |
| May 28, 2026 | Archiving Started | [Yankee Line](https://yankeeline.us/) | — | — | — | — | [yankeeline-ma-us.zip](https://data.trilliumtransit.com/gtfs/yankeeline-ma-us/yankeeline-ma-us.zip) | Static GTFS only |

_**Note:** [GATRA](https://www.gatra.org/) is excluded from this dataset because, as of May 28, 2026, their [Static GTFS Feed](https://data.trilliumtransit.com/gtfs/gatra-ma-us/gatra-ma-us.zip) seems to have been last updated in March 2025. However, they use the same platform as [WRTA](https://therta.com/) for their [Bus Tracker](https://swiv.gatra.cadavl.com/SWIV/GATRA), so that might be a future area of exploration.__

## Docker Compose Files

A Docker Compose file for all Static GTFS Feeds for the MBTA, 13 of the 14 Massachusetts Regional transit Agencies agencies (excluding [GATRA](https://www.gatra.org/), for reasons above), Yankee Line, and Massport is available in this repo as [`compose-ma-rtas.yaml`](https://github.com/mxdrew/gtfs-data-archiver/blob/main/compose-ma-rtas.yaml). 

---

## Directory Structure
```
data/
├── events/   (Active daily JSONL files)
└── archive/  (Compacted Parquet files + Static GTFS Parquet files)
    └── gtfs/ (Static GTFS Parquet files)
```

## 1. `data/events/` (Active Write-Ahead Logs)

**Purpose:** Active *.jsonl files that stream in real-time.

**Schema:**
| Column | Type | Notes | Example |
|:------:|:----:|:------:|:--------:|
| `hash_id` | `str` | SHA-256 fingerprint. | `c5fdc9...` |
| `ts` | `TIMESTAMP` | [ISO 8601 timestamp](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations), timezone-aware via `SYNC_TIMEZONE`. | `2026-05-25T...` |
| `event` | `str` | Type of event: add, update, remove, reset, snapshot, error. | `update` |
| `id` | `str` | API entity ID. | `prediction-123` |
| `data` | `str` | Raw JSON payload. | `{"attributes": {...}}` |

**Event Semantics:**
- `add`: New entity observed in stream  
- `update`: Incremental change or periodic re-emission of state (including enhanced feeds)  
- `remove`: Entity removed from upstream system  
- `snapshot`: Full-state response from scheduled or manual pull (not a delta)  
- `reset`: Upstream feed reinitialization or state reset event  
- `error`: Malformed or failed ingestion payload captured for traceability  

**Ingestion Guarantee:**
- At-least-once delivery model  
- Deduplication enforced via `hash_id`  
- No ordering guarantees across streams, endpoints, or threads  

**Source Types:**
- SSE streams: Real-time event ingestion  
- Snapshot pulls: Periodic full-state API responses  
- Enhanced feeds: Polled bulk datasets (may repeat full-state snapshots)  
- GTFS static: Scheduled dataset exports (non-event-based)  

**Generated Files:**
- `AGENCY_NAME_alerts_MMDDYYYY.parquet`
- `AGENCY_NAME_alerts_enhanced_MMDDYYYY.parquet`
- `AGENCY_NAME_facilities_MMDDYYYY.parquet`
- `AGENCY_NAME_lines_MMDDYYYY.parquet`
- `AGENCY_NAME_live_facilities_MMDDYYYY.parquet`
- `AGENCY_NAME_predictions_MMDDYYYY.parquet`
- `AGENCY_NAME_route_patterns_MMDDYYYY.parquet`
- `AGENCY_NAME_routes_MMDDYYYY.parquet`
- `AGENCY_NAME_schedules_MMDDYYYY.parquet`
- `AGENCY_NAME_services_MMDDYYYY.parquet`
- `AGENCY_NAME_shapes_MMDDYYYY.parquet`
- `AGENCY_NAME_stop_events_MMDDYYYY.parquet`
- `AGENCY_NAME_stops_MMDDYYYY.parquet`
- `AGENCY_NAME_trips_MMDDYYYY.parquet`
- `AGENCY_NAME_trips_enhanced_MMDDYYYY.parquet`
- `AGENCY_NAME_vehicles_MMDDYYYY.parquet`
- `AGENCY_NAME_vehicles_enhanced_MMDDYYYY.parquet`

---

## 2. `data/archive/` (Compacted Event Logs)

**Purpose:** Nightly compaction thread compresses `data/events/` into flat *.parquet files.

**Key Behavior:**
- Archives are append-merged across restarts  
- Deduplication is performed using `hash_id`  
- Orphaned `.jsonl` files are automatically recovered on startup  
- At-least-once ingestion semantics  

**Storage Format Notes:**
- All Parquet files use ZSTD compression  
- Files are partitioned by `MMDDYYYY` snapshot date derived from `SYNC_TIMEZONE`  

**Schema:**
| Column | Parquet Type | Notes | Example |
|:------:|:------------:|:------:|:--------:|
| `hash_id` | `BINARY` | SHA-256 deduplication fingerprint. | `c5fdc9...` |
| `ts` | `TIMESTAMP` | [ISO 8601 timestamp](https://en.wikipedia.org/wiki/ISO_8601#Combined_date_and_time_representations), timezone-aware via `SYNC_TIMEZONE`. | `2026-05-25T...` |
| `event` | `STRING` | Type of event: add, update, remove, reset, snapshot, error. | `update` |
| `id` | `STRING` | API entity ID. | `prediction-123` |
| `data` | `STRING` | Stringified JSON payload. | `{"attributes": {...}}` |

**Generated Files:**
- `AGENCY_NAME_alerts_MMDDYYYY.parquet`
- `AGENCY_NAME_alerts_enhanced_MMDDYYYY.parquet`
- `AGENCY_NAME_facilities_MMDDYYYY.parquet`
- `AGENCY_NAME_lines_MMDDYYYY.parquet`
- `AGENCY_NAME_live_facilities_MMDDYYYY.parquet`
- `AGENCY_NAME_predictions_MMDDYYYY.parquet`
- `AGENCY_NAME_route_patterns_MMDDYYYY.parquet`
- `AGENCY_NAME_routes_MMDDYYYY.parquet`
- `AGENCY_NAME_schedules_MMDDYYYY.parquet`
- `AGENCY_NAME_services_MMDDYYYY.parquet`
- `AGENCY_NAME_shapes_MMDDYYYY.parquet`
- `AGENCY_NAME_stop_events_MMDDYYYY.parquet`
- `AGENCY_NAME_stops_MMDDYYYY.parquet`
- `AGENCY_NAME_trips_MMDDYYYY.parquet`
- `AGENCY_NAME_trips_enhanced_MMDDYYYY.parquet`
- `AGENCY_NAME_vehicles_MMDDYYYY.parquet`
- `AGENCY_NAME_vehicles_enhanced_MMDDYYYY.parquet`

---

## 3. `data/archive/gtfs/` (Static Schedule Data)

**Purpose:** Twice-daily static GTFS snapshots, merged into one cumulative Parquet file per table.

**Behavior:**
- Each table is written to a stable file named `AGENCY_NAME_gtfs_<table>.parquet`.
- Generated at fixed times: 03:00 and 15:00 (local `SYNC_TIMEZONE`).
- Each record is assigned a stable `hash_id` (SHA-256 fingerprint), a `first_logged` timestamp, and a `last_logged` timestamp. This prevents redundant storage of identical records across snapshots while keeping track of when each unique entity first appeared and was most recently seen.

**Schema:** - These files strictly adhere to the [MBTA GTFS Documentation](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md).
- Each Parquet file corresponds to a standard GTFS table (e.g., `stops.txt` becomes `gtfs_stops.parquet`).
- **Metadata Fields:**
    - `hash_id`: Deterministic fingerprint based on record content for automated deduplication.
    - `first_logged`: Immutable timestamp stamped at the moment of first ingestion to support point-in-time analysis.
    - `last_logged`: Timestamp updated when the same record appears again in a later GTFS pull.

**Freshness Guarantee:**
- Updated twice daily (03:00 and 15:00 local time).
- Each run merges into the existing table file, deduplicates identical rows by `hash_id`, and updates `last_logged` for rows that reappear.
- This is not a money-back guarantee as there is no money involved - its more of a pinky promise at best.

**Generated Files:**
- `AGENCY_NAME_gtfs_agency.parquet`
- `AGENCY_NAME_gtfs_areas.parquet`
- `AGENCY_NAME_gtfs_calendar.parquet`
- `AGENCY_NAME_gtfs_calendar_attributes.parquet`
- `AGENCY_NAME_gtfs_calendar_dates.parquet`
- `AGENCY_NAME_gtfs_checkpoints.parquet`
- `AGENCY_NAME_gtfs_directions.parquet`
- `AGENCY_NAME_gtfs_facilities.parquet`
- `AGENCY_NAME_gtfs_facilities_properties.parquet`
- `AGENCY_NAME_gtfs_facilities_properties_definitions.parquet`
- `AGENCY_NAME_gtfs_fare_leg_join_rules.parquet`
- `AGENCY_NAME_gtfs_fare_leg_rules.parquet`
- `AGENCY_NAME_gtfs_fare_media.parquet`
- `AGENCY_NAME_gtfs_fare_products.parquet`
- `AGENCY_NAME_gtfs_fare_transfer_rules.parquet`
- `AGENCY_NAME_gtfs_feed_info.parquet`
- `AGENCY_NAME_gtfs_levels.parquet`
- `AGENCY_NAME_gtfs_lines.parquet`
- `AGENCY_NAME_gtfs_linked_datasets.parquet`
- `AGENCY_NAME_gtfs_multi_route_trips.parquet`
- `AGENCY_NAME_gtfs_pathways.parquet`
- `AGENCY_NAME_gtfs_route_patterns.parquet`
- `AGENCY_NAME_gtfs_routes.parquet`
- `AGENCY_NAME_gtfs_shapes.parquet`
- `AGENCY_NAME_gtfs_stop_areas.parquet`
- `AGENCY_NAME_gtfs_stop_times.parquet`
- `AGENCY_NAME_gtfs_stops.parquet`
- `AGENCY_NAME_gtfs_timeframes.parquet`
- `AGENCY_NAME_gtfs_transfers.parquet`
- `AGENCY_NAME_gtfs_trips.parquet`
- `AGENCY_NAME_gtfs_trips_properties.parquet`
- `AGENCY_NAME_gtfs_trips_properties_definitions.parquet`

## Note

File naming and similar functions are driven by the compose configuration:
- The `MMDDYYYY` suffix comes from `SYNC_TIMEZONE` in the compose configuration.
- `AGENCY_NAME` sets the filename prefix.
- The enable flags control which file families are produced.
- Static GTFS files are cumulative per table and use a stable `.parquet` name instead of a date suffix.
