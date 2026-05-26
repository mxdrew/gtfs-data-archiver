# Archived Data Overview

## Archive Dates

| Event | Date | Notes |
| ----- | ---- | ----- |
| Archiving Started | May 26, 2026 | There may be a few records from May 25, 2026 due to building and testing of the script. |

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
- `alerts_MMDDYYYY.parquet`
- `alerts_enhanced_MMDDYYYY.parquet`
- `facilities_MMDDYYYY.parquet`
- `lines_MMDDYYYY.parquet`
- `live_facilities_MMDDYYYY.parquet`
- `predictions_MMDDYYYY.parquet`
- `route_patterns_MMDDYYYY.parquet`
- `routes_MMDDYYYY.parquet`
- `schedules_MMDDYYYY.parquet`
- `services_MMDDYYYY.parquet`
- `shapes_MMDDYYYY.parquet`
- `stop_events_MMDDYYYY.parquet`
- `stops_MMDDYYYY.parquet`
- `trips_MMDDYYYY.parquet`
- `trips_enhanced_MMDDYYYY.parquet`
- `vehicles_MMDDYYYY.parquet`
- `vehicles_enhanced_MMDDYYYY.parquet`

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
- `alerts_MMDDYYYY.parquet`
- `alerts_enhanced_MMDDYYYY.parquet`
- `facilities_MMDDYYYY.parquet`
- `lines_MMDDYYYY.parquet`
- `live_facilities_MMDDYYYY.parquet`
- `predictions_MMDDYYYY.parquet`
- `route_patterns_MMDDYYYY.parquet`
- `routes_MMDDYYYY.parquet`
- `schedules_MMDDYYYY.parquet`
- `services_MMDDYYYY.parquet`
- `shapes_MMDDYYYY.parquet`
- `stop_events_MMDDYYYY.parquet`
- `stops_MMDDYYYY.parquet`
- `trips_MMDDYYYY.parquet`
- `trips_enhanced_MMDDYYYY.parquet`
- `vehicles_MMDDYYYY.parquet`
- `vehicles_enhanced_MMDDYYYY.parquet`

---

## 3. `data/archive/gtfs/` (Static Schedule Data)

**Purpose:** Twice-daily static GTFS snapshots, converted from CSV to Parquet.

**Behavior:**
- Full dataset replacement per run (not incremental updates).
- Generated at fixed times: 03:00 and 15:00 (local `SYNC_TIMEZONE`).
- Each record is assigned a stable `hash_id` (SHA-256 fingerprint) and a `first_logged` timestamp (`YYYY-MM-DD`). This prevents redundant storage of identical records across snapshots while maintaining a permanent record of when each unique entity first appeared in the archive.

**Schema:** - These files strictly adhere to the [MBTA GTFS Documentation](https://github.com/mbta/gtfs-documentation/blob/master/reference/gtfs.md).
- Each Parquet file corresponds to a standard GTFS table (e.g., `stops.txt` becomes `gtfs_stops_MMDDYYYY.parquet`).
- **Metadata Fields:**
    - `hash_id`: Deterministic fingerprint based on record content for automated deduplication.
    - `first_logged`: Immutable date string stamped at the moment of first ingestion to support point-in-time analysis.

**Freshness Guarantee:**
- Updated twice daily (03:00 and 15:00 local time).
- Each run produces a complete replacement dataset for that timestamp, with legacy records automatically updated to include `first_logged` metadata.
- This is not a money-back guarantee as there is no money involved - its more of a pinky promise at best.

**Generated Files:**
- `gtfs_agency_MMDDYYYY.parquet`
- `gtfs_areas_MMDDYYYY.parquet`
- `gtfs_calendar_MMDDYYYY.parquet`
- `gtfs_calendar_attributes_MMDDYYYY.parquet`
- `gtfs_calendar_dates_MMDDYYYY.parquet`
- `gtfs_checkpoints_MMDDYYYY.parquet`
- `gtfs_directions_MMDDYYYY.parquet`
- `gtfs_facilities_MMDDYYYY.parquet`
- `gtfs_facilities_properties_MMDDYYYY.parquet`
- `gtfs_facilities_properties_definitions_MMDDYYYY.parquet`
- `gtfs_fare_leg_join_rules_MMDDYYYY.parquet`
- `gtfs_fare_leg_rules_MMDDYYYY.parquet`
- `gtfs_fare_media_MMDDYYYY.parquet`
- `gtfs_fare_products_MMDDYYYY.parquet`
- `gtfs_fare_transfer_rules_MMDDYYYY.parquet`
- `gtfs_feed_info_MMDDYYYY.parquet`
- `gtfs_levels_MMDDYYYY.parquet`
- `gtfs_lines_MMDDYYYY.parquet`
- `gtfs_linked_datasets_MMDDYYYY.parquet`
- `gtfs_multi_route_trips_MMDDYYYY.parquet`
- `gtfs_pathways_MMDDYYYY.parquet`
- `gtfs_route_patterns_MMDDYYYY.parquet`
- `gtfs_routes_MMDDYYYY.parquet`
- `gtfs_shapes_MMDDYYYY.parquet`
- `gtfs_stop_areas_MMDDYYYY.parquet`
- `gtfs_stop_times_MMDDYYYY.parquet`
- `gtfs_stops_MMDDYYYY.parquet`
- `gtfs_timeframes_MMDDYYYY.parquet`
- `gtfs_transfers_MMDDYYYY.parquet`
- `gtfs_trips_MMDDYYYY.parquet`
- `gtfs_trips_properties_MMDDYYYY.parquet`
- `gtfs_trips_properties_definitions_MMDDYYYY.parquet`

---

## Note

The `MMDDYYYY` suffix represents the data pull date, determined using the `SYNC_TIMEZONE` environment variable defined in `.env`, ensuring all snapshots align with the configured synchronization timezone.