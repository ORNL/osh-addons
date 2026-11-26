# Parquet-Primary Observation Mode (`v3.12.0`)

`OscarTieredObsSystemDatabase` supports an explicit `PARQUET_PRIMARY` mode for
OSCAR databases. Ordinary PostGIS stores and the default `HOT_RETENTION` tiered
mode (`RAW_GATED`) remain unchanged for backward compatibility.

## Storage Architecture: Split-at-Export + Event-Only Multi-Tier Compaction

In `PARQUET_PRIMARY`, PostgreSQL provides a short-lived, crash-recoverable
30-minute landing layer (`oscar_obs_stage_partition`, `immutableStageWindowMinutes = 30`,
`< 60` minute staging residence SLO). When each 30-minute UTC window closes and
clears `parquetPrimaryArchiveDelayMinutes` (default `5` minutes),
`ArchiveExporter` streams rows through a file-backed DuckDB session and native
`DuckDBAppender` directly into two strictly complementary Parquet V2 profiles:

| Profile | Streams Included | Share of 50-Lane Rows | Parquet V2 Format & Indexing | Write Amplification | Compaction Lifecycle |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **`RAW_EVENTS`** (`archive_version = 2`) | **9** searchable event/summary streams: `occupancy`, `alarm`, `adjudication`, `report`, `vehicleOcr`, `emlAnalysis`, `tamper`, `siteStatistics`, `nationalSummary` | `~1.3%` (`~150k` rows/day, `~1.8 MiB/day`) | 20 columns (11 base + 9 projected scalar columns), `ROW_GROUP_SIZE = 8192`, Split-Block Bloom filters (`0.01` FPR), full `oscar_archive_schema` summary counters, selective `ocr_bloom` | Compacted (`~1.8 MiB/day`) | Compacted at Day-1 into `DAILY_EVENTS`, then consolidated at Month+15d into `MONTHLY_EVENTS` |
| **`RAW_TELEMETRY`** (`archive_version = 2`) | All non-event allowlisted streams (exact complement via `LEFT JOIN`): `gammaCounts`, `gammaThreshold`, `neutronCounts`, `dailyFile`, `connectionStatus`, `occupancyStatus`, `emlScanContextual`, `emlContextual`, `setupGamma`, `setupNeutron`, `speed`, `backgroundReport`, `foregroundReport`, `status`, `location`, `KromekDetectorRadiometricsV1Report`, `KromekSerialRadiometricStatusReport` | `~98.7%` (`~12.2M` rows/day, `~172 MiB/day`) | 11 base columns, `ROW_GROUP_SIZE = 65536`, ZSTD level 6, `effectiveTelemetryExportUncompressedBytes = 128 MiB` (at `256 MiB` DuckDB limit), `ocr_bloom = NULL` | **`1.0x` (Written once, NEVER rewritten)** | Permanent immutable 30-minute Parquet V2 files (`~3.6 MiB`/file, 1 file per 30m window on 50 lanes) |
| **`DAILY_EVENTS`** (`archive_version = 2`) | Event streams compacted from sealed `RAW_EVENTS` (`window_end <= now - 2h`, `activated_at <= now - 2h`) | `~1.3%` | 20 columns, `ROW_GROUP_SIZE = 8192`, Split-Block Bloom filters (`0.01` FPR), folds settled overlays (`>= 2h`), explicit rollover ceilings (`1,000,000` rows / `192 MiB` uncompressed) | Event tier only | Consolidated into `MONTHLY_EVENTS` once month ends + 15d (`DAILY_EVENTS.activated_at <= now - 7d`) |
| **`MONTHLY_EVENTS`** (`archive_version = 2`) | Event streams consolidated from sealed `DAILY_EVENTS` | `~1.3%` | 20 columns, `ROW_GROUP_SIZE = 16384`, Split-Block Bloom filters (`0.01` FPR), folds settled overlays (`>= 7d`), explicit rollover ceilings (`1,000,000` rows / `192 MiB` uncompressed), independent per-part defrag (`>= 64` settled overlays `>= 7d`) | Event tier only | Long-term monthly Parquet V2 event bundles (`date=YYYY-MM-01/profile=MONTHLY_EVENTS/`) |
| **`RAW_GATED`** / **`DAILY_TELEMETRY`** (Legacy Compatibility) | Historical `3.11.x` / pre-split archives and `HOT_RETENTION` mode | N/A | Read-only transparent compatibility (`union_by_name=true` with V2 projected columns) | `1.0x` (Never triggers upgrade rewrite storm) | Preserved in place; retryable pre-upgrade `RAW_GATED` jobs drain cleanly before new split exports |

### Why High-Rate Telemetry Is Never Re-Compacted

High-rate `1 Hz` telemetry streams (`gammaCounts`, `gammaThreshold`,
`neutronCounts`, `dailyFile`, `connectionStatus`, `occupancyStatus`) account
for `98.77%` of all ingested rows on a 50-lane site (`~12.2M` rows/day), yet
operational queries only ever read them for narrow 15-second to 5-minute
event-inspection chart windows (`1-2` 30-minute files at most). Exporting them
directly into one right-sized `~3.6 MiB` Parquet V2 `RAW_TELEMETRY` file per
30-minute window eliminates `2x` write amplification, removes sustained
background DuckDB/9p I/O pressure on constrained hosts (especially Windows 11 +
WSL 2), and keeps `oscar_archive_schema` completely free of compaction churn.

## Storage Contract & Safety Invariants

- **Crash-recoverable landing tier:** PostgreSQL commits observations into
  LOGGED, UTC-aligned 30-minute physical staging children (`osg_*`) keyed by
  detector `phenomenon_time`. A write becomes durable when the OSH store
  `commit()` completes. Detector clocks farther ahead than
  `maximumFuturePhenomenonSkewMinutes` (default `5`) are rejected before
  routing.
- **Window-first split-at-export:** `OscarTieredObsSystemDatabase.runArchiveCycle()`
  drains any pre-upgrade retryable `RAW_GATED` job first, then exports the
  oldest completed window's `RAW_EVENTS` and `RAW_TELEMETRY` back-to-back
  before advancing to the next window. Because the two queries form an exact
  mathematical partition (`JOIN` on the 9 `EVENT_OUTPUT_NAMES` vs `LEFT JOIN`
  on `NOT (COALESCE(outputName, '') = ANY(EVENT_OUTPUT_NAMES))`), a renamed or
  deleted datastream label can never strand staged rows.
- **Manifest-last publication & whole-child retirement:** Every generation uses
  a transactionally claimed `oscar_archive_job`, `.partial` files, file and
  directory `fsync`, atomic rename, SHA-256 + row/ID verification, exact
  `oscar_obs_stage_owner` ledger activation, and manifest-last publication.
  Once every row in a staging child has a parity-verified owner, PostgreSQL
  detaches and drops the complete staging relation (`DROP TABLE`) — eliminating
  high-volume row `DELETE`s and autovacuum bloat.
- **Projected V2 scalar columns & Bloom filters:** `RAW_EVENTS`, `DAILY_EVENTS`,
  and `MONTHLY_EVENTS` pre-extract 9 scalar columns in Java
  (`ProjectedEventColumns`: `ocr_norm`, `occ_obs_id`, `event_id`, `adj_code`,
  `adj_count`, `gamma_alarm`, `neutron_alarm`, `max_gamma`, `max_neutron`) and
  write Parquet V2 with Split-Block Bloom filters (`0.01` FPR). `JsonCqlEvaluator`
  pushes scalar CQL predicates directly onto these columns while preserving
  `union_by_name=true` fallback for legacy `RAW_GATED` files.
- **Bounded-memory rollover & disjoint multi-part compaction:**
  `ArchiveCompactionManager` clusters overlapping source generations
  (`clusterOverlappingSources`), streams source reads in `50,000`-row chunks,
  spills intermediate chunks through `DuckDbResourcePolicy.ScratchDirectory` so
  JVM heap stays `< 20 MiB` and `<= 1` DuckDB session is held at any instant,
  and enforces explicit part rollover ceilings
  (`effectiveCompactionPartMaxRows() = 1,000,000` rows,
  `effectiveCompactionPartUncompressedBytes() = 192 MiB` uncompressed =
  `~16-32 MiB` ZSTD Parquet, with a `2x` hard cap for pathological equal
  timestamps). Rollovers split on strict `phenomenon_time` boundaries so
  multi-part `DAILY_EVENTS` and `MONTHLY_EVENTS` receive strictly disjoint
  `[window_start, window_end)` intervals and can be independently defragmented.
- **Transactional swap, overlays, holds, and online-backup safety:**
  Compaction verifies an order-independent 576-bit SHA-256 multiset digest
  across all target parts before committing an atomic PostgreSQL swap that
  remaps `oscar_obs_latest`, `oscar_event_locator`, unsettled
  `oscar_obs_overlay` rows, and `oscar_tombstone` rows, folds settled overlays,
  zeroes `row_count` on superseded `DELETING` rows immediately, honors legal
  `oscar_retention_hold` locks and active reader pins, and retains superseded
  files on disk for `compactionUnlinkGraceMinutes` (default `15` minutes) for
  online `pg_basebackup` consistency.
- **Self-healing stale `.partial` cleanup:** During
  `drainDeletingGenerations(now)`, when no `EXPORTING` job is active,
  `sweepStaleUnregisteredPartials` removes unregistered `.partial` files older
  than `2` hours (`7200` seconds) and prunes empty `profile=` / `date=`
  directories, keeping `oscar.sh archive verify` continuously clean while
  failing closed on any file whose UUID appears in `oscar_archive_generation`.
- **Metadata acceleration:** `oscar_obs_latest` serves `resultTime=latest` /
  `phenomenonTime=latest` in `< 1 ms`; `oscar_archive_schema` stores 23 exact
  per-generation, per-datastream summary counters plus selective 256-byte
  `ocr_bloom` + min/max zone-maps (`NULL` on `RAW_TELEMETRY`, 1-byte sentinel
  on non-OCR event streams); `oscar_event_locator` indexes `eventId`; and
  `oscar_archive_id_block` prunes point lookups before opening Parquet.

## Index Policy

On a fresh `PARQUET_PRIMARY` mutable observation table (`ONLY obs`), OSCAR
omits the redundant generic `GIN(result)` index while keeping the targeted
`jsonb_path_ops` and numeric event-filter indexes for mutable workflows. Each
30-minute staging child retains only its bounded local indexes (no JSON GIN or
event-value indexes) and releases both heap and indexes as whole OS relations
when dropped. Existing upgraded databases do not execute destructive DDL on
their base tables.

## Example Configuration

```json
{
  "archive": {
    "enabled": true,
    "storageMode": "PARQUET_PRIMARY",
    "parquetPrimaryArchiveDelayMinutes": 5,
    "immutableStageWindowMinutes": 30,
    "maximumFuturePhenomenonSkewMinutes": 5,
    "parquetPrimaryIntervalMinutes": 1,
    "maximumStagePartitionsPerCycle": 8,
    "intervalMinutes": 60,
    "maximumGenerationsPerCycle": 2,
    "maximumVerificationsPerCycle": 2,
    "maximumDeletionsPerCycle": 2,
    "maximumRowsPerGeneration": 2000000,
    "duckDbMemoryLimitMegabytes": 256,
    "duckDbThreads": 2,
    "duckDbMaximumConcurrentSessions": 2,
    "compactionUnlinkGraceMinutes": 15,
    "deleteHotAfterArchive": true,
    "rollbackGraceDays": 3,
    "outputNames": [
      "gammaCounts",
      "gammaThreshold",
      "neutronCounts",
      "setupGamma",
      "setupNeutron",
      "speed",
      "backgroundReport",
      "foregroundReport",
      "status",
      "emlContextual",
      "emlScanContextual",
      "emlAnalysis",
      "dailyFile",
      "connectionStatus",
      "occupancyStatus",
      "tamper",
      "occupancy",
      "adjudication",
      "report",
      "siteStatistics",
      "nationalSummary",
      "vehicleOcr",
      "location",
      "alarm",
      "KromekDetectorRadiometricsV1Report",
      "KromekSerialRadiometricStatusReport"
    ]
  }
}
```

Do not describe this mode as synchronous direct-to-Parquet ingestion. The
30-minute LOGGED PostgreSQL staging child supplies the crash-recoverable commit
and batch boundary before split-at-export writes immutable `RAW_EVENTS` and
`RAW_TELEMETRY` Parquet V2 generations.
