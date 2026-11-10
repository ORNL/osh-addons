# Parquet-primary observation mode

`OscarTieredObsSystemDatabase` supports an explicit `PARQUET_PRIMARY` mode for
fresh OSCAR databases. Ordinary PostGIS stores and the default
`HOT_RETENTION` tiered mode are unchanged.

## Storage contract

- PostgreSQL provides a short-lived, crash-recoverable landing tier. A write becomes durable
  when the normal OSH store `commit()` completes; `add()` alone is not a
  durability boundary because the batch store intentionally buffers writes.
- Only output names in `archive.outputNames` are eligible (by default all 21
  operational and evidentiary streams: `gammaCounts`, `gammaThreshold`,
  `neutronCounts`, `setupGamma`, `setupNeutron`, `speed`, `backgroundReport`,
  `foregroundReport`, `status`, `emlContextual`, `emlScanContextual`,
  `emlAnalysis`, `dailyFile`, `connectionStatus`, `occupancyStatus`, `tamper`,
  `occupancy`, `adjudication`, `report`, `siteStatistics`, `nationalSummary`).
  Static singleton configuration streams (`sensorLocation`, `adjudicationCodes`,
  `siteInfo`) remain in `ONLY oscar.obs`. Mutations to archived observations
  (such as appending an adjudication ID onto an archived `occupancy` row) are
  recorded in `oscar_obs_overlay` and synchronized with `oscar_obs_latest`.
  The storage class is recorded when a datastream first receives a
  row; changing that datastream ID between mutable and immutable classes is
  rejected. Export selects physical stage membership rather than re-reading a
  mutable output name, so a later label edit cannot strand already accepted
  rows.
- Allowlisted rows are routed at receipt into LOGGED, UTC-aligned physical
  staging children keyed by detector phenomenon time. PostgreSQL records a
  separate server-side `received_at` timestamp, so a late historical sample is
  immediately eligible instead of waiting for a new retention period. Detector
  clocks more than `maximumFuturePhenomenonSkewMinutes` ahead of PostgreSQL are
  rejected before routing; the default maximum skew is five minutes. The
  default window is 30 minutes
  (`immutableStageWindowMinutes=30`); the primary worker runs every minute
  (`parquetPrimaryIntervalMinutes=1`). A completed window becomes eligible
  after `parquetPrimaryArchiveDelayMinutes`. Row and byte caps bound each
  generation; a large window is exported as deterministic revisions and its
  physical child remains until every revision passes parity.
- Export continues to use a transactionally claimed job, `.partial` files,
  file and directory sync, atomic rename, checksum/count/range validation, an
  exact activation ledger, and manifest-last publication. A restart reclaims
  stale jobs and reuses deterministic generation object names.
- After atomic publication, exact activation, checksum validation, and
  row/ID/payload parity, OSCAR detaches and drops the complete staging child.
  It does not issue high-volume row DELETEs, retain a three-day raw overlap, or
  wait for autovacuum to reclaim those raw indexes. Mutable rows in the base
  table are never part of this drop. `deleteHotAfterArchive=true` remains the
  explicit operator opt-in required for this authority transition.
- Activation records exact row ownership in a compact, LOGGED sidecar instead
  of rewriting every staged JSON tuple. This prevents a second full MVCC copy
  and generation-index growth while a size-capped child is published across
  several Parquet generations. The ownership rows are removed transactionally
  with the physical child.
- Primary mode verifies up to `maximumVerificationsPerCycle` older generations
  before exporting the next bounded batch. Configuration validation requires
  that cap to be at least `maximumGenerationsPerCycle`, preventing each cycle
  from creating parity work faster than a later cycle can process it. A newly
  activated generation is verified again in the same bounded cycle when
  possible. The verification cap is separate because parity rereads both
  Parquet and PostgreSQL. Whole-child disposal has its own
  `maximumStagePartitionsPerCycle` bound; a busy relation backs off after a
  short lock timeout rather than blocking ingestion.
- Mixed hot/cold queries, mutation overlays, tombstones, stable observation
  IDs, and late-arrival revisions continue to use the existing tiered reader.
  `oscar_obs_latest` maintains the newest observation per datastream in
  PostgreSQL so `resultTime=latest` / `phenomenonTime=latest` lookups return in
  `< 1 ms` even when a sparse stream has no row in the active 30-minute stage
  window. `oscar_archive_schema` stores 16 per-generation, per-datastream
  summary counters (`row_count`, `gamma_alarm_count`, `neutron_alarm_count`,
  `gamma_neutron_alarm_count`, `any_alarm_count`, `non_alarm_count`,
  `unadjudicated_alarm_count`, `eml_non_threat_count`,
  `extended_occupancy_gamma_count`, `fault_gamma_high_count`,
  `fault_gamma_low_count`, `fault_neutron_high_count`, `fault_tamper_count`,
  `fault_extended_occupancy_count`, `tamper_true_count`, `summary_populated`)
  and `oscar_event_locator` indexes `event_id` so `StatisticsOutput`, Dashboard
  alarm badges, and Event Log counts sum interior unmutated generations in SQL
  and prune zero-match generations without scanning Parquet files.
  A single-ID lookup first prunes the catalog through
  `oscar_archive_id_block`, then applies Parquet predicate pushdown; it does not
  scan every generation.

`rollbackGraceDays` applies to `HOT_RETENTION`, not to primary-mode raw staging.
In primary mode, a failed parity receipt, missing/corrupt archive object,
unfinished capped revision, busy relation, or stopped scheduler safely keeps
the complete child attached. Restart reconciliation resumes a cataloged
detached child; an attached catalog entry with a missing relation is treated as
data loss and fails closed. Retention holds protect the verified Parquet
record, not its redundant landing copy, so a long-lived hold cannot pin an
entire high-volume PostgreSQL child.

With the default 30-minute window, five-minute close delay, one-minute worker,
and five-minute permitted future clock skew, a normally operating row has a
41-minute nominal eligibility budget. Export, parity validation, and a bounded
drop add workload-dependent time. The scheduler warns when any staging receipt
is one hour old. One hour is therefore an operating SLO, not permission to
discard data: failed export, insufficient disk, corruption, a busy table, or a
stopped process keeps the durable PostgreSQL child until recovery succeeds.

OSCAR's public event identifier is decoded to the stable OSH observation ID
before this store is queried. The direct cold lookup then uses
`oscar_archive_id_block`; this slice does not claim that the currently unused
`oscar_event_locator` table provides UUID indexing.

## Index policy

On a fresh primary-mode mutable observation table, OSCAR omits the redundant generic
`GIN(result)` index, which was the largest payload index on the measured test
system. The purpose-built `jsonb_path_ops` and numeric event-filter indexes
remain because mutable occupancy, alarm, and adjudication workflow rows are
still PostgreSQL-authoritative and must remain searchable. Each raw staging
child has only three local indexes: stable ID, null-safe natural identity, and
datastream/phenomenon-time/ID. Exact temporary Parquet ownership lives in the
compact sidecar, so neither the child nor the mutable parent needs a primary-mode
generation index. A child has no JSON GIN or event-value indexes. Dropping the
child removes its heap and all three indexes
as filesystem relations, which bounds raw PostgreSQL index growth instead of
merely creating reusable dead space. Retained mutable workflow data can still
grow and must be governed by its own lifecycle.

This mode does not drop indexes from an existing database. That deliberate
fresh-schema rule prevents a configuration change from causing destructive DDL
or surprising an upgraded installation.

Eliminating every PostgreSQL payload index is intentionally out of scope for
this slice. It first requires a reliable finalized-event transition and hot
query predicates that exclude archive-owned overlap rows; removing those
indexes before that work would trade storage savings for progressively slower
alarm searches.

Parquet supplies column statistics and DuckDB predicate pushdown for the typed
ID, datastream, FOI, and time columns. Arbitrary predicates inside the retained
JSON result still scan the already-pruned candidate generations; this change
does not claim a Parquet secondary or Bloom index that the checked dependency
does not provide.

## Example

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
      "nationalSummary"
    ]
  }
}
```

Do not describe this mode as synchronous direct-to-Parquet ingestion. The
small LOGGED staging child is intentional: it supplies the crash-recoverable
commit and efficient batch boundary. It is not a multi-day hot copy. Parquet
and the daily-file product are the long-term raw detector representations;
PostgreSQL retains only mutable workflows, compact metadata, archive catalog,
overlays, and tombstones after verified child retirement.

Before enabling this profile in a release, run the disposable real-PostgreSQL
integration suite against a fresh schema and verify the actual index catalog,
advanced hot event filters, archive/parity/deletion, restart recovery, and
single-ID hot/cold lookup. Unit query-builder tests do not replace that gate.
