package org.sensorhub.impl.datastore.postgis.database;

import org.sensorhub.api.common.SensorHubException;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisObsStoreImpl;

import java.time.Instant;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Optional;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** PostGIS database module that exposes hot and cold observations as one OSH store. */
public class OscarTieredObsSystemDatabase extends PostgisObsSystemDatabase {
    private ScheduledExecutorService archiveExecutor;

    private TieredArchiveConfig archiveConfig() {
        if (!(config instanceof OscarTieredObsSystemDatabaseConfig tiered))
            throw new IllegalStateException("Tiered database requires OscarTieredObsSystemDatabaseConfig");
        return tiered.archive;
    }

    @Override
    protected PostgisObsStoreImpl createObsStore(String url, String dbName, String login, String password,
                                                  String tableName, int idScope,
                                                  IdProviderType idProviderType, boolean useBatch) {
        return new OscarTieredObsStore(url, dbName, login, password, tableName, idScope,
                idProviderType, archiveConfig());
    }

    @Override
    protected void afterStart() {
        super.afterStart();
        TieredArchiveConfig archive = archiveConfig();
        if (!archive.enabled)
            return;
        OscarTieredObsStore store = (OscarTieredObsStore) obsStore;
        int reclaimed = store.reclaimInterruptedExportingJobsOnStartup();
        if (reclaimed > 0) {
            getLogger().info("Reclaimed {} interrupted OSCAR archive export job(s) on startup for immediate retry",
                    reclaimed);
        }
        archiveExecutor = Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "oscar-observation-archive");
            thread.setDaemon(true);
            return thread;
        });
        long delay = Math.max(1L, archive.isParquetPrimary()
                ? archive.parquetPrimaryIntervalMinutes
                : archive.intervalMinutes);
        // Run once immediately so a restarted or newly upgraded installation
        // begins bounded catch-up without waiting for the first interval.
        archiveExecutor.scheduleWithFixedDelay(this::runArchiveCycleSafely, 0L, delay, TimeUnit.MINUTES);
    }

    private void runArchiveCycleSafely() {
        try {
            runArchiveCycle();
        } catch (Throwable error) {
            getLogger().error("OSCAR observation archive cycle failed; hot data was retained", error);
        }
    }

    private void runArchiveCycle() {
        OscarTieredObsStore store = (OscarTieredObsStore) obsStore;
        TieredArchiveConfig archive = archiveConfig();
        Instant cutoff = archive.archiveCutoff(Instant.now());
        Instant deadline = Instant.now().plus(Math.max(15, archive.maximumCycleMinutes), ChronoUnit.MINUTES);
        boolean drainedLegacyAny = false;
        for (int pass = 0; pass < 128 && Instant.now().isBefore(deadline); pass++) {
            store.maintainArchiveLifecycle();
            boolean stagedLegacy = store.stageLegacyAllowlistedRowsBatch();
            drainedLegacyAny |= stagedLegacy;
            boolean hasBacklogPartitions = archive.isParquetPrimary()
                    && store.getImmutableStagePartitions().size() > 2;
            int generationBudget = (stagedLegacy || hasBacklogPartitions)
                    ? Math.max(16, Math.max(archive.maximumGenerationsPerCycle, archive.maximumStagePartitionsPerCycle))
                    : (archive.isParquetPrimary()
                            ? Math.max(4, archive.maximumGenerationsPerCycle)
                            : archive.maximumGenerationsPerCycle);
            int activated = 0;
            while (activated < generationBudget
                    && (stagedLegacy || hasBacklogPartitions || Instant.now().isBefore(deadline))) {
                String selectedProfile = null;
                Instant start = null;
                Instant end = null;
                if (archive.isParquetPrimary()) {
                    // Drain any pre-upgrade retryable RAW_GATED job first so an upgrade from
                    // 3.11.x never strands an interrupted job row, then split closed 30-minute
                    // staging partitions window-by-window into RAW_EVENTS and RAW_TELEMETRY so
                    // each completed stage partition retires immediately even during catch-up.
                    var legacyRetry = store.nextRetryableArchiveWindow(cutoff, TieredArchiveConfig.PROFILE_RAW_GATED).orElse(null);
                    if (legacyRetry != null) {
                        selectedProfile = TieredArchiveConfig.PROFILE_RAW_GATED;
                        start = legacyRetry.start();
                        end = legacyRetry.end();
                    } else {
                        var eventRetry = store.nextRetryableArchiveWindow(cutoff, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElse(null);
                        var telRetry = store.nextRetryableArchiveWindow(cutoff, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElse(null);
                        if (eventRetry != null && (telRetry == null || !eventRetry.start().isAfter(telRetry.start()))) {
                            selectedProfile = TieredArchiveConfig.PROFILE_RAW_EVENTS;
                            start = eventRetry.start();
                            end = eventRetry.end();
                        } else if (telRetry != null) {
                            selectedProfile = TieredArchiveConfig.PROFILE_RAW_TELEMETRY;
                            start = telRetry.start();
                            end = telRetry.end();
                        } else {
                            int windowMinutes = archive.immutableStageWindowMinutes;
                            Instant oldestEvent = store.nextArchiveWindowStart(cutoff, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElse(null);
                            Instant oldestTel = store.nextArchiveWindowStart(cutoff, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElse(null);
                            Instant eventWin = oldestEvent == null ? null : utcWindowStart(oldestEvent, windowMinutes);
                            Instant telWin = oldestTel == null ? null : utcWindowStart(oldestTel, windowMinutes);
                            if (eventWin != null && (telWin == null || !eventWin.isAfter(telWin))) {
                                selectedProfile = TieredArchiveConfig.PROFILE_RAW_EVENTS;
                                start = eventWin;
                            } else if (telWin != null) {
                                selectedProfile = TieredArchiveConfig.PROFILE_RAW_TELEMETRY;
                                start = telWin;
                            }
                            if (start != null) {
                                end = start.plus(windowMinutes, ChronoUnit.MINUTES);
                                if (end.isAfter(cutoff))
                                    end = cutoff;
                            }
                        }
                    }
                } else {
                    // HOT_RETENTION preserves the established UTC-hour RAW_GATED windows.
                    var retryable = store.nextRetryableArchiveWindow(cutoff, TieredArchiveConfig.PROFILE_RAW_GATED).orElse(null);
                    if (retryable != null) {
                        selectedProfile = TieredArchiveConfig.PROFILE_RAW_GATED;
                        start = retryable.start();
                        end = retryable.end();
                    } else {
                        Instant oldest = store.nextArchiveWindowStart(cutoff, TieredArchiveConfig.PROFILE_RAW_GATED).orElse(null);
                        if (oldest != null) {
                            selectedProfile = TieredArchiveConfig.PROFILE_RAW_GATED;
                            start = utcWindowStart(oldest, 60);
                            end = start.plus(60, ChronoUnit.MINUTES);
                            if (end.isAfter(cutoff))
                                end = cutoff;
                        }
                    }
                }
                if (selectedProfile == null || start == null || end == null || !start.isBefore(end))
                    break;
                Optional<org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration> generation =
                        store.archiveWindow(start, end, selectedProfile);
                if (generation.isEmpty())
                    break; // another owner has the window; avoid a tight retry loop
                activated++;
                getLogger().info("Activated {} archive generation {} with {} rows for [{}, {})",
                        selectedProfile, generation.get().id(), generation.get().rowCount(), start, end);
            }
            if (activated > 0)
                getLogger().info("OSCAR archive cycle completed {} generation(s); remaining backlog will continue next cycle",
                        activated);
            // In Parquet-primary mode, a successful export should be eligible for
            // verification and staging retirement without waiting for the next
            // scheduler tick. HOT_RETENTION keeps its historical one-pass cycle.
            if (activated > 0 && archive.isParquetPrimary()) {
                store.maintainArchiveLifecycle();
            }
            if (!stagedLegacy && activated == 0)
                break;
        }
        if (drainedLegacyAny && !store.stageLegacyAllowlistedRowsBatch()) {
            store.compactDrainedMutableHotTables();
            store.reindexMutableHotTables();
        }
        var compactionSummary = store.runCompactionCycle(Instant.now());
        if (compactionSummary.daysCompacted() > 0 ||
                compactionSummary.monthsConsolidated() > 0 ||
                compactionSummary.monthlyGenerationsDefragmented() > 0 ||
                compactionSummary.deletingGenerationsDrained() > 0) {
            getLogger().info("OSCAR archive compaction cycle: days={}, months={}, defrag={}, drained={}",
                    compactionSummary.daysCompacted(),
                    compactionSummary.monthsConsolidated(),
                    compactionSummary.monthlyGenerationsDefragmented(),
                    compactionSummary.deletingGenerationsDrained());
        }
        if (commandStore != null) {
            int prunedCommands = commandStore.purgeCompletedTransientCommandsBefore(
                    Instant.now().minus(24, ChronoUnit.HOURS));
            if (prunedCommands > 0) {
                getLogger().info("Pruned {} completed transient command(s) older than 24 hours", prunedCommands);
            }
        }
        if (archive.isParquetPrimary()) {
            store.getOldestImmutableStageReceipt().ifPresent(oldest -> {
                Duration residence = Duration.between(oldest, Instant.now());
                if (residence.compareTo(Duration.ofHours(1)) >= 0)
                    getLogger().warn("OSCAR immutable PostgreSQL staging SLO exceeded: " +
                            "oldest row was received {} minutes ago; raw data is retained until " +
                            "a verified Parquet generation can be activated", residence.toMinutes());
            });
        }
    }

    /** Anchors every normal generation to a stable UTC Hive partition window. */
    public static Instant utcHourWindowStart(Instant oldestEligibleObservation) {
        return utcWindowStart(oldestEligibleObservation, 60);
    }

    /** Anchors every normal generation to a configurable stable UTC window. */
    public static Instant utcWindowStart(Instant oldestEligibleObservation, int windowMinutes) {
        if (oldestEligibleObservation == null)
            throw new IllegalArgumentException("Oldest eligible observation time is required");
        return TieredArchiveConfig.utcWindowStart(oldestEligibleObservation, windowMinutes);
    }

    @Override
    protected void beforeStop() {
        if (archiveExecutor != null) {
            archiveExecutor.shutdown();
            try {
                if (!archiveExecutor.awaitTermination(30, TimeUnit.SECONDS))
                    archiveExecutor.shutdownNow();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                archiveExecutor.shutdownNow();
            }
        }
        super.beforeStop();
    }
}
