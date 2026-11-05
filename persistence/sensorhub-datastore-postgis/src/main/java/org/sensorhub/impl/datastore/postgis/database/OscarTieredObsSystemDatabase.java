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
        store.maintainArchiveLifecycle();
        Instant deadline = Instant.now().plus(archive.maximumCycleMinutes, ChronoUnit.MINUTES);
        int activated = 0;
        while (activated < archive.maximumGenerationsPerCycle && Instant.now().isBefore(deadline)) {
            // Recover the exact shape of a failed/stale legacy job first.
            // Older builds could create sub-hour bisection windows; replacing
            // them immediately with a full-hour job would strand their FAILED
            // catalog row even when all of its observations later archived.
            var retryable = store.nextRetryableArchiveWindow(cutoff, "RAW_GATED").orElse(null);
            Instant start;
            Instant end;
            if (retryable != null) {
                start = retryable.start();
                end = retryable.end();
            } else {
                // PARQUET_PRIMARY uses its short immutable UTC staging windows;
                // HOT_RETENTION preserves the established UTC-hour windows.
                // Both choices bound snapshots and make restart/idempotency
                // behaviour independent of local DST.
                Instant oldest = store.nextArchiveWindowStart(cutoff, "RAW_GATED").orElse(null);
                if (oldest == null)
                    break;
                int windowMinutes = archive.isParquetPrimary()
                        ? archive.immutableStageWindowMinutes
                        : 60;
                start = utcWindowStart(oldest, windowMinutes);
                end = start.plus(windowMinutes, ChronoUnit.MINUTES);
                if (end.isAfter(cutoff))
                    end = cutoff;
            }
            if (!start.isBefore(end))
                break;
            Optional<org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration> generation =
                    store.archiveWindow(start, end, "RAW_GATED");
            if (generation.isEmpty())
                break; // another owner has the window; avoid a tight retry loop
            activated++;
            getLogger().info("Activated archive generation {} with {} rows for [{}, {})",
                    generation.get().id(), generation.get().rowCount(), start, end);
        }
        if (activated > 0)
            getLogger().info("OSCAR archive cycle completed {} generation(s); remaining backlog will continue next cycle",
                    activated);
        // In Parquet-primary mode, a successful export should be eligible for
        // verification and staging retirement without waiting for the next
        // scheduler tick. HOT_RETENTION keeps its historical one-pass cycle.
        if (activated > 0 && archive.isParquetPrimary())
            store.maintainArchiveLifecycle();
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
