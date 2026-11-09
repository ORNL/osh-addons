package org.sensorhub.impl.datastore.postgis.archive;

import org.sensorhub.api.config.DisplayInfo;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/**
 * Configuration for OSCAR's immutable Parquet observation tier.
 *
 * <p>The defaults deliberately copy data without deleting PostgreSQL rows.
 * Hot deletion is a separately controlled rollout step and requires verified
 * query parity plus a rollback grace period.</p>
 */
public class TieredArchiveConfig {
    /**
     * Controls when an allowlisted immutable observation becomes eligible for
     * Parquet.  The default retains the established age-based cold-tier
     * behaviour.  PARQUET_PRIMARY still commits observations to PostgreSQL
     * first, but treats short-lived LOGGED children as a bounded durable staging tier
     * and exports completed UTC windows shortly after they close.
     */
    public enum StorageMode {
        HOT_RETENTION,
        PARQUET_PRIMARY
    }

    /**
     * DuckDB needs room for the staged table, sort keys, compression buffers,
     * and the Parquet writer at the same time.  Limiting source payload to one
     * eighth of the session cap keeps those structures bounded without raising
     * the native-memory budget on small installations.
     */
    private static final long EXPORT_MEMORY_SAFETY_DIVISOR = 8L;
    private static final long BYTES_PER_MEBIBYTE = 1024L * 1024L;

    @DisplayInfo(label = "Enable cold archive")
    public boolean enabled = true;

    @DisplayInfo(label = "Observation storage mode",
            desc = "PARQUET_PRIMARY is an explicit fresh-database mode for allowlisted immutable outputs; " +
                    "ordinary PostGIS and the default tiered mode are unchanged")
    public StorageMode storageMode = StorageMode.HOT_RETENTION;

    @DisplayInfo(label = "Archive root", desc = "Durable path mounted outside the PostgreSQL volume")
    public String rootDirectory = "/var/lib/oscar/archive";

    @Min(1)
    @DisplayInfo(label = "Hot retention (days)")
    public int hotRetentionDays = 30;

    @Min(1)
    @DisplayInfo(label = "Parquet-primary archive delay (minutes)",
            desc = "Safety delay after a UTC staging window closes before its immutable rows are eligible for export")
    public int parquetPrimaryArchiveDelayMinutes = 5;

    @Min(1)
    @Max(60)
    @DisplayInfo(label = "Immutable staging window (minutes)",
            desc = "UTC-aligned Parquet-primary window; must divide one day evenly and cannot exceed one hour")
    public int immutableStageWindowMinutes = 30;

    @Min(0)
    @Max(15)
    @DisplayInfo(label = "Maximum future detector-clock skew (minutes)",
            desc = "Rejects immutable detector observations farther ahead of the PostgreSQL receipt clock; " +
                    "this prevents a bad detector clock from defeating bounded staging")
    public int maximumFuturePhenomenonSkewMinutes = 5;

    @Min(1)
    @DisplayInfo(label = "Parquet-primary worker interval (minutes)",
            desc = "How often completed immutable staging windows are exported, verified, and retired")
    public int parquetPrimaryIntervalMinutes = 1;

    @Min(1)
    @DisplayInfo(label = "Maximum staging partitions retired per cycle",
            desc = "Bounds whole-partition PostgreSQL reclamation work in one archive cycle")
    public int maximumStagePartitionsPerCycle = 8;

    @Min(1)
    @DisplayInfo(label = "Archive interval (minutes)")
    public int intervalMinutes = 60;

    @Min(1)
    @DisplayInfo(label = "Maximum generations per cycle",
            desc = "Bounds catch-up work while allowing an installation with a backlog to archive more than one window per interval")
    public int maximumGenerationsPerCycle = 24;

    @Min(1)
    @DisplayInfo(label = "Maximum parity verifications per cycle",
            desc = "Independent bound on generations reread from Parquet and PostgreSQL before new exports start")
    public int maximumVerificationsPerCycle = 2;

    @Min(1)
    @DisplayInfo(label = "Maximum hot-generation deletions per cycle",
            desc = "Independent bound on verified generations whose PostgreSQL staging rows may be deleted per cycle")
    public int maximumDeletionsPerCycle = 10;

    @Min(1)
    @DisplayInfo(label = "Maximum archive cycle duration (minutes)",
            desc = "Stops starting new generations after this duration; an in-progress generation is allowed to finish safely")
    public int maximumCycleMinutes = 15;

    @Min(1000)
    @DisplayInfo(label = "Maximum rows per generation")
    public int maximumRowsPerGeneration = 2_000_000;

    @Min(1)
    @DisplayInfo(label = "Maximum uncompressed generation bytes",
            desc = "Operator cap for JSON and row payload admitted to one archive generation; " +
                    "the runtime also applies a conservative cap derived from DuckDB memory")
    public long maximumUncompressedBytesPerGeneration = 2L * 1024 * 1024 * 1024;

    @Min(16)
    @DisplayInfo(label = "DuckDB memory limit (MiB)",
            desc = "Per archive export, verification, or cold-query connection; larger work spills to the bounded temporary directory")
    public int duckDbMemoryLimitMegabytes = 256;

    @Min(1)
    @Max(64)
    @DisplayInfo(label = "DuckDB threads",
            desc = "Maximum worker threads used by each archive export, verification, or cold-query connection")
    public int duckDbThreads = 2;

    @Min(16)
    @DisplayInfo(label = "DuckDB temporary limit (MiB)",
            desc = "Maximum spill space used by one archive operation")
    public long duckDbMaximumTemporaryMegabytes = 4096;

    @Min(1)
    @Max(32)
    @DisplayInfo(label = "Maximum concurrent DuckDB sessions",
            desc = "Process-wide admission limit shared by archive exports, verification, deletion, and cold queries")
    public int duckDbMaximumConcurrentSessions = 2;

    @Min(1)
    @Max(300)
    @DisplayInfo(label = "DuckDB admission timeout (seconds)",
            desc = "Maximum time an archive operation waits for process-wide DuckDB capacity before failing busy")
    public int duckDbSessionAcquireTimeoutSeconds = 30;

    @DisplayInfo(label = "DuckDB temporary directory",
            desc = "Private writable parent for per-operation spill directories; do not place it inside the immutable archive tree")
    public String duckDbTemporaryDirectory = "/var/lib/oscar/duckdb-tmp";

    @Min(1)
    @DisplayInfo(label = "Delete transaction size")
    public int deleteBatchSize = 5_000;

    @Min(1)
    @DisplayInfo(label = "Rollback grace (days)")
    public int rollbackGraceDays = 7;

    @DisplayInfo(label = "Retire verified PostgreSQL source",
            desc = "HOT_RETENTION deletes verified rows; PARQUET_PRIMARY drops a verified staging child")
    public boolean deleteHotAfterArchive = false;

    @DisplayInfo(label = "Require parity receipt (mandatory)",
            desc = "Safety invariant: false is rejected; source retirement always requires successful parity")
    public boolean requireParityReceipt = true;

    @Min(1)
    @Max(22)
    @DisplayInfo(label = "ZSTD compression level")
    public int zstdCompressionLevel = 6;

    private static final List<String> LEGACY_DEFAULT_OUTPUT_NAMES = List.of(
            "gammaCounts", "gammaThreshold", "neutronCounts", "setupGamma", "setupNeutron");

    private static final List<String> DEFAULT_OUTPUT_NAMES = List.of(
            "gammaCounts", "gammaThreshold", "neutronCounts", "setupGamma", "setupNeutron",
            "speed", "backgroundReport", "foregroundReport", "status",
            "emlContextual", "emlScanContextual", "emlAnalysis");

    @DisplayInfo(label = "Archive output names", desc = "Only high-volume raw/rollup streams are cold-tiered by default")
    public List<String> outputNames = new ArrayList<>(DEFAULT_OUTPUT_NAMES);

    /**
     * Returns the actual source-payload budget for one Parquet generation.
     *
     * <p>The configured generation cap remains an operator-controlled upper
     * bound, but it cannot admit a batch grossly larger than the memory budget
     * of the DuckDB session that must stage, sort, and encode it.  This also
     * makes upgraded installations with the former 2 GiB generation setting
     * safe without rewriting their persisted configuration.</p>
     */
    public long effectiveMaximumUncompressedBytesPerGeneration() {
        long duckDbMemoryBytes = Math.multiplyExact(
                (long) duckDbMemoryLimitMegabytes, BYTES_PER_MEBIBYTE);
        long memorySafePayloadBytes = Math.max(1L,
                duckDbMemoryBytes / EXPORT_MEMORY_SAFETY_DIVISOR);
        return Math.min(maximumUncompressedBytesPerGeneration, memorySafePayloadBytes);
    }

    public boolean isParquetPrimary() {
        return storageMode == StorageMode.PARQUET_PRIMARY;
    }

    /**
     * Returns the exclusive archive cutoff while preserving completed UTC
     * staging windows. PostgreSQL remains the durable landing layer until
     * manifest-last activation, exact parity, and verified whole-child retirement.
     */
    public Instant archiveCutoff(Instant now) {
        if (now == null)
            throw new IllegalArgumentException("Archive clock value is required");
        if (isParquetPrimary())
            return utcWindowStart(
                    now.minus(parquetPrimaryArchiveDelayMinutes, ChronoUnit.MINUTES),
                    immutableStageWindowMinutes);
        // Preserve the established HOT_RETENTION behaviour exactly. Existing
        // deployments continue to archive whole UTC hours after the day-based
        // retention period, irrespective of the immutable staging setting.
        return now.minus(hotRetentionDays, ChronoUnit.DAYS).truncatedTo(ChronoUnit.HOURS);
    }

    /** Floors an instant to a stable UTC-aligned window boundary. */
    public static Instant utcWindowStart(Instant instant, int windowMinutes) {
        if (instant == null)
            throw new IllegalArgumentException("Archive window instant is required");
        if (windowMinutes < 1 || windowMinutes > 60 || 1440 % windowMinutes != 0)
            throw new IllegalArgumentException(
                    "Archive window minutes must be between 1 and 60 and divide 1440 evenly");
        long windowSeconds = Math.multiplyExact((long) windowMinutes, 60L);
        return Instant.ofEpochSecond(Math.multiplyExact(
                Math.floorDiv(instant.getEpochSecond(), windowSeconds), windowSeconds));
    }

    /** Rejects unsafe compatibility settings that cannot weaken invariants. */
    public void validate() {
        if (storageMode == null)
            throw new IllegalArgumentException("storageMode is required");
        if (isParquetPrimary() && !enabled)
            throw new IllegalArgumentException("PARQUET_PRIMARY requires the cold archive to be enabled");
        if (isParquetPrimary() && !deleteHotAfterArchive)
            throw new IllegalArgumentException(
                    "PARQUET_PRIMARY requires deleteHotAfterArchive so PostgreSQL remains a bounded staging tier");
        if (parquetPrimaryArchiveDelayMinutes < 1)
            throw new IllegalArgumentException("parquetPrimaryArchiveDelayMinutes must be positive");
        if (immutableStageWindowMinutes < 1 || immutableStageWindowMinutes > 60 ||
                1440 % immutableStageWindowMinutes != 0)
            throw new IllegalArgumentException(
                    "immutableStageWindowMinutes must be between 1 and 60 and divide 1440 evenly");
        if (maximumFuturePhenomenonSkewMinutes < 0 ||
                maximumFuturePhenomenonSkewMinutes > 15)
            throw new IllegalArgumentException(
                    "maximumFuturePhenomenonSkewMinutes must be between 0 and 15");
        if (parquetPrimaryIntervalMinutes < 1)
            throw new IllegalArgumentException("parquetPrimaryIntervalMinutes must be positive");
        if (isParquetPrimary() && immutableStageWindowMinutes +
                parquetPrimaryArchiveDelayMinutes + parquetPrimaryIntervalMinutes +
                maximumFuturePhenomenonSkewMinutes > 60)
            throw new IllegalArgumentException(
                    "PARQUET_PRIMARY staging window, archive delay, worker interval, and " +
                            "maximum future clock skew must total no more than 60 minutes");
        if (maximumStagePartitionsPerCycle < 1)
            throw new IllegalArgumentException("maximumStagePartitionsPerCycle must be positive");
        if (maximumGenerationsPerCycle < 1)
            throw new IllegalArgumentException("maximumGenerationsPerCycle must be positive");
        if (maximumVerificationsPerCycle < 1)
            throw new IllegalArgumentException("maximumVerificationsPerCycle must be positive");
        if (maximumDeletionsPerCycle < 1)
            throw new IllegalArgumentException("maximumDeletionsPerCycle must be positive");
        if (isParquetPrimary() && maximumVerificationsPerCycle < maximumGenerationsPerCycle)
            throw new IllegalArgumentException(
                    "PARQUET_PRIMARY requires maximumVerificationsPerCycle to be at least " +
                            "maximumGenerationsPerCycle so parity backlog cannot grow by configuration");
        if (outputNames != null && outputNames.equals(LEGACY_DEFAULT_OUTPUT_NAMES))
            outputNames = new ArrayList<>(DEFAULT_OUTPUT_NAMES);
        if (isParquetPrimary() && (outputNames == null || outputNames.isEmpty()))
            throw new IllegalArgumentException("At least one immutable archive output name is required");
        if (isParquetPrimary() && outputNames.stream().anyMatch(name -> name == null || name.isBlank()))
            throw new IllegalArgumentException("Archive output names must be non-blank");
        if (isParquetPrimary() && new HashSet<>(outputNames).size() != outputNames.size())
            throw new IllegalArgumentException("Archive output names must be unique");
        if (!requireParityReceipt)
            throw new IllegalArgumentException(
                    "requireParityReceipt is a mandatory safety invariant and cannot be disabled");
        if (maximumRowsPerGeneration < 1)
            throw new IllegalArgumentException("maximumRowsPerGeneration must be positive");
        if (maximumUncompressedBytesPerGeneration < 1)
            throw new IllegalArgumentException("maximumUncompressedBytesPerGeneration must be positive");
        if (duckDbMemoryLimitMegabytes < 16)
            throw new IllegalArgumentException("duckDbMemoryLimitMegabytes must be at least 16 MiB");
        if (effectiveMaximumUncompressedBytesPerGeneration() < 1)
            throw new IllegalArgumentException("Effective archive generation byte limit must be positive");
        if (duckDbThreads < 1 || duckDbThreads > 64)
            throw new IllegalArgumentException("duckDbThreads must be between 1 and 64");
        if (duckDbMaximumTemporaryMegabytes < 16)
            throw new IllegalArgumentException("duckDbMaximumTemporaryMegabytes must be at least 16 MiB");
        if (duckDbMaximumConcurrentSessions < 1 || duckDbMaximumConcurrentSessions > 32)
            throw new IllegalArgumentException(
                    "duckDbMaximumConcurrentSessions must be between 1 and 32");
        if (duckDbSessionAcquireTimeoutSeconds < 1 || duckDbSessionAcquireTimeoutSeconds > 300)
            throw new IllegalArgumentException(
                    "duckDbSessionAcquireTimeoutSeconds must be between 1 and 300");
        if (duckDbTemporaryDirectory == null || duckDbTemporaryDirectory.isBlank())
            throw new IllegalArgumentException("duckDbTemporaryDirectory is required");
        if (rootDirectory == null || rootDirectory.isBlank())
            throw new IllegalArgumentException("rootDirectory is required");
        final Path archiveRoot;
        final Path temporaryRoot;
        final Path persistedStateRoot;
        try {
            archiveRoot = Path.of(rootDirectory).toAbsolutePath().normalize();
            temporaryRoot = Path.of(duckDbTemporaryDirectory).toAbsolutePath().normalize();
            persistedStateRoot = Path.of("/var/lib/oscar").toAbsolutePath().normalize();
        } catch (InvalidPathException error) {
            throw new IllegalArgumentException("Archive and DuckDB temporary paths must be valid", error);
        }
        Path reservedStateTemporaryRoot = persistedStateRoot.resolve("duckdb-tmp");
        if (temporaryRoot.startsWith(persistedStateRoot) &&
                !temporaryRoot.equals(reservedStateTemporaryRoot))
            throw new IllegalArgumentException(
                    "DuckDB scratch inside /var/lib/oscar must use /var/lib/oscar/duckdb-tmp so backups exclude it");
        if (archiveRoot.equals(temporaryRoot) || archiveRoot.startsWith(temporaryRoot) ||
                temporaryRoot.startsWith(archiveRoot))
            throw new IllegalArgumentException(
                    "DuckDB temporary directory and immutable archive root must be disjoint");
    }
}
