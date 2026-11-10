package org.sensorhub.impl.datastore.postgis;

import org.junit.Test;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig.StorageMode;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderDataStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.database.OscarTieredObsSystemDatabase;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.api.datastore.obs.IObsStore;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestTieredArchiveConfiguration {
    @Test
    public void hotDeletionIsFailSafeByDefault() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        assertTrue(config.enabled);
        assertEquals(StorageMode.HOT_RETENTION, config.storageMode);
        assertFalse(config.isParquetPrimary());
        assertEquals(2, config.maximumVerificationsPerCycle);
        assertFalse(config.deleteHotAfterArchive);
        assertTrue(config.requireParityReceipt);
        assertTrue(config.maximumGenerationsPerCycle > 1);
        assertTrue(config.maximumCycleMinutes > 0);
        assertEquals(30, config.immutableStageWindowMinutes);
        assertEquals(5, config.maximumFuturePhenomenonSkewMinutes);
        assertEquals(1, config.parquetPrimaryIntervalMinutes);
        assertTrue(config.maximumStagePartitionsPerCycle > 0);
        assertTrue(config.maximumUncompressedBytesPerGeneration > 0);
        assertTrue(config.effectiveMaximumUncompressedBytesPerGeneration() <=
                config.maximumUncompressedBytesPerGeneration);
        assertTrue(config.duckDbMemoryLimitMegabytes >= 16);
        assertTrue(config.duckDbThreads > 0);
        assertTrue(config.duckDbMaximumTemporaryMegabytes >= 16);
        assertTrue(config.duckDbMaximumConcurrentSessions > 0);
        assertTrue(config.duckDbSessionAcquireTimeoutSeconds > 0);
        assertFalse(config.duckDbTemporaryDirectory.isBlank());
        assertTrue(config.outputNames.contains("gammaCounts"));
        assertTrue(config.outputNames.contains("occupancy"));
        assertTrue(config.outputNames.contains("adjudication"));
        assertTrue(config.outputNames.contains("report"));
        assertTrue(config.outputNames.contains("connectionStatus"));
        assertTrue(config.outputNames.contains("occupancyStatus"));
        assertFalse(config.outputNames.contains("sensorLocation"));
    }

    @Test
    public void parquetPrimaryIsExplicitAndRequiresVerifiedStageRetirement() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.storageMode = StorageMode.PARQUET_PRIMARY;

        IllegalArgumentException retained = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(retained.getMessage().contains("deleteHotAfterArchive"));

        config.deleteHotAfterArchive = true;
        IllegalArgumentException verificationBacklog = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(verificationBacklog.getMessage().contains("maximumVerificationsPerCycle"));

        config.maximumGenerationsPerCycle = 2;
        config.validate();
        assertTrue(config.isParquetPrimary());
        assertEquals(config.maximumGenerationsPerCycle, config.maximumVerificationsPerCycle);

        config.enabled = false;
        IllegalArgumentException disabled = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(disabled.getMessage().contains("requires the cold archive"));

        config.enabled = true;
        config.outputNames = java.util.List.of("gammaCounts", "gammaCounts");
        IllegalArgumentException duplicate = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(duplicate.getMessage().contains("unique"));
    }

    @Test
    public void parquetPrimaryArchivesOnlyCompletedUtcStageWindows() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.hotRetentionDays = 2;
        config.immutableStageWindowMinutes = 15;
        // HOT_RETENTION keeps its historical whole-hour cutoff and does not
        // inherit the Parquet-primary staging window.
        assertEquals(Instant.parse("2026-01-01T13:00:00Z"),
                config.archiveCutoff(Instant.parse("2026-01-03T13:47:00Z")));

        config.storageMode = StorageMode.PARQUET_PRIMARY;
        config.deleteHotAfterArchive = true;
        config.parquetPrimaryArchiveDelayMinutes = 5;
        config.immutableStageWindowMinutes = 30;

        assertEquals(Instant.parse("2026-01-01T13:00:00Z"),
                config.archiveCutoff(Instant.parse("2026-01-01T13:34:59.999999999Z")));
        assertEquals(Instant.parse("2026-01-01T13:30:00Z"),
                config.archiveCutoff(Instant.parse("2026-01-01T13:35:00Z")));
        assertEquals(Instant.parse("2026-01-01T23:30:00Z"),
                config.archiveCutoff(Instant.parse("2026-01-02T00:04:59Z")));
        assertEquals(Instant.parse("2026-01-02T00:00:00Z"),
                config.archiveCutoff(Instant.parse("2026-01-02T00:05:00Z")));
    }

    @Test
    public void archiveGenerationAndVerificationBudgetCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.maximumGenerationsPerCycle = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.maximumGenerationsPerCycle = 1;
        config.maximumVerificationsPerCycle = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.maximumVerificationsPerCycle = 1;
        config.maximumDeletionsPerCycle = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void immutableStageWindowMustDivideUtcDayAndNotExceedOneHour() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.immutableStageWindowMinutes = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.immutableStageWindowMinutes = 50;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.immutableStageWindowMinutes = 61;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.immutableStageWindowMinutes = 60;
        config.validate();
        config.parquetPrimaryIntervalMinutes = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.parquetPrimaryIntervalMinutes = 1;
        config.maximumStagePartitionsPerCycle = 0;
        assertThrows(IllegalArgumentException.class, config::validate);

        config.maximumStagePartitionsPerCycle = 1;
        config.maximumFuturePhenomenonSkewMinutes = -1;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.maximumFuturePhenomenonSkewMinutes = 16;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void parquetPrimaryNominalStageResidenceMustFitOneHour() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.storageMode = StorageMode.PARQUET_PRIMARY;
        config.deleteHotAfterArchive = true;
        config.immutableStageWindowMinutes = 30;
        config.parquetPrimaryArchiveDelayMinutes = 5;
        config.parquetPrimaryIntervalMinutes = 1;
        config.maximumFuturePhenomenonSkewMinutes = 5;
        config.maximumGenerationsPerCycle = 2;
        config.validate();

        config.parquetPrimaryArchiveDelayMinutes = 25;
        IllegalArgumentException overBudget = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(overBudget.getMessage().contains("total no more than 60 minutes"));
    }

    @Test
    public void parquetPrimaryLifecycleBudgetsMustKeepPaceWithExports() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.storageMode = StorageMode.PARQUET_PRIMARY;
        config.deleteHotAfterArchive = true;
        config.maximumGenerationsPerCycle = 3;
        config.maximumVerificationsPerCycle = 2;
        IllegalArgumentException verificationBacklog = assertThrows(
                IllegalArgumentException.class, config::validate);
        assertTrue(verificationBacklog.getMessage().contains("maximumVerificationsPerCycle"));

        config.maximumVerificationsPerCycle = 3;
        config.maximumDeletionsPerCycle = 1;
        config.validate(); // row-deletion budget is HOT_RETENTION-only
    }

    @Test
    public void parquetPrimaryOmitsGenericPayloadIndexButRetainsEventIndexes() {
        QueryBuilderObsStore ordinary = new QueryBuilderObsStore("obs", true);
        String ordinaryInitialization = String.join("\n", ordinary.createInitializationQueries());
        assertTrue(ordinaryInitialization.contains("USING GIN(result)"));
        assertTrue(ordinary.createEventFilterIndexQueries().length > 0);

        QueryBuilderObsStore primary = new QueryBuilderObsStore("obs", false);
        String primaryInitialization = String.join("\n", primary.createInitializationQueries());
        assertFalse(primaryInitialization.contains("USING GIN"));
        String primaryEventIndexes = String.join("\n", primary.createEventFilterIndexQueries());
        assertTrue(primaryEventIndexes.contains("jsonb_path_ops"));
        assertTrue(primaryEventIndexes.contains("occupancy_count_idx"));
        assertTrue(primaryInitialization.contains("obs_unique_constraint_v2"));
        assertTrue(primaryEventIndexes.contains("obs_datastream_phenomenon_time_id_nulls_first_idx"));
        assertTrue(primaryInitialization.contains("DROP INDEX IF EXISTS obs_datastream_idx"));
        assertFalse(primaryInitialization.contains("CREATE INDEX IF NOT EXISTS obs_datastream_idx"));
    }

    @Test
    public void parityReceiptCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.requireParityReceipt = false;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void archivePayloadByteCapCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.maximumUncompressedBytesPerGeneration = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void archivePayloadBudgetIsBoundedByDuckDbMemory() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.duckDbMemoryLimitMegabytes = 256;
        config.maximumUncompressedBytesPerGeneration = Long.MAX_VALUE;
        assertEquals(32L * 1024 * 1024,
                config.effectiveMaximumUncompressedBytesPerGeneration());

        config.maximumUncompressedBytesPerGeneration = 8L * 1024 * 1024;
        assertEquals(8L * 1024 * 1024,
                config.effectiveMaximumUncompressedBytesPerGeneration());
    }

    @Test
    public void duckDbResourceCapsCannotBeDisabled() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.duckDbMemoryLimitMegabytes = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbMemoryLimitMegabytes = 256;
        config.duckDbThreads = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbThreads = 2;
        config.duckDbMaximumTemporaryMegabytes = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbMaximumTemporaryMegabytes = 4096;
        config.duckDbMaximumConcurrentSessions = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 0;
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbSessionAcquireTimeoutSeconds = 30;
        config.duckDbTemporaryDirectory = "";
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void duckDbScratchInsidePersistedStateMustUseReservedExcludedPath() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.validate();

        config.duckDbTemporaryDirectory = "/var/lib/oscar/other-scratch";
        assertThrows(IllegalArgumentException.class, config::validate);
        config.duckDbTemporaryDirectory = "/var/lib/oscar/../oscar/duckdb-tmp/child";
        assertThrows(IllegalArgumentException.class, config::validate);

        config.duckDbTemporaryDirectory = "/srv/oscar-duckdb-tmp";
        config.validate();
    }

    @Test
    public void duckDbScratchAndArchiveRootsMustBeDisjoint() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = "/srv/oscar-archive";
        config.duckDbTemporaryDirectory = "/srv/oscar-archive/duckdb-tmp";
        assertThrows(IllegalArgumentException.class, config::validate);

        config.duckDbTemporaryDirectory = "/srv";
        assertThrows(IllegalArgumentException.class, config::validate);
    }

    @Test
    public void archiveWindowsAreAnchoredToUtcHours() {
        assertEquals(Instant.parse("2026-01-01T12:00:00Z"),
                OscarTieredObsSystemDatabase.utcHourWindowStart(
                        Instant.parse("2026-01-01T12:34:56.789Z")));
        assertEquals(Instant.parse("2026-01-01T12:30:00Z"),
                OscarTieredObsSystemDatabase.utcWindowStart(
                        Instant.parse("2026-01-01T12:34:56.789Z"), 30));
        assertEquals(Instant.parse("1969-12-31T23:30:00Z"),
                OscarTieredObsSystemDatabase.utcWindowStart(
                        Instant.parse("1969-12-31T23:59:59.999Z"), 30));
        assertThrows(IllegalArgumentException.class, () ->
                OscarTieredObsSystemDatabase.utcWindowStart(
                        Instant.parse("2026-01-01T12:34:56.789Z"), 50));
    }

    @Test
    public void archiveWindowBisectionHalvesAndClampsAtOneSecond() {
        Instant start = Instant.parse("2026-01-01T12:00:00Z");
        assertEquals(start.plusSeconds(30), OscarTieredObsStore.bisectArchiveWindowEnd(
                start, start.plusSeconds(60)));
        assertEquals(start.plusSeconds(1), OscarTieredObsStore.bisectArchiveWindowEnd(
                start, start.plusMillis(1_500)));
        assertThrows(IllegalArgumentException.class, () ->
                OscarTieredObsStore.bisectArchiveWindowEnd(start, start.plusSeconds(1)));
    }

    @Test
    public void archiveByteEstimateCountsUtf8RatherThanJavaCharacters() {
        String resultJson = "{\"label\":\"gamma-γ-🚀\"}";
        assertEquals(ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW +
                        resultJson.getBytes(StandardCharsets.UTF_8).length,
                ArchiveExporter.estimateUncompressedRowBytes(resultJson));
        assertEquals(ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW,
                ArchiveExporter.estimateUncompressedRowBytes(null));
    }

    @Test
    public void postgresTimestampPrecisionMatchesPgjdbcHalfUpMicroseconds() {
        assertEquals(Instant.parse("2026-01-01T00:00:00.123457Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.123456500Z")));
        assertEquals(Instant.parse("2026-01-01T00:00:00.123456Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.123455500Z")));
        assertEquals(Instant.parse("2026-01-01T00:00:01Z"),
                UtcJdbcTime.toDatabasePrecision(Instant.parse("2026-01-01T00:00:00.999999999Z")));
    }

    @Test
    public void datastreamRangeTriggerAggregatesTransitionTable() {
        QueryBuilderDataStreamStore dataStreams = new QueryBuilderDataStreamStore("obs_datastreams");
        IObsStore observations = (IObsStore) Proxy.newProxyInstance(
                IObsStore.class.getClassLoader(), new Class<?>[]{IObsStore.class},
                (proxy, method, args) -> "getDatastoreName".equals(method.getName()) ? "obs" : null);
        dataStreams.linkTo(observations);
        String sql = dataStreams.createMinMaxTimeTriggerQuery();
        assertTrue(sql.contains("REFERENCING NEW TABLE AS inserted_observations"));
        assertTrue(sql.contains("FOR EACH STATEMENT"));
        assertFalse(sql.contains("FOR EACH ROW"));
        assertTrue(sql.contains("GROUP BY dataStreamID"));
    }

    @Test
    public void archiveMarkerMigrationIsIdempotentAndIndexesOnlyUnarchivedRows() {
        QueryBuilderObsStore observations = new QueryBuilderObsStore("obs");
        assertTrue(observations.addArchiveGenerationColumnQuery()
                .contains("ADD COLUMN IF NOT EXISTS archivegenerationid uuid"));
        String index = observations.createUnarchivedTimeIndexQuery();
        assertTrue(index.contains("CREATE INDEX IF NOT EXISTS"));
        assertTrue(index.contains("(phenomenontime, id)"));
        assertTrue(index.contains("WHERE archivegenerationid IS NULL"));
        String generationIndex = observations.createArchiveGenerationIndexQuery();
        assertTrue(generationIndex.contains("(archivegenerationid, id)"));
        assertTrue(generationIndex.contains("WHERE archivegenerationid IS NOT NULL"));
    }
}
