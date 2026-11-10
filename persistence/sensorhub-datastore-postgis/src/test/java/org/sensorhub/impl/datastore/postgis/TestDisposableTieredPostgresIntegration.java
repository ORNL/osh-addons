package org.sensorhub.impl.datastore.postgis;

import com.google.common.collect.Range;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.opengis.swe.v20.DataBlock;
import org.junit.Test;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveCatalog;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration;
import org.sensorhub.impl.datastore.postgis.archive.ArchivedObservation;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.database.OscarTieredObsSystemDatabase;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;
import org.sensorhub.impl.datastore.postgis.store.feature.PostgisFoiStoreImpl;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisDataStreamStoreImpl;
import org.sensorhub.impl.datastore.postgis.store.obs.PostgisObsStoreImpl;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.vast.data.DataBlockDouble;
import org.vast.data.TextEncodingImpl;
import org.vast.ogc.gml.GenericTemporalFeatureImpl;
import org.vast.swe.SWEHelper;
import org.vast.util.TimeExtent;

import javax.xml.namespace.QName;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/** Disposable, explicitly selected real-PostgreSQL integration probe. */
public class TestDisposableTieredPostgresIntegration extends TestPostgisObsStore {
    private static final String TABLE = "codex_storage_it_obs";
    private static final int ACTIVATION_LOCK_CLASS = 0x4f534341;
    private static final int ACTIVATION_LOCK_OBJECT = 0x41524348;
    private TieredArchiveConfig archiveConfig;
    private PostgisFoiStoreImpl disposableFoiStore;

    @Override
    protected PostgisObsStoreImpl initStore() throws Exception {
        String archiveDir = System.getenv("OSCAR_IT_ARCHIVE_DIR");
        org.junit.Assume.assumeTrue("OSCAR_IT_ARCHIVE_DIR enables the disposable PostgreSQL probe",
                archiveDir != null && !archiveDir.isBlank());
        Path root = Path.of(archiveDir).toAbsolutePath().normalize();
        Files.createDirectories(root);

        archiveConfig = new TieredArchiveConfig();
        archiveConfig.rootDirectory = root.toString();
        archiveConfig.duckDbTemporaryDirectory = root.resolveSibling(
                root.getFileName() + ".duckdb-test-tmp").toString();
        archiveConfig.outputNames = List.of("gammaCounts");
        archiveConfig.deleteBatchSize = 2;
        archiveConfig.rollbackGraceDays = 1;
        archiveConfig.deleteHotAfterArchive = false;
        this.postgisObsStore = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        return this.postgisObsStore;
    }

    @Override
    @Test
    public void testGetDatastoreName() throws Exception {
        assertEquals(TABLE, postgisObsStore.getDatastoreName());
        forceReadBackFromStorage();
    }

    @Test
    public void hotOnlyCountUsesSqlFastPathWithoutBypassingJavaPredicate() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        var stream = addSimpleDataStream(bigId(51), "hotOnlyCount");
        Instant start = Instant.parse("2026-02-01T00:00:00Z");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 6, 1_000L);
        forceReadBackFromStorage();

        assertEquals(6, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(stream)
                .build()));
        assertEquals("Exact FOI IDs must not require a linked FOI metadata store", 6,
                tiered.countMatchingEntries(new ObsFilter.Builder()
                        .withDataStreams(stream)
                        .withFois(BigId.NONE)
                        .build()));
        assertEquals(3, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(stream)
                .withValuePredicate(observation ->
                        observation.getResult().getDoubleValue(0) >= 3.0)
                .build()));
    }

    @Test
    public void hotOnlyCountCapsSqlFastPathAtFiniteLimit() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId stream = addSimpleDataStream(bigId(53), "finiteCount");
        addSimpleObsWithoutResultTime(stream, BigId.NONE,
                Instant.parse("2026-02-01T00:30:00Z"), 6, 1_000L);
        forceReadBackFromStorage();

        assertEquals(2, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(stream)
                .withLimit(2)
                .build()));
    }

    @Test
    public void hotOnlyLatestCountRetainsOneObservationPerStream() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId first = addSimpleDataStream(bigId(54), "latestCountFirst");
        BigId second = addSimpleDataStream(bigId(55), "latestCountSecond");
        Instant start = Instant.parse("2026-02-01T00:45:00Z");
        addSimpleObsWithoutResultTime(first, BigId.NONE, start, 3, 1_000L);
        addSimpleObsWithoutResultTime(second, BigId.NONE, start, 2, 1_000L);
        forceReadBackFromStorage();

        assertEquals(2, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(first, second)
                .withLatestResult()
                .build()));
    }

    @Test
    public void hotOnlyCountResolvesNestedLatestAndCurrentDataStreamVersions() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);
        BigId system = bigId(57);
        String outputName = "versionedCount";
        BigId oldVersion = addVersionedDataStream(system, outputName,
                TimeExtent.period(now.minus(2, ChronoUnit.DAYS),
                        now.minus(1, ChronoUnit.DAYS)));
        BigId currentVersion = addVersionedDataStream(system, outputName,
                TimeExtent.beginAt(now.minus(1, ChronoUnit.HOURS)));
        addSimpleObsWithoutResultTime(oldVersion, BigId.NONE,
                now.minus(2, ChronoUnit.DAYS), 5, 1_000L);
        addSimpleObsWithoutResultTime(currentVersion, BigId.NONE,
                now.minus(30, ChronoUnit.MINUTES), 3, 1_000L);
        forceReadBackFromStorage();

        for (TemporalFilter validTime : List.of(
                new TemporalFilter.Builder().withLatestTime().build(),
                new TemporalFilter.Builder().withCurrentTime().build())) {
            ObsFilter filter = new ObsFilter.Builder()
                    .withDataStreams(new DataStreamFilter.Builder()
                            .withOutputNames(outputName)
                            .withValidTime(validTime)
                            .build())
                    .build();
            assertEquals(3, tiered.countMatchingEntries(filter));
            try (var rows = tiered.selectEntries(filter)) {
                assertEquals(3, rows.count());
            }
        }
    }

    @Test
    public void hotOnlyCountDoesNotMultiplyObservationsAcrossFoiVersions() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        disposableFoiStore = new PostgisFoiStoreImpl(url, DB_NAME, login, password,
                TABLE + "_fois", DATABASE_NUM, IdProviderType.SEQUENTIAL, false);
        tiered.linkTo(disposableFoiStore);

        String uid = "urn:oscar:test:versioned-foi";
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC).truncatedTo(ChronoUnit.SECONDS);
        GenericTemporalFeatureImpl oldVersion = temporalFoi(uid, "old",
                now.minusDays(2), now.minusDays(1));
        GenericTemporalFeatureImpl currentVersion = temporalFoi(uid, "current",
                now.minusHours(1), now.plusDays(1));
        var oldKey = disposableFoiStore.add(BigId.NONE, oldVersion);
        var currentKey = disposableFoiStore.add(BigId.NONE, currentVersion);
        // Reproduce the normal temporal-version layout explicitly. The legacy
        // feature writer can assign a fresh ID when adding the second version,
        // while production migrations preserve the logical FOI ID.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE " + TABLE + "_fois SET id=? WHERE id=?")) {
            statement.setLong(1, oldKey.getInternalID().getIdAsLong());
            statement.setLong(2, currentKey.getInternalID().getIdAsLong());
            assertEquals(1, statement.executeUpdate());
        }

        BigId stream = addSimpleDataStream(bigId(58), "foiVersionCount");
        addSimpleObsWithoutResultTime(stream, oldKey.getInternalID(),
                now.toInstant().minus(30, ChronoUnit.MINUTES), 4, 1_000L);
        forceReadBackFromStorage();

        ObsFilter filter = new ObsFilter.Builder()
                .withFois(new FoiFilter.Builder().withUniqueIDs(uid).build())
                .build();
        assertEquals(4, tiered.countMatchingEntries(filter));
        try (var rows = tiered.selectEntries(filter)) {
            assertEquals(4, rows.count());
        }
    }

    @Test
    public void keysetPagingCrossesTwentyThousandEqualTimesWithSparsePredicate() throws Exception {
        BigId stream = addSimpleDataStream(bigId(56), "keysetBoundary");
        Instant equalTime = Instant.parse("2026-02-01T01:00:00Z");
        BigId seed = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, equalTime, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        String resultJson = hotResultJson(seed.getIdAsLong());

        int rowCount = 20_005;
        long equalIdBase = 900_000_000L;
        long equalFoiBase = 100_000L;
        bulkInsertKeysetRows(stream.getIdAsLong(), equalIdBase, equalFoiBase,
                equalTime, resultJson, rowCount);

        for (boolean descending : List.of(false, true)) {
            Set<Long> equalMatches = Set.of(equalFoiBase + 1, equalFoiBase + rowCount);
            ObsFilter equalFilter = new ObsFilter.Builder()
                    .withDataStreams(stream)
                    .withPhenomenonTime(new TemporalFilter.Builder()
                            .withSingleValue(equalTime)
                            .descendingOrder(descending)
                            .build())
                    .withValuePredicate(observation -> observation.getFoiID() != null &&
                            equalMatches.contains(observation.getFoiID().getIdAsLong()))
                    .withLimit(2)
                    .build();
            assertSparseKeysetIds(equalFilter,
                    List.of(equalIdBase + 1, equalIdBase + rowCount));
        }
    }

    @Test
    public void bulkRemoveDecodesOnlyWhenJavaPredicateNeedsTheObservation() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        var stream = addSimpleDataStream(bigId(52), "predicateRemove");
        Instant start = Instant.parse("2026-02-01T01:00:00Z");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 6, 1_000L);
        forceReadBackFromStorage();

        ObsFilter predicate = new ObsFilter.Builder()
                .withDataStreams(stream)
                .withValuePredicate(observation ->
                        observation.getResult().getDoubleValue(0) >= 3.0)
                .withLimit(2)
                .build();
        assertEquals(2, tiered.removeEntries(predicate));
        assertEquals(4, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(stream)
                .build()));
    }

    @Test
    public void scopedAbortCancelsRealPostgresWorkWithoutTouchingAnotherConnection() throws Exception {
        ConnectionManager manager = new ConnectionManager(url, DB_NAME, login, password);
        String mutationTable = "codex_cancel_scope_" + UUID.randomUUID().toString().replace("-", "");
        AtomicReference<ICancellableDataStore.CancellableOperation> operation = new AtomicReference<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicInteger backendPid = new AtomicInteger();
        AtomicBoolean completedNormally = new AtomicBoolean();
        CountDownLatch statementEntered = new CountDownLatch(1);
        CountDownLatch workerExited = new CountDownLatch(1);
        Thread worker = null;

        try (Connection unrelated = manager.getConnection();
             Statement setup = unrelated.createStatement()) {
            setup.execute("CREATE TABLE " + mutationTable + " (value integer NOT NULL)");

            worker = new Thread(() -> {
                try (ICancellableDataStore.CancellableOperation scope =
                             ConnectionManager.beginCancellableOperation();
                     Connection scoped = manager.getConnection()) {
                    operation.set(scope);
                    scoped.setAutoCommit(false);
                    try (Statement statement = scoped.createStatement()) {
                        statement.executeUpdate("INSERT INTO " + mutationTable + " VALUES (1)");
                        try (ResultSet result = statement.executeQuery("SELECT pg_backend_pid()")) {
                            assertTrue(result.next());
                            backendPid.set(result.getInt(1));
                        }
                        statementEntered.countDown();
                        statement.execute("SELECT pg_sleep(30)");
                    }
                    scoped.commit();
                    completedNormally.set(true);
                } catch (Throwable error) {
                    workerFailure.set(error);
                } finally {
                    workerExited.countDown();
                }
            }, "real-postgres-cancellable-operation");
            worker.setDaemon(true);
            worker.start();

            assertTrue("The worker must reach the blocking PostgreSQL statement",
                    statementEntered.await(5, TimeUnit.SECONDS));
            waitForActivePgSleep(unrelated, backendPid.get());
            assertEquals("An unrelated pooled connection must remain usable while the worker blocks",
                    41L, queryLong(unrelated, "SELECT 41"));

            long cancelStarted = System.nanoTime();
            operation.get().cancel();
            operation.get().cancel(); // Explicitly verify idempotency against the real driver.
            assertTrue("Connection.abort must release the blocked JDBC worker promptly",
                    workerExited.await(5, TimeUnit.SECONDS));
            waitForBackendExit(unrelated, backendPid.get());
            long cancelMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - cancelStarted);
            worker.join(1_000L);

            assertFalse("The cancelled transaction must not reach commit", completedNormally.get());
            assertNotNull("The blocked statement must observe connection cancellation", workerFailure.get());
            assertTrue("Cancellation must leave headroom inside the core 10 second stop budget: "
                    + cancelMillis + " ms", cancelMillis < 5_000L);
            assertEquals("The unrelated connection must survive scoped cancellation",
                    42L, queryLong(unrelated, "SELECT 42"));
            assertEquals("Aborting the scoped transaction must roll back its mutation",
                    0L, queryLong(unrelated, "SELECT count(*) FROM " + mutationTable));
            Thread.sleep(100L);
            assertEquals("No cancelled worker may mutate after termination",
                    0L, queryLong(unrelated, "SELECT count(*) FROM " + mutationTable));

            try (Connection replacement = manager.getConnection()) {
                assertEquals("The Hikari pool must remain healthy after evicting the aborted connection",
                        43L, queryLong(replacement, "SELECT 43"));
            }
        } finally {
            ICancellableDataStore.CancellableOperation active = operation.get();
            if (worker != null && worker.isAlive() && active != null)
                active.cancel();
            if (worker != null)
                worker.join(5_000L);
            try (Connection cleanup = DriverManager.getConnection(
                    "jdbc:postgresql://" + url + "/" + DB_NAME, login, password);
                 Statement statement = cleanup.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + mutationTable);
            } finally {
                manager.close();
            }
        }
    }

    @Override
    protected void forceReadBackFromStorage() {
        try {
            postgisObsStore.commit();
            postgisObsStore.getDataStreams().commit();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    @org.junit.After
    public void cleanup() throws Exception {
        if (disposableFoiStore != null) {
            disposableFoiStore.close();
            disposableFoiStore = null;
        }
        if (postgisObsStore != null) {
            postgisObsStore.close();
            dropDisposableTieredTables(TABLE);
            if (postgisObsStore.getDataStreams() != null)
                ((PostgisDataStreamStoreImpl) postgisObsStore.getDataStreams()).clearCache();
        }
        // This probe is opt-in and must point at a uniquely named disposable
        // PostgreSQL instance. Keep its global archive catalog isolated
        // between test methods without touching any OSCAR deployment.
        if (System.getenv("OSCAR_IT_ARCHIVE_DIR") != null) {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("TRUNCATE oscar_archive_job CASCADE");
                statement.execute("TRUNCATE oscar_retention_hold, oscar_deletion_plan, " +
                        "oscar_media_manifest, oscar_backup_epoch CASCADE");
            }
            Path root = Path.of(System.getenv("OSCAR_IT_ARCHIVE_DIR")).toAbsolutePath().normalize();
            if (Files.isDirectory(root)) {
                try (var paths = Files.walk(root)) {
                    for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                        if (!path.equals(root))
                            Files.deleteIfExists(path);
                    }
                }
            }
        }
    }

    @Test
    public void parquetPrimaryFreshTableUsesLeanIndexesAndSurvivesColdRestart() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;

        // Recreate the disposable table so this test proves the schema chosen
        // at first startup rather than observing indexes left by an upgrade.
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.parquetPrimaryArchiveDelayMinutes = 1;
        primaryConfig.maximumGenerationsPerCycle = 1;
        primaryConfig.maximumVerificationsPerCycle = 1;
        primaryConfig.maximumDeletionsPerCycle = 1;
        primaryConfig.rollbackGraceDays = 1;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        List<String> indexes = observationIndexNames();
        assertFalse("A fresh PARQUET_PRIMARY table must omit the broad result GIN",
                indexes.contains(TABLE + "_data_idx"));
        for (String required : List.of(
                TABLE + "_pkey",
                TABLE + "_unique_constraint_v2",
                TABLE + "_result_time_idx",
                TABLE + "_datastream_result_time_id_idx",
                TABLE + "_phenomenon_time_id_nulls_first_idx",
                TABLE + "_phenomenon_time_desc_id_nulls_first_idx",
                TABLE + "_datastream_phenomenon_time_id_nulls_first_idx",
                TABLE + "_datastream_phenomenon_time_desc_id_nulls_first_idx",
                TABLE + "_foi_idx",
                TABLE + "_unadjudicated_alarm_idx",
                TABLE + "_result_path_ops_idx",
                TABLE + "_result_occupancy_count_idx",
                TABLE + "_result_max_gamma_idx",
                TABLE + "_result_max_neutron_idx",
                TABLE + "_non_alarm_ptime_desc_idx")) {
            String pgName = required.length() > 63 ? required.substring(0, 63) : required;
            assertTrue("Missing required PARQUET_PRIMARY index " + pgName,
                    indexes.contains(pgName));
        }
        for (String redundant : List.of(
                TABLE + "_datastream_idx",
                TABLE + "_phenomenon_time_idx",
                TABLE + "_datastream_id_phenomenon_time_idx",
                TABLE + "_datastream_id_result_time_idx",
                TABLE + "_unarchived_time_idx",
                TABLE + "_archive_generation_idx")) {
            String pgName = redundant.length() > 63 ? redundant.substring(0, 63) : redundant;
            assertFalse("PARQUET_PRIMARY must omit redundant/unused index " + pgName,
                    indexes.contains(pgName));
        }
        assertTrue(indexDefinition(TABLE + "_result_path_ops_idx")
                .toLowerCase().contains("using gin (result jsonb_path_ops)"));
        for (String numeric : List.of("occupancy_count", "max_gamma", "max_neutron")) {
            String definition = indexDefinition(TABLE + "_result_" + numeric + "_idx")
                    .toLowerCase();
            assertTrue(definition.contains("datastreamid"));
            assertTrue(definition.contains("::numeric"));
        }

        Instant start = Instant.parse("2026-07-01T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(70), "gammaCounts");
        List<BigId> archivedKeys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 3, 60_000L).keySet().stream()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        BigId laterHotKey = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plus(2, ChronoUnit.HOURS), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(3, generation.rowCount());
        assertEquals(0, archivedHotRows());
        assertEquals(3, stageOwnerRows());

        tiered.maintainArchiveLifecycle();
        assertTrue(oneGeneration(tiered.getArchiveCatalog()).parityVerified());
        ageGeneration(generation.id());
        tiered.maintainArchiveLifecycle();

        assertEquals("Only the out-of-window control row may remain hot", 1, hotRows());
        assertEquals(0, archivedHotRows());
        assertEquals(0, stageOwnerRows());
        assertEquals("SEALED", oneGeneration(tiered.getArchiveCatalog()).state());
        assertTrue(hotRowExists(laterHotKey.getIdAsLong()));
        BigId coldKey = archivedKeys.get(1);
        assertNotNull(tiered.get(coldKey));
        try (var selected = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(coldKey)
                .build())) {
            var rows = selected.toList();
            assertEquals(1, rows.size());
            assertEquals(coldKey, rows.get(0).getKey());
        }

        // Reopen from only durable PostgreSQL catalog state and Parquet
        // objects; a point lookup must still find exactly the cold row.
        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;
        assertNotNull(tiered.get(coldKey));
        try (var selected = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(coldKey)
                .build())) {
            var rows = selected.toList();
            assertEquals(1, rows.size());
            assertEquals(coldKey, rows.get(0).getKey());
        }
        assertEquals(4, tiered.getNumRecords());
        assertFalse(observationIndexNames().contains(TABLE + "_data_idx"));
    }

    @Test
    public void parquetPrimaryRoutesRawRowsToLoggedDroppablePartitions() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant start = Instant.parse("2026-08-01T00:00:00Z");
        BigId rawStream = addSimpleDataStream(bigId(80), "gammaCounts");
        BigId mutableStream = addSimpleDataStream(bigId(81), "occupancy");
        BigId rawId = addSimpleObsWithoutResultTime(
                rawStream, BigId.NONE, start.plusSeconds(1), 1, 0)
                .keySet().iterator().next();
        BigId mutableId = addSimpleObsWithoutResultTime(
                mutableStream, BigId.NONE, start.plusSeconds(2), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();

        try (Connection connection = open()) {
            assertEquals("Only mutable workflow metadata belongs in the base table",
                    1, queryLong(connection, "SELECT count(*) FROM ONLY " + TABLE));
        }
        assertNotNull(tiered.get(rawId));
        assertNotNull(tiered.get(mutableId));
        assertEquals(2, tiered.getNumRecords());

        var partitions = tiered.getImmutableStagePartitions();
        assertEquals(1, partitions.size());
        String child = partitions.get(0).childTable();
        try (Connection connection = open()) {
            try (PreparedStatement persistence = connection.prepareStatement(
                    "SELECT relpersistence FROM pg_class WHERE oid=to_regclass(?)")) {
                persistence.setString(1, child);
                try (ResultSet result = persistence.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals("p", result.getString(1)); // LOGGED, crash-recoverable
                }
            }
            try (PreparedStatement indexes = connection.prepareStatement(
                    "SELECT indexdef FROM pg_indexes WHERE tablename=?")) {
                indexes.setString(1, child);
                try (ResultSet result = indexes.executeQuery()) {
                    int count = 0;
                    while (result.next()) {
                        count++;
                        String definition = result.getString(1).toLowerCase();
                        assertFalse(definition.contains("using gin"));
                        assertFalse(definition.contains("jsonb_path_ops"));
                    }
                    assertEquals("The raw stage must retain only its seven bounded indexes", 7, count);
                }
            }
        }

        // Archive eligibility is fixed when the row is received. A later
        // metadata rename must not strand staged raw data.
        try (Connection connection = open(); PreparedStatement rename = connection.prepareStatement(
                "UPDATE " + TABLE + "_datastreams SET data=jsonb_set(data,'{outputName}'," +
                        "to_jsonb(?::text),true) WHERE id=?")) {
            rename.setString(1, "renamedRawOutput");
            rename.setLong(2, rawStream.getIdAsLong());
            assertEquals(1, rename.executeUpdate());
        }

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
        assertEquals(1, generation.rowCount());
        assertEquals("Activation uses only the compact temporary ownership ledger",
                0, archivedHotRows());
        assertEquals(1, stageOwnerRows());
        try (Connection connection = open(); PreparedStatement ownership = connection.prepareStatement(
                "SELECT generation_id,child_table FROM oscar_obs_stage_owner " +
                        "WHERE store_key=? AND id_scope=? AND observation_id=?")) {
            ownership.setString(1, TABLE);
            ownership.setInt(2, DATABASE_NUM);
            ownership.setLong(3, rawId.getIdAsLong());
            try (ResultSet result = ownership.executeQuery()) {
                assertTrue(result.next());
                assertEquals(generation.id(), result.getObject(1, UUID.class));
                assertEquals(child, result.getString(2));
            }
        }

        // Corrupt/extra ownership must fail closed rather than authorizing a
        // whole-relation drop. Removing the injected inconsistency lets the
        // next bounded lifecycle pass retire the verified child normally.
        long orphanOwnerId = Math.addExact(rawId.getIdAsLong(), 1_000_000L);
        try (Connection connection = open(); PreparedStatement orphan = connection.prepareStatement(
                "INSERT INTO oscar_obs_stage_owner(" +
                        "store_key,id_scope,child_table,observation_id,generation_id) " +
                        "VALUES (?,?,?,?,?)")) {
            orphan.setString(1, TABLE);
            orphan.setInt(2, DATABASE_NUM);
            orphan.setString(3, child);
            orphan.setLong(4, orphanOwnerId);
            orphan.setObject(5, generation.id());
            assertEquals(1, orphan.executeUpdate());
        }
        tiered.maintainArchiveLifecycle();
        assertEquals("Ownership without a physical staged row keeps the child fail-closed",
                1, tiered.getImmutableStagePartitions().size());
        try (Connection connection = open(); PreparedStatement orphan = connection.prepareStatement(
                "DELETE FROM oscar_obs_stage_owner WHERE store_key=? AND id_scope=? " +
                        "AND observation_id=?")) {
            orphan.setString(1, TABLE);
            orphan.setInt(2, DATABASE_NUM);
            orphan.setLong(3, orphanOwnerId);
            assertEquals(1, orphan.executeUpdate());
        }
        tiered.maintainArchiveLifecycle();

        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        try (Connection connection = open(); PreparedStatement relation = connection.prepareStatement(
                "SELECT to_regclass(?) IS NULL")) {
            relation.setString(1, child);
            try (ResultSet result = relation.executeQuery()) {
                assertTrue(result.next());
                assertTrue("Verified stage relfile must be dropped as a whole", result.getBoolean(1));
            }
        }
        try (Connection connection = open()) {
            assertEquals("Mutable workflow row remains in PostgreSQL",
                    1, queryLong(connection, "SELECT count(*) FROM ONLY " + TABLE));
        }
        assertEquals("SEALED", oneGeneration(tiered.getArchiveCatalog()).state());
        assertEquals(0, stageOwnerRows());
        assertNotNull("Raw row remains queryable from Parquet", tiered.get(rawId));
        assertNotNull(tiered.get(mutableId));
        assertEquals(2, tiered.getNumRecords());
    }

    @Test
    public void parquetPrimaryUsesExactShortLivedOwnersAcrossCappedGenerations() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumRowsPerGeneration = 2;
        primaryConfig.maximumGenerationsPerCycle = 3;
        primaryConfig.maximumVerificationsPerCycle = 3;
        primaryConfig.maximumDeletionsPerCycle = 3;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant start = Instant.parse("2026-08-02T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(82), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start.plusSeconds(1), 5, 1_000L);
        forceReadBackFromStorage();
        assertEquals(1, tiered.getImmutableStagePartitions().size());

        for (int generationNumber = 1; generationNumber <= 3; generationNumber++) {
            ArchiveGeneration generation = tiered.archiveWindow(
                    start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
            assertEquals(generationNumber < 3 ? 2 : 1, generation.rowCount());
            assertEquals(Math.min(5, generationNumber * 2L), stageOwnerRows());
            assertEquals("Primary staging rows never receive full-row archive markers",
                    0, archivedHotRows());
            tiered.maintainArchiveLifecycle();
            if (generationNumber < 3) {
                assertEquals("The physical child remains until every row has a verified owner",
                        1, tiered.getImmutableStagePartitions().size());
            }
        }

        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        assertEquals(0, stageOwnerRows());
        assertEquals(0, hotRows());
        assertEquals(5, tiered.getNumRecords());
        try (Connection connection = open()) {
            assertEquals(3, queryLong(connection,
                    "SELECT count(*) FROM oscar_archive_generation WHERE store_key='" +
                            TABLE + "' AND id_scope=" + DATABASE_NUM));
            assertEquals(5, queryLong(connection,
                    "SELECT sum(row_count) FROM oscar_archive_generation WHERE store_key='" +
                            TABLE + "' AND id_scope=" + DATABASE_NUM));
        }
        try (Stream<Map.Entry<BigId, IObsData>> rows = tiered.selectEntries(
                new ObsFilter.Builder().build())) {
            assertEquals(5, rows.count());
        }
    }

    @Test
    public void parquetPrimaryRetirementSerializesWithAWriterAlreadyRoutedToTheChild() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant start = Instant.parse("2026-08-03T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(84), "gammaCounts");
        BigId base = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plusSeconds(1), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();
        String resultJson = hotResultJson(base.getIdAsLong());
        tiered.archiveWindow(start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED")
                .orElseThrow();

        long lateId = Math.addExact(base.getIdAsLong(), 1_000_000L);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection writer = open(); PreparedStatement insert = writer.prepareStatement(
                "INSERT INTO " + TABLE +
                        "(id,datastreamid,foiid,phenomenontime,resulttime,result) " +
                        "VALUES (?,?,NULL,?,NULL,?::jsonb)")) {
            writer.setAutoCommit(false);
            insert.setLong(1, lateId);
            insert.setLong(2, stream.getIdAsLong());
            UtcJdbcTime.setInstant(insert, 3, start.plusSeconds(2));
            insert.setString(4, resultJson);
            assertEquals(0, insert.executeUpdate()); // BEFORE trigger routes to the child

            Future<?> retirement = executor.submit(tiered::maintainArchiveLifecycle);
            waitForStageCatalogLockWaiter();
            writer.commit();
            retirement.get(30, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }

        assertEquals("The just-committed unowned row prevents stale-snapshot retirement",
                1, tiered.getImmutableStagePartitions().size());
        assertEquals(1, stageOwnerRows());
        assertEquals(2, hotRows());

        ArchiveGeneration late = tiered.archiveWindow(
                start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
        assertEquals(1, late.rowCount());
        tiered.maintainArchiveLifecycle();
        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        assertEquals(0, stageOwnerRows());
        assertEquals(2, tiered.getNumRecords());
    }

    @Test
    public void parquetPrimaryMutationsRespectExactSidecarOwnershipBeforeChildDrop() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumRowsPerGeneration = 2;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant start = Instant.parse("2026-08-04T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(85), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plusSeconds(1), 3, 1_000L)
                .keySet().stream().sorted(Comparator.comparingLong(BigId::getIdAsLong)).toList();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
        assertEquals(2, generation.rowCount());
        assertEquals(2, stageOwnerRows());

        BigId corrected = keys.get(0);
        String immutableBase = hotResultJson(corrected.getIdAsLong());
        tiered.put(corrected, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start.plusSeconds(1))
                .withResult(values(501))
                .build());
        assertEquals("An archive-owned staged base is never rewritten",
                immutableBase, hotResultJson(corrected.getIdAsLong()));
        assertEquals(501.0, tiered.get(corrected).getResult().getDoubleValue(0), 0.0);
        assertEquals(generation.id(), tiered.getArchiveCatalog().overlayWithGeneration(
                corrected.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow().generationId());

        BigId tombstoned = keys.get(1);
        assertNotNull(tiered.remove(tombstoned));
        assertTrue("The owned parity base remains physical until child retirement",
                hotRowExists(tombstoned.getIdAsLong()));
        assertTrue(tiered.get(tombstoned) == null);
        assertEquals(generation.id(), tombstoneGeneration(tombstoned.getIdAsLong()));

        BigId unowned = keys.get(2);
        assertNotNull(tiered.remove(unowned));
        assertFalse("An unowned staged row is still an ordinary live delete",
                hotRowExists(unowned.getIdAsLong()));

        tiered.maintainArchiveLifecycle();
        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        assertEquals(0, stageOwnerRows());
        assertEquals(1, tiered.getNumRecords());
        assertEquals(501.0, tiered.get(corrected).getResult().getDoubleValue(0), 0.0);
        assertTrue(tiered.get(tombstoned) == null);
        assertTrue(tiered.get(unowned) == null);
    }

    @Test
    public void parquetPrimaryReplayDuringDetachedCrashWindowLeavesNoDuplicate() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumStagePartitionsPerCycle = 4;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant start = Instant.parse("2026-08-05T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(86), "gammaCounts");
        BigId stableId = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plusSeconds(1), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
        String detachedChild = tiered.getImmutableStagePartitions().get(0).childTable();

        // Simulate a power loss after the verified NO INHERIT transaction has
        // committed but before the lifecycle's separately committed DROP.
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            try (PreparedStatement verify = connection.prepareStatement(
                    "UPDATE oscar_archive_generation SET parity_verified=true " +
                            "WHERE id=? AND store_key=? AND id_scope=?")) {
                verify.setObject(1, generation.id());
                verify.setString(2, TABLE);
                verify.setInt(3, DATABASE_NUM);
                assertEquals(1, verify.executeUpdate());
            }
            statement.execute("ALTER TABLE " + detachedChild + " NO INHERIT " + TABLE);
            try (PreparedStatement detach = connection.prepareStatement(
                    "UPDATE oscar_obs_stage_partition SET state='DETACHED',detached_at=now() " +
                            "WHERE store_key=? AND id_scope=? AND child_table=? AND state='ATTACHED'")) {
                detach.setString(1, TABLE);
                detach.setInt(2, DATABASE_NUM);
                detach.setString(3, detachedChild);
                assertEquals(1, detach.executeUpdate());
            }
            connection.commit();
        }

        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        // An exact replay temporarily routes to a replacement attached child,
        // then reconcile removes only that physical duplicate. The global
        // owner for the detached child remains authoritative.
        BigId replay = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(start.plusSeconds(1)).withResult(values(0)).build());
        assertEquals(stableId, replay);
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(1, stageOwnerRows());
        assertEquals(1, tiered.getNumRecords());
        assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                stableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").isEmpty());

        // A correction follows the same cleanup path but persists its logical
        // replacement as a generation-owned overlay.
        BigId correction = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(start.plusSeconds(1)).withResult(values(700)).build());
        assertEquals(stableId, correction);
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(1, tiered.getNumRecords());
        assertEquals(700.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
        assertEquals(generation.id(), tiered.getArchiveCatalog().overlayWithGeneration(
                stableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow().generationId());

        tiered.maintainArchiveLifecycle();
        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        assertEquals(0, stageOwnerRows());
        try (Connection connection = open(); PreparedStatement relation = connection.prepareStatement(
                "SELECT to_regclass(?) IS NULL")) {
            relation.setString(1, detachedChild);
            try (ResultSet result = relation.executeQuery()) {
                assertTrue(result.next());
                assertTrue(result.getBoolean(1));
            }
        }
        assertEquals("SEALED", oneGeneration(tiered.getArchiveCatalog()).state());
        assertEquals(1, tiered.getNumRecords());
        assertEquals(700.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
    }

    @Test
    public void parquetPrimaryRejectsFutureDetectorClockBeforeCreatingStageState() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.maximumFuturePhenomenonSkewMinutes = 5;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;
        BigId stream = addSimpleDataStream(bigId(83), "gammaCounts");
        tiered.getDataStreams().commit();

        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            SQLException skew = assertThrows(SQLException.class, () ->
                    tiered.prepareImmutableForTest(connection, 90_001L,
                            stream.getIdAsLong(), Instant.now().plus(10, ChronoUnit.MINUTES)));
            assertTrue(skew.getMessage().contains("exceeds the PostgreSQL receipt clock"));
            connection.rollback();
        }
        assertTrue(tiered.getImmutableStagePartitions().isEmpty());
        try (Connection connection = open()) {
            assertEquals(0, queryLong(connection,
                    "SELECT count(*) FROM oscar_obs_storage_route WHERE store_key='" +
                            TABLE + "' AND id_scope=" + DATABASE_NUM));
        }
    }

    @Test
    public void completeHotColdLifecycleAgainstRealPostgres() throws Exception {
        String expectedZone = System.getenv("OSCAR_IT_EXPECTED_ZONE");
        if (expectedZone != null)
            assertEquals(expectedZone, ZoneId.systemDefault().getId());
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant requestedStart = Instant.parse("2026-01-01T00:00:00.123456789Z");
        Instant start = UtcJdbcTime.toDatabasePrecision(requestedStart);

        BigId stream = addSimpleDataStream(bigId(10), "gammaCounts");
        // Real OSCAR datastreams use a human-facing name and a separate
        // machine-facing outputName. Archive eligibility must use the latter.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE " + TABLE + "_datastreams SET data=jsonb_set(data, '{name}', " +
                        "to_jsonb(?::text), true) WHERE id=?")) {
            statement.setString(1, "Lane06 - RPM - Gamma Counts");
            statement.setLong(2, stream.getIdAsLong());
            assertEquals(1, statement.executeUpdate());
        }
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, requestedStart, 6, 60_000);
        forceReadBackFromStorage();

        long firstId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        BigId firstKey = BigId.fromLong(DATABASE_NUM, firstId);
        assertEquals(start, tiered.get(firstKey).getPhenomenonTime());
        DataBlock hotReplacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) hotReplacement.setDoubleValue(i, 500 + i);
        tiered.put(firstKey, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(requestedStart)
                .withResultTime(requestedStart.plusSeconds(30))
                .withResult(hotReplacement)
                .build());
        assertEquals(start, tiered.get(firstKey).getPhenomenonTime());
        assertEquals(start.plusSeconds(30), tiered.get(firstKey).getResultTime());
        assertEquals(500.0, tiered.get(firstKey).getResult().getDoubleValue(0), 0.0);
        var range = tiered.getDataStreamPhenomenonTimeRanges(List.of(stream.getIdAsLong())).get(stream.getIdAsLong());
        assertEquals(start, range.begin());
        assertEquals(start.plus(5, ChronoUnit.MINUTES), range.end());
        var resultRange = tiered.getDataStreamResultTimeRanges(List.of(stream.getIdAsLong())).get(stream.getIdAsLong());
        assertEquals(start.plusSeconds(30), resultRange.begin());
        // ObsData defaults an omitted result time to its phenomenon time.
        assertEquals(start.plus(5, ChronoUnit.MINUTES), resultRange.end());

        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));
        assertStatementTriggerRange(stream, start, start.plus(5, ChronoUnit.MINUTES));
        assertIdempotentInsertContract(inserted.keySet().iterator().next().getIdAsLong());
        assertEquals(start, tiered.nextArchiveWindowStart(start.plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow());
        assertEquals(6, tiered.countArchiveCandidates(start, start.plus(1, ChronoUnit.HOURS), 100));

        ArchiveGeneration generation = tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow();
        assertEquals(6, generation.rowCount());
        assertTrue(Files.isRegularFile(root.resolve(generation.objectPath())));
        try (var paths = Files.walk(root)) {
            assertEquals(0, paths.filter(path -> path.getFileName().toString().endsWith(".partial")).count());
        }
        assertCatalogV5AndActivation(generation);

        // Grace overlap must be deduplicated by both the full query and exact cardinality paths.
        assertEquals(6, selectAll(tiered));
        assertEquals(6, tiered.getNumRecords());
        assertGlobalOrderingAndLatest(tiered, start);
        try (var early = tiered.selectEntries(new ObsFilter.Builder().build())) {
            assertTrue(early.findFirst().isPresent());
            assertEquals(1, tiered.getOpenArchiveCursorCount());
        }
        assertEquals(0, tiered.getOpenArchiveCursorCount());
        try (var limited = tiered.selectEntries(new ObsFilter.Builder().withLimit(1).build())) {
            assertEquals(1, limited.count());
        }
        assertEquals(0, tiered.getOpenArchiveCursorCount());

        // A sparse overlay can change a row's sort key while its original hot
        // copy is still present during the rollback grace period. It must be
        // emitted once from the independently sorted mutation stream, not
        // substituted in place into the SQL-ordered hot stream.
        var schema = tiered.getDataStreams().get(new DataStreamKey(stream));
        String immutableHotBefore = hotResultJson(firstId);
        DataBlock graceReplacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) graceReplacement.setDoubleValue(i, 700 + i);
        tiered.put(firstKey, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start.plus(10, ChronoUnit.MINUTES))
                .withResult(graceReplacement)
                .build());
        assertEquals(700.0, tiered.get(firstKey).getResult().getDoubleValue(0), 0.0);
        assertEquals(immutableHotBefore, hotResultJson(firstId));
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            assertEquals(List.of(start.plus(1, ChronoUnit.MINUTES), start.plus(2, ChronoUnit.MINUTES),
                            start.plus(3, ChronoUnit.MINUTES), start.plus(4, ChronoUnit.MINUTES),
                            start.plus(5, ChronoUnit.MINUTES), start.plus(10, ChronoUnit.MINUTES)),
                    rows.map(entry -> entry.getValue().getPhenomenonTime()).toList());
        }

        long removedDuringGrace = inserted.keySet().stream().mapToLong(BigId::getIdAsLong)
                .sorted().skip(1).findFirst().orElseThrow();
        assertTrue(tiered.remove(BigId.fromLong(DATABASE_NUM, removedDuringGrace)) != null);
        assertEquals(null, tiered.get(BigId.fromLong(DATABASE_NUM, removedDuringGrace)));
        assertTrue(hotRowExists(removedDuringGrace));

        // Sparse logical mutations must not alter the immutable hot parity
        // source during grace. Verification therefore still succeeds.
        tiered.maintainArchiveLifecycle();
        assertTrue(oneGeneration(catalog).parityVerified());
        try (Connection connection = open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_obs_overlay WHERE store_key=? AND id_scope=? AND observation_id=?")) {
                statement.setString(1, TABLE);
                statement.setInt(2, DATABASE_NUM);
                statement.setLong(3, firstId);
                assertEquals(1, statement.executeUpdate());
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                            "AND subject_type='OBSERVATION' AND subject_id=?")) {
                statement.setString(1, TABLE);
                statement.setInt(2, DATABASE_NUM);
                statement.setString(3, Long.toString(removedDuringGrace));
                assertEquals(1, statement.executeUpdate());
            }
        }
        assertEquals(6, selectAll(tiered));

        tiered.maintainArchiveLifecycle();
        ArchiveGeneration verified = oneGeneration(catalog);
        assertTrue(verified.parityVerified());

        long heldId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).sorted().skip(2).findFirst().orElseThrow();
        UUID holdId = UUID.randomUUID();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_retention_hold(id, store_key, id_scope, subject_type, subject_id, " +
                        "resolution_mode, reason, created_by) VALUES (?, ?, ?, 'OBSERVATION', ?, " +
                        "'HOT_EXACT', 'integration hold', 'codex-integration')")) {
            statement.setObject(1, holdId);
            statement.setString(2, TABLE);
            statement.setInt(3, DATABASE_NUM);
            statement.setString(4, Long.toString(heldId));
            statement.executeUpdate();
        }
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(1, hotRows());
        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));
        assertEquals("ACTIVE", oneGeneration(catalog).state());

        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_retention_hold SET released_at=now(), released_by='codex-integration' WHERE id=?")) {
            statement.setObject(1, holdId);
            assertEquals(1, statement.executeUpdate());
        }
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());
        assertEquals("SEALED", oneGeneration(catalog).state());
        assertEquals(6, tiered.getNumRecords());
        assertEquals(6, selectAll(tiered));

        // Recreate the catalog and reader to model a process restart after hot deletion.
        ArchiveCatalog restartedCatalog = new ArchiveCatalog(() -> {
            try {
                return open();
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }, TABLE, DATABASE_NUM);
        restartedCatalog.migrate();
        ArchiveGeneration restartedGeneration = oneGeneration(restartedCatalog);
        ParquetArchiveReader restartedReader = new ParquetArchiveReader(root);
        assertEquals(6, restartedReader.read(List.of(restartedGeneration), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE).size());

        // Sparse mutation records remain queryable through the normal OSH store contract.
        long overlayId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        DataBlock replacement = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) replacement.setDoubleValue(i, 999 + i);
        String replacementJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), replacement);
        String overlaySchemaJson = SerializerUtils.writeIDataStreamInfoToJson(schema);
        restartedCatalog.upsertOverlay(generation.id(), new ArchivedObservation(DATABASE_NUM, overlayId, DATABASE_NUM,
                stream.getIdAsLong(), null, null, start, null, replacementJson, "RAW_GATED"),
                overlaySchemaJson);
        long overlayRowsBefore = catalog.getOverlayRowsRead();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_obs_overlay(store_key, id_scope, observation_id, generation_id, datastream_id, foi_id, " +
                "phenomenon_time, result_time, result_json, schema_json) " +
                        "SELECT ?, ?, ? + value, ?, ?, NULL, ?, NULL, ?::jsonb, ?::jsonb " +
                        "FROM generate_series(1, 10000) value")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setLong(3, overlayId + 10_000_000L);
            statement.setObject(4, generation.id());
            statement.setLong(5, stream.getIdAsLong());
            UtcJdbcTime.setInstant(statement, 6, start.plus(2, ChronoUnit.HOURS));
            statement.setString(7, replacementJson);
            statement.setString(8, overlaySchemaJson);
            assertEquals(10_000, statement.executeUpdate());
        }

        // Both cold bases and overlays must be self-describing. Removing the
        // mutable live metadata after publication must not change or break
        // decoding of historical payloads.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM " + TABLE + "_datastreams WHERE id=?")) {
            statement.setLong(1, stream.getIdAsLong());
            assertEquals(1, statement.executeUpdate());
        }
        ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).clearCache();

        org.sensorhub.api.data.IObsData overlaid;
        try (var rows = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(DATABASE_NUM, overlayId)).build())) {
            overlaid = rows.findFirst().orElseThrow().getValue();
            assertEquals(1, catalog.getOpenOverlayCursorCount());
        }
        assertEquals(999.0, overlaid.getResult().getDoubleValue(0), 0.0);
        assertEquals(1, catalog.getOverlayRowsRead() - overlayRowsBefore);
        assertEquals(0, catalog.getOpenOverlayCursorCount());
        assertEquals(0, tiered.getOpenArchiveCursorCount());
        try (var currentPhenomenon = tiered.selectEntries(new ObsFilter.Builder()
                .withPhenomenonTime().withCurrentTime().done().build())) {
            assertEquals(0, currentPhenomenon.count());
        }
        try (var currentResult = tiered.selectEntries(new ObsFilter.Builder()
                .withResultTime(new TemporalFilter.Builder().withCurrentTime().build()).build())) {
            assertEquals(0, currentResult.count());
        }
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "DELETE FROM oscar_obs_overlay WHERE store_key=? AND id_scope=? AND observation_id > ?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setLong(3, overlayId + 10_000_000L);
            assertEquals(10_000, statement.executeUpdate());
        }

        long deletedId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).max().orElseThrow();
        restartedCatalog.tombstone(deletedId, generation.id(), "integration deletion", "TEST");
        assertEquals(5, tiered.getNumRecords());
        assertEquals(5, selectAll(tiered));

        // A new reader must reject an object that no longer matches its catalog checksum.
        Path parquet = root.resolve(generation.objectPath());
        byte[] original = Files.readAllBytes(parquet);
        byte[] damaged = original.clone();
        damaged[damaged.length - 1] ^= 1;
        Files.write(parquet, damaged);
        try {
            assertThrows(IllegalStateException.class, () -> new ParquetArchiveReader(root).read(
                    List.of(restartedGeneration), new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE));
        } finally {
            Files.write(parquet, original);
        }
        assertEquals(6, new ParquetArchiveReader(root).read(List.of(restartedGeneration),
                new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE).size());

        // PURGED generations are no longer query-visible. Sparse mutation
        // records may remain for audit, but must never resurrect their rows in
        // get/select/count after a restart or lifecycle transition.
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET state='PURGED' WHERE id=?")) {
            statement.setObject(1, generation.id());
            assertEquals(1, statement.executeUpdate());
        }

        // A different live generation can have an overlapping coarse ID
        // block when eligible and ineligible streams interleave. It must not
        // inherit mutations owned by the purged generation.
        UUID overlappingJob = UUID.randomUUID();
        UUID overlappingGeneration = UUID.randomUUID();
        try (Connection connection = open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_job(id, store_key, id_scope, profile, window_start, window_end, revision, state) " +
                            "VALUES (?, ?, ?, 'OVERLAP_TEST', ?, ?, 1, 'ACTIVE')")) {
                statement.setObject(1, overlappingJob);
                statement.setString(2, TABLE);
                statement.setInt(3, DATABASE_NUM);
                UtcJdbcTime.setInstant(statement, 4, start);
                UtcJdbcTime.setInstant(statement, 5, start.plus(1, ChronoUnit.HOURS));
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_generation(id, job_id, store_key, id_scope, profile, window_start, window_end, state, " +
                            "object_path, manifest_path, sha256, row_count, min_id, max_id) " +
                            "VALUES (?, ?, ?, ?, 'OVERLAP_TEST', ?, ?, 'ACTIVE', 'never-opened.parquet', " +
                            "'never-opened.manifest.json', 'none', 0, ?, ?)")) {
                statement.setObject(1, overlappingGeneration);
                statement.setObject(2, overlappingJob);
                statement.setString(3, TABLE);
                statement.setInt(4, DATABASE_NUM);
                UtcJdbcTime.setInstant(statement, 5, start);
                UtcJdbcTime.setInstant(statement, 6, start.plus(1, ChronoUnit.HOURS));
                statement.setLong(7, overlayId - 1);
                statement.setLong(8, overlayId + 1);
                statement.executeUpdate();
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_archive_id_block(generation_id, min_id, max_id) VALUES (?, ?, ?)")) {
                statement.setObject(1, overlappingGeneration);
                statement.setLong(2, overlayId - 1);
                statement.setLong(3, overlayId + 1);
                statement.executeUpdate();
            }
        }
        assertTrue(catalog.overlay(overlayId, DATABASE_NUM, "OVERLAY").isEmpty());
        assertFalse(catalog.mutatedObservationIds(List.of(overlayId)).contains(overlayId));
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET state='PURGED' WHERE id=?")) {
            statement.setObject(1, overlappingGeneration);
            assertEquals(1, statement.executeUpdate());
        }
        assertEquals(null, tiered.get(BigId.fromLong(DATABASE_NUM, overlayId)));
        assertEquals(0, selectAll(tiered));
        assertEquals(0, tiered.getNumRecords());
        assertTrue(catalog.overlay(overlayId, DATABASE_NUM, "OVERLAY").isEmpty());
        assertFalse(catalog.isTombstoned(deletedId));
    }

    @Test
    public void latestResultPushdownPreservesFinalLimitAcrossHotAndColdTiers() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-01-03T00:00:00Z");
        BigId first = addSimpleDataStream(bigId(701), "gammaCounts");
        BigId second = addSimpleDataStream(bigId(702), "gammaCounts");
        BigId third = addSimpleDataStream(bigId(703), "gammaCounts");

        addSimpleObsWithoutResultTime(first, BigId.NONE, start.plusSeconds(10), 1, 0);
        addSimpleObsWithoutResultTime(second, BigId.NONE, start.plusSeconds(20), 1, 0);
        addSimpleObsWithoutResultTime(third, BigId.NONE, start.plusSeconds(30), 1, 0);
        forceReadBackFromStorage();
        tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();

        // Result time selects these hot rows as latest for each stream, while
        // phenomenon time deliberately puts the highest stream ID first in
        // the final public ordering. A tier-local LIMIT 1 would incorrectly
        // keep only the lowest ID before the hot/cold union.
        BigId[] streams = {first, second, third};
        long[] phenomenonMinutes = {30, 20, 10};
        for (int index = 0; index < streams.length; index++) {
            DataBlock result = new DataBlockDouble(5);
            tiered.add(new ObsData.Builder()
                    .withDataStream(streams[index])
                    .withPhenomenonTime(start.plus(phenomenonMinutes[index], ChronoUnit.MINUTES))
                    .withResultTime(start.plus(2, ChronoUnit.HOURS).plusSeconds(index))
                    .withResult(result)
                    .build());
        }
        forceReadBackFromStorage();

        ObsFilter latestOne = new ObsFilter.Builder()
                .withDataStreams(first, second, third)
                .withLatestResult()
                .withLimit(1)
                .build();
        try (var rows = tiered.selectEntries(latestOne)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(third, selected.get(0).getValue().getDataStreamID());
            assertEquals(start.plus(10, ChronoUnit.MINUTES),
                    selected.get(0).getValue().getPhenomenonTime());
        }
        assertEquals(0, tiered.getOpenArchiveCursorCount());
    }

    @Test
    public void latestResultProbeUsesGreatestIdForNullAndEqualResultTimes() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-01-04T00:00:00Z");
        BigId allNull = addSimpleDataStream(bigId(711), "gammaCounts");
        BigId equalTime = addSimpleDataStream(bigId(712), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> nullRows =
                addSimpleObsWithoutResultTime(allNull, BigId.NONE, start.plusSeconds(20), 2, 1_000);
        Map<BigId, org.sensorhub.api.data.IObsData> tiedRows =
                addSimpleObsWithoutResultTime(equalTime, BigId.NONE, start, 2, 1_000);
        forceReadBackFromStorage();

        Instant sharedResultTime = start.plus(1, ChronoUnit.HOURS);
        try (Connection connection = open()) {
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET resulttime=NULL WHERE datastreamid=?")) {
                statement.setLong(1, allNull.getIdAsLong());
                assertEquals(2, statement.executeUpdate());
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET resulttime=? WHERE datastreamid=?")) {
                UtcJdbcTime.setInstant(statement, 1, sharedResultTime);
                statement.setLong(2, equalTime.getIdAsLong());
                assertEquals(2, statement.executeUpdate());
            }
        }

        ObsFilter latest = new ObsFilter.Builder()
                .withDataStreams(allNull, equalTime)
                .withLatestResult()
                .build();
        try (var rows = tiered.selectEntries(latest)) {
            var selected = rows.toList();
            assertEquals(2, selected.size());
            assertEquals("The optimized probe must retain public phenomenon-time ordering",
                    List.of(equalTime, allNull), selected.stream()
                            .map(entry -> entry.getValue().getDataStreamID()).toList());
            Map<BigId, BigId> selectedIds = selected.stream().collect(Collectors.toMap(
                    entry -> entry.getValue().getDataStreamID(), Map.Entry::getKey));
            assertEquals(tiedRows.keySet().stream().max(Comparator.comparingLong(BigId::getIdAsLong)).orElseThrow(),
                    selectedIds.get(equalTime));
            assertEquals(nullRows.keySet().stream().max(Comparator.comparingLong(BigId::getIdAsLong)).orElseThrow(),
                    selectedIds.get(allNull));
        }
    }

    @Test
    public void tieredRemoveRevalidatesHalfOpenPredicateAgainstRealPostgres() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-01T00:00:00Z");
        Instant endExclusive = start.plus(10, ChronoUnit.MINUTES);
        BigId stream = addSimpleDataStream(bigId(60), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 3, 60_000);
        forceReadBackFromStorage();

        long movedId = inserted.keySet().stream()
                .mapToLong(BigId::getIdAsLong)
                .min()
                .orElseThrow();
        tiered.beforeNextRemoveBatch(() -> {
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET phenomenontime=? WHERE id=?")) {
                UtcJdbcTime.setInstant(statement, 1, endExclusive);
                statement.setLong(2, movedId);
                assertEquals(1, statement.executeUpdate());
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        });

        ObsFilter halfOpen = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, endExclusive))
                        .build())
                .build();

        // The hook moves one already-selected row exactly onto the open upper
        // endpoint. The final locked predicate check must spare it.
        assertEquals(2, tiered.removeEntries(halfOpen));
        assertEquals(1, tiered.getNumRecords());
        assertEquals(endExclusive,
                tiered.get(BigId.fromLong(DATABASE_NUM, movedId)).getPhenomenonTime());
        assertEquals(0, tiered.removeEntries(halfOpen));
        try (var endpoint = tiered.selectEntries(new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withSingleValue(endExclusive)
                        .build())
                .build())) {
            assertEquals(1, endpoint.count());
        }
    }

    @Test
    public void bulkRemoveRechecksArchiveOwnershipBeforeSetBasedDelete() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-01T00:30:00Z");
        BigId stream = addSimpleDataStream(bigId(601), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        AtomicReference<ArchiveGeneration> activated = new AtomicReference<>();
        tiered.beforeNextRemoveBatch(() -> activated.set(tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow()));

        assertEquals(1, tiered.removeEntries(new ObsFilter.Builder()
                .withInternalIDs(key)
                .build()));
        assertNotNull(activated.get());
        assertTrue("An archive-owned row must not enter the live batch DELETE",
                hotRowExists(key.getIdAsLong()));
        assertTrue(tiered.getArchiveCatalog().isTombstoned(key.getIdAsLong()));
        assertEquals(activated.get().id(), tombstoneGeneration(key.getIdAsLong()));
        assertNull(tiered.get(key));
    }

    @Test
    public void tieredRemoveTombstonesArchivedRowsExactlyOnce() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(61), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();

        ObsFilter filter = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, start.plus(1, ChronoUnit.HOURS)))
                        .build())
                .build();
        assertEquals(2, tiered.removeEntries(filter));
        assertEquals(0, tiered.getNumRecords());
        assertEquals(0, selectAll(tiered));
        assertEquals(2, hotRows());
        assertEquals(0, tiered.removeEntries(filter));

        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION'")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(2, result.getLong(1));
            }
        }
    }

    @Test
    public void activeObservationHoldBlocksSingleHotRemoveUntilRelease() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(610), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        UUID hold = insertRetentionHold("OBSERVATION",
                Long.toString(key.getIdAsLong()), "HOT_EXACT");
        assertThrows(ArchiveCatalog.RetentionHoldException.class, () -> tiered.remove(key));
        assertNotNull(tiered.get(key));
        assertTrue(hotRowExists(key.getIdAsLong()));

        releaseRetentionHold(hold);
        assertNotNull(tiered.remove(key));
        assertTrue(tiered.get(key) == null);
        assertFalse(hotRowExists(key.getIdAsLong()));
    }

    @Test
    public void activeObservationHoldBlocksSingleHotRemoveWhenArchivingIsDisabled() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:30:00Z");
        BigId stream = addSimpleDataStream(bigId(613), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        UUID hold = insertRetentionHold("OBSERVATION",
                Long.toString(key.getIdAsLong()), "HOT_EXACT");
        archiveConfig.enabled = false;
        assertThrows(ArchiveCatalog.RetentionHoldException.class, () -> tiered.remove(key));
        assertNotNull(tiered.get(key));
        assertTrue(hotRowExists(key.getIdAsLong()));

        releaseRetentionHold(hold);
        assertNotNull(tiered.remove(key));
        assertNull(tiered.get(key));
        assertFalse(hotRowExists(key.getIdAsLong()));
    }

    @Test
    public void activeObservationHoldIsSkippedByBulkRemoveWhenArchivingIsDisabled() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:45:00Z");
        BigId stream = addSimpleDataStream(bigId(614), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 2, 60_000).keySet().stream()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        forceReadBackFromStorage();

        UUID hold = insertRetentionHold("OBSERVATION",
                Long.toString(keys.get(0).getIdAsLong()), "HOT_EXACT");
        archiveConfig.enabled = false;
        assertEquals(1, tiered.removeEntries(new ObsFilter.Builder().build()));
        assertNotNull(tiered.get(keys.get(0)));
        assertNull(tiered.get(keys.get(1)));

        releaseRetentionHold(hold);
        assertEquals(1, tiered.removeEntries(new ObsFilter.Builder().build()));
        assertEquals(0, tiered.getNumRecords());
    }

    @Test
    public void disabledModeDoesNotRemoveInvisibleColdOnlyObservation() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Instant start = Instant.parse("2026-02-02T12:47:00Z");
        BigId stream = addSimpleDataStream(bigId(617), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(key.getIdAsLong()));

        archiveConfig.enabled = false;
        assertNull(tiered.get(key));
        assertNull(tiered.remove(key));
        assertFalse(catalog.isTombstoned(key.getIdAsLong()));
    }

    @Test
    public void disabledModeRemovesVisibleGraceRowsConsistently() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Instant start = Instant.parse("2026-02-02T12:48:00Z");
        BigId stream = addSimpleDataStream(bigId(618), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 2, 60_000).keySet().stream()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        forceReadBackFromStorage();
        tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        assertEquals(2, archivedHotRows());

        archiveConfig.enabled = false;
        assertNotNull(tiered.get(keys.get(0)));
        assertNotNull(tiered.remove(keys.get(0)));
        assertNull(tiered.get(keys.get(0)));
        assertFalse(hotRowExists(keys.get(0).getIdAsLong()));
        assertTrue(catalog.isTombstoned(keys.get(0).getIdAsLong()));

        assertEquals(1, tiered.removeEntries(new ObsFilter.Builder().build()));
        assertEquals(0, tiered.getNumRecords());
        assertEquals(0, selectAll(tiered));
        assertFalse(hotRowExists(keys.get(1).getIdAsLong()));
        assertTrue(catalog.isTombstoned(keys.get(1).getIdAsLong()));
        assertEquals(0, tiered.removeEntries(new ObsFilter.Builder().build()));
    }

    @Test
    public void clearCannotBypassRetentionHolds() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:50:00Z");
        BigId stream = addSimpleDataStream(bigId(615), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        insertRetentionHold("OBSERVATION",
                Long.toString(key.getIdAsLong()), "HOT_EXACT");
        assertThrows(UnsupportedOperationException.class, tiered::clear);
        assertThrows(UnsupportedOperationException.class, tiered::drop);
        assertThrows(UnsupportedOperationException.class, () -> tiered.getDataStreams().clear());
        assertThrows(UnsupportedOperationException.class,
                () -> ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).drop());
        assertThrows(UnsupportedOperationException.class,
                () -> tiered.backup(new java.io.ByteArrayOutputStream()));
        assertThrows(UnsupportedOperationException.class,
                () -> tiered.restore(new java.io.ByteArrayInputStream(new byte[0])));
        assertThrows(UnsupportedOperationException.class,
                () -> ((PostgisDataStreamStoreImpl) tiered.getDataStreams())
                        .backup(new java.io.ByteArrayOutputStream()));
        assertThrows(UnsupportedOperationException.class,
                () -> ((PostgisDataStreamStoreImpl) tiered.getDataStreams())
                        .restore(new java.io.ByteArrayInputStream(new byte[0])));
        assertNotNull(tiered.getDataStreams().get(new DataStreamKey(stream)));
        assertNotNull(tiered.get(key));
        assertTrue(hotRowExists(key.getIdAsLong()));
    }

    @Test
    public void datastreamRemovalCannotOrphanHeldHotObservation() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:55:00Z");
        BigId stream = addSimpleDataStream(bigId(616), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 2, 60_000).keySet().stream()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong)).toList();
        forceReadBackFromStorage();

        UUID hold = insertRetentionHold("OBSERVATION",
                Long.toString(keys.get(0).getIdAsLong()), "HOT_EXACT");
        DataStreamKey dataStreamKey = new DataStreamKey(stream);
        assertThrows(IllegalStateException.class,
                () -> tiered.getDataStreams().remove(dataStreamKey));
        assertNotNull(tiered.getDataStreams().get(dataStreamKey));
        assertNotNull(tiered.get(keys.get(0)));
        assertNotNull("Hold preflight must preserve an unheld sibling too",
                tiered.get(keys.get(1)));

        releaseRetentionHold(hold);
        assertNotNull(tiered.getDataStreams().remove(dataStreamKey));
        assertNull(tiered.get(keys.get(0)));
        assertNull(tiered.get(keys.get(1)));
    }

    @Test
    public void datastreamRemovalCompletesWithOneDatastreamConnection() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:57:00Z");
        BigId stream = addSimpleDataStream(bigId(617), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 2, 60_000).keySet().stream().toList();
        forceReadBackFromStorage();
        tiered.limitDataStreamPool(1);
        tiered.beforeNextRemoveBatch(() ->
                ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).clearCache());

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> removal = executor.submit(() ->
                    tiered.getDataStreams().remove(new DataStreamKey(stream)));
            assertNotNull("Exact-ID child removal must not recursively borrow the metadata pool",
                    removal.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
        assertNull(tiered.getDataStreams().get(new DataStreamKey(stream)));
        for (BigId key : keys)
            assertNull(tiered.get(key));
    }

    @Test
    public void datastreamRemovalFailureKeepsMetadataAndRetryCompletes() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:58:00Z");
        BigId stream = addSimpleDataStream(bigId(619), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 513, 1_000);
        forceReadBackFromStorage();
        DataStreamKey key = new DataStreamKey(stream);

        tiered.failBeforeRemoveBatch(2);
        assertThrows(IllegalStateException.class,
                () -> tiered.getDataStreams().remove(key));
        assertNotNull("Metadata must remain when a later child batch fails",
                tiered.getDataStreams().get(key));
        assertEquals("The first bounded child batch may commit, making the operation retry-safe",
                1, tiered.countMatchingEntries(new ObsFilter.Builder()
                        .withDataStreams(stream)
                        .build()));

        assertNotNull("Retry must remove the remaining child and then metadata",
                tiered.getDataStreams().remove(key));
        assertNull(tiered.getDataStreams().get(key));
        assertEquals(0, tiered.countMatchingEntries(new ObsFilter.Builder()
                .withDataStreams(stream)
                .build()));
    }

    @Test
    public void hotExactDatastreamHoldDoesNotExpandThroughArchiveRange() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T12:59:00Z");
        BigId heldStream = addSimpleDataStream(bigId(620), "gammaCounts");
        BigId siblingStream = addSimpleDataStream(bigId(621), "gammaCounts");
        BigId heldObservation = addSimpleObsWithoutResultTime(
                heldStream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        addSimpleObsWithoutResultTime(
                siblingStream, BigId.NONE, start.plus(1, ChronoUnit.MINUTES), 1, 0);
        forceReadBackFromStorage();
        tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow();

        UUID hold = insertRetentionHold("OBSERVATION",
                Long.toString(heldObservation.getIdAsLong()), "HOT_EXACT");
        try {
            assertNotNull("An exact hold must not broaden through its generation ID range",
                    tiered.getDataStreams().remove(new DataStreamKey(siblingStream)));
            assertThrows(IllegalStateException.class,
                    () -> tiered.getDataStreams().remove(new DataStreamKey(heldStream)));
            assertNotNull(tiered.getDataStreams().get(new DataStreamKey(heldStream)));
            assertNotNull(tiered.get(heldObservation));
        } finally {
            releaseRetentionHold(hold);
        }
    }

    @Test
    public void coldRangeAndLegacyUnknownDatastreamHoldsUseConservativeProvenance() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T13:01:00Z");
        BigId firstStream = addSimpleDataStream(bigId(622), "gammaCounts");
        BigId gapStream = addSimpleDataStream(bigId(623), "notArchived");
        BigId secondStream = addSimpleDataStream(bigId(624), "gammaCounts");
        BigId unrelatedStream = addSimpleDataStream(bigId(625), "gammaCounts");
        BigId first = addSimpleObsWithoutResultTime(
                firstStream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        BigId gap = addSimpleObsWithoutResultTime(
                gapStream, BigId.NONE, start.plus(1, ChronoUnit.MINUTES), 1, 0)
                .keySet().iterator().next();
        BigId second = addSimpleObsWithoutResultTime(
                secondStream, BigId.NONE, start.plus(2, ChronoUnit.MINUTES), 1, 0)
                .keySet().iterator().next();
        addSimpleObsWithoutResultTime(
                unrelatedStream, BigId.NONE, start.plus(2, ChronoUnit.HOURS), 1, 0);
        forceReadBackFromStorage();
        assertNotNull(tiered.remove(gap));
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertTrue(gap.getIdAsLong() > first.getIdAsLong());
        assertTrue(gap.getIdAsLong() < second.getIdAsLong());

        UUID cold = insertRetentionHold("OBSERVATION",
                Long.toString(gap.getIdAsLong()), "COLD_RANGE_CONSERVATIVE");
        try {
            assertThrows(IllegalStateException.class,
                    () -> tiered.getDataStreams().remove(new DataStreamKey(firstStream)));
            assertThrows(IllegalStateException.class,
                    () -> tiered.getDataStreams().remove(new DataStreamKey(secondStream)));
            assertNotNull("A cold range hold is scoped to candidate generation schemas",
                    tiered.getDataStreams().remove(new DataStreamKey(unrelatedStream)));
        } finally {
            releaseRetentionHold(cold);
        }

        BigId retryProbe = addSimpleDataStream(bigId(626), "gammaCounts");
        addSimpleObsWithoutResultTime(
                retryProbe, BigId.NONE, start.plus(3, ChronoUnit.HOURS), 1, 0);
        forceReadBackFromStorage();
        UUID legacy = insertRetentionHold("OBSERVATION",
                Long.toString(gap.getIdAsLong()), "LEGACY_UNKNOWN");
        try {
            assertThrows("A legacy hold spanning multiple generation schemas must fail closed",
                    IllegalStateException.class,
                    () -> tiered.getDataStreams().remove(new DataStreamKey(retryProbe)));
            assertNotNull(tiered.getDataStreams().get(new DataStreamKey(retryProbe)));
        } finally {
            releaseRetentionHold(legacy);
        }

        UUID exactGeneration = insertRetentionHold("ARCHIVE_GENERATION",
                generation.id().toString(), "GENERATION_EXACT");
        try {
            assertThrows(IllegalStateException.class,
                    () -> tiered.getDataStreams().remove(new DataStreamKey(firstStream)));
            assertNotNull("An exact generation hold must not block an unrelated stream",
                    tiered.getDataStreams().remove(new DataStreamKey(retryProbe)));
        } finally {
            releaseRetentionHold(exactGeneration);
        }
    }

    @Test
    public void activeGenerationHoldBlocksSingleColdRemoveAndIsScopedToCandidates() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Instant start = Instant.parse("2026-02-02T13:00:00Z");
        BigId stream = addSimpleDataStream(bigId(611), "gammaCounts");
        BigId archived = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        BigId unrelatedHot = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plus(2, ChronoUnit.HOURS), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(archived.getIdAsLong()));
        assertTrue(hotRowExists(unrelatedHot.getIdAsLong()));

        UUID hold = insertRetentionHold(
                "ARCHIVE_GENERATION", generation.id().toString(), "GENERATION_EXACT");
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            catalog.lockForMutation(connection, unrelatedHot.getIdAsLong());
            ArchiveCatalog.ActiveRemovalHolds scoped = catalog.lockActiveRemovalHolds(
                    connection, List.of(unrelatedHot.getIdAsLong()));
            assertFalse(scoped.generationIds().contains(generation.id()));
            connection.rollback();
        }
        assertNotNull(tiered.remove(unrelatedHot));

        assertThrows(ArchiveCatalog.RetentionHoldException.class, () -> catalog.tombstone(
                archived.getIdAsLong(), generation.id(), "blocked integration delete", "TEST"));
        assertThrows(ArchiveCatalog.RetentionHoldException.class,
                () -> tiered.remove(archived));
        assertNotNull(tiered.get(archived));
        assertFalse(catalog.isTombstoned(archived.getIdAsLong()));

        releaseRetentionHold(hold);
        assertNotNull(tiered.remove(archived));
        assertTrue(catalog.isTombstoned(archived.getIdAsLong()));
    }

    @Test
    public void bulkRemoveSkipsHeldRowsIncludingHoldCommittedAfterMutationLocks() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T14:00:00Z");
        BigId stream = addSimpleDataStream(bigId(612), "gammaCounts");
        List<BigId> keys = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 3, 60_000).keySet().stream()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        forceReadBackFromStorage();

        UUID firstHold = insertRetentionHold(
                "OBSERVATION", Long.toString(keys.get(0).getIdAsLong()), "HOT_EXACT");
        AtomicReference<UUID> racingHold = new AtomicReference<>();
        tiered.afterNextRemoveBatchLocked(() -> {
            try {
                racingHold.set(insertRetentionHold(
                        "OBSERVATION", Long.toString(keys.get(1).getIdAsLong()), "HOT_EXACT"));
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        });

        assertEquals(1, tiered.removeEntries(new ObsFilter.Builder().build()));
        assertNotNull(tiered.get(keys.get(0)));
        assertNotNull(tiered.get(keys.get(1)));
        assertTrue(tiered.get(keys.get(2)) == null);

        releaseRetentionHold(firstHold);
        releaseRetentionHold(racingHold.get());
        assertEquals(2, tiered.removeEntries(new ObsFilter.Builder().build()));
        assertEquals(0, tiered.getNumRecords());
    }

    @Test
    public void heldGenerationUsesOneBoundedCandidateSnapshotPass() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T14:30:00Z");
        BigId stream = addSimpleDataStream(bigId(618), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 513, 1_000);
        List<BigId> removable = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plus(2, ChronoUnit.HOURS), 2, 1_000)
                .keySet().stream().toList();
        forceReadBackFromStorage();

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        UUID hold = insertRetentionHold(
                "ARCHIVE_GENERATION", generation.id().toString(), "GENERATION_EXACT");
        try {
            tiered.beginRemovalPagingProbe();
            assertEquals(2, tiered.removeEntries(new ObsFilter.Builder()
                    .withDataStreams(stream)
                    .build()));
            tiered.endRemovalPagingProbe();

            // A 513-row held prefix exceeds the normal 512-row page. The old
            // growing-prefix algorithm selected it three or more times and
            // retained every held ID. The bounded strategy performs one
            // normal page plus one linear scratch snapshot, while every
            // revalidation transaction remains capped at 512 candidates.
            assertEquals(2, tiered.removalSelectionPasses());
            assertEquals(3, tiered.removalBatchPasses());
            assertTrue(tiered.maximumRemovalBatchSize() <= 512);
            assertEquals(513, tiered.countMatchingEntries(new ObsFilter.Builder()
                    .withDataStreams(stream)
                    .build()));
            for (BigId key : removable)
                assertNull(tiered.get(key));
        } finally {
            tiered.endRemovalPagingProbe();
            releaseRetentionHold(hold);
        }
    }

    @Test
    public void limitedRemovalBehindLargeHeldPrefixUsesFixedBatches() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-02T17:30:00Z");
        BigId stream = addSimpleDataStream(bigId(619), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1_025, 1_000);
        List<BigId> removable = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start.plus(2, ChronoUnit.HOURS), 2, 1_000)
                .keySet().stream().toList();
        forceReadBackFromStorage();

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        UUID hold = insertRetentionHold(
                "ARCHIVE_GENERATION", generation.id().toString(), "GENERATION_EXACT");
        try {
            tiered.beginRemovalPagingProbe();
            assertEquals(1, tiered.removeEntries(new ObsFilter.Builder()
                    .withDataStreams(stream)
                    .withLimit(1)
                    .build()));
            tiered.endRemovalPagingProbe();

            // The first one-row selection encounters a held row. The snapshot
            // then advances through 1,025 held candidates in 512-row pages and
            // removes exactly one later row. It must not open 1,026 one-row
            // revalidation transactions merely because the delete limit is 1.
            assertEquals(2, tiered.removalSelectionPasses());
            assertEquals(4, tiered.removalBatchPasses());
            assertEquals(512, tiered.maximumRemovalBatchSize());
            assertEquals(1_026, tiered.countMatchingEntries(new ObsFilter.Builder()
                    .withDataStreams(stream)
                    .build()));
            assertEquals(1, removable.stream().filter(key -> tiered.get(key) == null).count());
        } finally {
            tiered.endRemovalPagingProbe();
            releaseRetentionHold(hold);
        }
    }

    @Test
    public void activationSerializesDirectPutIntoAnOverlay() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-03T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(62), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        String immutableBase = hotResultJson(key.getIdAsLong());
        ObsData correction = new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start)
                .withResult(values(501))
                .build();

        installActivationBarrier();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<java.util.Optional<ArchiveGeneration>> activation = executor.submit(() ->
                    tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();
            Future<org.sensorhub.api.data.IObsData> update = executor.submit(() ->
                    tiered.put(key, correction));
            // The activation barrier owns the exclusive store lock; direct put
            // must be a second advisory waiter, not a timing-based race.
            waitForAdvisoryWaiters(2);
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ArchiveGeneration generation = activation.get(30, TimeUnit.SECONDS).orElseThrow();
            update.get(30, TimeUnit.SECONDS);
            assertEquals(immutableBase, hotResultJson(key.getIdAsLong()));
            assertEquals(501.0, tiered.get(key).getResult().getDoubleValue(0), 0.0);
            assertEquals(generation.id(), tiered.getArchiveCatalog().overlayWithGeneration(
                    key.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow().generationId());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }
    }

    @Test
    public void activationSerializesDirectRemoveIntoATombstone() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-04T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(63), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();

        installActivationBarrier();
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<java.util.Optional<ArchiveGeneration>> activation = executor.submit(() ->
                    tiered.archiveWindow(start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();
            Future<org.sensorhub.api.data.IObsData> removal = executor.submit(() -> tiered.remove(key));
            waitForAdvisoryWaiters(2);
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ArchiveGeneration generation = activation.get(30, TimeUnit.SECONDS).orElseThrow();
            assertTrue(removal.get(30, TimeUnit.SECONDS) != null);
            assertTrue(hotRowExists(key.getIdAsLong()));
            assertTrue(tiered.getArchiveCatalog().isTombstoned(key.getIdAsLong()));
            assertEquals(generation.id(), tombstoneGeneration(key.getIdAsLong()));
            assertTrue(tiered.get(key) == null);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }
    }

    @Test
    public void filteredColdRemoveSerializesAnOverlappingOverlayPut() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-05T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(64), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(key.getIdAsLong()));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicReference<Future<org.sensorhub.api.data.IObsData>> update = new AtomicReference<>();
        tiered.afterNextRemoveBatchLocked(() -> {
            update.set(executor.submit(() -> tiered.put(key, new ObsData.Builder()
                    .withDataStream(stream)
                    .withPhenomenonTime(start)
                    .withResult(values(601))
                    .build())));
            try {
                waitForAdvisoryWaiters(1);
            } catch (Exception error) {
                throw new IllegalStateException(error);
            }
        });
        try {
            assertEquals(1, tiered.removeEntries(new ObsFilter.Builder()
                    .withInternalIDs(key)
                    .build()));
            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> update.get().get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof UnsupportedOperationException);
            assertTrue(tiered.getArchiveCatalog().isTombstoned(key.getIdAsLong()));
            assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                    key.getIdAsLong(), DATABASE_NUM, "OVERLAY").isEmpty());
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void coldFilteredRemoveCompletesWithOneObservationConnection() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-06T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(65), "gammaCounts");
        BigId key = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 1, 0).keySet().iterator().next();
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(key.getIdAsLong()));

        // Force the initial candidate query through all three sources. Before
        // overlay keyset paging, the eager overlay cursor retained the pool's
        // sole connection while base mutation filtering tried to borrow it.
        tiered.put(key, new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start)
                .withResult(values(651))
                .build());
        assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                key.getIdAsLong(), DATABASE_NUM, "OVERLAY").isPresent());
        tiered.limitObservationPool(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Long> removal = executor.submit(() -> tiered.removeEntries(
                    new ObsFilter.Builder()
                            .withInternalIDs(key)
                            .withDataStreams(stream)
                            .build()));
            assertEquals(Long.valueOf(1), removal.get(5, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
        }
    }

    @Test
    public void restartAllocatesAboveFullyColdCatalogMaximum() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-07T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(66), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        long archivedMaximum = inserted.keySet().stream()
                .mapToLong(BigId::getIdAsLong).max().orElseThrow();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());

        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        BigId next = tiered.add(new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(start.plus(2, ChronoUnit.HOURS))
                .withResult(values(701))
                .build());
        assertEquals(archivedMaximum + 1, next.getIdAsLong());
        tiered.commit();
        assertEquals(3, tiered.getNumRecords());
        assertEquals(3, selectAll(tiered));
    }

    @Test
    public void coldOpenBoundsPreserveLimitCountAndRemoval() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-02-08T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(67), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 3, 60_000);
        forceReadBackFromStorage();
        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());

        ObsFilter limited = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.openClosed(start, start.plus(2, ChronoUnit.MINUTES)))
                        .build())
                .withLimit(1)
                .build();
        try (var selected = tiered.selectEntries(limited)) {
            var rows = selected.toList();
            assertEquals(1, rows.size());
            assertEquals(start.plus(1, ChronoUnit.MINUTES),
                    rows.get(0).getValue().getPhenomenonTime());
        }
        ObsFilter openCount = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.open(start, start.plus(2, ChronoUnit.MINUTES)))
                        .build())
                .build();
        assertEquals(1, tiered.countMatchingEntries(openCount));
        assertEquals(1, tiered.removeEntries(limited));

        BigId first = inserted.entrySet().stream()
                .filter(entry -> entry.getValue().getPhenomenonTime().equals(start))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        BigId middle = inserted.entrySet().stream()
                .filter(entry -> entry.getValue().getPhenomenonTime().equals(
                        start.plus(1, ChronoUnit.MINUTES)))
                .map(Map.Entry::getKey).findFirst().orElseThrow();
        assertTrue(tiered.get(first) != null);
        assertTrue(tiered.get(middle) == null);
    }

    @Test
    public void lateRowsCreateANewRevisionOfTheAnchoredUtcHour() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant firstTime = Instant.parse("2026-03-04T12:34:56.123456789Z");
        Instant windowStart = OscarTieredObsSystemDatabase.utcHourWindowStart(firstTime);
        Instant windowEnd = windowStart.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(20), "gammaCounts");

        addSimpleObsWithoutResultTime(stream, BigId.NONE, firstTime, 1, 0);
        forceReadBackFromStorage();
        assertEquals(UtcJdbcTime.toDatabasePrecision(firstTime),
                tiered.nextArchiveWindowStart(windowEnd, "RAW_GATED").orElseThrow());
        ArchiveGeneration first = tiered.archiveWindow(windowStart, windowEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, first.rowCount());

        Instant lateTime = Instant.parse("2026-03-04T12:45:00.987654321Z");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, lateTime, 1, 0);
        forceReadBackFromStorage();
        assertEquals(UtcJdbcTime.toDatabasePrecision(lateTime),
                tiered.nextArchiveWindowStart(windowEnd, "RAW_GATED").orElseThrow());
        ArchiveGeneration revision = tiered.archiveWindow(windowStart, windowEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, revision.rowCount());
        assertFalse(first.id().equals(revision.id()));

        List<Integer> revisions = new ArrayList<>();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT revision FROM oscar_archive_job WHERE profile='RAW_GATED' " +
                        "AND store_key=? AND id_scope=? AND window_start=? AND window_end=? ORDER BY revision")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            UtcJdbcTime.setInstant(statement, 3, windowStart);
            UtcJdbcTime.setInstant(statement, 4, windowEnd);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    revisions.add(result.getInt(1));
            }
        }
        assertEquals(List.of(1, 2), revisions);
        assertEquals(2, tiered.getArchiveCatalog().candidates(windowStart, windowEnd, null).size());
        assertEquals(2, tiered.getNumRecords());
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            var selected = rows.map(entry -> entry.getKey().getIdAsLong() + "@" +
                    entry.getValue().getPhenomenonTime()).toList();
            assertEquals(selected.toString(), 2, selected.size());
        }
    }

    @Test
    public void failedActivationRemovesPublishedAttemptObjects() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant start = Instant.parse("2026-05-06T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(30), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();

        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION codex_storage_it_block_archive_mark() " +
                    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " +
                    "IF NEW.archivegenerationid IS NOT NULL THEN RETURN NULL; END IF; RETURN NEW; END $$");
            statement.execute("CREATE TRIGGER codex_storage_it_block_archive_mark " +
                    "BEFORE UPDATE ON " + TABLE + " FOR EACH ROW " +
                    "EXECUTE FUNCTION codex_storage_it_block_archive_mark()");
        }
        try {
            assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            try (var paths = Files.walk(root)) {
                assertEquals(0, paths.filter(Files::isRegularFile).count());
            }
            try (Connection connection = open(); Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT count(*) FROM oscar_archive_job WHERE state='FAILED'")) {
                assertTrue(result.next());
                assertEquals(1, result.getLong(1));
            }
        } finally {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_block_archive_mark ON " + TABLE);
                statement.execute("DROP FUNCTION IF EXISTS codex_storage_it_block_archive_mark()");
            }
        }
    }

    @Test
    public void concurrentResultCorrectionRejectsStaleExportAndRetriesExactly() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Path root = Path.of(archiveConfig.rootDirectory);
        Instant start = Instant.parse("2026-05-07T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(31), "gammaCounts");
        Map<BigId, org.sensorhub.api.data.IObsData> inserted =
                addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 60_000);
        forceReadBackFromStorage();
        long correctedId = inserted.keySet().stream().mapToLong(BigId::getIdAsLong).min().orElseThrow();
        String correctedJson = correctedResultJson(hotResultJson(correctedId), 10_000.0);

        installActivationBarrier();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<?> export = executor.submit(() -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();

            try (Connection connection = open(); PreparedStatement update = connection.prepareStatement(
                    "UPDATE " + TABLE + " SET result=?::jsonb WHERE id=?")) {
                update.setString(1, correctedJson);
                update.setLong(2, correctedId);
                assertEquals(1, update.executeUpdate());
            }
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> export.get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }

        assertEquals(JsonParser.parseString(correctedJson),
                JsonParser.parseString(hotResultJson(correctedId)));
        assertEquals(0, archivedHotRows());
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());
        assertEquals(0, regularArchiveFiles(root));

        // FAILED reuses the same job/generation identity, but regenerates its
        // deterministic files from the now-current exact row values.
        ArchiveGeneration recovered = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(2, recovered.rowCount());
        assertEquals(2, archivedHotRows());
        assertEquals(JsonParser.parseString(correctedJson),
                JsonParser.parseString(hotResultJson(correctedId)));
        assertEquals(2, selectAll(tiered));
    }

    @Test
    public void concurrentDeleteAndInsertCannotSubstituteForExportedIdentity() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        TestableTieredStore writable = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-05-08T12:00:00Z");
        BigId stream = addSimpleDataStream(bigId(32), "gammaCounts");
        var schema = tiered.getDataStreams().get(new DataStreamKey(stream));
        String firstJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), values(1));
        String secondJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), values(101));
        long firstId = 8_000_000L;
        long removedId = firstId + 2;
        long replacementId = firstId + 1;
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            writable.insertVerified(connection, firstId, stream.getIdAsLong(), null,
                    start, null, firstJson);
            writable.insertVerified(connection, removedId, stream.getIdAsLong(), null,
                    start.plusSeconds(2), null, secondJson);
            connection.commit();
        }

        installActivationBarrier();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Connection barrier = open(); Statement lock = barrier.createStatement()) {
            lock.execute("SELECT pg_advisory_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");
            Future<?> export = executor.submit(() -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            waitForBlockedActivation();

            try (Connection connection = open()) {
                connection.setAutoCommit(false);
                try (PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM " + TABLE + " WHERE id=?")) {
                    delete.setLong(1, removedId);
                    assertEquals(1, delete.executeUpdate());
                }
                writable.insertVerified(connection, replacementId, stream.getIdAsLong(), null,
                        start.plusSeconds(1), null, secondJson);
                connection.commit();
            }
            lock.execute("SELECT pg_advisory_unlock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + ")");

            ExecutionException failure = assertThrows(ExecutionException.class,
                    () -> export.get(30, TimeUnit.SECONDS));
            assertTrue(failure.getCause() instanceof IllegalStateException);
        } finally {
            executor.shutdownNow();
            executor.awaitTermination(30, TimeUnit.SECONDS);
            dropActivationBarrier();
        }

        // The old range/count update saw two rows before and two rows after,
        // so it could silently assign the inserted replacement to a Parquet
        // generation that did not contain it. Exact activation rolls back.
        assertEquals(0, archivedHotRows());
        assertFalse(hotRowExists(removedId));
        assertTrue(hotRowExists(replacementId));
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());

        ArchiveGeneration recovered = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(2, recovered.rowCount());
        assertEquals(firstId, recovered.minimumId());
        assertEquals(replacementId, recovered.maximumId());
        assertEquals(2, archivedHotRows());
        assertEquals(2, selectAll(tiered));
    }

    @Test
    public void rejectedNaturalKeyDoesNotPoisonTheBatchQueue() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId stream = addSimpleDataStream(bigId(40), "gammaCounts");
        Instant time = Instant.parse("2026-06-01T00:00:00.000002500Z");
        DataBlockDouble initial = values(1);
        DataBlockDouble correction = values(101);

        BigId stableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        tiered.commit();

        // Reopen to prove the batch store initializes its per-datastream
        // high-water mark from PostgreSQL rather than relying on process-local
        // state to return the stable logical ID.
        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;

        // Exact replay is idempotent. Several corrected samples at the same
        // OSH identity in one timed batch coalesce to the newest result, while
        // an unrelated row commits normally.
        BigId replayId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        BigId firstCorrectionId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(51)).build());
        BigId finalCorrectionId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(correction).build());
        assertEquals(stableId, replayId);
        assertEquals(stableId, firstCorrectionId);
        assertEquals(stableId, finalCorrectionId);
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time.plusSeconds(1)).withResult(initial).build());
        tiered.commit();
        assertEquals(2, tiered.getNumRecords());
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);

        // A later exact replay neither changes the row nor poisons the next
        // timed commit, proving restart-style duplicate delivery is harmless.
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(correction).build());
        tiered.commit();
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time.plusSeconds(2)).withResult(initial).build());
        tiered.commit();
        assertEquals(3, tiered.getNumRecords());
    }

    @Test
    public void naturalKeyCorrectionOfArchivedRowUsesOverlayThroughHotDeletion() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        BigId stream = addSimpleDataStream(bigId(41), "gammaCounts");
        Instant time = Instant.parse("2026-06-01T01:00:00.000002500Z");
        Instant baseOnlyTime = time.plus(1, ChronoUnit.MINUTES);
        DataBlockDouble initial = values(1);

        BigId stableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(initial).build());
        BigId baseOnlyStableId = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(baseOnlyTime).withResult(values(301)).build());
        tiered.commit();
        String immutableBase = hotResultJson(stableId.getIdAsLong());

        ArchiveGeneration generation = tiered.archiveWindow(
                time.truncatedTo(ChronoUnit.HOURS),
                time.truncatedTo(ChronoUnit.HOURS).plus(1, ChronoUnit.HOURS),
                "RAW_GATED").orElseThrow();
        tiered.maintainArchiveLifecycle();
        assertTrue(oneGeneration(catalog).parityVerified());

        // The batch coalescer must retain the original logical ID and apply
        // the final submitted value. Because the base now belongs to an
        // immutable archive generation, the correction belongs in an overlay
        // rather than in the hot parity source.
        BigId firstCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(51)).build());
        BigId finalCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(101)).build());
        assertEquals(stableId, firstCorrection);
        assertEquals(stableId, finalCorrection);
        tiered.commit();

        assertEquals(immutableBase, hotResultJson(stableId.getIdAsLong()));
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
        ArchiveCatalog.ArchivedOverlay overlay = catalog.overlayWithGeneration(
                stableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").orElseThrow();
        assertEquals(generation.id(), overlay.generationId());
        assertEquals(JsonParser.parseString(SerializerUtils.writeDataBlockToJson(
                        tiered.getDataStreams().get(new DataStreamKey(stream)).getRecordStructure(),
                        tiered.getDataStreams().get(new DataStreamKey(stream)).getRecordEncoding(),
                        values(101))),
                JsonParser.parseString(overlay.observation().resultJson()));

        // Once the verified grace copy is deleted, normal point and filtered
        // reads must still expose exactly the corrected logical observation.
        ageGeneration(generation.id());
        archiveConfig.deleteHotAfterArchive = true;
        tiered.maintainArchiveLifecycle();
        assertFalse(hotRowExists(stableId.getIdAsLong()));
        assertFalse(hotRowExists(baseOnlyStableId.getIdAsLong()));
        assertEquals("SEALED", oneGeneration(catalog).state());
        assertEquals(101.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
        try (var selected = tiered.selectEntries(new ObsFilter.Builder()
                .withInternalIDs(stableId).build())) {
            var entries = selected.toList();
            assertEquals(1, entries.size());
            assertEquals(stableId, entries.get(0).getKey());
            assertEquals(101.0, entries.get(0).getValue().getResult().getDoubleValue(0), 0.0);
        }

        // Reopen after every hot row is gone. The tiered high-water mark must
        // still recognize this as an old identity, find its stable ID in cold
        // storage, and avoid inserting a duplicate PostgreSQL row.
        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        ((TestableTieredStore) tiered).limitObservationPool(1);
        BigId coldReplay = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(101)).build());
        BigId coldBaseReplay = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(baseOnlyTime).withResult(values(301)).build());
        assertEquals(stableId, coldReplay);
        assertEquals(baseOnlyStableId, coldBaseReplay);
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(2, tiered.getNumRecords());
        assertEquals(2, selectAll(tiered));
        assertTrue(tiered.getArchiveCatalog().overlayWithGeneration(
                baseOnlyStableId.getIdAsLong(), DATABASE_NUM, "OVERLAY").isEmpty());
        assertEquals(301.0, tiered.get(baseOnlyStableId).getResult().getDoubleValue(0), 0.0);

        BigId coldFirstCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(151)).build());
        BigId coldFinalCorrection = tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(time).withResult(values(201)).build());
        assertEquals(stableId, coldFirstCorrection);
        assertEquals(stableId, coldFinalCorrection);
        // Force reconcileProtectedObservations to resolve the schema through
        // the datastream store's independent pool while the observation pool's
        // sole connection is held by the batch transaction.
        ((PostgisDataStreamStoreImpl) tiered.getDataStreams()).clearCache();
        tiered.commit();
        assertEquals(0, hotRows());
        assertEquals(2, tiered.getNumRecords());
        assertEquals(2, selectAll(tiered));
        assertEquals(201.0, tiered.get(stableId).getResult().getDoubleValue(0), 0.0);
    }

    private static DataBlockDouble values(double first) {
        DataBlockDouble values = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) values.setDoubleValue(i, first + i);
        return values;
    }

    @Test
    public void existingObservationTableReceivesNullSafeNaturalKeyIndex() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP INDEX " + TABLE + "_unique_constraint_v2");
        }
        TestableTieredStore reopened = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT indisvalid FROM pg_index WHERE indexrelid='" +
                             TABLE + "_unique_constraint_v2'::regclass")) {
            assertTrue(result.next());
            assertTrue(result.getBoolean(1));
        } finally {
            reopened.close();
        }
    }

    @Test
    public void naturalKeyLookupGenericPlanUsesNullSafeUniqueIndex() throws Exception {
        QueryBuilderObsStore builder = new QueryBuilderObsStore(TABLE);
        String preparedQuery = builder.findByUniqueFieldsQuery();
        for (int parameter = 1; parameter <= 4; parameter++) {
            int marker = preparedQuery.indexOf('?');
            assertTrue("Expected all natural-key parameters in the generated lookup", marker >= 0);
            preparedQuery = preparedQuery.substring(0, marker) + "$" + parameter +
                    preparedQuery.substring(marker + 1);
        }
        assertFalse(preparedQuery.contains("?"));

        String preparedName = "oscar_natural_key_lookup";
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("SET plan_cache_mode=force_generic_plan");
            statement.execute("SET enable_seqscan=off");
            statement.execute("PREPARE " + preparedName +
                    "(bigint,bigint,timestamp,timestamp) AS " + preparedQuery);

            for (String arguments : List.of(
                    "9223372036854775806, NULL, NULL, NULL",
                    "9223372036854775806, 7, TIMESTAMP '2031-01-01 00:00:00.000003', " +
                            "TIMESTAMP '2031-01-01 00:00:00.000003'")) {
                StringBuilder plan = new StringBuilder();
                try (ResultSet rows = statement.executeQuery(
                        "EXPLAIN (COSTS OFF) EXECUTE " + preparedName + "(" + arguments + ")")) {
                    while (rows.next())
                        plan.append(rows.getString(1)).append('\n');
                }
                String rendered = plan.toString();
                assertTrue("Generic natural-key plan must use the matching expression index:\n" + plan,
                        rendered.contains(TABLE + "_unique_constraint_v2"));
                int conditionStart = rendered.indexOf("Index Cond:");
                int conditionEnd = conditionStart < 0 ? -1 : rendered.indexOf('\n', conditionStart);
                assertTrue("Natural-key equality must be an index condition, not a residual filter:\n" + plan,
                        conditionStart >= 0 && conditionEnd > conditionStart);
                String indexCondition = rendered.substring(conditionStart, conditionEnd);
                assertTrue(indexCondition.contains("COALESCE(foiid"));
                assertTrue(indexCondition.contains("COALESCE(phenomenontime"));
                assertTrue(indexCondition.contains("COALESCE(resulttime"));
                assertFalse("Natural-key columns must not be evaluated after the index scan:\n" + plan,
                        rendered.contains("Filter:"));
            }
        }
    }

    @Test
    public void closeSurfacesFinalFlushFailureAndRetainsPendingObservation() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        BigId stream = addSimpleDataStream(bigId(45), "gammaCounts");
        DataBlockDouble values = new DataBlockDouble(5);
        for (int i = 0; i < 5; i++) values.setDoubleValue(i, i + 10);
        tiered.add(new ObsData.Builder().withDataStream(stream)
                .withPhenomenonTime(Instant.parse("2026-06-02T00:00:00Z"))
                .withResult(values).build());

        String unavailable = TABLE + "_temporarily_unavailable";
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("ALTER TABLE " + TABLE + " RENAME TO " + unavailable);
        }
        try {
            assertThrows(IllegalStateException.class, tiered::close);
        } finally {
            try (Connection connection = open(); Statement statement = connection.createStatement()) {
                statement.execute("ALTER TABLE " + unavailable + " RENAME TO " + TABLE);
            }
        }
        tiered.commit();
        assertEquals(1, tiered.getNumRecords());
    }

    @Test
    public void archivePayloadByteCapFailsBeforePublication() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(46), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 2, 1_000);
        forceReadBackFromStorage();
        archiveConfig.maximumUncompressedBytesPerGeneration = 1;

        IllegalStateException planningFailure = assertThrows(IllegalStateException.class, () ->
                tiered.selectArchiveWindowEnd(start, start.plusSeconds(1)));
        assertTrue(planningFailure.getMessage().contains("minimum one-second window"));
        assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
        assertEquals(2, hotRows());
        assertTrue(tiered.getArchiveCatalog().candidates(
                start, start.plus(1, ChronoUnit.HOURS), null).isEmpty());
        try (var paths = Files.walk(Path.of(archiveConfig.rootDirectory))) {
            assertEquals(0, paths.filter(Files::isRegularFile).count());
        }
    }

    @Test
    public void byteHeavyArchiveWindowSplitsAndActivatesBothPrefixes() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T12:00:00Z");
        Instant midpoint = start.plus(30, ChronoUnit.MINUTES);
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(49), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 4, 15 * 60_000);
        forceReadBackFromStorage();

        archiveConfig.maximumRowsPerGeneration = 100;
        OscarTieredObsStore.ArchiveCandidateFootprint full =
                tiered.measureArchiveCandidates(start, end, 100);
        OscarTieredObsStore.ArchiveCandidateFootprint first =
                tiered.measureArchiveCandidates(start, midpoint, 100);
        OscarTieredObsStore.ArchiveCandidateFootprint second =
                tiered.measureArchiveCandidates(midpoint, end, 100);
        assertEquals(4, full.rowCount());
        assertEquals(2, first.rowCount());
        assertEquals(2, second.rowCount());
        long exporterEstimate = 0;
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT result::text FROM " + TABLE + " ORDER BY id")) {
            while (result.next())
                exporterEstimate = Math.addExact(exporterEstimate,
                        ArchiveExporter.estimateUncompressedRowBytes(result.getString(1)));
        }
        assertEquals(exporterEstimate, full.estimatedUncompressedBytes());
        long byteCap = Math.max(first.estimatedUncompressedBytes(),
                second.estimatedUncompressedBytes());
        assertTrue(full.estimatedUncompressedBytes() > byteCap);
        archiveConfig.maximumUncompressedBytesPerGeneration = byteCap;

        assertEquals(midpoint, tiered.selectArchiveWindowEnd(start, end));
        ArchiveGeneration firstGeneration = tiered.archiveWindow(
                start, midpoint, "RAW_GATED").orElseThrow();
        assertEquals(2, firstGeneration.rowCount());
        assertEquals(midpoint, tiered.nextArchiveWindowStart(end, "RAW_GATED").orElseThrow());
        assertEquals(end, tiered.selectArchiveWindowEnd(midpoint, end));
        ArchiveGeneration secondGeneration = tiered.archiveWindow(
                midpoint, end, "RAW_GATED").orElseThrow();
        assertEquals(2, secondGeneration.rowCount());
        assertEquals(2, tiered.getArchiveCatalog().candidates(start, end, null).size());

        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT count(*), " +
                     "count(DISTINCT archivegenerationid) FROM " + TABLE +
                     " WHERE archivegenerationid IS NOT NULL")) {
            assertTrue(result.next());
            assertEquals(4, result.getLong(1));
            assertEquals(2, result.getLong(2));
        }
    }

    @Test
    public void denseHourUsesBoundedSameWindowRevisionsAndRecoversFailedJob() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T15:00:00Z");
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(52), "gammaCounts");
        List<Long> expectedIds = addSimpleObsWithoutResultTime(
                stream, BigId.NONE, start, 6, 10 * 60_000).keySet().stream()
                .map(BigId::getIdAsLong)
                .sorted()
                .toList();
        forceReadBackFromStorage();

        archiveConfig.maximumRowsPerGeneration = 100;
        archiveConfig.maximumUncompressedBytesPerGeneration = 1;
        assertThrows(IllegalStateException.class, () ->
                tiered.archiveWindow(start, end, "RAW_GATED"));

        UUID failedJob;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT id FROM oscar_archive_job WHERE store_key=? AND id_scope=? " +
                        "AND profile='RAW_GATED' AND window_start=? AND window_end=? AND state='FAILED'")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            UtcJdbcTime.setInstant(statement, 3, start);
            UtcJdbcTime.setInstant(statement, 4, end);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                failedJob = result.getObject(1, UUID.class);
                assertFalse(result.next());
            }
        }

        List<Long> rowBytes = new ArrayList<>();
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT result::text FROM " + TABLE + " ORDER BY phenomenontime, id")) {
            while (result.next())
                rowBytes.add(ArchiveExporter.estimateUncompressedRowBytes(result.getString(1)));
        }
        assertEquals(6, rowBytes.size());
        long twoRowBudget = Math.addExact(rowBytes.get(0), rowBytes.get(1));
        assertTrue(Math.addExact(twoRowBudget, rowBytes.get(2)) > twoRowBudget);
        archiveConfig.maximumUncompressedBytesPerGeneration = twoRowBudget;

        ArchiveGeneration first = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        assertEquals(2, first.rowCount());
        assertEquals(UUID.nameUUIDFromBytes(
                        ("oscar-archive-generation-v1:" + failedJob)
                                .getBytes(StandardCharsets.UTF_8)),
                first.id());
        ArchiveGeneration second = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        ArchiveGeneration third = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        assertEquals(2, second.rowCount());
        assertEquals(2, third.rowCount());
        assertTrue(tiered.nextArchiveWindowStart(end, "RAW_GATED").isEmpty());

        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FILTER (WHERE state='FAILED'), " +
                             "count(*) FILTER (WHERE state='ACTIVE'), max(revision) " +
                             "FROM oscar_archive_job WHERE store_key='" + TABLE +
                             "' AND id_scope=" + DATABASE_NUM + " AND profile='RAW_GATED' " +
                             "AND window_start='" + start + "' AND window_end='" + end + "'")) {
            assertTrue(result.next());
            assertEquals(0, result.getLong(1));
            assertEquals(3, result.getLong(2));
            assertEquals(3, result.getInt(3));
        }
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*), count(DISTINCT archivegenerationid), " +
                             "count(*) FILTER (WHERE archivegenerationid IS NULL) FROM " + TABLE)) {
            assertTrue(result.next());
            assertEquals(6, result.getLong(1));
            assertEquals(3, result.getLong(2));
            assertEquals(0, result.getLong(3));
        }
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        assertEquals(3, catalog.candidates(start, end, null).size());

        // All three revisions are query-visible during rollback grace, while
        // their linked hot copies remain present. The tiered union must return
        // each logical observation exactly once.
        List<Long> mixedTierIds;
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            mixedTierIds = rows.map(entry -> entry.getKey().getIdAsLong())
                    .sorted()
                    .toList();
        }
        assertEquals(expectedIds, mixedTierIds);

        for (int attempt = 0; attempt < 6 && catalog.candidates(start, end, null).stream()
                .anyMatch(generation -> !generation.parityVerified()); attempt++) {
            tiered.maintainArchiveLifecycle();
        }
        List<ArchiveGeneration> verified = catalog.candidates(start, end, null);
        assertEquals(3, verified.size());
        assertTrue(verified.stream().allMatch(ArchiveGeneration::parityVerified));

        ageGeneration(first.id());
        ageGeneration(second.id());
        ageGeneration(third.id());
        archiveConfig.deleteHotAfterArchive = true;
        for (int attempt = 0; attempt < 12 && hotRows() > 0; attempt++)
            tiered.maintainArchiveLifecycle();
        assertEquals(0, hotRows());

        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FILTER (WHERE state='SEALED'), " +
                        "count(*) FILTER (WHERE state<>'SEALED') " +
                        "FROM oscar_archive_generation WHERE store_key=? AND id_scope=? " +
                        "AND profile='RAW_GATED' AND window_start=? AND window_end=?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            UtcJdbcTime.setInstant(statement, 3, start);
            UtcJdbcTime.setInstant(statement, 4, end);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(3, result.getLong(1));
                assertEquals(0, result.getLong(2));
            }
        }
        assertEquals(6, tiered.getNumRecords());
        try (var rows = tiered.selectEntries(new ObsFilter.Builder().build())) {
            assertEquals(expectedIds, rows.map(entry -> entry.getKey().getIdAsLong())
                    .sorted()
                    .toList());
        }

        // A fresh process must reconstruct the same cold-only view from the
        // durable catalog and Parquet objects.
        tiered.close();
        TestableTieredStore restarted = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = restarted;
        assertEquals(6, restarted.getNumRecords());
        try (var rows = restarted.selectEntries(new ObsFilter.Builder().build())) {
            assertEquals(expectedIds, rows.map(entry -> entry.getKey().getIdAsLong())
                    .sorted()
                    .toList());
        }
    }

    @Test
    public void legacyFailedSubHourJobIsRetriedBeforeFullHourWork() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T16:00:00Z");
        Instant legacyEnd = start.plus(30, ChronoUnit.MINUTES);
        Instant cutoff = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(54), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 6, 10 * 60_000);
        forceReadBackFromStorage();

        archiveConfig.maximumUncompressedBytesPerGeneration = 1;
        assertThrows(IllegalStateException.class, () ->
                tiered.archiveWindow(start, legacyEnd, "RAW_GATED"));

        ArchiveCatalog.RetryableJobWindow retry = tiered
                .nextRetryableArchiveWindow(cutoff, "RAW_GATED")
                .orElseThrow();
        assertEquals(start, retry.start());
        assertEquals(legacyEnd, retry.end());

        archiveConfig.maximumUncompressedBytesPerGeneration = Long.MAX_VALUE;
        ArchiveGeneration recovered = tiered
                .archiveWindow(retry.start(), retry.end(), "RAW_GATED")
                .orElseThrow();
        assertEquals(3, recovered.rowCount());
        assertTrue(tiered.nextRetryableArchiveWindow(cutoff, "RAW_GATED").isEmpty());
        assertEquals(legacyEnd, tiered.nextArchiveWindowStart(cutoff, "RAW_GATED").orElseThrow());

        ArchiveGeneration remainder = tiered
                .archiveWindow(start, cutoff, "RAW_GATED")
                .orElseThrow();
        assertEquals(3, remainder.rowCount());
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FILTER (WHERE state='FAILED'), " +
                             "count(*) FILTER (WHERE state='ACTIVE') " +
                             "FROM oscar_archive_job WHERE store_key='" + TABLE +
                             "' AND id_scope=" + DATABASE_NUM + " AND profile='RAW_GATED'")) {
            assertTrue(result.next());
            assertEquals(0, result.getLong(1));
            assertEquals(2, result.getLong(2));
        }
        assertEquals(6, selectAll(tiered));
    }

    @Test
    public void emptyLegacyRetryIsSupersededWithoutStarvingLaterWork() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T16:00:00Z");
        Instant legacyEnd = start.plus(30, ChronoUnit.MINUTES);
        Instant cutoff = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(54), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 6, 10 * 60_000);
        forceReadBackFromStorage();

        archiveConfig.maximumUncompressedBytesPerGeneration = 1;
        assertThrows(IllegalStateException.class, () ->
                tiered.archiveWindow(start, legacyEnd, "RAW_GATED"));
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "DELETE FROM " + TABLE + " WHERE phenomenontime>=? AND phenomenontime<?")) {
            UtcJdbcTime.setInstant(statement, 1, start);
            UtcJdbcTime.setInstant(statement, 2, legacyEnd);
            assertEquals(3, statement.executeUpdate());
        }

        ArchiveCatalog.RetryableJobWindow emptyRetry = tiered
                .nextRetryableArchiveWindow(cutoff, "RAW_GATED")
                .orElseThrow();
        archiveConfig.maximumUncompressedBytesPerGeneration = Long.MAX_VALUE;
        assertTrue(tiered.archiveWindow(emptyRetry.start(), emptyRetry.end(), "RAW_GATED").isEmpty());
        assertTrue(tiered.nextRetryableArchiveWindow(cutoff, "RAW_GATED").isEmpty());

        assertEquals(legacyEnd, tiered.nextArchiveWindowStart(cutoff, "RAW_GATED").orElseThrow());
        ArchiveGeneration later = tiered.archiveWindow(start, cutoff, "RAW_GATED").orElseThrow();
        assertEquals(3, later.rowCount());
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FILTER (WHERE state='FAILED'), " +
                             "count(*) FILTER (WHERE state='SUPERSEDED'), " +
                             "count(*) FILTER (WHERE state='ACTIVE') " +
                             "FROM oscar_archive_job WHERE store_key='" + TABLE +
                             "' AND id_scope=" + DATABASE_NUM + " AND profile='RAW_GATED'")) {
            assertTrue(result.next());
            assertEquals(0, result.getLong(1));
            assertEquals(1, result.getLong(2));
            assertEquals(1, result.getLong(3));
        }
        assertEquals(3, selectAll(tiered));
    }

    @Test
    public void productionMemoryBudgetArchivesObservedDenseHourAcrossRestart() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T17:00:00Z");
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(53), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1, 0);
        forceReadBackFromStorage();

        String resultJson;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT result::text FROM " + TABLE + " WHERE datastreamid=? LIMIT 1")) {
            statement.setLong(1, stream.getIdAsLong());
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                resultJson = result.getString(1);
            }
        }
        int bulkRows = 184_194;
        bulkInsertKeysetRows(stream.getIdAsLong(), 5_000_000L, 6_000_000L,
                start.plusSeconds(1), resultJson, bulkRows);
        long expectedRows = bulkRows + 1L;
        assertEquals(32L * 1024 * 1024,
                archiveConfig.effectiveMaximumUncompressedBytesPerGeneration());

        ArchiveGeneration first = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        assertTrue(first.rowCount() > 0);
        assertTrue(first.rowCount() < expectedRows);

        tiered.close();
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;

        int generationCount = 1;
        while (tiered.nextArchiveWindowStart(end, "RAW_GATED").isPresent()) {
            assertTrue("Dense hour must finish in bounded revisions", generationCount < 16);
            ArchiveGeneration next = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
            assertTrue(next.rowCount() > 0);
            generationCount++;
        }
        assertTrue(generationCount > 1);

        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*), count(DISTINCT archivegenerationid), " +
                             "count(*) FILTER (WHERE archivegenerationid IS NULL) FROM " + TABLE)) {
            assertTrue(result.next());
            assertEquals(expectedRows, result.getLong(1));
            assertEquals(generationCount, result.getInt(2));
            assertEquals(0, result.getLong(3));
        }
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FILTER (WHERE state='FAILED'), " +
                             "count(*) FILTER (WHERE state='ACTIVE') " +
                             "FROM oscar_archive_job WHERE store_key='" + TABLE +
                             "' AND id_scope=" + DATABASE_NUM + " AND profile='RAW_GATED'")) {
            assertTrue(result.next());
            assertEquals(0, result.getLong(1));
            assertEquals(generationCount, result.getInt(2));
        }
        assertEquals(expectedRows, tiered.getNumRecords());
    }

    @Test
    public void sparseByteHeavyHourDoesNotSelectAnEmptyPrefix() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-03T18:00:00Z");
        Instant firstObservation = start.plus(45, ChronoUnit.MINUTES);
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(50), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, firstObservation, 2, 10_000);
        forceReadBackFromStorage();

        archiveConfig.maximumRowsPerGeneration = 100;
        OscarTieredObsStore.ArchiveCandidateFootprint firstRow =
                tiered.measureArchiveCandidates(start, firstObservation.plusSeconds(1), 100);
        archiveConfig.maximumUncompressedBytesPerGeneration =
                firstRow.estimatedUncompressedBytes();

        Instant selectedEnd = tiered.selectArchiveWindowEnd(start, end);
        assertEquals(firstObservation.plusSeconds(1), selectedEnd);
        ArchiveGeneration generation = tiered.archiveWindow(
                start, selectedEnd, "RAW_GATED").orElseThrow();
        assertEquals(1, generation.rowCount());
        assertEquals(firstObservation.plusSeconds(10),
                tiered.nextArchiveWindowStart(end, "RAW_GATED").orElseThrow());
    }

    @Test
    public void staleJobRetryReplacesItsDeterministicCrashObjects() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Instant start = Instant.parse("2026-06-04T00:00:00Z");
        Instant end = start.plus(1, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(47), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1, 0);
        forceReadBackFromStorage();

        UUID jobId = catalog.claimJob("RAW_GATED", start, end).orElseThrow();
        UUID expectedGeneration = UUID.nameUUIDFromBytes(
                ("oscar-archive-generation-v1:" + jobId).getBytes(StandardCharsets.UTF_8));
        Path directory = Path.of(archiveConfig.rootDirectory)
                .resolve("datasets/observations/date=2026-06-04/profile=RAW_GATED");
        Files.createDirectories(directory);
        for (String suffix : List.of(".parquet", ".schemas.json.gz", ".manifest.json"))
            Files.writeString(directory.resolve("generation-" + expectedGeneration + suffix), "crash-leftover");
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_job SET updated_at=now()-interval '3 hours' " +
                        "WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setObject(1, jobId);
            statement.setString(2, TABLE);
            statement.setInt(3, DATABASE_NUM);
            assertEquals(1, statement.executeUpdate());
        }

        ArchiveGeneration recovered = tiered.archiveWindow(start, end, "RAW_GATED").orElseThrow();
        assertEquals(expectedGeneration, recovered.id());
        assertTrue(Files.size(Path.of(archiveConfig.rootDirectory)
                .resolve(recovered.objectPath())) > "crash-leftover".length());
        assertEquals(1, catalog.candidates(start, end, null).size());
    }

    @Test
    public void archivePublicationRejectsSymlinkedPartitionPath() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-06-05T00:00:00Z");
        BigId stream = addSimpleDataStream(bigId(48), "gammaCounts");
        addSimpleObsWithoutResultTime(stream, BigId.NONE, start, 1, 0);
        forceReadBackFromStorage();
        Path root = Path.of(archiveConfig.rootDirectory);
        Path outside = Files.createTempDirectory("codex-oscar-archive-outside-");
        try {
            try {
                Files.createSymbolicLink(root.resolve("datasets"), outside);
            } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
                return;
            }
            assertThrows(IllegalStateException.class, () -> tiered.archiveWindow(
                    start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED"));
            try (var paths = Files.list(outside)) {
                assertEquals(0, paths.count());
            }
        } finally {
            Files.deleteIfExists(root.resolve("datasets"));
            Files.deleteIfExists(outside);
        }
    }

    @Test
    public void catalogMigrationPreservesUnknownLegacyObservationHoldProvenance() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        UUID generationHold = UUID.randomUUID();
        UUID observationHold = UUID.randomUUID();
        tiered.close();
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            connection.setAutoCommit(false);
            statement.execute("DELETE FROM oscar_storage_schema_version WHERE version=5");
            statement.execute("ALTER TABLE oscar_retention_hold DROP CONSTRAINT IF EXISTS " +
                    "oscar_retention_hold_resolution_check");
            statement.execute("ALTER TABLE oscar_retention_hold DROP COLUMN resolution_mode");
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO oscar_retention_hold(id,store_key,id_scope,subject_type," +
                            "subject_id,reason,created_by) VALUES (?,?,?,?,?,'legacy test','migration')")) {
                insert.setObject(1, generationHold);
                insert.setString(2, TABLE);
                insert.setInt(3, DATABASE_NUM);
                insert.setString(4, "ARCHIVE_GENERATION");
                insert.setString(5, UUID.randomUUID().toString());
                assertEquals(1, insert.executeUpdate());
                insert.setObject(1, observationHold);
                insert.setString(4, "OBSERVATION");
                insert.setString(5, "123456789");
                assertEquals(1, insert.executeUpdate());
            }
            connection.commit();
        }

        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT id,resolution_mode FROM oscar_retention_hold WHERE id IN (?,?) ORDER BY id")) {
            statement.setObject(1, generationHold);
            statement.setObject(2, observationHold);
            try (ResultSet result = statement.executeQuery()) {
                Map<UUID, String> modes = new java.util.HashMap<>();
                while (result.next())
                    modes.put(result.getObject(1, UUID.class), result.getString(2));
                assertEquals("GENERATION_EXACT", modes.get(generationHold));
                assertEquals("LEGACY_UNKNOWN", modes.get(observationHold));
            }
        }
    }

    @Test
    public void catalogMigrationCompactsLegacyBackupGenerationInventoryReceipt() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        UUID epochId = UUID.randomUUID();
        tiered.close();
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM oscar_storage_schema_version WHERE version=5");
            statement.execute("ALTER TABLE oscar_backup_epoch " +
                    "ADD COLUMN archive_generation_ids uuid[] NOT NULL DEFAULT '{}'");
            statement.execute("ALTER TABLE oscar_backup_epoch " +
                    "DROP COLUMN archive_generation_count, " +
                    "DROP COLUMN archive_generation_inventory_sha256, " +
                    "DROP COLUMN archive_generation_inventory_bytes");
            statement.execute("INSERT INTO oscar_backup_epoch(" +
                    "id,state,postgres_lsn,archive_generation_ids,manifest_path,sha256) VALUES ('" +
                    epochId + "','VERIFIED','0/ABC',ARRAY[" +
                    "'22222222-2222-4222-8222-222222222222'::uuid," +
                    "'11111111-1111-4111-8111-111111111111'::uuid]," +
                    "'backup/epoch.json',repeat('a',64))");
        }

        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        assertEquals(5, ArchiveCatalog.SCHEMA_VERSION);
        assertEquals("sha256:b37753a17c972d57418545c05a2544068affc7804b8622db3a0f0c4b422c63b5",
                ArchiveCatalog.SCHEMA_CHECKSUM);
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery(
                    "SELECT archive_generation_count,archive_generation_inventory_sha256," +
                            "archive_generation_inventory_bytes,postgres_lsn::text,manifest_path,sha256 " +
                            "FROM oscar_backup_epoch WHERE id='" +
                            epochId + "'")) {
                assertTrue(result.next());
                assertEquals(2L, result.getLong(1));
                assertEquals("be2001bca1476f8f8480dd7db6264be12c94cb7be24332f370976c1ebc46e729",
                        result.getString(2));
                assertEquals(74L, result.getLong(3));
                assertEquals("0/ABC", result.getString(4));
                assertEquals("backup/epoch.json", result.getString(5));
                assertEquals("a".repeat(64), result.getString(6));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT count(*) FROM information_schema.columns " +
                            "WHERE table_name='oscar_backup_epoch' " +
                            "AND column_name='archive_generation_ids'")) {
                assertTrue(result.next());
                assertEquals(0L, result.getLong(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT column_name,is_nullable FROM information_schema.columns " +
                            "WHERE table_name='oscar_backup_epoch' AND column_name IN (" +
                            "'archive_generation_count','archive_generation_inventory_sha256'," +
                            "'archive_generation_inventory_bytes') ORDER BY column_name")) {
                List<String> columns = new ArrayList<>();
                while (result.next()) {
                    columns.add(result.getString(1));
                    assertEquals("NO", result.getString(2));
                }
                assertEquals(List.of("archive_generation_count",
                        "archive_generation_inventory_bytes",
                        "archive_generation_inventory_sha256"), columns);
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT conname FROM pg_constraint WHERE conrelid='oscar_backup_epoch'::regclass " +
                            "AND conname LIKE 'oscar_backup_epoch_archive_generation_%_check' " +
                            "ORDER BY conname")) {
                List<String> constraints = new ArrayList<>();
                while (result.next())
                    constraints.add(result.getString(1));
                assertEquals(List.of(
                        "oscar_backup_epoch_archive_generation_count_check",
                        "oscar_backup_epoch_archive_generation_inventory_bytes_check",
                        "oscar_backup_epoch_archive_generation_inventory_sha256_check"), constraints);
            }
            String validHash = "'" + "0".repeat(64) + "'";
            assertThrows(SQLException.class, () -> statement.execute(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',-1," + validHash + ",0)"));
            assertThrows(SQLException.class, () -> statement.execute(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',1000001," + validHash + ",37000037)"));
            assertThrows(SQLException.class, () -> statement.execute(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',0,repeat('A',64),0)"));
            assertThrows(SQLException.class, () -> statement.execute(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',0," + validHash + ",-1)"));
            assertThrows(SQLException.class, () -> statement.execute(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',1," + validHash + ",36)"));
            assertEquals(1, statement.executeUpdate(
                    "INSERT INTO oscar_backup_epoch(id,state,archive_generation_count," +
                            "archive_generation_inventory_sha256,archive_generation_inventory_bytes) VALUES (" +
                            "'" + UUID.randomUUID() + "','CREATED',1000000," + validHash + ",37000000)"));
        }
    }

    @Test
    public void catalogMigrationRejectsFutureSchemaVersion() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_storage_schema_version(version,checksum) VALUES (6,?)")) {
            statement.setString(1, "future-test-checksum");
            assertEquals(1, statement.executeUpdate());
        }
        tiered.close();
        assertThrows(IllegalStateException.class, () -> new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig));
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            assertEquals(1, statement.executeUpdate(
                    "DELETE FROM oscar_storage_schema_version WHERE version=6"));
        }
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
    }

    @Test
    public void catalogMigrationRejectsTamperedPriorSchemaChecksum() throws Exception {
        TestableTieredStore tiered = (TestableTieredStore) postgisObsStore;
        tiered.close();
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DELETE FROM oscar_storage_schema_version WHERE version=5");
            statement.execute("INSERT INTO oscar_storage_schema_version(version,checksum) " +
                    "VALUES (4,'tampered-v4') ON CONFLICT(version) DO UPDATE SET checksum=excluded.checksum");
        }

        assertThrows(IllegalStateException.class, () -> new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig));
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            assertEquals(0L, queryLong(connection,
                    "SELECT count(*) FROM oscar_storage_schema_version WHERE version=5"));
            statement.execute("UPDATE oscar_storage_schema_version SET checksum=" +
                    "'sha256:7aa2b6dd22cbed4499eb9fb16966323bbf90d14bff4d35e03a1670868fb2f8e8' " +
                    "WHERE version=4");
        }
        tiered = new TestableTieredStore(url, DB_NAME, login, password, TABLE,
                DATABASE_NUM, IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            assertEquals(1L, queryLong(connection,
                    "SELECT count(*) FROM oscar_storage_schema_version WHERE version=5"));
            statement.execute("DELETE FROM oscar_storage_schema_version WHERE version=4");
        }
    }

    @Test
    public void archiveCatalogNamespacesIndependentObservationStores() throws Exception {
        String secondTable = TABLE + "_second";
        int secondScope = DATABASE_NUM + 100;
        TestableTieredStore second = new TestableTieredStore(
                url, DB_NAME, login, password, secondTable, secondScope,
                IdProviderType.SEQUENTIAL, archiveConfig);
        try {
            ArchiveCatalog firstCatalog = ((OscarTieredObsStore) postgisObsStore).getArchiveCatalog();
            ArchiveCatalog secondCatalog = second.getArchiveCatalog();
            Instant start = Instant.parse("2026-07-01T00:00:00Z");
            Instant end = start.plus(1, ChronoUnit.HOURS);
            UUID firstJob = firstCatalog.claimJob("RAW_GATED", start, end).orElseThrow();
            UUID secondJob = secondCatalog.claimJob("RAW_GATED", start, end).orElseThrow();
            assertFalse(firstJob.equals(secondJob));
            UUID firstGeneration = insertManualGeneration(
                    firstJob, TABLE, DATABASE_NUM, start, end, "first.parquet");
            UUID secondGeneration = insertManualGeneration(
                    secondJob, secondTable, secondScope, start, end, "second.parquet");
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "UPDATE oscar_archive_generation SET max_id=? WHERE id=?")) {
                statement.setLong(1, 9_000_000L);
                statement.setObject(2, secondGeneration);
                assertEquals(1, statement.executeUpdate());
            }
            assertEquals(Long.valueOf(42), firstCatalog.maximumObservationId().orElseThrow());
            assertEquals(Long.valueOf(9_000_000L), secondCatalog.maximumObservationId().orElseThrow());

            ArchivedObservation firstOverlay = new ArchivedObservation(
                    DATABASE_NUM, 42, DATABASE_NUM, 10, null, null,
                    start, start, "{\"value\":1}", "OVERLAY");
            ArchivedObservation secondOverlay = new ArchivedObservation(
                    secondScope, 42, secondScope, 10, null, null,
                    start, start, "{\"value\":2}", "OVERLAY");
            assertThrows(IllegalArgumentException.class,
                    () -> firstCatalog.upsertOverlay(secondGeneration, firstOverlay, "{}"));
            assertThrows(IllegalArgumentException.class,
                    () -> firstCatalog.tombstone(42, secondGeneration, "wrong namespace", "TEST"));
            assertThrows(IllegalStateException.class,
                    () -> firstCatalog.recordVerification(secondGeneration,
                            "HOT_COLD_PARITY", true, 1, 1, "wrong namespace"));
            firstCatalog.upsertOverlay(firstGeneration, firstOverlay, "{}");
            secondCatalog.upsertOverlay(secondGeneration, secondOverlay, "{}");
            assertEquals(com.google.gson.JsonParser.parseString("{\"value\":1}"),
                    com.google.gson.JsonParser.parseString(firstCatalog.overlayWithGeneration(
                            42, DATABASE_NUM, "OVERLAY").orElseThrow().observation().resultJson()));
            assertEquals(com.google.gson.JsonParser.parseString("{\"value\":2}"),
                    com.google.gson.JsonParser.parseString(secondCatalog.overlayWithGeneration(
                            42, secondScope, "OVERLAY").orElseThrow().observation().resultJson()));

            firstCatalog.tombstone(42, firstGeneration, "first only", "TEST");
            assertTrue(firstCatalog.isTombstoned(42));
            assertFalse(secondCatalog.isTombstoned(42));
            assertEquals(1, firstCatalog.candidates(start, end, null).size());
            assertEquals(1, secondCatalog.candidates(start, end, null).size());
            assertEquals(0, firstCatalog.countTieredRecords(TABLE));
            assertEquals(1, secondCatalog.countTieredRecords(secondTable));
        } finally {
            second.close();
            dropDisposableTieredTables(secondTable);
            ((PostgisDataStreamStoreImpl) second.getDataStreams()).clearCache();
        }
    }

    private void dropDisposableTieredTables(String observationTable) throws Exception {
        try (Connection connection = open()) {
            List<String> stageChildren = new ArrayList<>();
            try (PreparedStatement partitions = connection.prepareStatement(
                    "SELECT child_table FROM oscar_obs_stage_partition WHERE store_key=?")) {
                partitions.setString(1, observationTable);
                try (ResultSet result = partitions.executeQuery()) {
                    while (result.next())
                        stageChildren.add(result.getString(1));
                }
            } catch (SQLException absentOnLegacySchema) {
                if (!"42P01".equals(absentOnLegacySchema.getSQLState()))
                    throw absentOnLegacySchema;
            }
            try (Statement statement = connection.createStatement()) {
                for (String stageChild : stageChildren)
                    statement.execute("DROP TABLE IF EXISTS " + stageChild);
            }
            try (PreparedStatement routes = connection.prepareStatement(
                    "DELETE FROM oscar_obs_storage_route WHERE store_key=?")) {
                routes.setString(1, observationTable);
                routes.executeUpdate();
            } catch (SQLException absentOnLegacySchema) {
                if (!"42P01".equals(absentOnLegacySchema.getSQLState()))
                    throw absentOnLegacySchema;
            }
            try (PreparedStatement owners = connection.prepareStatement(
                    "DELETE FROM oscar_obs_stage_owner WHERE store_key=?")) {
                owners.setString(1, observationTable);
                owners.executeUpdate();
            } catch (SQLException absentOnLegacySchema) {
                if (!"42P01".equals(absentOnLegacySchema.getSQLState()))
                    throw absentOnLegacySchema;
            }
            try (PreparedStatement partitions = connection.prepareStatement(
                    "DELETE FROM oscar_obs_stage_partition WHERE store_key=?")) {
                partitions.setString(1, observationTable);
                partitions.executeUpdate();
            } catch (SQLException absentOnLegacySchema) {
                if (!"42P01".equals(absentOnLegacySchema.getSQLState()))
                    throw absentOnLegacySchema;
            }
            for (String mutableCountTable : List.of("oscar_mutable_stream_counts",
                    "oscar_mutable_stream_counts_delta",
                    "oscar_mutable_stream_counts_state",
                    "oscar_obs_latest")) {
                try (PreparedStatement counts = connection.prepareStatement(
                        "DELETE FROM " + mutableCountTable + " WHERE store_key=?")) {
                    counts.setString(1, observationTable);
                    counts.executeUpdate();
                } catch (SQLException absentOnLegacySchema) {
                    if (!"42P01".equals(absentOnLegacySchema.getSQLState()))
                        throw absentOnLegacySchema;
                }
            }
            try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS " + observationTable);
            statement.execute("DROP TABLE IF EXISTS " + observationTable + "_datastreams");
            statement.execute("DROP TABLE IF EXISTS " + observationTable + "_fois");
            }
        }
    }

    private BigId addVersionedDataStream(BigId systemId, String outputName,
                                         TimeExtent validTime) throws Exception {
        SWEHelper swe = new SWEHelper();
        var record = swe.createRecord().name(outputName);
        for (int i = 0; i < 5; i++)
            record.addField("comp" + i, swe.createQuantity().build());
        var info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(systemId, "urn:oscar:test:system:" + systemId))
                .withRecordDescription(record.build())
                .withRecordEncoding(new TextEncodingImpl())
                .withValidTime(validTime)
                .build();
        return postgisObsStore.getDataStreams().add(info).getInternalID();
    }

    private static GenericTemporalFeatureImpl temporalFoi(
            String uid, String name, OffsetDateTime begin, OffsetDateTime end) {
        GenericTemporalFeatureImpl feature = new GenericTemporalFeatureImpl(
                new QName("urn:oscar:test", "VersionedFoi"));
        feature.setUniqueIdentifier(uid);
        feature.setName(name);
        feature.setValidTimePeriod(begin, end);
        return feature;
    }

    private UUID insertManualGeneration(UUID jobId, String storeKey, int scope,
                                        Instant start, Instant end, String objectPath) throws Exception {
        UUID generationId = UUID.randomUUID();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_archive_generation(id,job_id,store_key,id_scope,profile,window_start,window_end,state," +
                        "object_path,manifest_path,sha256,row_count,min_id,max_id) " +
                        "VALUES (?,?,?,?,?,?,?,'ACTIVE',?,?,'none',1,42,42)")) {
            statement.setObject(1, generationId);
            statement.setObject(2, jobId);
            statement.setString(3, storeKey);
            statement.setInt(4, scope);
            statement.setString(5, "RAW_GATED");
            UtcJdbcTime.setInstant(statement, 6, start);
            UtcJdbcTime.setInstant(statement, 7, end);
            statement.setString(8, objectPath);
            statement.setString(9, objectPath + ".manifest.json");
            statement.executeUpdate();
        }
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_archive_id_block(generation_id,min_id,max_id) VALUES (?,42,42)")) {
            statement.setObject(1, generationId);
            statement.executeUpdate();
        }
        return generationId;
    }

    private static long selectAll(OscarTieredObsStore store) {
        try (var rows = store.selectEntries(new ObsFilter.Builder().build())) {
            return rows.count();
        }
    }

    private void bulkInsertKeysetRows(long streamId, long idBase, long foiBase,
                                      Instant phenomenonTime, String resultJson,
                                      int rowCount) throws Exception {
        String sql = "INSERT INTO " + TABLE +
                " (id,datastreamid,foiid,phenomenontime,resulttime,result) " +
                "SELECT ? + series, ?, ? + series, ?, NULL, ?::jsonb " +
                "FROM generate_series(1, ?) series";
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, idBase);
            statement.setLong(2, streamId);
            statement.setLong(3, foiBase);
            if (phenomenonTime == null)
                statement.setTimestamp(4, null);
            else
                UtcJdbcTime.setInstant(statement, 4, phenomenonTime);
            statement.setString(5, resultJson);
            statement.setInt(6, rowCount);
            assertEquals(rowCount, statement.executeUpdate());
        }
    }

    private void assertSparseKeysetIds(ObsFilter filter, List<Long> expectedIds) {
        try (var rows = postgisObsStore.selectEntries(filter)) {
            assertEquals(expectedIds, rows
                    .map(entry -> entry.getKey().getIdAsLong())
                    .toList());
        }
    }

    private void assertStatementTriggerRange(BigId stream, Instant minimum, Instant maximum) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT min_phenomenon_time, max_phenomenon_time FROM " + TABLE + "_datastreams WHERE id=?")) {
            statement.setLong(1, stream.getIdAsLong());
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                assertEquals(minimum, result.getObject(1, LocalDateTime.class).toInstant(ZoneOffset.UTC));
                assertEquals(maximum, result.getObject(2, LocalDateTime.class).toInstant(ZoneOffset.UTC));
            }
        }
    }

    private void assertIdempotentInsertContract(long existingId) throws Exception {
        TestableTieredStore store = (TestableTieredStore) postgisObsStore;
        long streamId;
        Long foiId;
        Instant phenomenon;
        Instant resultTime;
        String resultJson;
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT datastreamid, foiid, phenomenontime, resulttime, result::text " +
                        "FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, existingId);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                streamId = result.getLong(1);
                foiId = result.getObject(2, Long.class);
                phenomenon = UtcJdbcTime.getInstant(result, 3);
                resultTime = UtcJdbcTime.getInstant(result, 4);
                resultJson = result.getString(5);
            }
        }

        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            String correctedJson = correctedResultJson(resultJson, 1000.0);
            // An exact primary-key replay with identical content is idempotent.
            store.insertVerified(connection, existingId, streamId, foiId,
                    phenomenon, resultTime, resultJson);
            connection.commit();
            // A corrected result at the same OSH observation identity uses
            // last-write-wins while retaining the stable stored ID.
            store.insertVerified(connection, existingId, streamId, foiId,
                    phenomenon, resultTime, correctedJson);
            connection.commit();

            // Establish a row for different-requested-ID replay/correction.
            long firstNaturalId = existingId + 1_000_000;
            Instant naturalPhenomenon = Instant.parse("2030-01-01T00:00:00Z");
            // Null result time is the common OSCAR shape and was not protected
            // by the former PostgreSQL unique-index definition.
            Instant naturalResult = null;
            store.insertVerified(connection, firstNaturalId, streamId, null,
                    naturalPhenomenon, naturalResult, resultJson);
            connection.commit();
            store.insertVerified(connection, firstNaturalId + 1,
                    streamId, null, naturalPhenomenon, naturalResult, resultJson);
            connection.commit();
            store.insertVerified(connection, firstNaturalId + 2,
                    streamId, null, naturalPhenomenon, naturalResult, correctedJson);
            connection.commit();
            try (PreparedStatement selected = connection.prepareStatement(
                    "SELECT id, result::text FROM " + TABLE + " WHERE datastreamid=? " +
                            "AND foiid IS NULL AND phenomenontime=?::timestamp AND resulttime IS NULL")) {
                selected.setLong(1, streamId);
                UtcJdbcTime.setInstant(selected, 2, naturalPhenomenon);
                try (ResultSet actual = selected.executeQuery()) {
                    assertTrue(actual.next());
                    assertEquals(firstNaturalId, actual.getLong(1));
                    assertEquals(JsonParser.parseString(correctedJson),
                            JsonParser.parseString(actual.getString(2)));
                    assertFalse(actual.next());
                }
            }

            // Reusing a primary key for a genuinely different observation is
            // still an integrity error; last-write-wins is scoped only to the
            // natural observation identity.
            assertThrows(SQLException.class, () -> store.insertVerified(connection, firstNaturalId,
                    streamId, null, naturalPhenomenon.plusSeconds(1), naturalResult, resultJson));
            connection.rollback();

            // pgjdbc rounds an exact 500-nanosecond tie upward. Conflict
            // verification must normalize identically or this valid insert
            // wedges the batch retry queue.
            long tieId = firstNaturalId + 3;
            Instant tie = Instant.parse("2031-01-01T00:00:00.000002500Z");
            store.insertVerified(connection, tieId, streamId, null, tie, tie, resultJson);
            connection.commit();
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT phenomenontime FROM " + TABLE + " WHERE id=?")) {
                select.setLong(1, tieId);
                try (ResultSet actual = select.executeQuery()) {
                    assertTrue(actual.next());
                    assertEquals(Instant.parse("2031-01-01T00:00:00.000003Z"),
                            UtcJdbcTime.getInstant(actual, 1));
                }
            }
            try (PreparedStatement delete = connection.prepareStatement("DELETE FROM " + TABLE + " WHERE id >= ?")) {
                delete.setLong(1, firstNaturalId);
                assertEquals(2, delete.executeUpdate());
            }
            connection.commit();
        }
    }

    private static String correctedResultJson(String original, double delta) {
        JsonObject corrected = JsonParser.parseString(original).getAsJsonObject().deepCopy();
        String numericField = corrected.entrySet().stream()
                .filter(entry -> entry.getValue().isJsonPrimitive() &&
                        entry.getValue().getAsJsonPrimitive().isNumber())
                .map(Map.Entry::getKey)
                .findFirst()
                .orElseThrow(() -> new AssertionError("Test observation result has no numeric field"));
        corrected.addProperty(numericField, corrected.get(numericField).getAsDouble() + delta);
        return corrected.toString();
    }

    private static final class TestableTieredStore extends OscarTieredObsStore {
        private Runnable beforeRemoveBatch;
        private Runnable afterRemoveBatchLocked;
        private boolean probeRemovalPaging;
        private int removalSelectionPasses;
        private int removalBatchPasses;
        private int maximumRemovalBatchSize;
        private int failBeforeRemoveBatch;
        private int removeBatchOrdinal;

        TestableTieredStore(String url, String dbName, String login, String password, String dataStoreName,
                            int idScope, IdProviderType idProviderType, TieredArchiveConfig config) {
            super(url, dbName, login, password, dataStoreName, idScope, idProviderType, config);
        }

        void beforeNextRemoveBatch(Runnable action) {
            beforeRemoveBatch = action;
        }

        void afterNextRemoveBatchLocked(Runnable action) {
            afterRemoveBatchLocked = action;
        }

        void limitObservationPool(int maximumPoolSize) {
            connectionManager.setMaximumPoolSize(maximumPoolSize);
        }

        void limitDataStreamPool(int maximumPoolSize) {
            ((PostgisDataStreamStoreImpl) getDataStreams())
                    .setMaximumPoolSize(maximumPoolSize);
        }

        void beginRemovalPagingProbe() {
            removalSelectionPasses = 0;
            removalBatchPasses = 0;
            maximumRemovalBatchSize = 0;
            probeRemovalPaging = true;
        }

        void failBeforeRemoveBatch(int ordinal) {
            if (ordinal < 1)
                throw new IllegalArgumentException("Removal batch ordinal must be positive");
            failBeforeRemoveBatch = ordinal;
            removeBatchOrdinal = 0;
        }

        void endRemovalPagingProbe() {
            probeRemovalPaging = false;
        }

        int removalSelectionPasses() {
            return removalSelectionPasses;
        }

        int removalBatchPasses() {
            return removalBatchPasses;
        }

        int maximumRemovalBatchSize() {
            return maximumRemovalBatchSize;
        }

        @Override
        public Stream<Map.Entry<BigId, org.sensorhub.api.data.IObsData>> selectEntries(
                ObsFilter filter, Set<IObsStore.ObsField> fields) {
            if (probeRemovalPaging)
                removalSelectionPasses++;
            return super.selectEntries(filter, fields);
        }

        @Override
        protected void beforeRemoveBatchRevalidation(List<BigId> candidateIds) {
            removeBatchOrdinal++;
            if (removeBatchOrdinal == failBeforeRemoveBatch) {
                failBeforeRemoveBatch = 0;
                throw new IllegalStateException("Injected child-removal batch failure");
            }
            if (probeRemovalPaging) {
                removalBatchPasses++;
                maximumRemovalBatchSize = Math.max(
                        maximumRemovalBatchSize, candidateIds.size());
            }
            Runnable action = beforeRemoveBatch;
            beforeRemoveBatch = null;
            if (action != null)
                action.run();
        }

        @Override
        protected void afterRemoveBatchLocked(List<BigId> candidateIds) {
            Runnable action = afterRemoveBatchLocked;
            afterRemoveBatchLocked = null;
            if (action != null)
                action.run();
        }

        void insertVerified(Connection connection, long id, long streamId, Long foiId,
                            Instant phenomenon, Instant resultTime, String resultJson) throws SQLException {
            PreparedObservation observation = preparedObservation(
                    id, streamId, foiId, phenomenon, resultTime, resultJson);
            try (PreparedStatement statement = connection.prepareStatement(queryBuilder.insertObsPreparedQuery())) {
                bindPreparedObservation(statement, observation);
                statement.executeUpdate();
            }
            validateInsertOutcomes(connection, List.of(observation));
        }

        void prepareImmutableForTest(Connection connection, long id, long streamId,
                                     Instant phenomenonTime) throws Exception {
            beforeObservationWrite(connection, List.of(preparedObservation(
                    id, streamId, null, phenomenonTime, null, "{}")));
        }
    }

    private void assertCatalogV5AndActivation(ArchiveGeneration generation) throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            try (ResultSet result = statement.executeQuery(
                    "SELECT checksum FROM oscar_storage_schema_version WHERE version=5")) {
                assertTrue(result.next());
                assertEquals(ArchiveCatalog.SCHEMA_CHECKSUM, result.getString(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT is_nullable, column_default FROM information_schema.columns " +
                            "WHERE table_name='oscar_retention_hold' AND column_name='resolution_mode'")) {
                assertTrue(result.next());
                assertEquals("NO", result.getString(1));
                assertNull(result.getString(2));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT pg_get_constraintdef(oid) FROM pg_constraint " +
                            "WHERE conrelid='oscar_retention_hold'::regclass " +
                            "AND conname='oscar_retention_hold_resolution_check'")) {
                assertTrue(result.next());
                assertTrue(result.getString(1).contains("LEGACY_UNKNOWN"));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT column_name, is_nullable FROM information_schema.columns " +
                            "WHERE table_name='oscar_obs_overlay' " +
                            "AND column_name IN ('store_key','id_scope','generation_id','schema_json') " +
                            "ORDER BY column_name")) {
                List<String> columns = new ArrayList<>();
                while (result.next()) {
                    columns.add(result.getString(1));
                    assertEquals("NO", result.getString(2));
                }
                assertEquals(List.of("generation_id", "id_scope", "schema_json", "store_key"), columns);
            }
            for (String table : List.of("oscar_deletion_plan", "oscar_deletion_ledger")) {
                try (ResultSet result = statement.executeQuery(
                        "SELECT column_name, is_nullable FROM information_schema.columns " +
                                "WHERE table_name='" + table + "' " +
                                "AND column_name IN ('store_key','id_scope') ORDER BY column_name")) {
                    List<String> columns = new ArrayList<>();
                    while (result.next()) {
                        columns.add(result.getString(1));
                        assertEquals("NO", result.getString(2));
                    }
                    assertEquals(List.of("id_scope", "store_key"), columns);
                }
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT indexdef FROM pg_indexes " +
                            "WHERE indexname='oscar_archive_object_relative_path_idx'")) {
                assertTrue(result.next());
                assertTrue(result.getString(1).startsWith("CREATE UNIQUE INDEX"));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT state, row_count, parity_verified FROM oscar_archive_generation WHERE id='" +
                            generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals("ACTIVE", result.getString(1));
                assertEquals(6, result.getLong(2));
                assertFalse(result.getBoolean(3));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT count(*) FROM oscar_archive_object WHERE generation_id='" + generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals(3, result.getLong(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT count(*) FROM " + TABLE + " WHERE archivegenerationid='" + generation.id() + "'")) {
                assertTrue(result.next());
                assertEquals(6, result.getLong(1));
            }
            try (ResultSet result = statement.executeQuery(
                    "SELECT to_regclass('public." + TABLE + "_unique_constraint_v2')")) {
                assertTrue(result.next());
                assertTrue(result.getString(1) != null);
            }
        }
    }

    private static void assertGlobalOrderingAndLatest(OscarTieredObsStore store, Instant start) {
        List<Instant> ascending;
        try (var rows = store.selectEntries(new ObsFilter.Builder().build())) {
            ascending = rows.map(entry -> entry.getValue().getPhenomenonTime()).toList();
        }
        List<Instant> expectedAscending = new ArrayList<>();
        for (int i = 0; i < 6; i++)
            expectedAscending.add(start.plus(i, ChronoUnit.MINUTES));
        assertEquals(expectedAscending, ascending);

        ObsFilter descending = new ObsFilter.Builder().withPhenomenonTime()
                .withAllTimes().descendingOrder(true).done().build();
        try (var rows = store.selectEntries(descending)) {
            assertEquals(expectedAscending.stream().sorted(Comparator.reverseOrder()).toList(),
                    rows.map(entry -> entry.getValue().getPhenomenonTime()).toList());
        }

        ObsFilter latest = new ObsFilter.Builder().withPhenomenonTime()
                .withLatestTime().done().build();
        try (var rows = store.selectEntries(latest)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(start.plus(5, ChronoUnit.MINUTES),
                    selected.get(0).getValue().getPhenomenonTime());
        }
        // DATASTREAM_ID can be omitted from the external projection, but the
        // tiered union still needs it internally to compute latest-per-stream.
        try (var rows = store.selectEntries(latest,
                Set.of(IObsStore.ObsField.PHENOMENON_TIME, IObsStore.ObsField.RESULT))) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(start.plus(5, ChronoUnit.MINUTES),
                    selected.get(0).getValue().getPhenomenonTime());
            assertTrue(selected.get(0).getValue().getResult() != null);
        }
        assertEquals(0, store.getOpenArchiveCursorCount());
    }

    private static ArchiveGeneration oneGeneration(ArchiveCatalog catalog) {
        List<ArchiveGeneration> generations = catalog.candidates(
                Instant.parse("2025-01-01T00:00:00Z"), Instant.parse("2027-01-01T00:00:00Z"), null);
        assertEquals(1, generations.size());
        return generations.get(0);
    }

    private void ageGeneration(UUID generationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_generation SET activated_at=? WHERE id=?")) {
            UtcJdbcTime.setInstant(statement, 1, Instant.now().minus(2, ChronoUnit.DAYS));
            statement.setObject(2, generationId);
            assertEquals(1, statement.executeUpdate());
        }
    }

    private long hotRows() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT count(*) FROM " + TABLE)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private long archivedHotRows() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(
                     "SELECT count(*) FROM " + TABLE + " WHERE archivegenerationid IS NOT NULL")) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private long stageOwnerRows() throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT count(*) FROM oscar_obs_stage_owner WHERE store_key=? AND id_scope=?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getLong(1);
            }
        }
    }

    private List<String> observationIndexNames() throws Exception {
        List<String> indexes = new ArrayList<>();
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() " +
                        "AND tablename=? ORDER BY indexname")) {
            statement.setString(1, TABLE);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    indexes.add(result.getString(1));
            }
        }
        return indexes;
    }

    private String indexDefinition(String indexName) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT indexdef FROM pg_indexes WHERE schemaname=current_schema() " +
                        "AND tablename=? AND indexname=?")) {
            statement.setString(1, TABLE);
            statement.setString(2, indexName);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue("Missing index definition for " + indexName, result.next());
                return result.getString(1);
            }
        }
    }

    private UUID tombstoneGeneration(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT generation_id FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            statement.setString(1, TABLE);
            statement.setInt(2, DATABASE_NUM);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getObject(1, UUID.class);
            }
        }
    }

    private UUID insertRetentionHold(String subjectType, String subjectId,
                                     String resolutionMode) throws Exception {
        UUID id = UUID.randomUUID();
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            try (Statement lock = connection.createStatement()) {
                lock.execute("LOCK TABLE oscar_retention_hold IN SHARE ROW EXCLUSIVE MODE");
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "INSERT INTO oscar_retention_hold(id, store_key, id_scope, subject_type, " +
                            "subject_id, resolution_mode, reason, created_by) " +
                            "VALUES (?, ?, ?, ?, ?, ?, 'integration hold', 'codex-integration')")) {
                statement.setObject(1, id);
                statement.setString(2, TABLE);
                statement.setInt(3, DATABASE_NUM);
                statement.setString(4, subjectType);
                statement.setString(5, subjectId);
                statement.setString(6, resolutionMode);
                assertEquals(1, statement.executeUpdate());
            }
            connection.commit();
        }
        return id;
    }

    private void releaseRetentionHold(UUID id) throws Exception {
        try (Connection connection = open()) {
            connection.setAutoCommit(false);
            try (Statement lock = connection.createStatement()) {
                lock.execute("LOCK TABLE oscar_retention_hold IN SHARE ROW EXCLUSIVE MODE");
            }
            try (PreparedStatement statement = connection.prepareStatement(
                    "UPDATE oscar_retention_hold SET released_at=now(), released_by='codex-integration' " +
                            "WHERE id=? AND released_at IS NULL")) {
                statement.setObject(1, id);
                assertEquals(1, statement.executeUpdate());
            }
            connection.commit();
        }
    }

    private static long regularArchiveFiles(Path root) throws Exception {
        try (var paths = Files.walk(root)) {
            return paths.filter(Files::isRegularFile).count();
        }
    }

    private void installActivationBarrier() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE OR REPLACE FUNCTION codex_storage_it_pause_archive_activation() " +
                    "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN " +
                    "PERFORM pg_advisory_xact_lock(" + ACTIVATION_LOCK_CLASS + "," +
                    ACTIVATION_LOCK_OBJECT + "); RETURN NULL; END $$");
            statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_pause_archive_activation ON " + TABLE);
            statement.execute("CREATE TRIGGER codex_storage_it_pause_archive_activation " +
                    "BEFORE UPDATE OF archivegenerationid ON " + TABLE + " FOR EACH STATEMENT " +
                    "EXECUTE FUNCTION codex_storage_it_pause_archive_activation()");
        }
    }

    private void waitForBlockedActivation() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                    "SELECT 1 FROM pg_locks WHERE locktype='advisory' AND classid=?::oid " +
                            "AND objid=?::oid AND NOT granted")) {
                statement.setInt(1, ACTIVATION_LOCK_CLASS);
                statement.setInt(2, ACTIVATION_LOCK_OBJECT);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next())
                        return;
                }
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Archive activation did not reach the PostgreSQL barrier");
    }

    private void waitForStageCatalogLockWaiter() throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            try (Connection observer = open(); Statement statement = observer.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT count(*) FROM pg_stat_activity " +
                                 "WHERE datname=current_database() AND pid<>pg_backend_pid() " +
                                 "AND wait_event_type='Lock' " +
                                 "AND query ILIKE '%oscar_obs_stage_partition%' " +
                                 "AND query ILIKE '%FOR UPDATE%'")) {
                assertTrue(result.next());
                if (result.getLong(1) >= 1)
                    return;
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Timed out waiting for staging-catalog retirement lock");
    }

    private void waitForAdvisoryWaiters(int minimum) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            try (Connection connection = open(); Statement statement = connection.createStatement();
                 ResultSet result = statement.executeQuery(
                         "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND NOT granted")) {
                assertTrue(result.next());
                if (result.getLong(1) >= minimum)
                    return;
            }
            Thread.sleep(20);
        }
        throw new AssertionError("Expected at least " + minimum + " waiting advisory locks");
    }

    private void dropActivationBarrier() throws Exception {
        try (Connection connection = open(); Statement statement = connection.createStatement()) {
            statement.execute("DROP TRIGGER IF EXISTS codex_storage_it_pause_archive_activation ON " + TABLE);
            statement.execute("DROP FUNCTION IF EXISTS codex_storage_it_pause_archive_activation()");
        }
    }

    private String hotResultJson(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT result::text FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                assertTrue(result.next());
                return result.getString(1);
            }
        }
    }

    private boolean hotRowExists(long observationId) throws Exception {
        try (Connection connection = open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM " + TABLE + " WHERE id=?")) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private static void waitForActivePgSleep(Connection observer, int backendPid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = observer.prepareStatement(
                    "SELECT 1 FROM pg_stat_activity WHERE pid=? AND state='active' " +
                            "AND query LIKE '%pg_sleep%'") ) {
                statement.setInt(1, backendPid);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next())
                        return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Worker did not enter pg_sleep on PostgreSQL backend " + backendPid);
    }

    private static long queryLong(Connection connection, String sql) throws Exception {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery(sql)) {
            assertTrue(result.next());
            return result.getLong(1);
        }
    }

    private static void waitForBackendExit(Connection observer, int backendPid) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() < deadline) {
            try (PreparedStatement statement = observer.prepareStatement(
                    "SELECT 1 FROM pg_stat_activity WHERE pid=?")) {
                statement.setInt(1, backendPid);
                try (ResultSet result = statement.executeQuery()) {
                    if (!result.next())
                        return;
                }
            }
            Thread.sleep(20L);
        }
        throw new AssertionError("Cancelled PostgreSQL backend remained active: " + backendPid);
    }

    private Connection open() throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://" + url + "/" + DB_NAME, login, password);
    }

    @Test
    public void nonArchivedDataStreamQueriesAndHotLatestQueriesPruneColdGenerations() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant start = Instant.parse("2026-01-08T00:00:00Z");
        BigId archivedStream = addSimpleDataStream(bigId(901), "gammaCounts");
        BigId nonArchivedStream = addSimpleDataStream(bigId(902), "sensorLocation");

        addSimpleObsWithoutResultTime(archivedStream, BigId.NONE, start, 3, 60_000L);
        addSimpleObsWithoutResultTime(nonArchivedStream, BigId.NONE, start, 3, 60_000L);
        forceReadBackFromStorage();

        ArchiveGeneration generation = tiered.archiveWindow(
                start, start.plus(1, ChronoUnit.HOURS), "RAW_GATED").orElseThrow();
        assertEquals(3, generation.rowCount());

        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        assertTrue("Non-archived stream must be pruned by oscar_archive_schema",
                catalog.candidates(start, start.plus(1, ChronoUnit.HOURS), null,
                        null, null, Set.of(nonArchivedStream.getIdAsLong())).isEmpty());
        assertEquals(1, catalog.candidates(start, start.plus(1, ChronoUnit.HOURS), null,
                null, null, Set.of(archivedStream.getIdAsLong())).size());

        // Add a newer hot observation on the archived stream so hot supersedes cold.
        Instant newerHotTime = start.plus(2, ChronoUnit.HOURS);
        addSimpleObsWithoutResultTime(archivedStream, BigId.NONE, newerHotTime, 1, 1_000L);
        forceReadBackFromStorage();

        ObsFilter latestResult = new ObsFilter.Builder()
                .withDataStreams(archivedStream)
                .withLatestResult()
                .build();
        try (var rows = tiered.selectEntries(latestResult)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(newerHotTime, selected.get(0).getValue().getPhenomenonTime());
        }
    }

    @Test
    public void boundedResultTimeQueriesPruneOlderColdGenerationsAndBackfillLegacyNullMaxResultTime() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant firstWindow = Instant.parse("2026-01-09T00:00:00Z");
        Instant secondWindow = firstWindow.plus(1, ChronoUnit.HOURS);
        Instant thirdWindow = firstWindow.plus(2, ChronoUnit.HOURS);
        BigId stream = addSimpleDataStream(bigId(903), "gammaCounts");

        DataBlock firstBlock = new DataBlockDouble(5);
        firstBlock.setDoubleValue(0, 111.0);
        Instant firstResultTime = firstWindow.plus(10, ChronoUnit.MINUTES);
        tiered.add(new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(firstWindow.plus(5, ChronoUnit.MINUTES))
                .withResultTime(firstResultTime)
                .withResult(firstBlock)
                .build());

        DataBlock secondBlock = new DataBlockDouble(5);
        secondBlock.setDoubleValue(0, 222.0);
        Instant secondResultTime = secondWindow.plus(15, ChronoUnit.MINUTES);
        tiered.add(new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(secondWindow.plus(5, ChronoUnit.MINUTES))
                .withResultTime(secondResultTime)
                .withResult(secondBlock)
                .build());
        forceReadBackFromStorage();

        ArchiveGeneration olderGen = tiered.archiveWindow(firstWindow, secondWindow, "RAW_GATED").orElseThrow();
        ArchiveGeneration newerGen = tiered.archiveWindow(secondWindow, thirdWindow, "RAW_GATED").orElseThrow();
        try (Connection connection = open();
             PreparedStatement deleteHot = connection.prepareStatement(
                     "DELETE FROM " + TABLE + " WHERE datastreamid=?")) {
            deleteHot.setLong(1, stream.getIdAsLong());
            deleteHot.executeUpdate();
        }

        ArchiveCatalog catalog = tiered.getArchiveCatalog();
        Set<Long> streamSet = Set.of(stream.getIdAsLong());
        assertEquals(List.of(newerGen.id()),
                catalog.candidates(firstWindow, thirdWindow, null, null, null, streamSet, secondWindow)
                        .stream().map(ArchiveGeneration::id).toList());
        assertEquals(List.of(newerGen.id()),
                catalog.candidatesInitialClusters(firstWindow, thirdWindow, streamSet, secondWindow, false, 8)
                        .generations().stream().map(ArchiveGeneration::id).toList());

        // Simulate a pre-3.11.0 upgraded generation row with NULL max_result_time.
        try (Connection connection = open();
             PreparedStatement clearMaxResult = connection.prepareStatement(
                     "UPDATE oscar_archive_generation SET max_result_time=NULL WHERE id=?")) {
            clearMaxResult.setObject(1, olderGen.id());
            assertEquals(1, clearMaxResult.executeUpdate());
        }
        assertEquals(2, catalog.candidates(firstWindow, thirdWindow, null, null, null, streamSet, secondWindow).size());

        ObsFilter boundedResultFilter = new ObsFilter.Builder()
                .withDataStreams(stream)
                .withResultTimeDuring(secondWindow, thirdWindow)
                .withLimit(1)
                .build();
        try (var rows = tiered.selectEntries(boundedResultFilter)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(222.0, selected.get(0).getValue().getResult().getDoubleValue(0), 0.0);
        }

        // Legacy NULL max_result_time must now be backfilled so subsequent SQL plans prune olderGen directly.
        assertEquals(List.of(newerGen.id()),
                catalog.candidates(firstWindow, thirdWindow, null, null, null, streamSet, secondWindow)
                        .stream().map(ArchiveGeneration::id).toList());
    }

    @Test
    public void parquetPrimaryHotMetadataScanDeduplicatesStageOverlapInSingleTableScan() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant firstWindow = Instant.parse("2026-08-10T00:00:00Z");
        Instant secondWindow = firstWindow.plus(30, ChronoUnit.MINUTES);
        Instant thirdWindow = secondWindow.plus(30, ChronoUnit.MINUTES);
        BigId rawStream = addSimpleDataStream(bigId(92), "gammaCounts");
        BigId mutableStream = addSimpleDataStream(bigId(93), "occupancy");

        // 3 immutable rows in firstWindow (will be archived into Parquet while still attached in stage),
        // 2 immutable rows in secondWindow (hot stage only), and 1 mutable row in base table.
        addSimpleObsWithoutResultTime(rawStream, BigId.NONE, firstWindow.plusSeconds(5), 3, 1_000L);
        addSimpleObsWithoutResultTime(rawStream, BigId.NONE, secondWindow.plusSeconds(5), 2, 1_000L);
        addSimpleObsWithoutResultTime(mutableStream, BigId.NONE, firstWindow.plusSeconds(10), 1, 0L);
        forceReadBackFromStorage();

        ArchiveGeneration archived = tiered.archiveWindow(firstWindow, secondWindow, "RAW_GATED").orElseThrow();
        assertEquals(3, archived.rowCount());
        String firstStageChild = tiered.getImmutableStagePartitions().get(0).childTable();

        // Unfiltered count, stream-filtered count, and time-range scan all hit scanMetadata -> scanHotMetadata
        // because an ACTIVE Parquet generation overlaps firstWindow while stage partition remains attached.
        ObsFilter overlapFilter = new ObsFilter.Builder()
                .withDataStreams(rawStream)
                .withPhenomenonTimeDuring(firstWindow, thirdWindow)
                .build();
        assertEquals(6, tiered.getNumRecords());
        assertEquals(5, tiered.countMatchingEntries(overlapFilter));
        var rawRange = tiered.getDataStreamPhenomenonTimeRanges(List.of(rawStream.getIdAsLong()))
                .get(rawStream.getIdAsLong());
        assertNotNull(rawRange);
        assertEquals(firstWindow.plusSeconds(5), rawRange.begin());
        assertEquals(secondWindow.plusSeconds(6), rawRange.end());

        // Differential scan check: compare 1 vs 4 countMatchingEntries calls across identical store lifecycles
        // so fixed startup catalog checks cancel out and per-query stage scans must equal 1 (not 2 from self-join).
        tiered.close();
        long baselineScans;
        try (Connection connection = open(); Statement stmt = connection.createStatement()) {
            stmt.execute("SELECT pg_stat_clear_snapshot()");
            baselineScans = queryLong(connection,
                    "SELECT COALESCE(seq_scan,0) + COALESCE(idx_scan,0) FROM pg_stat_user_tables WHERE relname='" +
                            firstStageChild + "'");
        }

        tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        assertEquals(5, tiered.countMatchingEntries(overlapFilter));
        tiered.close();

        long afterOneCallScans;
        try (Connection connection = open(); Statement stmt = connection.createStatement()) {
            stmt.execute("SELECT pg_stat_clear_snapshot()");
            afterOneCallScans = queryLong(connection,
                    "SELECT COALESCE(seq_scan,0) + COALESCE(idx_scan,0) FROM pg_stat_user_tables WHERE relname='" +
                            firstStageChild + "'");
        }
        long oneLifecycleDelta = afterOneCallScans - baselineScans;

        tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        for (int i = 0; i < 4; i++) {
            assertEquals(5, tiered.countMatchingEntries(overlapFilter));
        }
        tiered.close();

        long afterFourCallsScans;
        try (Connection connection = open(); Statement stmt = connection.createStatement()) {
            stmt.execute("SELECT pg_stat_clear_snapshot()");
            afterFourCallsScans = queryLong(connection,
                    "SELECT COALESCE(seq_scan,0) + COALESCE(idx_scan,0) FROM pg_stat_user_tables WHERE relname='" +
                            firstStageChild + "'");
        }
        long fourCallsLifecycleDelta = afterFourCallsScans - afterOneCallScans;

        // Reopen for @After cleanup expectations.
        tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        assertEquals("Three extra countMatchingEntries calls must add exactly 3 stage-child scans (1 per query, zero self-join)",
                3L, fourCallsLifecycleDelta - oneLifecycleDelta);
    }

    @Test
    public void tombstonedOverlayIsExcludedFromSelectGetAndCount() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant windowStart = Instant.parse("2026-09-01T00:00:00Z");
        Instant windowEnd = windowStart.plus(1, ChronoUnit.HOURS);
        BigId stream = addAlarmDataStream(bigId(940), "gammaCounts");

        BigId obsId = tiered.add(alarmObs(stream, windowStart.plusSeconds(10),
                windowStart.plusSeconds(11), false, false, 15.0));
        forceReadBackFromStorage();

        ArchiveGeneration gen = tiered.archiveWindow(windowStart, windowEnd, "RAW_GATED").orElseThrow();
        assertEquals(1L, gen.rowCount());

        // Overlay while hot grace base still exists, then delete hot base so overlay is the sole cold representation.
        tiered.put(obsId, alarmObs(stream, windowStart.plusSeconds(10),
                windowStart.plusSeconds(11), true, false, 120.0));
        try (Connection connection = open();
             PreparedStatement deleteHot = connection.prepareStatement(
                     "DELETE FROM " + TABLE + " WHERE datastreamid=?")) {
            deleteHot.setLong(1, stream.getIdAsLong());
            assertEquals(1, deleteHot.executeUpdate());
        }

        ObsFilter alarmFilter = new ObsFilter.Builder()
                .withDataStreams(stream)
                .withCQLFilter("gammaAlarm = true AND neutronAlarm = false")
                .build();
        assertNotNull(tiered.get(obsId));
        assertEquals(1L, tiered.countMatchingEntries(alarmFilter));
        assertEquals(1L, selectCount(tiered, alarmFilter));

        // Tombstone the overlaid observation and verify complete suppression across get, selectEntries, and countMatchingEntries.
        assertNotNull(tiered.remove(obsId));
        assertNull(tiered.get(obsId));
        assertEquals(0L, selectCount(tiered, new ObsFilter.Builder().withDataStreams(stream).build()));
        assertEquals(0L, tiered.countMatchingEntries(new ObsFilter.Builder().withDataStreams(stream).build()));
        assertEquals(0L, selectCount(tiered, alarmFilter));
        assertEquals(0L, tiered.countMatchingEntries(alarmFilter));
    }

    @Test
    public void directTieredCountMatchesSelectEntriesAcrossOverlapOverlaysTombstonesAndCql() throws Exception {
        OscarTieredObsStore tiered = (OscarTieredObsStore) postgisObsStore;
        Instant coldStart = Instant.parse("2026-09-02T00:00:00Z");
        Instant coldEnd = coldStart.plus(1, ChronoUnit.HOURS);
        Instant overlapStart = coldEnd;
        Instant overlapEnd = overlapStart.plus(1, ChronoUnit.HOURS);
        Instant hotStart = overlapEnd;
        BigId stream = addAlarmDataStream(bigId(941), "gammaCounts");

        // Cold window (hot rows will be removed after archive): 4 observations
        BigId coldNoneKeep = tiered.add(alarmObs(stream, coldStart.plusSeconds(5),
                coldStart.plusSeconds(5), false, false, 10.0));
        BigId coldNoneToAlarmOverlay = tiered.add(alarmObs(stream, coldStart.plusSeconds(10),
                coldStart.plusSeconds(10), false, false, 12.0));
        BigId coldAlarmToNoneOverlay = tiered.add(alarmObs(stream, coldStart.plusSeconds(15),
                coldStart.plusSeconds(15), true, false, 85.0));
        BigId coldTombstoned = tiered.add(alarmObs(stream, coldStart.plusSeconds(20),
                coldStart.plusSeconds(20), false, false, 14.0));

        // Grace-overlap window (archived into Parquet AND left in hot PostgreSQL): 2 observations
        BigId overlapNone = tiered.add(alarmObs(stream, overlapStart.plusSeconds(5),
                overlapStart.plusSeconds(5), false, false, 20.0));
        BigId overlapOverlaid = tiered.add(alarmObs(stream, overlapStart.plusSeconds(10),
                overlapStart.plusSeconds(10), false, false, 22.0));

        // Pure hot unarchived window: 2 observations (one after cutoff)
        Instant cutoff = hotStart.plusSeconds(8);
        tiered.add(alarmObs(stream, hotStart.plusSeconds(5),
                hotStart.plusSeconds(5), false, false, 30.0));
        tiered.add(alarmObs(stream, hotStart.plusSeconds(15),
                hotStart.plusSeconds(15), true, true, 150.0));
        forceReadBackFromStorage();

        tiered.archiveWindow(coldStart, coldEnd, "RAW_GATED").orElseThrow();
        tiered.archiveWindow(overlapStart, overlapEnd, "RAW_GATED").orElseThrow();
        try (Connection connection = open();
             PreparedStatement deleteColdWindowHot = connection.prepareStatement(
                     "DELETE FROM " + TABLE + " WHERE datastreamid=? AND phenomenontime < ?")) {
            deleteColdWindowHot.setLong(1, stream.getIdAsLong());
            UtcJdbcTime.setInstant(deleteColdWindowHot, 2, coldEnd);
            assertEquals(4, deleteColdWindowHot.executeUpdate());
        }

        // Apply mutations across cold and grace-overlap rows.
        tiered.put(coldNoneToAlarmOverlay, alarmObs(stream, coldStart.plusSeconds(10),
                coldStart.plusSeconds(10), true, false, 95.0));
        tiered.put(coldAlarmToNoneOverlay, alarmObs(stream, coldStart.plusSeconds(15),
                coldStart.plusSeconds(15), false, false, 9.0));
        tiered.put(overlapOverlaid, alarmObs(stream, overlapStart.plusSeconds(10),
                overlapStart.plusSeconds(10), true, false, 77.0));
        tiered.remove(coldTombstoned);
        assertNotNull(tiered.get(coldNoneKeep));
        assertNotNull(tiered.get(overlapNone));

        List<ObsFilter> filters = List.of(
                new ObsFilter.Builder().withDataStreams(stream).build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withCQLFilter("gammaAlarm = false AND neutronAlarm = false").build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withCQLFilter("gammaAlarm = true").build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withCQLFilter("maxGamma >= 25").build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withResultTime(new TemporalFilter.Builder()
                                .withRange(Range.closed(Instant.MIN, cutoff)).build()).build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withResultTime(new TemporalFilter.Builder()
                                .withRange(Range.closedOpen(Instant.MIN, cutoff)).build())
                        .withCQLFilter("gammaAlarm = false AND neutronAlarm = false").build(),
                new ObsFilter.Builder().withDataStreams(stream)
                        .withCQLFilter("gammaAlarm = false AND neutronAlarm = false")
                        .withLimit(2).build()
        );
        for (int i = 0; i < filters.size(); i++) {
            ObsFilter candidateFilter = filters.get(i);
            assertEquals("Count mismatch at filter index " + i,
                    selectCount(tiered, candidateFilter),
                    tiered.countMatchingEntries(candidateFilter));
        }
    }

    @Test
    public void parquetPrimaryMutableStreamCountsStayExactAndAccelerateBoundedQueries() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant base = Instant.parse("2026-09-03T00:00:00Z");
        Instant cutoff = base.plusSeconds(25);
        BigId rawStream = addAlarmDataStream(bigId(942), "gammaCounts");
        BigId occStreamA = addAlarmDataStream(bigId(943), "occupancy");
        BigId occStreamB = addAlarmDataStream(bigId(944), "occupancy");

        // Immutable telemetry (must be routed to osg_* children and excluded from oscar_mutable_stream_counts)
        tiered.add(alarmObs(rawStream, base.plusSeconds(5), base.plusSeconds(5), false, false, 11.0));
        tiered.add(alarmObs(rawStream, base.plusSeconds(10), base.plusSeconds(10), true, false, 88.0));
        tiered.add(alarmObs(rawStream, base.plus(35, ChronoUnit.MINUTES),
                base.plus(35, ChronoUnit.MINUTES), false, false, 13.0));

        // Mutable occupancy rows: mix of <= cutoff, == cutoff, > cutoff, and NULL resultTime
        BigId occNullResult = tiered.add(alarmObs(occStreamA, base.plusSeconds(5), null, false, false, 10.0));
        tiered.add(alarmObs(occStreamA, base.plusSeconds(15), base.plusSeconds(15), false, false, 20.0));
        tiered.add(alarmObs(occStreamA, base.plusSeconds(25), cutoff, true, false, 90.0));
        BigId occAfterCutoff = tiered.add(alarmObs(occStreamA, base.plusSeconds(35),
                base.plusSeconds(35), false, false, 30.0));
        tiered.add(alarmObs(occStreamB, base.plusSeconds(20), base.plusSeconds(20), false, true, 75.0));

        // Natural-key upsert on mutable stream (must not double-count total_rows)
        tiered.add(alarmObs(occStreamA, base.plusSeconds(15), base.plusSeconds(15), false, false, 25.0));
        forceReadBackFromStorage();

        // Update NULL resultTime -> non-null resultTime via put, then remove one post-cutoff row
        tiered.put(occNullResult, alarmObs(occStreamA, base.plusSeconds(5),
                base.plusSeconds(6), false, false, 10.0));
        tiered.remove(occAfterCutoff);

        // Archive first immutable 30-minute stage window so cold + hot both exist
        tiered.archiveWindow(base, base.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();

        try (Connection connection = open()) {
            assertEquals("Immutable streams must never enter oscar_mutable_stream_counts", 0L,
                    queryLong(connection, "SELECT count(*) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "' AND datastream_id=" + rawStream.getIdAsLong()));
            assertEquals("Immutable streams must never enter oscar_mutable_stream_counts_delta", 0L,
                    queryLong(connection, "SELECT count(*) FROM oscar_mutable_stream_counts_delta " +
                            "WHERE store_key='" + TABLE + "' AND datastream_id=" + rawStream.getIdAsLong()));
            assertEquals(4L, queryLong(connection,
                    "SELECT COALESCE((SELECT sum(total_rows) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "' AND datastream_id IN (" +
                            occStreamA.getIdAsLong() + "," + occStreamB.getIdAsLong() + ")),0) + " +
                            "COALESCE((SELECT sum(total_rows) FROM oscar_mutable_stream_counts_delta " +
                            "WHERE store_key='" + TABLE + "' AND datastream_id IN (" +
                            occStreamA.getIdAsLong() + "," + occStreamB.getIdAsLong() + ")),0)"));
            assertEquals(0L, queryLong(connection,
                    "SELECT COALESCE((SELECT sum(null_result_time_rows) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "'),0) + " +
                            "COALESCE((SELECT sum(null_result_time_rows) FROM oscar_mutable_stream_counts_delta " +
                            "WHERE store_key='" + TABLE + "'),0)"));
        }

        // Verify fast mutable count + CQL count + bounded descending CQL merge parity BEFORE delta compaction
        List<ObsFilter> checks = List.of(
                new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB).build(),
                new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB)
                        .withResultTime(new TemporalFilter.Builder()
                                .withRange(Range.closed(Instant.MIN, cutoff)).build()).build(),
                new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB)
                        .withResultTime(new TemporalFilter.Builder()
                                .withRange(Range.closedOpen(Instant.MIN, cutoff)).build()).build(),
                new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB)
                        .withResultTime(new TemporalFilter.Builder()
                                .withRange(Range.closed(Instant.MIN, cutoff)).build())
                        .withCQLFilter("gammaAlarm = false AND neutronAlarm = false").build(),
                new ObsFilter.Builder().withDataStreams(rawStream)
                        .withPhenomenonTime(new TemporalFilter.Builder()
                                .withAllTimes().descendingOrder(true).build())
                        .withCQLFilter("gammaAlarm = false AND neutronAlarm = false")
                        .withLimit(1).build()
        );
        for (int i = 0; i < checks.size(); i++) {
            ObsFilter check = checks.get(i);
            assertEquals("Count parity failed before compaction at check index " + i,
                    selectCount(tiered, check), tiered.countMatchingEntries(check));
        }

        // Lifecycle maintenance compacts oscar_mutable_stream_counts_delta into oscar_mutable_stream_counts
        tiered.maintainArchiveLifecycle();
        try (Connection connection = open()) {
            assertEquals("Compaction must drain oscar_mutable_stream_counts_delta", 0L,
                    queryLong(connection, "SELECT count(*) FROM oscar_mutable_stream_counts_delta " +
                            "WHERE store_key='" + TABLE + "'"));
            assertEquals(4L, queryLong(connection,
                    "SELECT COALESCE(sum(total_rows),0) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "' AND datastream_id IN (" +
                            occStreamA.getIdAsLong() + "," + occStreamB.getIdAsLong() + ")"));
            assertEquals(0L, queryLong(connection,
                    "SELECT COALESCE(sum(null_result_time_rows),0) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "'"));
        }
        for (int i = 0; i < checks.size(); i++) {
            ObsFilter check = checks.get(i);
            assertEquals("Count parity failed after compaction at check index " + i,
                    selectCount(tiered, check), tiered.countMatchingEntries(check));
        }

        // Bounded descending CQL merge on cold+hot rawStream must return the newest matching observation
        ObsFilter newestNone = checks.get(checks.size() - 1);
        try (var rows = tiered.selectEntries(newestNone)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(base.plus(35, ChronoUnit.MINUTES), selected.get(0).getValue().getPhenomenonTime());
        }

        // Reopen store (idempotent O(1) table_oid match) and also verify self-healing when table_oid=0
        tiered.close();
        try (Connection connection = open(); Statement stmt = connection.createStatement()) {
            stmt.execute("UPDATE oscar_mutable_stream_counts SET total_rows = 999 WHERE store_key='" + TABLE + "'");
            stmt.execute("UPDATE oscar_mutable_stream_counts_state SET table_oid = 0::oid WHERE store_key='" + TABLE + "'");
        }
        tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;
        assertEquals(4L, tiered.countMatchingEntries(
                new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB).build()));
    }

    @Test
    public void parquetPrimaryConcurrentOutOfOrderMutableTransactionsNeverDeadlockAndCompactCleanly() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant base = Instant.parse("2026-09-04T00:00:00Z");
        BigId occStreamA = addAlarmDataStream(bigId(961), "occupancy");
        BigId occStreamB = addAlarmDataStream(bigId(962), "occupancy");

        BigId idA1 = tiered.add(alarmObs(occStreamA, base.plusSeconds(1), base.plusSeconds(1), false, false, 10.0));
        tiered.add(alarmObs(occStreamA, base.plusSeconds(2), base.plusSeconds(2), false, false, 11.0));
        BigId idB1 = tiered.add(alarmObs(occStreamB, base.plusSeconds(3), base.plusSeconds(3), false, false, 12.0));
        tiered.add(alarmObs(occStreamB, base.plusSeconds(4), base.plusSeconds(4), false, false, 13.0));
        forceReadBackFromStorage();

        java.util.concurrent.CyclicBarrier barrier = new java.util.concurrent.CyclicBarrier(2);
        ExecutorService pool = Executors.newFixedThreadPool(2);
        try {
            Future<?> batchWriterBThenA = pool.submit(() -> {
                try (Connection conn = open()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement ins = conn.prepareStatement(
                            "INSERT INTO " + TABLE + " (id, datastreamid, phenomenontime, resulttime, result) " +
                                    "VALUES (?, ?, ?::timestamp, ?::timestamp, " +
                                    "'{\"gammaAlarm\":false,\"neutronAlarm\":false,\"maxGamma\":14.0}'::jsonb)")) {
                        ins.setLong(1, 900_001L);
                        ins.setLong(2, occStreamB.getIdAsLong());
                        ins.setString(3, "2026-09-04 00:01:00");
                        ins.setString(4, "2026-09-04 00:01:00");
                        assertEquals(1, ins.executeUpdate());

                        barrier.await(5, TimeUnit.SECONDS);

                        ins.setLong(1, 900_002L);
                        ins.setLong(2, occStreamA.getIdAsLong());
                        ins.setString(3, "2026-09-04 00:01:01");
                        ins.setString(4, "2026-09-04 00:01:01");
                        assertEquals(1, ins.executeUpdate());
                        conn.commit();
                    } catch (Exception e) {
                        conn.rollback();
                        throw e;
                    }
                }
                return null;
            });

            Future<?> purgerAThenB = pool.submit(() -> {
                try (Connection conn = open()) {
                    conn.setAutoCommit(false);
                    try (PreparedStatement del = conn.prepareStatement(
                            "DELETE FROM " + TABLE + " WHERE id = ?")) {
                        del.setLong(1, idA1.getIdAsLong());
                        assertEquals(1, del.executeUpdate());

                        barrier.await(5, TimeUnit.SECONDS);

                        del.setLong(1, idB1.getIdAsLong());
                        assertEquals(1, del.executeUpdate());
                        conn.commit();
                    } catch (Exception e) {
                        conn.rollback();
                        throw e;
                    }
                }
                return null;
            });

            batchWriterBThenA.get(10, TimeUnit.SECONDS);
            purgerAThenB.get(10, TimeUnit.SECONDS);
        } finally {
            pool.shutdownNow();
        }

        ObsFilter bothStreams = new ObsFilter.Builder().withDataStreams(occStreamA, occStreamB).build();
        assertEquals(4L, tiered.countMatchingEntries(bothStreams));
        assertEquals(4L, selectCount(tiered, bothStreams));

        tiered.maintainArchiveLifecycle();
        try (Connection conn = open()) {
            assertEquals(0L, queryLong(conn,
                    "SELECT count(*) FROM oscar_mutable_stream_counts_delta WHERE store_key='" + TABLE + "'"));
            assertEquals(4L, queryLong(conn,
                    "SELECT COALESCE(sum(total_rows),0) FROM oscar_mutable_stream_counts " +
                            "WHERE store_key='" + TABLE + "'"));
        }
        assertEquals(4L, tiered.countMatchingEntries(bothStreams));
    }

    @Test
    public void nonAlarmKeysetAndUnadjudicatedAlarmPartialIndexesSupportGenericPreparedPlans() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.outputNames = List.of("gammaCounts");
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        BigId occStreamA = addAlarmDataStream(bigId(971), "occupancy");
        BigId occStreamB = addAlarmDataStream(bigId(972), "occupancy");
        try (Connection conn = open()) {
            try (PreparedStatement ins = conn.prepareStatement(
                    "INSERT INTO " + TABLE + " (id, datastreamid, phenomenontime, resulttime, result) " +
                            "VALUES (?, ?, ?::timestamp, ?::timestamp, ?::jsonb)")) {
                for (int i = 1; i <= 60; i++) {
                    ins.setLong(1, 950_000L + i);
                    ins.setLong(2, (i % 2 == 0 ? occStreamA : occStreamB).getIdAsLong());
                    String ts = String.format("2026-09-05 00:%02d:00", i % 60);
                    ins.setString(3, ts);
                    ins.setString(4, ts);
                    String json = (i % 10 == 0)
                            ? "{\"gammaAlarm\":true,\"neutronAlarm\":false,\"adjudicatedIdsCount\":0,\"maxGamma\":88.0}"
                            : "{\"gammaAlarm\":false,\"neutronAlarm\":false,\"adjudicatedIdsCount\":0,\"maxGamma\":10.0}";
                    ins.setString(5, json);
                    ins.addBatch();
                }
                ins.executeBatch();
            }
            try (Statement stmt = conn.createStatement()) {
                stmt.execute("SET max_parallel_maintenance_workers = 0");
                stmt.execute("VACUUM ANALYZE " + TABLE);
            }
        }

        QueryBuilderObsStore builder = new QueryBuilderObsStore(TABLE, false);
        ObsFilter nonAlarmPageFilter = new ObsFilter.Builder()
                .withDataStreams(occStreamA, occStreamB)
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withAllTimes().descendingOrder(true).build())
                .withCQLFilter("gammaAlarm = false AND neutronAlarm = false")
                .withLimit(5)
                .build();
        String nonAlarmSql = toPostgresPositionalSql(
                builder.createParameterizedSelectEntriesQuery(nonAlarmPageFilter, null).sql());

        ObsFilter unadjudicatedAlarmFilter = new ObsFilter.Builder()
                .withDataStreams(occStreamA, occStreamB)
                .withResultTime(new TemporalFilter.Builder()
                        .withRange(Range.closed(Instant.MIN, Instant.parse("2026-09-05T01:00:00Z"))).build())
                .withCQLFilter("(gammaAlarm = true OR neutronAlarm = true) AND adjudicatedIdsCount = 0")
                .build();
        String alarmCountSql = toPostgresPositionalSql(
                builder.createParameterizedSelectEntriesCountQuery(unadjudicatedAlarmFilter).sql());

        try (Connection conn = open(); Statement stmt = conn.createStatement()) {
            stmt.execute("SET plan_cache_mode = force_generic_plan");
            stmt.execute("SET enable_seqscan = off");
            stmt.execute("SET enable_bitmapscan = off");

            stmt.execute("PREPARE oscar_non_alarm_page(bigint, bigint) AS " + nonAlarmSql);
            StringBuilder nonAlarmPlan = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("EXPLAIN (COSTS OFF) EXECUTE oscar_non_alarm_page(" +
                    occStreamA.getIdAsLong() + ", " + occStreamB.getIdAsLong() + ")")) {
                while (rs.next())
                    nonAlarmPlan.append(rs.getString(1)).append('\n');
            }
            assertTrue("Expected non-alarm partial index scan in generic plan:\n" + nonAlarmPlan,
                    nonAlarmPlan.toString().contains(TABLE + "_non_alarm_ptime_desc_idx"));
            assertFalse("Ordered partial index scan must not require a Sort:\n" + nonAlarmPlan,
                    nonAlarmPlan.toString().contains("Sort"));

            stmt.execute("PREPARE oscar_alarm_count(bigint, bigint, text) AS " + alarmCountSql);
            StringBuilder alarmPlan = new StringBuilder();
            try (ResultSet rs = stmt.executeQuery("EXPLAIN (COSTS OFF) EXECUTE oscar_alarm_count(" +
                    occStreamA.getIdAsLong() + ", " + occStreamB.getIdAsLong() + ", '2026-09-05T01:00:00Z')")) {
                while (rs.next())
                    alarmPlan.append(rs.getString(1)).append('\n');
            }
            assertTrue("Expected unadjudicated alarm partial index in generic plan:\n" + alarmPlan,
                    alarmPlan.toString().contains(TABLE + "_unadjudicated_alarm_idx"));
        }

        assertEquals(6L, tiered.countMatchingEntries(unadjudicatedAlarmFilter));
    }

    @Test
    public void parquetPrimaryFullOperationalStreamsUseLatestCacheSummaryCountersAndScopedOverlays() throws Exception {
        String archiveRoot = archiveConfig.rootDirectory;
        String temporaryRoot = archiveConfig.duckDbTemporaryDirectory;
        postgisObsStore.close();
        dropDisposableTieredTables(TABLE);
        allDataStreams.clear();
        allObs.clear();

        TieredArchiveConfig primaryConfig = new TieredArchiveConfig();
        primaryConfig.rootDirectory = archiveRoot;
        primaryConfig.duckDbTemporaryDirectory = temporaryRoot;
        primaryConfig.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        primaryConfig.immutableStageWindowMinutes = 30;
        primaryConfig.maximumGenerationsPerCycle = 2;
        primaryConfig.maximumVerificationsPerCycle = 2;
        primaryConfig.maximumDeletionsPerCycle = 2;
        primaryConfig.deleteHotAfterArchive = true;
        primaryConfig.validate();
        archiveConfig = primaryConfig;

        TestableTieredStore tiered = new TestableTieredStore(
                url, DB_NAME, login, password, TABLE, DATABASE_NUM,
                IdProviderType.SEQUENTIAL, archiveConfig);
        postgisObsStore = tiered;
        obsStore = tiered;

        Instant base = Instant.parse("2026-09-06T00:00:00Z");
        BigId occStream = addAlarmDataStream(bigId(981), "occupancy");
        BigId statusStream = addSimpleDataStream(bigId(982), "connectionStatus");
        BigId staticStream = addSimpleDataStream(bigId(983), "sensorLocation");

        BigId occ1 = tiered.add(alarmObs(occStream, base.plusSeconds(10), base.plusSeconds(10), false, false, 10.0));
        BigId occ2 = tiered.add(alarmObs(occStream, base.plusSeconds(20), base.plusSeconds(20), true, false, 99.0));
        BigId status1 = addSimpleObsWithoutResultTime(statusStream, BigId.NONE, base.plusSeconds(15), 1, 0)
                .keySet().iterator().next();
        BigId static1 = addSimpleObsWithoutResultTime(staticStream, BigId.NONE, base.plusSeconds(5), 1, 0)
                .keySet().iterator().next();
        forceReadBackFromStorage();

        try (Connection conn = open()) {
            assertEquals("Only static config streams belong in ONLY " + TABLE,
                    1L, queryLong(conn, "SELECT count(*) FROM ONLY " + TABLE));
            assertEquals(3L, queryLong(conn, "SELECT count(*) FROM " + TABLE + " WHERE id <> " + static1.getIdAsLong()));
        }

        ArchiveGeneration gen = tiered.archiveWindow(
                base, base.plus(30, ChronoUnit.MINUTES), "RAW_GATED").orElseThrow();
        assertEquals(3L, gen.rowCount());
        tiered.maintainArchiveLifecycle();
        ageGeneration(gen.id());
        tiered.maintainArchiveLifecycle();

        try (Connection conn = open()) {
            assertEquals("Stage partition must be dropped after verification, leaving only static row in PG",
                    1L, queryLong(conn, "SELECT count(*) FROM " + TABLE));
            assertEquals(2L, queryLong(conn,
                    "SELECT count(*) FROM oscar_obs_latest WHERE store_key='" + TABLE + "'"));
            assertEquals(1L, queryLong(conn,
                    "SELECT COALESCE(sum(gamma_alarm_count),0) FROM oscar_archive_schema " +
                            "WHERE generation_id='" + gen.id() + "' AND datastream_id=" + occStream.getIdAsLong()));
        }

        // Fast-path latest lookup served from oscar_obs_latest after stage partition is dropped
        ObsFilter latestOcc = new ObsFilter.Builder()
                .withDataStreams(occStream)
                .withLatestResult()
                .withLimit(1)
                .build();
        try (var rows = tiered.selectEntries(latestOcc)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(occ2, selected.get(0).getKey());
            assertTrue(selected.get(0).getValue().getResult().getBooleanValue(0));
        }

        // Summary-counter fast path for CQL alarm counts
        ObsFilter gammaAlarmFilter = new ObsFilter.Builder()
                .withDataStreams(occStream)
                .withCQLFilter("gammaAlarm = true")
                .build();
        assertEquals(1L, tiered.countMatchingEntries(gammaAlarmFilter));

        ObsFilter neutronAlarmFilter = new ObsFilter.Builder()
                .withDataStreams(occStream)
                .withCQLFilter("neutronAlarm = true")
                .build();
        assertEquals(0L, tiered.countMatchingEntries(neutronAlarmFilter));
        assertEquals(0L, selectCount(tiered, neutronAlarmFilter));

        // Update archived occupancy row via put() -> oscar_obs_overlay + oscar_obs_latest sync
        tiered.put(occ2, alarmObs(occStream, base.plusSeconds(20), base.plusSeconds(20), false, true, 120.0));
        assertEquals("Mutation count must be scoped to the mutated datastream",
                1L, tiered.getArchiveCatalog().countObservationMutations(Set.of(occStream.getIdAsLong())));
        assertEquals("Unmutated datastreams must see zero mutations",
                0L, tiered.getArchiveCatalog().countObservationMutations(Set.of(statusStream.getIdAsLong())));

        try (var rows = tiered.selectEntries(latestOcc)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertEquals(occ2, selected.get(0).getKey());
            assertFalse("oscar_obs_latest must reflect overlay update",
                    selected.get(0).getValue().getResult().getBooleanValue(0));
            assertTrue(selected.get(0).getValue().getResult().getBooleanValue(1));
        }
        assertEquals(0L, tiered.countMatchingEntries(gammaAlarmFilter));
        assertEquals(1L, tiered.countMatchingEntries(neutronAlarmFilter));
        assertNotNull(tiered.get(occ1));
        assertNotNull(tiered.get(status1));
        assertNotNull(tiered.get(static1));

        // Second overlay mutation (re-adjudication / state update on an already-overlaid cold row)
        // must replace the prior overlay in-place and update both oscar_obs_overlay and oscar_obs_latest.
        tiered.put(occ2, alarmObs(occStream, base.plusSeconds(20), base.plusSeconds(20), true, true, 155.0));
        assertEquals(1L, tiered.getArchiveCatalog().countObservationMutations(Set.of(occStream.getIdAsLong())));
        assertEquals(1L, tiered.countMatchingEntries(gammaAlarmFilter));
        assertEquals(1L, tiered.countMatchingEntries(neutronAlarmFilter));
        try (var rows = tiered.selectEntries(latestOcc)) {
            var selected = rows.toList();
            assertEquals(1, selected.size());
            assertTrue(selected.get(0).getValue().getResult().getBooleanValue(0));
            assertTrue(selected.get(0).getValue().getResult().getBooleanValue(1));
            assertEquals(155.0, selected.get(0).getValue().getResult().getDoubleValue(2), 1e-6);
        }
    }

    private static String toPostgresPositionalSql(String jdbcSql) {
        StringBuilder out = new StringBuilder();
        int index = 1;
        for (int i = 0; i < jdbcSql.length(); i++) {
            char ch = jdbcSql.charAt(i);
            if (ch == '?')
                out.append('$').append(index++);
            else
                out.append(ch);
        }
        return out.toString();
    }

    private BigId addAlarmDataStream(BigId systemId, String outputName) throws Exception {
        SWEHelper swe = new SWEHelper();
        var record = swe.createRecord().name(outputName)
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .addField("maxGamma", swe.createQuantity().build())
                .build();
        var info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(systemId, "urn:oscar:test:alarm-system:" + systemId.getIdAsLong()))
                .withRecordDescription(record)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return postgisObsStore.getDataStreams().add(info).getInternalID();
    }

    private static IObsData alarmObs(BigId stream, Instant phenomenonTime, Instant resultTime,
                                     boolean gammaAlarm, boolean neutronAlarm, double maxGamma) {
        SWEHelper swe = new SWEHelper();
        var record = swe.createRecord().name("obs")
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .addField("maxGamma", swe.createQuantity().build())
                .build();
        DataBlock block = record.createDataBlock();
        block.setBooleanValue(0, gammaAlarm);
        block.setBooleanValue(1, neutronAlarm);
        block.setDoubleValue(2, maxGamma);
        ObsData.Builder builder = new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(phenomenonTime)
                .withResult(block);
        if (resultTime != null)
            builder.withResultTime(resultTime);
        return builder.build();
    }

    private static long selectCount(OscarTieredObsStore store, ObsFilter filter) {
        try (var stream = store.selectEntries(filter)) {
            return stream.count();
        }
    }
}
