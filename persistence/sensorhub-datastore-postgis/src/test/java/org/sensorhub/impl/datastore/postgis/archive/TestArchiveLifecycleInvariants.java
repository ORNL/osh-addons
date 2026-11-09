package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Test;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestArchiveLifecycleInvariants {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void onlyFailedOrStaleExportingJobsAreRetried() {
        assertTrue(ArchiveCatalog.shouldRetryJob("FAILED", false));
        assertTrue(ArchiveCatalog.shouldRetryJob("EXPORTING", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("EXPORTING", false));
        assertFalse(ArchiveCatalog.shouldRetryJob("ACTIVE", true));
        assertFalse(ArchiveCatalog.shouldRetryJob("SEALED", true));
    }

    @Test
    public void archiveJobRevisionsIncreaseMonotonically() {
        assertEquals(1, ArchiveCatalog.nextRevision(0));
        assertEquals(42, ArchiveCatalog.nextRevision(41));
        assertThrows(ArithmeticException.class, () -> ArchiveCatalog.nextRevision(Integer.MAX_VALUE));
    }

    @Test
    public void legacyBackupInventoryMigrationEnforcesCanonicalLauncherBounds() throws Exception {
        ArchiveCatalog.requireBackupGenerationInventoryBounds(0, 0);
        ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_000, 37_000_000);

        SQLException tooMany = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_001, 37_000_037));
        assertTrue(tooMany.getMessage().contains("1,000,000-entry migration limit"));
        SQLException tooLarge = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1_000_000, 37_000_001));
        assertTrue(tooLarge.getMessage().contains("37,000,000-byte migration limit"));
        SQLException noncanonical = assertThrows(SQLException.class,
                () -> ArchiveCatalog.requireBackupGenerationInventoryBounds(1, 36));
        assertTrue(noncanonical.getMessage().contains("canonical UUID-line form"));
    }

    @Test
    public void sha256Uses9pSafeChunkBufferAndMatchesMultiChunkDigest() throws Exception {
        assertTrue("SHA-256 read buffer must stay below WSL2 9p msize=65536 and order-4 allocations",
                ArchiveExporter.SHA256_READ_BUFFER_BYTES > 0 &&
                        ArchiveExporter.SHA256_READ_BUFFER_BYTES <= 16 * 1024);
        assertTrue(ArchiveExporter.isTransientAllocationIoError(
                new java.io.IOException("Cannot allocate memory")));
        assertFalse(ArchiveExporter.isTransientAllocationIoError(
                new java.io.IOException("Permission denied")));
        assertFalse(ArchiveExporter.isTransientAllocationIoError(new java.io.IOException()));

        Path sample = temporary.newFile("multi-chunk.bin").toPath();
        byte[] payload = new byte[ArchiveExporter.SHA256_READ_BUFFER_BYTES * 5 + 317];
        for (int i = 0; i < payload.length; i++)
            payload[i] = (byte) ((i * 31 + 17) & 0xff);
        Files.write(sample, payload);
        String expected = java.util.HexFormat.of().formatHex(
                java.security.MessageDigest.getInstance("SHA-256").digest(payload));
        assertEquals(expected, ArchiveExporter.sha256(sample));
    }

    @Test
    public void exactObservationHoldsAreExcludedFromDeletionBatch() {
        assertEquals(List.of(10L, 12L), ArchiveLifecycleManager.withoutHeld(
                List.of(10L, 11L, 12L, 13L), Set.of(11L, 13L)));
    }

    @Test
    public void mutationLocksUseOneOrderedSetBasedStatementForLargeBatches() throws Exception {
        List<String> sql = new ArrayList<>();
        AtomicInteger executions = new AtomicInteger();
        AtomicReference<List<Long>> boundIds = new AtomicReference<>();
        AtomicBoolean arrayFreed = new AtomicBoolean();

        Array array = (Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Array.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        arrayFreed.set(true);
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> false;
                    case "createArrayOf" -> {
                        assertEquals("bigint", arguments[0]);
                        Object[] values = (Object[]) arguments[1];
                        boundIds.set(java.util.Arrays.stream(values).map(value -> (Long) value).toList());
                        yield array;
                    }
                    case "prepareStatement" -> {
                        String statementSql = (String) arguments[0];
                        sql.add(statementSql);
                        yield preparedStatement(executions, boundIds);
                    }
                    default -> defaultValue(method.getReturnType());
                });

        List<Long> requested = new ArrayList<>(10_001);
        for (long id = 10_000; id >= 1; id--)
            requested.add(id);
        requested.add(5_000L); // duplicates must not request the same xact lock twice

        ArchiveCatalog catalog = new ArchiveCatalog(
                () -> { throw new AssertionError("provider must not be used"); }, "lock_probe", 7);
        catalog.lockForMutation(connection, requested);

        assertEquals("store and all observation locks need only two executions", 2, executions.get());
        assertEquals(2, sql.size());
        assertTrue(sql.get(0).contains("pg_advisory_xact_lock_shared"));
        assertTrue(sql.get(1).contains("WITH ORDINALITY"));
        assertTrue(sql.get(1).contains("ORDER BY ordinal"));
        assertEquals(10_000, boundIds.get().size());
        for (int index = 0; index < boundIds.get().size(); index++)
            assertEquals(index + 1L, boundIds.get().get(index).longValue());
        assertTrue(arrayFreed.get());
    }

    @Test
    public void removalHoldSnapshotIsSetBasedScopedAndUsesCallerTransaction() throws Exception {
        List<String> sql = new ArrayList<>();
        AtomicReference<List<Long>> boundIds = new AtomicReference<>();
        AtomicBoolean arrayFreed = new AtomicBoolean();
        UUID generation = UUID.randomUUID();

        Array array = (Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Array.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        arrayFreed.set(true);
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        Statement lock = (Statement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Statement.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("execute")) {
                        sql.add((String) arguments[0]);
                        return true;
                    }
                    return defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> false;
                    case "createStatement" -> lock;
                    case "createArrayOf" -> {
                        assertEquals("bigint", arguments[0]);
                        boundIds.set(java.util.Arrays.stream((Object[]) arguments[1])
                                .map(value -> (Long) value).toList());
                        yield array;
                    }
                    case "prepareStatement" -> {
                        sql.add((String) arguments[0]);
                        yield holdSnapshotStatement(generation);
                    }
                    default -> defaultValue(method.getReturnType());
                });

        ArchiveCatalog catalog = new ArchiveCatalog(
                () -> { throw new AssertionError("provider must not be used"); }, "hold_probe", 9);
        ArchiveCatalog.ActiveRemovalHolds holds = catalog.lockActiveRemovalHolds(
                connection, List.of(12L, 11L, 11L));

        assertEquals(List.of(11L, 12L), boundIds.get());
        assertTrue(sql.get(0).contains("LOCK TABLE oscar_retention_hold IN SHARE MODE"));
        assertTrue(sql.get(1).contains("WITH requested(id)"));
        assertTrue(sql.get(1).contains("oscar_archive_id_block"));
        assertTrue(sql.get(1).contains("requested.id BETWEEN block.min_id AND block.max_id"));
        assertEquals(Set.of(11L), holds.observationIds());
        assertEquals(Set.of(generation), holds.generationIds());
        assertTrue(holds.blocks(11L, null));
        assertTrue(holds.blocks(12L, generation));
        assertFalse(holds.blocks(12L, UUID.randomUUID()));
        assertTrue(arrayFreed.get());
    }

    @Test
    public void lifecycleRejectsSymbolicLinkArchiveObjects() throws Exception {
        Path root = temporary.newFolder("archive-root").toPath();
        Path outside = temporary.newFile("outside.parquet").toPath();
        Path link = root.resolve("linked.parquet");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }

        assertThrows(IllegalStateException.class,
                () -> ArchiveLifecycleManager.resolveArchiveObject(root, Path.of("linked.parquet")));
    }

    @Test
    public void candidatesAndInitialClustersPushBoundedMinResultTimePredicateIntoSql() throws Exception {
        List<String> capturedSql = new ArrayList<>();
        List<List<Object>> capturedBinds = new ArrayList<>();
        AtomicInteger freedArrays = new AtomicInteger();

        Array array = (Array) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{Array.class}, (proxy, method, arguments) -> {
                    if (method.getName().equals("free")) {
                        freedArrays.incrementAndGet();
                        return null;
                    }
                    return defaultValue(method.getReturnType());
                });
        ResultSet emptyResult = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ResultSet.class}, (proxy, method, arguments) ->
                        method.getName().equals("next") ? false : defaultValue(method.getReturnType()));
        ResultSet schemaExistsResult = (ResultSet) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{ResultSet.class}, (proxy, method, arguments) ->
                        method.getName().equals("next") ? true : defaultValue(method.getReturnType()));
        SqlConnectionProvider provider = () -> (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "createArrayOf" -> {
                        assertEquals("bigint", arguments[0]);
                        yield array;
                    }
                    case "prepareStatement" -> {
                        String sqlText = (String) arguments[0];
                        boolean isSchemaPrecheck = sqlText.startsWith(
                                "SELECT 1 FROM oscar_archive_schema WHERE datastream_id=ANY");
                        List<Object> binds = new ArrayList<>();
                        if (!isSchemaPrecheck) {
                            capturedSql.add(sqlText);
                            capturedBinds.add(binds);
                        }
                        yield Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{PreparedStatement.class},
                                (stmtProxy, stmtMethod, stmtArgs) -> switch (stmtMethod.getName()) {
                                    case "setString", "setInt", "setLong", "setTimestamp", "setArray", "setObject" -> {
                                        int index = (int) stmtArgs[0];
                                        while (binds.size() < index)
                                            binds.add(null);
                                        binds.set(index - 1, stmtArgs[1]);
                                        yield null;
                                    }
                                    case "executeQuery" -> isSchemaPrecheck ? schemaExistsResult : emptyResult;
                                    default -> defaultValue(stmtMethod.getReturnType());
                                });
                    }
                    default -> defaultValue(method.getReturnType());
                });

        ArchiveCatalog catalog = new ArchiveCatalog(provider, "result_probe", 5);
        java.time.Instant start = java.time.Instant.parse("2026-11-01T00:00:00Z");
        java.time.Instant end = java.time.Instant.parse("2026-11-05T00:00:00Z");
        java.time.Instant minResultTime = java.time.Instant.parse("2026-11-04T01:04:36.850Z");

        assertTrue(catalog.candidates(start, end, null, null, null, Set.of(42L), minResultTime).isEmpty());
        assertTrue(catalog.candidates(start, end, null, null, null, Set.of(42L)).isEmpty());
        assertTrue(catalog.candidatesInitialClusters(start, end, Set.of(42L), minResultTime, false, 8)
                .generations().isEmpty());
        assertTrue(catalog.candidatesInitialClusters(start, end, Set.of(42L), false, 8)
                .generations().isEmpty());

        assertEquals(4, capturedSql.size());
        assertTrue(capturedSql.get(0).contains("AND (g.max_result_time IS NULL OR g.max_result_time >= ?)"));
        assertFalse(capturedSql.get(1).contains("g.max_result_time >= ?"));
        assertTrue(capturedSql.get(2).contains("AND (g.max_result_time IS NULL OR g.max_result_time >= ?)"));
        assertTrue(capturedSql.get(2).endsWith("ORDER BY g.window_start ASC, g.window_end ASC, g.id ASC LIMIT ?"));
        assertFalse(capturedSql.get(3).contains("g.max_result_time >= ?"));

        assertEquals("result_probe", capturedBinds.get(0).get(0));
        assertEquals(5, capturedBinds.get(0).get(1));
        assertEquals(6, capturedBinds.get(0).size());
        assertEquals(5, capturedBinds.get(1).size());
        assertEquals(7, capturedBinds.get(2).size());
        assertEquals(9, capturedBinds.get(2).get(6));
        assertEquals(6, capturedBinds.get(3).size());
        assertEquals(4, freedArrays.get());
    }

    @Test
    public void parquetReaderPrunesGenerationsOlderThanMinResultTimeAndCachesLegacyMetadata() throws Exception {
        Path root = temporary.newFolder("parquet-result-prune").toPath().toAbsolutePath().normalize();
        Path spill = temporary.getRoot().toPath().resolve("parquet-result-spill").toAbsolutePath().normalize();
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toString();
        config.duckDbTemporaryDirectory = spill.toString();

        Path olderRelative = Path.of("older.parquet");
        Path newerRelative = Path.of("newer.parquet");
        java.time.Instant olderStart = java.time.Instant.parse("2026-11-04T00:00:00Z");
        java.time.Instant olderResult = java.time.Instant.parse("2026-11-04T00:15:00Z");
        java.time.Instant newerStart = java.time.Instant.parse("2026-11-04T01:00:00Z");
        java.time.Instant newerResult = java.time.Instant.parse("2026-11-04T01:04:40Z");

        writeSingleObservationParquet(config, root.resolve(olderRelative), 101L, 42L, olderStart, olderResult);
        writeSingleObservationParquet(config, root.resolve(newerRelative), 102L, 42L, newerStart, newerResult);

        ArchiveGeneration catalogPruned = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                olderStart.minusSeconds(3600), olderStart,
                Path.of("never-opened.parquet"),
                "0".repeat(64),
                1L, 90L, 90L, "SEALED", true,
                olderStart.minusSeconds(60));
        ArchiveGeneration legacyOlder = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                olderStart, newerStart,
                olderRelative, ArchiveExporter.sha256(root.resolve(olderRelative)),
                1L, 101L, 101L, "SEALED", true,
                null);
        ArchiveGeneration legacyNewer = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                newerStart, newerStart.plusSeconds(3600),
                newerRelative, ArchiveExporter.sha256(root.resolve(newerRelative)),
                1L, 102L, 102L, "SEALED", true,
                null);

        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);
        List<ArchiveGeneration> retained = reader.generationsPotentiallyNewerThanResultTime(
                List.of(catalogPruned, legacyOlder, legacyNewer),
                java.time.Instant.parse("2026-11-04T01:04:36.850Z"));

        assertEquals(List.of(legacyNewer), retained);
        assertEquals(java.util.Optional.of(olderResult), reader.cachedMaxResultTime(legacyOlder));
        assertEquals(java.util.Optional.of(newerResult), reader.cachedMaxResultTime(legacyNewer));
    }

    @Test
    public void startupReclaimsInterruptedExportingJobsAndParquetPrimaryShortensStaleTimeout() throws Exception {
        List<String> capturedSql = new ArrayList<>();
        List<Integer> boundInts = new ArrayList<>();
        List<String> boundStrings = new ArrayList<>();

        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setInt" -> {
                        boundInts.add((Integer) args[1]);
                        yield null;
                    }
                    case "setString" -> {
                        boundStrings.add((String) args[1]);
                        yield null;
                    }
                    case "executeUpdate" -> 1;
                    case "executeQuery" -> Proxy.newProxyInstance(
                            getClass().getClassLoader(),
                            new Class<?>[]{ResultSet.class},
                            (rs, rsMethod, rsArgs) -> defaultValue(rsMethod.getReturnType()));
                    default -> defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                getClass().getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "prepareStatement" -> {
                        capturedSql.add((String) args[0]);
                        yield statement;
                    }
                    default -> defaultValue(method.getReturnType());
                });

        TieredArchiveConfig parquetPrimary = new TieredArchiveConfig();
        parquetPrimary.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        parquetPrimary.maximumCycleMinutes = 15;
        parquetPrimary.duckDbTemporaryDirectory = temporary.newFolder("pp-duck-tmp").toPath().toString();
        ArchiveCatalog ppCatalog = new ArchiveCatalog(() -> connection, "obs_store", 3, parquetPrimary);

        assertEquals(1, ppCatalog.reclaimInterruptedExportingJobsOnStartup());
        assertTrue(capturedSql.get(0).contains("SET state='FAILED'"));
        assertTrue(capturedSql.get(0).contains("NOT EXISTS (SELECT 1 FROM oscar_archive_generation gen"));
        assertEquals(List.of("obs_store"), boundStrings);
        assertEquals(List.of(3), boundInts);

        boundInts.clear();
        assertTrue(ppCatalog.nextRetryableJobWindow("RAW_GATED", java.time.Instant.now()).isEmpty());
        assertEquals("PARQUET_PRIMARY stale timeout must be 30 minutes (2x 15m cycle)",
                List.of(3, 30), boundInts);

        TieredArchiveConfig hotRetention = new TieredArchiveConfig();
        hotRetention.storageMode = TieredArchiveConfig.StorageMode.HOT_RETENTION;
        hotRetention.duckDbTemporaryDirectory = temporary.newFolder("hr-duck-tmp").toPath().toString();
        ArchiveCatalog hrCatalog = new ArchiveCatalog(() -> connection, "obs_store", 3, hotRetention);
        boundInts.clear();
        assertTrue(hrCatalog.nextRetryableJobWindow("RAW_GATED", java.time.Instant.now()).isEmpty());
        assertEquals("HOT_RETENTION stale timeout must remain 120 minutes",
                List.of(3, 120), boundInts);
    }

    @Test
    public void parquetArchiveReaderStagesLocallyOn9pReadMemoryFailureAndCleansUpTempDir() throws Exception {
        Path root = temporary.newFolder("wsl9p-archive").toPath();
        Path duckTmp = temporary.getRoot().toPath().resolve("wsl9p-duck-tmp");
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toString();
        config.duckDbTemporaryDirectory = duckTmp.toString();

        Path rel = Path.of("datasets/observations/date=2026-11-05/profile=RAW_GATED/g1.parquet");
        Path file = root.resolve(rel);
        Files.createDirectories(file.getParent());
        java.time.Instant t1 = java.time.Instant.parse("2026-11-05T10:00:00Z");
        java.time.Instant t2 = java.time.Instant.parse("2026-11-05T10:01:00Z");
        try (DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(config, "test-export");
             Statement create = session.connection().createStatement()) {
            create.execute("CREATE TABLE archive_rows (id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, " +
                    "datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, " +
                    "result_time TIMESTAMP, result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            try (PreparedStatement insert = session.connection().prepareStatement(
                    "INSERT INTO archive_rows VALUES (1, ?, 1, 10, NULL, NULL, ?, ?, ?, 1, 'RAW_GATED')")) {
                insert.setLong(1, 501L);
                UtcJdbcTime.setDuckDbInstant(insert, 2, t1);
                UtcJdbcTime.setDuckDbInstant(insert, 3, t1);
                insert.setString(4, "{\"adjudicatedIds\":[\"eml-non-threat\"]}");
                insert.executeUpdate();

                insert.setLong(1, 502L);
                UtcJdbcTime.setDuckDbInstant(insert, 2, t2);
                UtcJdbcTime.setDuckDbInstant(insert, 3, t2);
                insert.setString(4, "{\"adjudicatedIds\":[\"operator-review\"]}");
                insert.executeUpdate();
            }
            create.execute("COPY archive_rows TO '" + file.toString().replace("'", "''") + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }

        ArchiveGeneration generation = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                t1, t2.plusSeconds(60),
                rel, ArchiveExporter.sha256(file),
                2L, 501L, 502L, "SEALED", true, null);

        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);
        AtomicInteger injectedFaults = new AtomicInteger();
        reader.setDirectReadFaultInjectorForTesting(filenames -> {
            injectedFaults.incrementAndGet();
            throw new SQLException("java.sql.SQLException: IO Error: Could not read from file \"" +
                    filenames.iterator().next() + "\": Cannot allocate memory");
        });

        org.sensorhub.api.datastore.obs.ObsFilter notEmlFilter = new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                .withCQLFilter("adjudicatedIds!='eml-non-threat'")
                .build();
        org.sensorhub.api.datastore.obs.ObsFilter allFilter = new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                .build();

        assertEquals(1L, reader.countCql(List.of(generation), notEmlFilter, null, null, Set.of()));
        assertEquals(2L, reader.count(List.of(generation), allFilter, null, null));
        List<ParquetArchiveReader.ArchivedRow> rows = reader.read(List.of(generation), notEmlFilter, null, null, 10);
        assertEquals(1, rows.size());
        assertEquals(502L, rows.get(0).observation().id());

        List<Long> metadataIds = new ArrayList<>();
        reader.forEachMetadata(List.of(generation), allFilter, null, null, Long.MAX_VALUE,
                row -> metadataIds.add(row.id()));
        assertEquals(List.of(501L, 502L), metadataIds);

        List<ArchiveGeneration> newer = reader.generationsPotentiallyNewerThanResultTime(
                List.of(generation), t2);
        assertEquals(List.of(generation), newer);
        assertEquals(5, injectedFaults.get());

        try (var remaining = Files.list(duckTmp)) {
            List<String> names = remaining.map(p -> p.getFileName().toString()).toList();
            assertEquals("Only the root lock file may remain after staged fallback sessions close",
                    List.of(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME), names);
        }
    }

    @Test
    public void cqlNotEqualsArrayFilterMatchesBothPostgresBuilderAndColdJsonEvaluator() {
        org.sensorhub.api.datastore.obs.ObsFilter filter = new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                .withCQLFilter("adjudicatedIds!='eml-non-threat'")
                .build();

        var query = new org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore("obs")
                .createParameterizedSelectEntriesQuery(filter, null);
        assertTrue("PostgreSQL CQL builder must negate JSONB array containment for !=",
                query.sql().contains("NOT (") && query.sql().contains("result @> ?::jsonb"));

        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                filter.getCQLFilter(), "{\"adjudicatedIds\":[\"eml-non-threat\"]}"));
        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                filter.getCQLFilter(), "{\"adjudicatedIds\":[\"eml-non-threat\",\"other\"]}"));
        assertTrue(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                filter.getCQLFilter(), "{\"adjudicatedIds\":[]}"));
        assertTrue(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                filter.getCQLFilter(), "{\"adjudicatedIds\":[\"operator-threat\"]}"));
    }

    private static void writeSingleObservationParquet(
            TieredArchiveConfig config, Path target, long obsId, long streamId,
            java.time.Instant phenomenonTime, java.time.Instant resultTime) throws Exception {
        try (DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(config, "test-export");
             Statement create = session.connection().createStatement()) {
            create.execute("CREATE TABLE archive_rows (id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, " +
                    "datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, " +
                    "result_time TIMESTAMP, result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            try (PreparedStatement insert = session.connection().prepareStatement(
                    "INSERT INTO archive_rows VALUES (1, ?, 1, ?, NULL, NULL, ?, ?, '{\"v\":1}', 1, 'RAW_GATED')")) {
                insert.setLong(1, obsId);
                insert.setLong(2, streamId);
                UtcJdbcTime.setDuckDbInstant(insert, 3, phenomenonTime);
                UtcJdbcTime.setDuckDbInstant(insert, 4, resultTime);
                insert.executeUpdate();
            }
            String escaped = target.toString().replace("'", "''");
            create.execute("COPY archive_rows TO '" + escaped + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
    }

    private PreparedStatement preparedStatement(AtomicInteger executions,
                                                AtomicReference<List<Long>> boundIds) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> switch (method.getName()) {
                    case "execute" -> {
                        executions.incrementAndGet();
                        yield true;
                    }
                    case "executeQuery" -> {
                        executions.incrementAndGet();
                        AtomicBoolean first = new AtomicBoolean(true);
                        yield Proxy.newProxyInstance(getClass().getClassLoader(),
                                new Class<?>[]{ResultSet.class}, (resultProxy, resultMethod, resultArguments) ->
                                        switch (resultMethod.getName()) {
                                            case "next" -> first.getAndSet(false);
                                            case "getLong" -> (long) boundIds.get().size();
                                            default -> defaultValue(resultMethod.getReturnType());
                                        });
                    }
                    default -> defaultValue(method.getReturnType());
                });
    }

    private PreparedStatement holdSnapshotStatement(UUID generation) {
        return (PreparedStatement) Proxy.newProxyInstance(getClass().getClassLoader(),
                new Class<?>[]{PreparedStatement.class}, (proxy, method, arguments) -> {
                    if (!method.getName().equals("executeQuery"))
                        return defaultValue(method.getReturnType());
                    AtomicInteger row = new AtomicInteger(-1);
                    String[][] values = {{"OBSERVATION", "11"},
                            {"ARCHIVE_GENERATION", generation.toString()}};
                    return Proxy.newProxyInstance(getClass().getClassLoader(),
                            new Class<?>[]{ResultSet.class}, (resultProxy, resultMethod, resultArguments) ->
                                    switch (resultMethod.getName()) {
                                        case "next" -> row.incrementAndGet() < values.length;
                                        case "getString" -> values[row.get()][(int) resultArguments[0] - 1];
                                        default -> defaultValue(resultMethod.getReturnType());
                                    });
                });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive() || type == void.class)
            return null;
        if (type == boolean.class)
            return false;
        if (type == byte.class)
            return (byte) 0;
        if (type == short.class)
            return (short) 0;
        if (type == int.class)
            return 0;
        if (type == long.class)
            return 0L;
        if (type == float.class)
            return 0F;
        if (type == double.class)
            return 0D;
        if (type == char.class)
            return '\0';
        throw new AssertionError("Unhandled primitive " + type);
    }
}
