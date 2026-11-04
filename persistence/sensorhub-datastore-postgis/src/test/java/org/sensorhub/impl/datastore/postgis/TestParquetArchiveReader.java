package org.sensorhub.impl.datastore.postgis;

import com.google.common.collect.Range;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestParquetArchiveReader {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void readsOnlyMatchingIdsFromVerifiedCatalogObject() throws Exception {
        Path root = temporary.newFolder("archive").toPath();
        Path object = root.resolve("datasets/observations/date=2026-01-01/profile=RAW_GATED/test.parquet");
        java.nio.file.Files.createDirectories(object.getParent());
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:"); Statement statement = duck.createStatement()) {
            statement.execute("CREATE TABLE rows (id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, " +
                    "datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, " +
                    "result_time TIMESTAMP, result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            try (PreparedStatement insert = duck.prepareStatement("INSERT INTO rows VALUES (?, ?, 4, 10, NULL, NULL, ?, ?, ?, 1, 'RAW_GATED')")) {
                for (long id = 100; id <= 102; id++) {
                    insert.setInt(1, 4);
                    insert.setLong(2, id);
                    insert.setTimestamp(3, java.sql.Timestamp.from(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(id)));
                    insert.setTimestamp(4, java.sql.Timestamp.from(Instant.parse("2026-01-01T00:00:00Z").plusSeconds(id)));
                    insert.setString(5, "{\"value\":" + id + "}");
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            statement.execute("COPY rows TO '" + object.toString().replace("'", "''") + "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }

        ArchiveGeneration generation = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                root.relativize(object), ArchiveExporter.sha256(object), 3, 100, 102, "ACTIVE", true);
        ObsFilter filter = new ObsFilter.Builder().withInternalIDs(BigId.fromLong(4, 101)).build();

        var rows = new ParquetArchiveReader(root).read(generation, filter, null, null);
        assertEquals(1, rows.size());
        assertEquals(101, rows.get(0).observation().id());
        assertEquals("{\"value\":101}", rows.get(0).observation().resultJson());
    }

    @Test
    public void rejectsChecksumMismatchBeforeQuery() throws Exception {
        Path root = temporary.newFolder("bad-archive").toPath();
        Path object = root.resolve("bad.parquet");
        java.nio.file.Files.writeString(object, "not parquet");
        ArchiveGeneration generation = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                root.relativize(object), "wrong", 1, 1, 1, "ACTIVE", false);
        ObsFilter filter = new ObsFilter.Builder().build();

        assertThrows(IllegalStateException.class,
                () -> new ParquetArchiveReader(root).read(generation, filter, null, null));
    }

    @Test
    public void rechecksCachedObjectAfterItChanges() throws Exception {
        Path root = temporary.newFolder("mutated-archive").toPath();
        ArchiveGeneration generation = writeGeneration(root, "mutable.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        ObsFilter filter = new ObsFilter.Builder().build();

        assertEquals(1, reader.read(generation, filter, null, null).size());
        Files.writeString(root.resolve("mutable.parquet"), "replaced after successful verification");

        assertThrows(IllegalStateException.class,
                () -> reader.read(generation, filter, null, null));
    }

    @Test
    public void rejectsSymbolicLinkArchiveObjects() throws Exception {
        Path root = temporary.newFolder("linked-archive").toPath();
        Path outside = temporary.newFile("outside.parquet").toPath();
        Path link = root.resolve("linked.parquet");
        try {
            Files.createSymbolicLink(link, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        ArchiveGeneration generation = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                root.relativize(link), ArchiveExporter.sha256(outside), 1, 1, 1, "ACTIVE", true);

        assertThrows(IllegalStateException.class,
                () -> new ParquetArchiveReader(root).read(
                        generation, new ObsFilter.Builder().build(), null, null));
    }

    @Test
    public void mergesGenerationsBeforeGlobalOrderAndLimit() throws Exception {
        Path root = temporary.newFolder("multi-generation").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 20, "2026-01-01T00:04:00Z")
        });
        ArchiveGeneration second = writeGeneration(root, "second.parquet", new TestRow[]{
                new TestRow(3, 10, "2026-01-01T00:03:00Z"),
                new TestRow(4, 20, "2026-01-01T00:02:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        var all = reader.read(List.of(first, second), new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE);
        assertEquals(List.of(1L, 4L, 3L, 2L),
                all.stream().map(row -> row.observation().id()).toList());

        var limited = reader.read(List.of(first, second), new ObsFilter.Builder().build(), null, null, 2);
        assertEquals(List.of(1L, 4L),
                limited.stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void latestIsChosenAcrossAllGenerationsPerDataStream() throws Exception {
        Path root = temporary.newFolder("latest-generation").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 20, "2026-01-01T00:04:00Z")
        });
        ArchiveGeneration second = writeGeneration(root, "second.parquet", new TestRow[]{
                new TestRow(3, 10, "2026-01-01T00:03:00Z"),
                new TestRow(4, 20, "2026-01-01T00:02:00Z")
        });
        ObsFilter latest = new ObsFilter.Builder().withPhenomenonTime()
                .withLatestTime().done().build();

        var rows = new ParquetArchiveReader(root).read(
                List.of(first, second), latest, null, null, Long.MAX_VALUE);
        assertEquals(List.of(3L, 2L), rows.stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void descendingOrderIsGlobalAcrossGenerations() throws Exception {
        Path root = temporary.newFolder("descending-generation").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 20, "2026-01-01T00:04:00Z")
        });
        ArchiveGeneration second = writeGeneration(root, "second.parquet", new TestRow[]{
                new TestRow(3, 10, "2026-01-01T00:03:00Z"),
                new TestRow(4, 20, "2026-01-01T00:02:00Z")
        });
        ObsFilter descending = new ObsFilter.Builder().withPhenomenonTime()
                .withAllTimes().descendingOrder(true).done().build();

        var rows = new ParquetArchiveReader(root).read(
                List.of(first, second), descending, null, null, Long.MAX_VALUE);
        assertEquals(List.of(2L, 3L, 4L, 1L),
                rows.stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void equalTimestampsHaveDeterministicIdOrderInBothDirections() throws Exception {
        Path root = temporary.newFolder("equal-time-generation").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(8, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:01:00Z")
        });
        ArchiveGeneration second = writeGeneration(root, "second.parquet", new TestRow[]{
                new TestRow(5, 10, "2026-01-01T00:01:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        var ascending = reader.read(List.of(first, second),
                new ObsFilter.Builder().build(), null, null, Long.MAX_VALUE);
        assertEquals(List.of(2L, 5L, 8L),
                ascending.stream().map(row -> row.observation().id()).toList());

        ObsFilter descending = new ObsFilter.Builder().withPhenomenonTime()
                .withAllTimes().descendingOrder(true).done().build();
        var reversedTime = reader.read(List.of(first, second), descending,
                null, null, Long.MAX_VALUE);
        assertEquals(List.of(2L, 5L, 8L),
                reversedTime.stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void metadataCountUsesAllGenerationsWithoutPayloadDecoding() throws Exception {
        Path root = temporary.newFolder("count-generation").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 20, "2026-01-01T00:04:00Z")
        });
        ArchiveGeneration second = writeGeneration(root, "second.parquet", new TestRow[]{
                new TestRow(3, 10, "2026-01-01T00:03:00Z"),
                new TestRow(4, 20, "2026-01-01T00:02:00Z")
        });

        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        assertEquals(4, reader.count(
                List.of(first, second), new ObsFilter.Builder().build(), null, null));
        ObsFilter latest = new ObsFilter.Builder().withPhenomenonTime()
                .withLatestTime().done().build();
        assertEquals(2, reader.count(List.of(first, second), latest, null, null));
        ObsFilter limited = new ObsFilter.Builder().withLimit(3).build();
        assertEquals(3, reader.count(List.of(first, second), limited, null, null));
    }

    @Test
    public void aggregatePlansOmitOnlyPresentationOrdering() throws Exception {
        Path root = temporary.newFolder("aggregate-plan").toPath();
        ArchiveGeneration generation = writeGeneration(root, "aggregate.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        ObsFilter all = new ObsFilter.Builder().build();

        String aggregateSql = queryPlanSql(reader, List.of(generation), all, true, false);
        assertFalse("Order-independent metadata/count plans must not sort the complete archive",
                aggregateSql.contains(" ORDER BY phenomenon_time "));

        String streamSql = queryPlanSql(reader, List.of(generation), all, true, true);
        assertTrue("Row streams retain their deterministic presentation order",
                streamSql.contains(" ORDER BY phenomenon_time ASC NULLS FIRST, id_value"));

        ObsFilter latest = new ObsFilter.Builder().withPhenomenonTime()
                .withLatestTime().done().build();
        String latestAggregateSql = queryPlanSql(
                reader, List.of(generation), latest, true, false);
        assertTrue("Latest-per-stream reduction retains its semantic window ordering",
                latestAggregateSql.contains("row_number() OVER (PARTITION BY datastream_id " +
                        "ORDER BY phenomenon_time DESC NULLS LAST, id_value DESC)"));
        assertFalse("Latest aggregate plans still omit the outer presentation sort",
                latestAggregateSql.contains(") ranked WHERE archive_rank=1 ORDER BY"));
    }

    @Test
    public void finiteMetadataScanRetainsDeterministicSubsetOrder() throws Exception {
        Path root = temporary.newFolder("finite-metadata-order").toPath();
        ArchiveGeneration generation = writeGeneration(root, "metadata.parquet", new TestRow[]{
                new TestRow(3, 10, "2026-01-01T00:03:00Z"),
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z")
        });
        List<Long> ids = new java.util.ArrayList<>();

        new ParquetArchiveReader(root).forEachMetadata(
                List.of(generation), new ObsFilter.Builder().build(), null, null, 1,
                row -> ids.add(row.id()));

        assertEquals(List.of(1L), ids);
    }

    @Test
    public void cancellationAfterMaterializationClosesCursorSessionAndScratchPromptly()
            throws Exception {
        Path root = temporary.newFolder("materialized-cancellation").toPath();
        ArchiveGeneration generation = writeGeneration(root, "materialized.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z"),
                new TestRow(3, 10, "2026-01-01T00:03:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        Path scratchRoot = root.resolveSibling(root.getFileName() + ".duckdb-test-tmp");
        AtomicReference<Throwable> cancellationFailure = new AtomicReference<>();

        try (ICancellableDataStore.CancellableOperation operation =
                     ConnectionManager.beginCancellableOperation();
             Stream<ParquetArchiveReader.ArchivedRow> rows = reader.stream(
                     List.of(generation), new ObsFilter.Builder().build(),
                     null, null, Long.MAX_VALUE)) {
            Iterator<ParquetArchiveReader.ArchivedRow> iterator = rows.iterator();
            assertTrue(iterator.hasNext());
            assertEquals(1L, iterator.next().observation().id());

            Thread canceller = new Thread(() -> {
                try {
                    operation.cancel();
                } catch (Throwable error) {
                    cancellationFailure.set(error);
                }
            }, "materialized-duckdb-canceller");
            canceller.setDaemon(true);
            canceller.start();
            canceller.join(3_000L);
            assertFalse("DuckDB cancellation did not finish promptly", canceller.isAlive());
            assertNull(cancellationFailure.get());

            long started = System.nanoTime();
            IllegalStateException cancelled = assertThrows(
                    IllegalStateException.class, iterator::hasNext);
            long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
            assertTrue("Materialized row iteration did not observe cancellation promptly: " +
                    elapsedMillis + " ms", elapsedMillis < 500L);
            assertTrue(cancelled.getCause() instanceof CancellationException);
            assertEquals(0, reader.getOpenCursorCount());
        }

        assertEquals(0, reader.getOpenCursorCount());
        assertTrue(Files.isDirectory(scratchRoot));
        Path rootLock = scratchRoot.resolve(".oscar-duckdb.lock");
        try (Stream<Path> children = Files.list(scratchRoot)) {
            assertEquals("Cancelled materialized query left DuckDB operation scratch",
                    List.of(rootLock), children.toList());
        }
        assertTrue(Files.isRegularFile(rootLock, LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.isSymbolicLink(rootLock));
        if (Files.getFileAttributeView(
                rootLock, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null)
            assertEquals(PosixFilePermissions.fromString("rw-------"),
                    Files.getPosixFilePermissions(rootLock, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    public void openLowerBoundDoesNotConsumeBoundedColdLimit() throws Exception {
        Path root = temporary.newFolder("open-lower-limit").toPath();
        ArchiveGeneration generation = writeGeneration(root, "open-lower.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z"),
                new TestRow(3, 10, "2026-01-01T00:03:00Z")
        });
        TemporalFilter time = new TemporalFilter.Builder()
                .withRange(Range.openClosed(
                        Instant.parse("2026-01-01T00:01:00Z"),
                        Instant.parse("2026-01-01T00:03:00Z")))
                .build();
        ObsFilter filter = new ObsFilter.Builder()
                .withPhenomenonTime(time)
                .withLimit(1)
                .build();

        var rows = new ParquetArchiveReader(root).read(
                List.of(generation), filter, null, null, 1);
        assertEquals(List.of(2L),
                rows.stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void metadataCountHonorsBothOpenColdEndpoints() throws Exception {
        Path root = temporary.newFolder("open-count").toPath();
        ArchiveGeneration generation = writeGeneration(root, "open-count.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z"),
                new TestRow(3, 10, "2026-01-01T00:03:00Z")
        });
        ObsFilter filter = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.open(
                                Instant.parse("2026-01-01T00:01:00Z"),
                                Instant.parse("2026-01-01T00:03:00Z")))
                        .build())
                .build();

        assertEquals(1, new ParquetArchiveReader(root).count(
                List.of(generation), filter, null, null));
    }

    @Test
    public void lazyCursorClosesOnEarlyStreamCloseAndOnExhaustion() throws Exception {
        Path root = temporary.newFolder("cursor-lifecycle").toPath();
        ArchiveGeneration generation = writeGeneration(root, "cursor.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z"),
                new TestRow(3, 10, "2026-01-01T00:03:00Z")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        try (var rows = reader.stream(List.of(generation), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE)) {
            assertEquals(1, reader.getOpenCursorCount());
            assertEquals(1L, rows.findFirst().orElseThrow().observation().id());
            assertEquals(1, reader.getOpenCursorCount());
        }
        assertEquals(0, reader.getOpenCursorCount());

        try (var rows = reader.stream(List.of(generation), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE)) {
            assertEquals(3, rows.count());
            assertEquals(0, reader.getOpenCursorCount());
        }
        assertEquals(0, reader.getOpenCursorCount());

        // CQL runs in Java. Reaching this post-filter limit must still close
        // the underlying JDBC cursor even when the caller does not explicitly
        // close the returned stream.
        ObsFilter cqlLimited = new ObsFilter.Builder()
                .withCQLFilter("value >= 0").withLimit(1).build();
        var limitedRows = reader.stream(List.of(generation), cqlLimited,
                null, null, 1);
        assertEquals(1, limitedRows.count());
        assertEquals(0, reader.getOpenCursorCount());
        limitedRows.close();
    }

    @Test
    public void cqlOrdersIsoInstantsAcrossOffsetsAndRejectsMalformedStrings() throws Exception {
        Path root = temporary.newFolder("cql-instant-ordering").toPath();
        ArchiveGeneration generation = writeGeneration(root, "instant-ordering.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z",
                        "{\"occurredAt\":\"2026-01-01T11:59:59.581Z\",\"value\":9}"),
                new TestRow(2, 10, "2026-01-01T00:02:00Z",
                        "{\"occurredAt\":\"2026-01-01T12:00:00.581Z\",\"value\":10}"),
                new TestRow(3, 10, "2026-01-01T00:03:00Z",
                        "{\"occurredAt\":\"2026-01-01T08:00:01.581-04:00\",\"value\":11}"),
                new TestRow(4, 10, "2026-01-01T00:04:00Z",
                        "{\"occurredAt\":\"not-a-time\",\"value\":12}"),
                new TestRow(5, 10, "2026-01-01T00:05:00Z",
                        "{\"occurredAt\":\"zebra\",\"value\":13}"),
                new TestRow(6, 10, "2026-01-01T00:06:00Z",
                        "{\"occurredAt\":20260101,\"value\":14}"),
                new TestRow(7, 10, "2026-01-01T00:07:00Z",
                        "{\"occurredAt\":\"today\",\"value\":15}"),
                new TestRow(8, 10, "2026-01-01T00:08:00Z",
                        "{\"occurredAt\":\"infinity\",\"value\":16}"),
                new TestRow(9, 10, "2026-01-01T00:09:00Z",
                        "{\"occurredAt\":\"2026-01-01\",\"value\":17}")
        });
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        assertEquals(List.of(1L), matchingIds(reader, generation,
                "occurredAt < '2026-01-01T12:00:00.581Z'"));
        assertEquals(List.of(1L, 2L), matchingIds(reader, generation,
                "occurredAt <= '2026-01-01T12:00:00.581Z'"));
        assertEquals(List.of(3L), matchingIds(reader, generation,
                "occurredAt > '2026-01-02T06:00:00.581+18:00'"));
        assertEquals(List.of(2L, 3L), matchingIds(reader, generation,
                "occurredAt >= '2026-01-01T07:00:00.581-05:00'"));

        assertTrue(matchingIds(reader, generation, "occurredAt > 'not-a-time'").isEmpty());
        assertTrue(matchingIds(reader, generation, "occurredAt > 'aardvark'").isEmpty());
        assertEquals(List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
                matchingIds(reader, generation, "value >= 10"));
        assertEquals(List.of(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L),
                matchingIds(reader, generation, "value >= '10'"));
    }

    @Test
    public void cursorCloseReportsSpillCleanupFailure() throws Exception {
        Path root = temporary.newFolder("cleanup-failure-archive").toPath();
        Path spillRoot = temporary.newFolder("cleanup-failure-spill").toPath();
        if (Files.getFileAttributeView(
                spillRoot, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) == null)
            return;
        Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("rwx------"));
        ArchiveGeneration generation = writeGeneration(root, "cleanup-failure.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z")
        });
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toString();
        config.duckDbTemporaryDirectory = spillRoot.toString();
        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);
        var rows = reader.stream(List.of(generation), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE);

        assertEquals(1, reader.getOpenCursorCount());
        Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("r-x------"));
        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class, rows::close);
            assertTrue(failure.getMessage().contains("Cannot close archive query resources"));
            assertEquals(0, reader.getOpenCursorCount());
        } finally {
            Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void decodingFailureKeepsCleanupFailureSuppressed() throws Exception {
        Path root = temporary.newFolder("decode-cleanup-archive").toPath();
        Path spillRoot = temporary.newFolder("decode-cleanup-spill").toPath();
        if (Files.getFileAttributeView(
                spillRoot, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) == null)
            return;
        Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("rwx------"));
        Path object = root.resolve("malformed.parquet");
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = duck.createStatement()) {
            statement.execute("CREATE TABLE rows (id_scope VARCHAR, id_value BIGINT, " +
                    "datastream_scope INTEGER, datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, " +
                    "phenomenon_time TIMESTAMP, result_time TIMESTAMP, result_json VARCHAR, " +
                    "archive_version INTEGER, profile VARCHAR)");
            statement.execute("INSERT INTO rows VALUES ('not-an-integer', 1, 4, 10, NULL, NULL, " +
                    "TIMESTAMP '2026-01-01 00:01:00', TIMESTAMP '2026-01-01 00:01:00', " +
                    "'{\"value\":1}', 1, 'RAW_GATED')");
            statement.execute("COPY rows TO '" + object.toString().replace("'", "''") +
                    "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        ArchiveGeneration generation = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                root.relativize(object), ArchiveExporter.sha256(object), 1, 1, 1, "ACTIVE", true);
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toString();
        config.duckDbTemporaryDirectory = spillRoot.toString();
        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);
        var rows = reader.stream(List.of(generation), new ObsFilter.Builder().build(),
                null, null, Long.MAX_VALUE);

        Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("r-x------"));
        try {
            IllegalStateException failure = assertThrows(IllegalStateException.class, rows::findFirst);
            assertTrue(failure.getMessage().contains("Cannot decode archived observation"));
            assertTrue(java.util.Arrays.stream(failure.getCause().getSuppressed())
                    .anyMatch(cleanup -> cleanup.getMessage().contains("Cannot close archive query resources")));
            assertEquals(0, reader.getOpenCursorCount());
        } finally {
            Files.setPosixFilePermissions(spillRoot, PosixFilePermissions.fromString("rwx------"));
        }
    }

    @Test
    public void boundedQueryDoesNotAuthorizeDisjointLaterWindows() throws Exception {
        Path root = temporary.newFolder("bounded-window-authorization").toPath();
        ArchiveGeneration first = writeGeneration(root, "first.parquet", new TestRow[]{
                new TestRow(1, 10, "2026-01-01T00:01:00Z")
        });
        ArchiveGeneration missingLater = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T01:00:00Z"), Instant.parse("2026-01-01T02:00:00Z"),
                Path.of("missing-later.parquet"), "not-read", 1, 2, 2, "ACTIVE", true);
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        var rows = reader.read(List.of(first, missingLater),
                new ObsFilter.Builder().withLimit(1).build(), null, null, 1);
        assertEquals(List.of(1L), rows.stream().map(row -> row.observation().id()).toList());
        assertEquals(0, reader.getOpenCursorCount());

        ArchiveGeneration missingRevision = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                first.start(), first.end(), Path.of("missing-revision.parquet"),
                "not-read", 1, 2, 2, "ACTIVE", true);
        assertThrows(IllegalStateException.class, () -> reader.read(
                List.of(first, missingRevision), new ObsFilter.Builder().withLimit(1).build(),
                null, null, 1));
    }

    @Test
    public void latestPhenomenonAcrossWindowClustersDoesNotAuthorizeDisjointOlderWindows() throws Exception {
        Path root = temporary.newFolder("latest-phenomenon-window-authorization").toPath();
        ArchiveGeneration missingEarlier = new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                Path.of("missing-earlier.parquet"), "not-read", 1, 1, 1, "ACTIVE", true);
        ArchiveGeneration newer = writeGeneration(root, "newer.parquet", new TestRow[]{
                new TestRow(2, 10, "2026-01-01T01:05:00Z")
        });
        ArchiveGeneration newerWindow = new ArchiveGeneration(
                newer.id(), newer.profile(),
                Instant.parse("2026-01-01T01:00:00Z"), Instant.parse("2026-01-01T02:00:00Z"),
                newer.objectPath(), newer.sha256(), newer.rowCount(),
                newer.minimumId(), newer.maximumId(), newer.state(), newer.parityVerified());
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        ObsFilter latestPhenomenon = new ObsFilter.Builder()
                .withPhenomenonTime().withLatestTime().done()
                .build();
        var rows = reader.read(List.of(missingEarlier, newerWindow),
                latestPhenomenon, Set.of(10L), null, Long.MAX_VALUE);
        assertEquals(List.of(2L), rows.stream().map(row -> row.observation().id()).toList());
        assertEquals(0, reader.getOpenCursorCount());
        assertTrue(reader.generationsPotentiallyNewerThanResultTime(
                List.of(newerWindow), Instant.parse("2026-01-01T01:06:00Z")).isEmpty());
        assertEquals(List.of(newerWindow), reader.generationsPotentiallyNewerThanResultTime(
                List.of(newerWindow), Instant.parse("2026-01-01T01:05:00Z")));
    }

    private ArchiveGeneration writeGeneration(Path root, String filename, TestRow[] rows) throws Exception {
        Path object = root.resolve(filename);
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:"); Statement statement = duck.createStatement()) {
            statement.execute("CREATE TABLE rows (id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, " +
                    "datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, " +
                    "result_time TIMESTAMP, result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            try (PreparedStatement insert = duck.prepareStatement(
                    "INSERT INTO rows VALUES (4, ?, 4, ?, NULL, NULL, ?, ?, ?, 1, 'RAW_GATED')")) {
                for (TestRow row : rows) {
                    Instant time = Instant.parse(row.time());
                    insert.setLong(1, row.id());
                    insert.setLong(2, row.dataStreamId());
                    UtcJdbcTime.setDuckDbInstant(insert, 3, time);
                    UtcJdbcTime.setDuckDbInstant(insert, 4, time);
                    insert.setString(5, row.resultJson());
                    insert.addBatch();
                }
                insert.executeBatch();
            }
            statement.execute("COPY rows TO '" + object.toString().replace("'", "''") +
                    "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        long minimum = java.util.Arrays.stream(rows).mapToLong(TestRow::id).min().orElseThrow();
        long maximum = java.util.Arrays.stream(rows).mapToLong(TestRow::id).max().orElseThrow();
        return new ArchiveGeneration(UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"), Instant.parse("2026-01-01T01:00:00Z"),
                root.relativize(object), ArchiveExporter.sha256(object), rows.length,
                minimum, maximum, "ACTIVE", true);
    }

    private List<Long> matchingIds(ParquetArchiveReader reader,
                                   ArchiveGeneration generation,
                                   String cql) {
        ObsFilter filter = new ObsFilter.Builder().withCQLFilter(cql).build();
        return reader.read(generation, filter, null, null).stream()
                .map(row -> row.observation().id())
                .toList();
    }

    private String queryPlanSql(ParquetArchiveReader reader,
                                List<ArchiveGeneration> generations,
                                ObsFilter filter,
                                boolean applyLatest,
                                boolean ordered) throws Exception {
        var authorize = ParquetArchiveReader.class.getDeclaredMethod("authorize", List.class);
        authorize.setAccessible(true);
        Object authorized = authorize.invoke(reader, generations);
        var queryPlan = ParquetArchiveReader.class.getDeclaredMethod(
                "queryPlan", authorize.getReturnType(), ObsFilter.class,
                Set.class, Set.class, boolean.class, boolean.class, long.class, boolean.class);
        queryPlan.setAccessible(true);
        Object plan = queryPlan.invoke(reader, authorized, filter,
                null, null, false, applyLatest, Long.MAX_VALUE, ordered);
        var sql = plan.getClass().getDeclaredMethod("sql");
        sql.setAccessible(true);
        return (String) sql.invoke(plan);
    }

    @Test
    public void testCatalogMaxResultTimeSkipsParquetInspection() throws Exception {
        Path root = temporary.newFolder("catalog-max-result").toPath();
        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        ArchiveGeneration older = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T01:00:00Z"),
                Path.of("RAW_GATED/2026/01/01/nonexistent-older.parquet"),
                "0".repeat(64), 10L, 1L, 10L, "SEALED", true,
                Instant.parse("2026-01-01T00:59:59Z"));
        ArchiveGeneration newer = new ArchiveGeneration(
                UUID.randomUUID(), "RAW_GATED",
                Instant.parse("2026-01-01T01:00:00Z"),
                Instant.parse("2026-01-01T02:00:00Z"),
                Path.of("RAW_GATED/2026/01/01/nonexistent-newer.parquet"),
                "1".repeat(64), 10L, 11L, 20L, "SEALED", true,
                Instant.parse("2026-01-01T01:59:59Z"));

        List<ArchiveGeneration> filtered = reader.generationsPotentiallyNewerThanResultTime(
                List.of(older, newer),
                Instant.parse("2026-01-01T01:30:00Z"));
        assertEquals(List.of(newer), filtered);
        assertEquals(java.util.Optional.of(Instant.parse("2026-01-01T00:59:59Z")),
                reader.cachedMaxResultTime(older));
        assertEquals(java.util.Optional.of(Instant.parse("2026-01-01T01:59:59Z")),
                reader.cachedMaxResultTime(newer));
    }

    @Test
    public void cqlDuckDbPrefilterPreservesEscapedJsonAndBatchesDisjointWindows() throws Exception {
        Path root = temporary.newFolder("cql-prefilter-batches").toPath();
        ArchiveGeneration first = new ArchiveGeneration(
                UUID.randomUUID(), "PARQUET_PRIMARY",
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:30:00Z"),
                writeGeneration(root, "w1.parquet", new TestRow[]{
                        new TestRow(1, 10, "2026-01-01T00:01:00Z",
                                "{\"alarmState\":\"Background\",\"note\":\"Fault - Gamma Low\"}"),
                        new TestRow(2, 10, "2026-01-01T00:02:00Z",
                                "{\"alarmState\":\"\\u0046ault - Gamma Low\"}"),
                        new TestRow(3, 10, "2026-01-01T00:03:00Z",
                                "{\"gammaAlarm\":true,\"neutronAlarm\":false}")
                }).objectPath(),
                ArchiveExporter.sha256(root.resolve("w1.parquet")), 3, 1, 3, "SEALED", true);
        ArchiveGeneration second = new ArchiveGeneration(
                UUID.randomUUID(), "PARQUET_PRIMARY",
                Instant.parse("2026-01-01T00:30:00Z"),
                Instant.parse("2026-01-01T01:00:00Z"),
                writeGeneration(root, "w2.parquet", new TestRow[]{
                        new TestRow(4, 10, "2026-01-01T00:31:00Z",
                                "{\"alarmState\":\"Fault - Gamma High\"}"),
                        new TestRow(5, 10, "2026-01-01T00:32:00Z",
                                "{\"alarmState\":\"Background\"}")
                }).objectPath(),
                ArchiveExporter.sha256(root.resolve("w2.parquet")), 2, 4, 5, "SEALED", true);
        ParquetArchiveReader reader = new ParquetArchiveReader(root);

        ObsFilter faultFilter = new ObsFilter.Builder()
                .withCQLFilter("alarmState = 'Fault - Gamma Low' OR alarmState = 'Fault - Gamma High'")
                .build();
        assertTrue(JsonCqlEvaluator.hasSelectiveDuckDbPrefilter(faultFilter.getCQLFilter()));
        assertEquals(List.of(2L, 4L),
                reader.read(List.of(first, second), faultFilter, null, null, Long.MAX_VALUE)
                        .stream().map(row -> row.observation().id()).toList());

        ObsFilter faultDesc = new ObsFilter.Builder().withPhenomenonTime()
                .withAllTimes().descendingOrder(true).done()
                .withCQLFilter("alarmState = 'Fault - Gamma Low' OR alarmState = 'Fault - Gamma High'")
                .build();
        assertEquals(List.of(4L, 2L),
                reader.read(List.of(first, second), faultDesc, null, null, Long.MAX_VALUE)
                        .stream().map(row -> row.observation().id()).toList());

        ObsFilter boolFilter = new ObsFilter.Builder()
                .withCQLFilter("gammaAlarm = true AND neutronAlarm = false")
                .build();
        assertEquals(List.of(3L),
                reader.read(List.of(first, second), boolFilter, null, null, Long.MAX_VALUE)
                        .stream().map(row -> row.observation().id()).toList());
    }

    @Test
    public void countMatchingIdsAndCountCqlRespectExclusionsChunkingAndEmptyInputs() throws Exception {
        Path root = temporary.newFolder("count-cql-and-ids").toPath();
        ArchiveGeneration first = new ArchiveGeneration(
                UUID.randomUUID(), "PARQUET_PRIMARY",
                Instant.parse("2026-01-01T00:00:00Z"),
                Instant.parse("2026-01-01T00:30:00Z"),
                writeGeneration(root, "c1.parquet", new TestRow[]{
                        new TestRow(1, 10, "2026-01-01T00:01:00Z",
                                "{\"gammaAlarm\":false,\"neutronAlarm\":false,\"maxGamma\":12.5}"),
                        new TestRow(2, 10, "2026-01-01T00:02:00Z",
                                "{\"gammaAlarm\":true,\"neutronAlarm\":false,\"maxGamma\":99.0}"),
                        new TestRow(3, 20, "2026-01-01T00:03:00Z",
                                "{\"gammaAlarm\":false,\"neutronAlarm\":false,\"maxGamma\":8.0}")
                }).objectPath(),
                ArchiveExporter.sha256(root.resolve("c1.parquet")), 3, 1, 3, "SEALED", true);
        ArchiveGeneration second = new ArchiveGeneration(
                UUID.randomUUID(), "PARQUET_PRIMARY",
                Instant.parse("2026-01-01T00:30:00Z"),
                Instant.parse("2026-01-01T01:00:00Z"),
                writeGeneration(root, "c2.parquet", new TestRow[]{
                        new TestRow(2005, 10, "2026-01-01T00:31:00Z",
                                "{\"gammaAlarm\":false,\"neutronAlarm\":false,\"maxGamma\":42.0}"),
                        new TestRow(2006, 10, "2026-01-01T00:32:00Z",
                                "{\"alarmState\":\"\\u0046ault - Gamma Low\",\"gammaAlarm\":false,\"neutronAlarm\":false}")
                }).objectPath(),
                ArchiveExporter.sha256(root.resolve("c2.parquet")), 2, 2005, 2006, "SEALED", true);

        ParquetArchiveReader reader = new ParquetArchiveReader(root);
        ObsFilter all = new ObsFilter.Builder().build();
        assertEquals(0L, reader.count(List.of(), all, null, null));
        assertEquals(0L, reader.count(List.of(first), all, Set.of(), null));
        assertEquals(0L, reader.count(List.of(first), all, null, Set.of()));

        java.util.Set<Long> largeIdSet = new java.util.HashSet<>();
        for (long id = 10_000; id < 12_150; id++)
            largeIdSet.add(id);
        largeIdSet.add(1L);
        largeIdSet.add(3L);
        largeIdSet.add(2005L);

        assertEquals(2L, reader.countMatchingIds(
                List.of(first, second), 4, largeIdSet, all, Set.of(10L), null));

        ObsFilter noneFilter = new ObsFilter.Builder()
                .withCQLFilter("gammaAlarm = false AND neutronAlarm = false")
                .build();
        assertEquals(4L, reader.countCql(
                List.of(first, second), noneFilter, null, null, Set.of()));
        assertEquals(2L, reader.countCql(
                List.of(first, second), noneFilter, Set.of(10L), null, Set.of(2005L)));

        ObsFilter escapedFilter = new ObsFilter.Builder()
                .withCQLFilter("alarmState = 'Fault - Gamma Low'")
                .build();
        assertEquals(1L, reader.countCql(
                List.of(first, second), escapedFilter, null, null, Set.of()));
        assertEquals(0L, reader.countCql(
                List.of(first, second), escapedFilter, null, null, Set.of(2006L)));
    }

    private record TestRow(long id, long dataStreamId, String time, String resultJson) {
        private TestRow(long id, long dataStreamId, String time) {
            this(id, dataStreamId, time, "{\"value\":" + id + "}");
        }
    }
}
