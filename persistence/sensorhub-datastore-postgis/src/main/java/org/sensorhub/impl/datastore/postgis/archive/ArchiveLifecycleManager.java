package org.sensorhub.impl.datastore.postgis.archive;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Verifies PostgreSQL/Parquet parity. HOT_RETENTION also performs bounded
 * exact-ID deletion after grace; PARQUET_PRIMARY retires verified children in
 * {@link ImmutableObservationStageManager}.
 */
public final class ArchiveLifecycleManager {
    private static final int VERIFY_BATCH = 5_000;

    private final SqlConnectionProvider postgres;
    private final ArchiveCatalog catalog;
    private final TieredArchiveConfig config;
    private final String observationTable;
    private final Path root;

    public ArchiveLifecycleManager(SqlConnectionProvider postgres, ArchiveCatalog catalog, TieredArchiveConfig config,
                                   String observationTable) {
        this.postgres = postgres;
        this.catalog = catalog;
        this.config = config;
        this.observationTable = safeIdentifier(observationTable);
        this.root = Path.of(config.rootDirectory).toAbsolutePath().normalize();
    }

    public void maintain() {
        int verifyLimit = config.isParquetPrimary()
                ? Math.max(16, Math.max(config.maximumVerificationsPerCycle, config.maximumStagePartitionsPerCycle))
                : config.maximumVerificationsPerCycle;
        for (ArchiveGeneration generation : catalog.unverifiedGenerations(verifyLimit))
            verifyHotColdParity(generation);

        // PARQUET_PRIMARY uses verified whole-partition retirement. Row-wise
        // DELETE would recreate the table/index bloat this mode is designed to
        // eliminate and could partially empty a partition before a later
        // generation in the same window has passed parity.
        if (!config.deleteHotAfterArchive || config.isParquetPrimary())
            return;
        Instant graceCutoff = Instant.now().minus(config.rollbackGraceDays, ChronoUnit.DAYS);
        for (ArchiveGeneration generation : catalog.readyForHotDeletion(
                graceCutoff, config.maximumDeletionsPerCycle))
            deleteVerifiedHotRows(generation);
    }

    public boolean verifyHotColdParity(ArchiveGeneration generation) {
        long actualRows = 0;
        try {
            Path parquet = verifiedArchiveObject(generation.objectPath(), generation.sha256());

            MessageDigest archiveDigest = MessageDigest.getInstance("SHA-256");
            MessageDigest hotDigest = MessageDigest.getInstance("SHA-256");
            try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "parity-verify")) {
                Path localParquet = ArchiveExporter.stageToLocalDirectory(
                        parquet, duckSession.operationDirectory(), "parity-verify.parquet");
                try (PreparedStatement archived = duckSession.connection().prepareStatement(
                        "SELECT id_value, datastream_id, foi_id, phenomenon_time, result_time, result_json " +
                                "FROM read_parquet('" + sqlLiteral(localParquet.toString()) + "') ORDER BY id_value");
                     ResultSet rows = archived.executeQuery()) {
                    List<RowFingerprint> batch = new ArrayList<>(VERIFY_BATCH);
                    while (rows.next()) {
                        batch.add(fromResult(rows));
                        actualRows++;
                        if (batch.size() == VERIFY_BATCH) {
                            compareBatch(batch, archiveDigest, hotDigest);
                            batch.clear();
                        }
                    }
                    if (!batch.isEmpty())
                        compareBatch(batch, archiveDigest, hotDigest);
                }
            }
            boolean success = actualRows == generation.rowCount() &&
                    MessageDigest.isEqual(archiveDigest.digest(), hotDigest.digest());
            catalog.recordVerification(generation.id(), "HOT_COLD_PARITY", success,
                    generation.rowCount(), actualRows, success ? "Exact IDs and canonical row digests match" : "Row digest mismatch");
            return success;
        } catch (Exception e) {
            catalog.recordVerification(generation.id(), "HOT_COLD_PARITY", false,
                    generation.rowCount(), actualRows, String.valueOf(e.getMessage()));
            return false;
        }
    }

    private void compareBatch(List<RowFingerprint> archivedRows, MessageDigest archiveDigest,
                              MessageDigest hotDigest) throws Exception {
        List<Long> ids = archivedRows.stream().map(RowFingerprint::id).toList();
        Map<Long, RowFingerprint> hotById = new HashMap<>();
        try (Connection connection = postgres.open()) {
            List<String> attachedChildren = config.isParquetPrimary()
                    ? ArchiveCatalog.attachedStageChildTables(connection, catalog.storeKey(), catalog.idScope())
                    : List.of();
            String fromClause = (config.isParquetPrimary() && !attachedChildren.isEmpty())
                    ? ArchiveCatalog.stageChildrenFromClause(
                    attachedChildren, "id, datastreamid, foiid, phenomenontime, resulttime, result")
                    : observationTable + " o";
            String sql = "SELECT o.id, o.datastreamid, o.foiid, o.phenomenontime, o.resulttime, o.result::text FROM " +
                    fromClause + " WHERE o.id = ANY (?) ORDER BY o.id";
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, connection.createArrayOf("bigint", ids.toArray()));
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        RowFingerprint row = fromResult(result);
                        hotById.put(row.id(), row);
                    }
                }
            }
        }
        for (RowFingerprint archived : archivedRows) {
            update(archiveDigest, archived);
            RowFingerprint hot = hotById.get(archived.id());
            if (hot == null)
                throw new IllegalStateException("Hot row missing before parity receipt: " + archived.id());
            update(hotDigest, hot);
        }
    }

    public long deleteVerifiedHotRows(ArchiveGeneration generation) {
        if (!generation.parityVerified())
            throw new IllegalStateException("Hot deletion requires a successful parity receipt");
        Path parquet;
        try {
            parquet = verifiedArchiveObject(generation.objectPath(), generation.sha256());
        } catch (Exception e) {
            catalog.recordVerification(generation.id(), "HOT_DELETE", false,
                    generation.rowCount(), 0, String.valueOf(e.getMessage()));
            throw new IllegalStateException("Cannot reverify archive object before hot deletion", e);
        }
        long deleted = 0;
        long deferredHolds = 0;
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "hot-delete")) {
            Path localParquet = ArchiveExporter.stageToLocalDirectory(
                    parquet, duckSession.operationDirectory(), "hot-delete.parquet");
            try (PreparedStatement archived = duckSession.connection().prepareStatement(
                    "SELECT id_value FROM read_parquet('" + sqlLiteral(localParquet.toString()) + "') ORDER BY id_value");
                 ResultSet rows = archived.executeQuery()) {
                List<Long> batch = new ArrayList<>(config.deleteBatchSize);
                while (rows.next()) {
                    batch.add(rows.getLong(1));
                    if (batch.size() == config.deleteBatchSize) {
                        DeletionBatchResult result = deleteBatch(generation.id(), batch);
                        deleted += result.deleted();
                        deferredHolds += result.held();
                        batch.clear();
                    }
                }
                if (!batch.isEmpty()) {
                    DeletionBatchResult result = deleteBatch(generation.id(), batch);
                    deleted += result.deleted();
                    deferredHolds += result.held();
                }
            }
        } catch (Exception e) {
            catalog.recordVerification(generation.id(), "HOT_DELETE", false,
                    generation.rowCount(), deleted, String.valueOf(e.getMessage()));
            throw new IllegalStateException("Cannot delete verified hot rows for " + generation.id(), e);
        }
        if (deferredHolds > 0) {
            catalog.recordVerification(generation.id(), "HOT_DELETE_DEFERRED_HOLD", true,
                    generation.rowCount(), deleted, deferredHolds + " exact IDs remain under active observation holds");
            return deleted;
        }
        catalog.recordVerification(generation.id(), "HOT_DELETE", true,
                generation.rowCount(), deleted, "All exact generation IDs are absent from the hot table");
        catalog.markSealed(generation.id());
        return deleted;
    }

    private DeletionBatchResult deleteBatch(java.util.UUID generationId, List<Long> ids) throws Exception {
        try (Connection connection = postgres.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                // Participate in the same archive mutation protocol as direct
                // and filtered writes.  This prevents a physical hot-row
                // delete from racing a logical overlay/tombstone transition.
                catalog.lockForMutation(connection, ids);
                // Serialize hold creation/release with each small deletion
                // batch. Without this lock, a hold could commit after the
                // hold lookup but immediately before DELETE.
                try (Statement lock = connection.createStatement()) {
                    lock.execute("LOCK TABLE oscar_retention_hold IN SHARE MODE");
                }
                try (PreparedStatement generationHold = connection.prepareStatement(
                        "SELECT 1 FROM oscar_retention_hold WHERE store_key=? AND id_scope=? " +
                                "AND released_at IS NULL AND subject_type='ARCHIVE_GENERATION' AND subject_id=?")) {
                    generationHold.setString(1, catalog.storeKey());
                    generationHold.setInt(2, catalog.idScope());
                    generationHold.setString(3, generationId.toString());
                    try (ResultSet result = generationHold.executeQuery()) {
                        if (result.next()) {
                            connection.commit();
                            connection.setAutoCommit(oldAutoCommit);
                            return new DeletionBatchResult(0, ids.size());
                        }
                    }
                }
                Set<Long> held = new HashSet<>();
                try (PreparedStatement holds = connection.prepareStatement(
                        "SELECT subject_id::bigint FROM oscar_retention_hold WHERE store_key=? AND id_scope=? " +
                                "AND released_at IS NULL AND subject_type='OBSERVATION' AND subject_id = ANY (?)")) {
                    holds.setString(1, catalog.storeKey());
                    holds.setInt(2, catalog.idScope());
                    holds.setArray(3, connection.createArrayOf("text", ids.stream().map(String::valueOf).toArray()));
                    try (ResultSet result = holds.executeQuery()) {
                        while (result.next()) held.add(result.getLong(1));
                    }
                }
                List<Long> deletable = withoutHeld(ids, held);
                long deleted = 0;
                if (!deletable.isEmpty()) {
                    try (PreparedStatement delete = connection.prepareStatement(
                            "DELETE FROM " + observationTable + " WHERE id = ANY (?) AND archivegenerationid=?")) {
                        delete.setArray(1, connection.createArrayOf("bigint", deletable.toArray()));
                        delete.setObject(2, generationId);
                        deleted = delete.executeUpdate();
                    }
                    try (PreparedStatement verify = connection.prepareStatement(
                            "SELECT count(*) FROM " + observationTable + " WHERE id = ANY (?) AND archivegenerationid=?")) {
                        verify.setArray(1, connection.createArrayOf("bigint", deletable.toArray()));
                        verify.setObject(2, generationId);
                        try (ResultSet result = verify.executeQuery()) {
                            if (!result.next() || result.getLong(1) != 0)
                                throw new IllegalStateException("One or more exact IDs remained after hot deletion");
                        }
                    }
                }
                connection.commit();
                connection.setAutoCommit(oldAutoCommit);
                return new DeletionBatchResult(deleted, held.size());
            } catch (Exception e) {
                connection.rollback();
                connection.setAutoCommit(oldAutoCommit);
                throw e;
            }
        }
    }

    private Path verifiedArchiveObject(Path relative, String expectedSha256) throws Exception {
        Path object = resolveArchiveObject(root, relative);
        BasicFileAttributes before = readRegularFileAttributes(object);
        String actualSha256 = ArchiveExporter.sha256(object);
        BasicFileAttributes after = readRegularFileAttributes(object);
        if (!expectedSha256.equals(actualSha256) || !sameObject(before, after))
            throw new IllegalStateException("Verified archive object is missing or changed: " + object);
        return object;
    }

    static Path resolveArchiveObject(Path configuredRoot, Path relative) {
        if (relative.isAbsolute())
            throw new IllegalStateException("Archive object path must be relative");
        Path root = configuredRoot.toAbsolutePath().normalize();
        Path object = root.resolve(relative).normalize();
        if (!object.startsWith(root))
            throw new IllegalStateException("Archive object escapes archive root");
        try {
            Path realRoot = root.toRealPath();
            Path current = root;
            for (Path component : root.relativize(object)) {
                current = current.resolve(component);
                if (Files.isSymbolicLink(current))
                    throw new IllegalStateException("Archive object path contains a symbolic link: " + current);
            }
            Path realObject = object.toRealPath();
            if (!realObject.startsWith(realRoot))
                throw new IllegalStateException("Archive object resolves outside configured root");
            readRegularFileAttributes(realObject);
            return realObject;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve archive object " + object, e);
        }
    }

    private static BasicFileAttributes readRegularFileAttributes(Path object) throws Exception {
        BasicFileAttributes attributes = Files.readAttributes(
                object, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile())
            throw new IllegalStateException("Cataloged archive object is not a regular file: " + object);
        return attributes;
    }

    private static boolean sameObject(BasicFileAttributes first, BasicFileAttributes second) {
        return first.size() == second.size() &&
                first.lastModifiedTime().equals(second.lastModifiedTime()) &&
                Objects.equals(first.fileKey(), second.fileKey());
    }

    private static RowFingerprint fromResult(ResultSet result) throws Exception {
        Long foi = result.getObject(3, Long.class);
        Instant phenomenon = UtcJdbcTime.getInstant(result, 4);
        Instant resultTime = UtcJdbcTime.getInstant(result, 5);
        return new RowFingerprint(result.getLong(1), result.getLong(2), foi,
                phenomenon, resultTime, result.getString(6));
    }

    private static void update(MessageDigest digest, RowFingerprint row) {
        field(digest, Long.toString(row.id()));
        field(digest, Long.toString(row.dataStreamId()));
        field(digest, row.foiId() == null ? null : Long.toString(row.foiId()));
        field(digest, row.phenomenonTime() == null ? null : row.phenomenonTime().toString());
        field(digest, row.resultTime() == null ? null : row.resultTime().toString());
        field(digest, row.resultJson());
    }

    private static void field(MessageDigest digest, String value) {
        if (value == null) {
            digest.update(ByteBuffer.allocate(4).putInt(-1).array());
            return;
        }
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(4).putInt(bytes.length).array());
        digest.update(bytes);
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        return identifier;
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    static List<Long> withoutHeld(List<Long> ids, Set<Long> held) {
        return ids.stream().filter(id -> !held.contains(id)).toList();
    }

    private record RowFingerprint(long id, long dataStreamId, Long foiId, Instant phenomenonTime,
                                  Instant resultTime, String resultJson) {}
    private record DeletionBatchResult(long deleted, long held) {}
}
