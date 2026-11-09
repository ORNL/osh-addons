package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.zip.GZIPOutputStream;

/** Creates verified, manifest-last Parquet generations without locking live tables. */
public final class ArchiveExporter {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().setPrettyPrinting().create();
    private static final int FETCH_SIZE = 5_000;
    private static final int DUCKDB_INSERT_BATCH_SIZE = 250;
    public static final long ESTIMATED_FIXED_BYTES_PER_ROW = 128L;

    private final SqlConnectionProvider postgres;
    private final ArchiveCatalog catalog;
    private final TieredArchiveConfig config;
    private final int idScope;
    private final String observationTable;
    private final String dataStreamTable;
    private final Path root;

    public ArchiveExporter(SqlConnectionProvider postgres, ArchiveCatalog catalog, TieredArchiveConfig config,
                           int idScope, String observationTable, String dataStreamTable) {
        this.postgres = postgres;
        this.catalog = catalog;
        this.config = config;
        this.idScope = idScope;
        this.observationTable = safeIdentifier(observationTable);
        this.dataStreamTable = safeIdentifier(dataStreamTable);
        this.root = Path.of(config.rootDirectory).toAbsolutePath().normalize();
    }

    public Optional<ArchiveGeneration> exportWindow(Instant start, Instant end, String profile) {
        if (!start.isBefore(end))
            throw new IllegalArgumentException("Archive window must be non-empty");
        Optional<UUID> claimed = catalog.claimJob(profile, start, end);
        if (claimed.isEmpty())
            return Optional.empty();

        UUID jobId = claimed.get();
        // A retry of one catalog job must reuse the same object names. This
        // makes a hard crash after manifest publication recoverable instead
        // of leaking a new full copy on every retry.
        UUID generationId = UUID.nameUUIDFromBytes(
                ("oscar-archive-generation-v1:" + jobId).getBytes(StandardCharsets.UTF_8));
        LocalDate partitionDate = start.atZone(ZoneOffset.UTC).toLocalDate();
        Path directory = root.resolve("datasets/observations/date=" + partitionDate)
                .resolve("profile=" + safePathComponent(profile));
        Path relativeParquet = root.relativize(directory.resolve("generation-" + generationId + ".parquet"));
        Path finalParquet = root.resolve(relativeParquet);
        Path partialParquet = finalParquet.resolveSibling(finalParquet.getFileName() + ".partial");
        Path relativeManifest = root.relativize(directory.resolve("generation-" + generationId + ".manifest.json"));
        Path finalManifest = root.resolve(relativeManifest);
        Path partialManifest = finalManifest.resolveSibling(finalManifest.getFileName() + ".partial");
        Path relativeSchemas = root.relativize(directory.resolve("generation-" + generationId + ".schemas.json.gz"));
        Path finalSchemas = root.resolve(relativeSchemas);
        Path partialSchemas = finalSchemas.resolveSibling(finalSchemas.getFileName() + ".partial");
        boolean activationAttempted = false;
        boolean activationCommitted = false;

        try {
            ArchivePathPolicy.prepareDirectory(root, directory);
            ArchivePathPolicy.prepareWriteTarget(root, partialParquet);
            ArchivePathPolicy.prepareWriteTarget(root, partialSchemas);
            ArchivePathPolicy.prepareWriteTarget(root, partialManifest);
            ArchivePathPolicy.prepareWriteTarget(root, finalParquet);
            ArchivePathPolicy.prepareWriteTarget(root, finalSchemas);
            ArchivePathPolicy.prepareWriteTarget(root, finalManifest);
            if (!catalog.isGenerationRegistered(generationId)) {
                // Manifest-first removal makes an interrupted prior attempt
                // undiscoverable before its payloads are replaced.
                deleteOnFailure(new IllegalStateException("Cleaning an interrupted archive attempt"),
                        finalManifest, finalSchemas, finalParquet,
                        partialManifest, partialSchemas, partialParquet);
            }
            ExportStats stats = writeParquet(partialParquet, start, end, profile);
            if (stats.rowCount == 0) {
                Files.deleteIfExists(partialParquet);
                catalog.supersedeEmptyJob(jobId,
                        "No eligible unowned rows remain in the reclaimed archive window");
                return Optional.empty();
            }
            verifyParquet(partialParquet, stats);
            forceFile(partialParquet);
            atomicMove(partialParquet, finalParquet);
            forceDirectory(directory);

            String parquetSha = sha256(finalParquet);
            long parquetBytes = Files.size(finalParquet);
            ArchiveGeneration generation = new ArchiveGeneration(
                    generationId, profile, start, end, relativeParquet, parquetSha,
                    stats.rowCount, stats.minimumId, stats.maximumId, "ACTIVE", false);

            writeSchemaSnapshot(partialSchemas, stats.schemas);
            forceFile(partialSchemas);
            atomicMove(partialSchemas, finalSchemas);
            forceDirectory(directory);
            String schemaSha = sha256(finalSchemas);
            long schemaBytes = Files.size(finalSchemas);

            writeManifest(partialManifest, generation, parquetBytes, stats.schemas,
                    relativeSchemas, schemaSha, schemaBytes);
            forceFile(partialManifest);
            atomicMove(partialManifest, finalManifest);
            forceDirectory(directory);
            String manifestSha = sha256(finalManifest);
            long manifestBytes = Files.size(finalManifest);

            activationAttempted = true;
            catalog.activate(jobId, generation, finalParquet, relativeManifest, parquetBytes, manifestSha, manifestBytes,
                    relativeSchemas, schemaBytes, schemaSha, stats.schemas,
                    observationTable, dataStreamTable, config.outputNames);
            activationCommitted = true;
            return Optional.of(generation);
        } catch (Throwable error) {
            deleteOnFailure(error, partialManifest, partialSchemas, partialParquet);

            boolean registered = activationCommitted;
            if (!registered && activationAttempted) {
                try {
                    registered = catalog.isGenerationRegistered(generationId);
                } catch (Throwable registrationError) {
                    // An unknown activation outcome must retain the immutable
                    // files. Reconciliation can classify them after the
                    // catalog is reachable; deleting a committed generation
                    // would destroy the only cold copy.
                    error.addSuppressed(registrationError);
                    registered = true;
                }
            }
            if (!registered)
                // Manifest first prevents an unregistered attempt from
                // remaining discoverable while its payloads are removed.
                deleteOnFailure(error, finalManifest, finalSchemas, finalParquet);
            if (!registered) {
                try {
                    catalog.failJob(jobId, error);
                } catch (Throwable catalogError) {
                    error.addSuppressed(catalogError);
                }
            }
            throw new IllegalStateException("Archive export failed for " + start + " through " + end, error);
        }
    }

    private void deleteOnFailure(Throwable primary, Path... paths) {
        for (Path path : paths) {
            try {
                Files.deleteIfExists(ArchivePathPolicy.prepareWriteTarget(root, path));
            } catch (Exception cleanupError) {
                primary.addSuppressed(cleanupError);
            }
        }
    }

    private ExportStats writeParquet(Path target, Instant start, Instant end, String profile) throws Exception {
        Files.deleteIfExists(target);
        long count = 0;
        long uncompressedBytes = 0;
        long byteBudget = config.effectiveMaximumUncompressedBytesPerGeneration();
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        Map<Long, Boolean> schemaIds = new TreeMap<>();
        List<ArchiveCatalog.SchemaSnapshot> schemas = List.of();

        try (Connection source = postgres.open();
             DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "export")) {
            Connection duck = duckSession.connection();
            source.setAutoCommit(false);
            source.setReadOnly(true);
            source.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
            try (java.sql.Statement create = duck.createStatement()) {
                create.execute("CREATE TABLE archive_rows (id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, " +
                        "datastream_id BIGINT, foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, " +
                        "result_time TIMESTAMP, result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            }

            List<String> attachedChildren = config.isParquetPrimary()
                    ? ArchiveCatalog.attachedStageChildTables(source, observationTable, idScope)
                    : List.of();
            if (config.isParquetPrimary() && attachedChildren.isEmpty()) {
                source.rollback();
                return new ExportStats(0, 0, 0, List.of());
            }
            String query = config.isParquetPrimary()
                    ? "SELECT o.id, o.datastreamid, o.foiid, o.phenomenontime, o.resulttime, o.result::text " +
                    "FROM " + ArchiveCatalog.stageChildrenFromClause(
                    attachedChildren, "id, datastreamid, foiid, phenomenontime, resulttime, result") +
                    " WHERE NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                    "WHERE owner.store_key=? AND owner.id_scope=? AND owner.observation_id=o.id) " +
                    "AND o.phenomenontime >= ? AND o.phenomenontime < ? " +
                    "ORDER BY o.phenomenontime, o.id LIMIT ?"
                    : "SELECT o.id, o.datastreamid, o.foiid, o.phenomenontime, o.resulttime, o.result::text " +
                    "FROM " + observationTable + " o JOIN " + dataStreamTable + " d ON d.id=o.datastreamid " +
                    "WHERE o.archivegenerationid IS NULL AND o.phenomenontime >= ? AND o.phenomenontime < ? " +
                    "AND d.data->>'outputName' = ANY (?) " +
                    "ORDER BY o.phenomenontime, o.id LIMIT ?";
            try (PreparedStatement read = source.prepareStatement(query);
                 PreparedStatement fullBatchInsert = duck.prepareStatement(
                         multiRowInsertSql(DUCKDB_INSERT_BATCH_SIZE))) {
                read.setFetchSize(FETCH_SIZE);
                int parameter = 1;
                if (config.isParquetPrimary()) {
                    read.setString(parameter++, observationTable);
                    read.setInt(parameter++, idScope);
                }
                UtcJdbcTime.setInstant(read, parameter++, start);
                UtcJdbcTime.setInstant(read, parameter++, end);
                if (!config.isParquetPrimary()) {
                    Array names = source.createArrayOf("text", config.outputNames.toArray());
                    read.setArray(parameter++, names);
                }
                // One UTC archive window can contain more data than a bounded DuckDB
                // session can safely stage.  Export a deterministic prefix;
                // the next cycle creates a normal same-window revision for
                // the remaining unowned rows.
                read.setInt(parameter, config.maximumRowsPerGeneration);
                List<StagedArchiveRow> batch = new ArrayList<>(DUCKDB_INSERT_BATCH_SIZE);
                try (ResultSet rows = read.executeQuery()) {
                    while (rows.next()) {
                        long id = rows.getLong(1);
                        long dataStreamId = rows.getLong(2);
                        Long foiId = rows.getObject(3, Long.class);
                        String resultJson = rows.getString(6);
                        long rowBytes = estimateUncompressedRowBytes(resultJson);
                        if (rowBytes > byteBudget - uncompressedBytes) {
                            if (count == 0)
                                throw new IOException("One archive row exceeds the effective generation byte limit of " +
                                        byteBudget + " bytes");
                            break;
                        }
                        uncompressedBytes += rowBytes;
                        count++;
                        Instant phenomenonTime = UtcJdbcTime.getInstant(rows, 4);
                        Instant resultTime = UtcJdbcTime.getInstant(rows, 5);
                        batch.add(new StagedArchiveRow(
                                id, dataStreamId, foiId, phenomenonTime, resultTime, resultJson));
                        minId = Math.min(minId, id);
                        maxId = Math.max(maxId, id);
                        schemaIds.put(dataStreamId, Boolean.TRUE);
                        if (batch.size() == DUCKDB_INSERT_BATCH_SIZE) {
                            executeMultiRowInsert(fullBatchInsert, batch, profile, count);
                            batch.clear();
                        }
                    }
                }
                if (!batch.isEmpty()) {
                    try (PreparedStatement remainderInsert = duck.prepareStatement(
                            multiRowInsertSql(batch.size()))) {
                        executeMultiRowInsert(remainderInsert, batch, profile, count);
                    }
                    batch.clear();
                }
                schemas = loadSchemas(source, new ArrayList<>(schemaIds.keySet()));
            } finally {
                source.rollback();
            }

            if (count > 0) {
                Path stagedParquet = duckSession.operationDirectory().resolve("export.parquet");
                String path = sqlLiteral(stagedParquet.toString());
                try (java.sql.Statement copy = duck.createStatement()) {
                    copy.execute("COPY (SELECT * FROM archive_rows ORDER BY datastream_id, phenomenon_time, result_time, id_value) " +
                            "TO '" + path + "' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL " +
                            config.zstdCompressionLevel + ", ROW_GROUP_SIZE 122880)");
                }
                copyBounded(stagedParquet, target);
            }
        }
        return new ExportStats(count, minId, maxId, schemas);
    }

    private record StagedArchiveRow(long id, long dataStreamId, Long foiId,
                                    Instant phenomenonTime, Instant resultTime,
                                    String resultJson) {}

    private static String multiRowInsertSql(int rowCount) {
        StringBuilder sb = new StringBuilder(32 + rowCount * 36);
        sb.append("INSERT INTO archive_rows VALUES ");
        for (int i = 0; i < rowCount; i++) {
            if (i > 0)
                sb.append(',');
            sb.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        }
        return sb.toString();
    }

    private void executeMultiRowInsert(PreparedStatement insert, List<StagedArchiveRow> batch,
                                       String profile, long totalRowCount)
            throws java.sql.SQLException {
        try {
            int p = 1;
            for (StagedArchiveRow row : batch) {
                insert.setInt(p++, idScope);
                insert.setLong(p++, row.id());
                insert.setInt(p++, idScope);
                insert.setLong(p++, row.dataStreamId());
                if (row.foiId() == null) {
                    insert.setNull(p++, java.sql.Types.INTEGER);
                    insert.setNull(p++, java.sql.Types.BIGINT);
                } else {
                    insert.setInt(p++, idScope);
                    insert.setLong(p++, row.foiId());
                }
                if (row.phenomenonTime() == null)
                    insert.setNull(p++, java.sql.Types.TIMESTAMP);
                else
                    UtcJdbcTime.setDuckDbInstant(insert, p++, row.phenomenonTime());
                if (row.resultTime() == null)
                    insert.setNull(p++, java.sql.Types.TIMESTAMP);
                else
                    UtcJdbcTime.setDuckDbInstant(insert, p++, row.resultTime());
                insert.setString(p++, row.resultJson());
                insert.setInt(p++, 1);
                insert.setString(p++, profile);
            }
            insert.executeUpdate();
        } catch (java.sql.SQLException error) {
            throw new java.sql.SQLException(
                    "DuckDB archive staging failed after " + totalRowCount + " source rows: " +
                            error.getMessage(), error.getSQLState(), error.getErrorCode(), error);
        }
    }

    /**
     * Conservative per-row sizing shared with the archive-window preflight.
     * This is deliberately based on the exact UTF-8 text returned by
     * PostgreSQL's {@code result::text}, not Java character count.
     */
    public static long estimateUncompressedRowBytes(String resultJson) {
        return ESTIMATED_FIXED_BYTES_PER_ROW + (resultJson == null ? 0L :
                resultJson.getBytes(StandardCharsets.UTF_8).length);
    }

    private void verifyParquet(Path parquet, ExportStats expected) throws Exception {
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "export-verify")) {
            Path localParquet = stageToLocalDirectory(parquet, duckSession.operationDirectory(), "verify.parquet");
            try (java.sql.Statement statement = duckSession.connection().createStatement();
                 ResultSet result = statement.executeQuery("SELECT count(*), min(id_value), max(id_value) FROM read_parquet('" +
                         sqlLiteral(localParquet.toString()) + "')")) {
                if (!result.next() || result.getLong(1) != expected.rowCount ||
                        result.getLong(2) != expected.minimumId || result.getLong(3) != expected.maximumId)
                    throw new IOException("Parquet verification count/range mismatch");
            }
        }
    }

    private List<ArchiveCatalog.SchemaSnapshot> loadSchemas(Connection connection, List<Long> ids) throws Exception {
        List<ArchiveCatalog.SchemaSnapshot> schemas = new ArrayList<>();
        if (ids.isEmpty())
            return schemas;
        String query = "SELECT id, data::text FROM " + dataStreamTable + " WHERE id = ANY (?) ORDER BY id";
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            statement.setArray(1, connection.createArrayOf("bigint", ids.toArray()));
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    schemas.add(new ArchiveCatalog.SchemaSnapshot(result.getLong(1), result.getString(2)));
            }
        }
        if (schemas.size() != ids.size())
            throw new IOException("One or more datastream schemas disappeared during archive export");
        return schemas;
    }

    private void writeSchemaSnapshot(Path target, List<ArchiveCatalog.SchemaSnapshot> schemas) throws IOException {
        Map<String, Object> snapshot = new LinkedHashMap<>();
        snapshot.put("format", "oscar-datastream-schema-snapshot");
        snapshot.put("version", 1);
        snapshot.put("schemas", schemas);
        try (var output = new GZIPOutputStream(Files.newOutputStream(target,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE))) {
            output.write(GSON.toJson(snapshot).getBytes(StandardCharsets.UTF_8));
            output.finish();
        }
    }

    private void writeManifest(Path target, ArchiveGeneration generation, long bytes,
                               List<ArchiveCatalog.SchemaSnapshot> schemas, Path schemaPath,
                               String schemaSha256, long schemaBytes) throws IOException {
        Map<String, Object> manifest = new LinkedHashMap<>();
        manifest.put("format", "oscar-observation-archive");
        manifest.put("version", 1);
        manifest.put("generationId", generation.id().toString());
        manifest.put("profile", generation.profile());
        manifest.put("windowStart", generation.start().toString());
        manifest.put("windowEnd", generation.end().toString());
        manifest.put("object", generation.objectPath().toString().replace('\\', '/'));
        manifest.put("sha256", generation.sha256());
        manifest.put("bytes", bytes);
        manifest.put("rowCount", generation.rowCount());
        manifest.put("minimumId", generation.minimumId());
        manifest.put("maximumId", generation.maximumId());
        manifest.put("idScope", idScope);
        manifest.put("datastreamSchemaIds", schemas.stream().map(ArchiveCatalog.SchemaSnapshot::dataStreamId).toList());
        manifest.put("datastreamSchemas", Map.of(
                "object", schemaPath.toString().replace('\\', '/'),
                "sha256", schemaSha256,
                "bytes", schemaBytes,
                "compression", "gzip"));
        manifest.put("columns", List.of(
                "id_scope:INTEGER", "id_value:BIGINT", "datastream_scope:INTEGER", "datastream_id:BIGINT",
                "foi_scope:INTEGER?", "foi_id:BIGINT?", "phenomenon_time:TIMESTAMP",
                "result_time:TIMESTAMP", "result_json:VARCHAR", "archive_version:INTEGER", "profile:VARCHAR"));
        manifest.put("sort", List.of("datastream_id", "phenomenon_time", "result_time", "id_value"));
        manifest.put("compression", "zstd");
        manifest.put("createdAt", Instant.now().toString());
        byte[] json = GSON.toJson(manifest).getBytes(StandardCharsets.UTF_8);
        try (FileChannel channel = FileChannel.open(target, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(json);
            while (buffer.hasRemaining())
                channel.write(buffer);
            channel.force(true);
        }
    }

    /**
     * Keep each read syscall well below WSL2 9p drvFs {@code msize=65536} and
     * order-4 contiguous kernel page allocations so cold Parquet checksum
     * verification never fails with {@code IOException: Cannot allocate memory}
     * when guest page cache fragments higher-order kernel buddy blocks.
     */
    static final int SHA256_READ_BUFFER_BYTES = 16 * 1024;
    private static final int SHA256_TRANSIENT_ALLOCATION_RETRIES = 3;

    public static String sha256(Path path) throws Exception {
        for (int attempt = 1; ; attempt++) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(path)) {
                byte[] buffer = new byte[SHA256_READ_BUFFER_BYTES];
                int read;
                while ((read = input.read(buffer)) >= 0)
                    if (read > 0) digest.update(buffer, 0, read);
                return HexFormat.of().formatHex(digest.digest());
            } catch (IOException error) {
                if (attempt >= SHA256_TRANSIENT_ALLOCATION_RETRIES ||
                        !isTransientAllocationIoError(error)) {
                    throw error;
                }
                Thread.sleep(20L * attempt);
            }
        }
    }

    /**
     * Copies a Parquet object into or out of a session-private local directory
     * using {@link #SHA256_READ_BUFFER_BYTES} (16 KiB) syscalls so DuckDB native
     * C++ {@code pread}/{@code write} operations never issue 64 KiB+ order-4 page
     * allocations across a Windows WSL 2 {@code 9p}/{@code drvfs} bind mount.
     */
    static Path stageToLocalDirectory(Path source, Path operationDirectory, String fileName) throws Exception {
        Path staged = operationDirectory.resolve(fileName).normalize();
        if (!staged.getParent().equals(operationDirectory.normalize()))
            throw new IllegalArgumentException("Staged Parquet filename escaped operation directory");
        copyBounded(source, staged);
        return staged;
    }

    static void copyBounded(Path source, Path target) throws Exception {
        for (int attempt = 1; ; attempt++) {
            try {
                Files.deleteIfExists(target);
                try (var input = Files.newInputStream(source);
                     var output = Files.newOutputStream(target,
                             StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
                    byte[] buffer = new byte[SHA256_READ_BUFFER_BYTES];
                    int read;
                    while ((read = input.read(buffer)) >= 0) {
                        if (read > 0)
                            output.write(buffer, 0, read);
                    }
                    output.flush();
                }
                return;
            } catch (IOException error) {
                if (attempt >= SHA256_TRANSIENT_ALLOCATION_RETRIES ||
                        !isTransientAllocationIoError(error)) {
                    throw error;
                }
                Thread.sleep(20L * attempt);
            }
        }
    }

    static boolean isTransientAllocationIoError(IOException error) {
        String message = error.getMessage();
        return message != null && message.contains("Cannot allocate memory");
    }

    private static void forceFile(Path path) throws IOException {
        try (FileChannel channel = FileChannel.open(path, StandardOpenOption.READ)) {
            channel.force(true);
        }
    }

    private static void forceDirectory(Path directory) {
        try (FileChannel channel = FileChannel.open(directory, StandardOpenOption.READ)) {
            channel.force(true);
        } catch (IOException ignored) {
            // Some filesystems/JDKs do not permit opening a directory. The
            // files themselves are still forced before publication.
        }
    }

    private static void atomicMove(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            Files.move(source, target);
        }
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        return identifier;
    }

    private static String safePathComponent(String value) {
        if (value == null || !value.matches("[A-Za-z0-9_-]+"))
            throw new IllegalArgumentException("Unsafe archive profile: " + value);
        return value;
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static final class ExportStats {
        final long rowCount;
        final long minimumId;
        final long maximumId;
        final List<ArchiveCatalog.SchemaSnapshot> schemas;

        ExportStats(long rowCount, long minimumId, long maximumId, List<ArchiveCatalog.SchemaSnapshot> schemas) {
            this.rowCount = rowCount;
            this.minimumId = minimumId;
            this.maximumId = maximumId;
            this.schemas = schemas;
        }
    }
}
