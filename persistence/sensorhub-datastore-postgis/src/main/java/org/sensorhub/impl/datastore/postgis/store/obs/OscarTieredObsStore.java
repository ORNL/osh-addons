package org.sensorhub.impl.datastore.postgis.store.obs;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.gson.JsonParser;
import net.opengis.swe.v20.DataBlock;
import org.postgresql.util.PGobject;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.feature.FoiFilter;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStats;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveCatalog;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveExporter;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveGeneration;
import org.sensorhub.impl.datastore.postgis.archive.ArchiveLifecycleManager;
import org.sensorhub.impl.datastore.postgis.archive.ArchivedObservation;
import org.sensorhub.impl.datastore.postgis.archive.ImmutableObservationStageManager;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader;
import org.sensorhub.impl.datastore.postgis.archive.TieredArchiveConfig;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.vast.util.TimeExtent;
import org.vast.data.DataBlockByte;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * PostgreSQL/Parquet observation store preserving the standard OSH IObsStore contract.
 * PostgreSQL remains authoritative for metadata and recent observations; immutable
 * Parquet generations are merged transparently with mutation overlays and tombstones.
 */
public class OscarTieredObsStore extends PostgisBatchObsStoreImpl {
    private static final Instant MIN_ARCHIVE_TIME = Instant.parse("1000-01-01T00:00:00Z");
    private static final Instant MAX_ARCHIVE_TIME = Instant.parse("3000-01-01T00:00:00Z");
    private static final Duration MINIMUM_ARCHIVE_WINDOW = Duration.ofSeconds(1);
    private static final Duration MUTABLE_INDEX_REINDEX_INTERVAL = Duration.ofDays(7);
    private static final int INITIAL_CANDIDATE_CLUSTER_BATCH = 64;
    private static final int REMOVE_BATCH_SIZE = 512;
    private static final int POINT_READ_COALESCER_BATCH = 64;
    private static final int OVERLAY_WRITE_COALESCER_BATCH = 64;

    private final TieredArchiveConfig archiveConfig;
    private final ArchiveCatalog archiveCatalog;
    private final ArchiveExporter archiveExporter;
    private final ArchiveLifecycleManager archiveLifecycle;
    private final ParquetArchiveReader archiveReader;
    private final ImmutableObservationStageManager immutableStage;
    private final Map<String, IDataStreamInfo> archivedSchemas = new ConcurrentHashMap<>();
    private final Cache<String, IDataStreamInfo> decodedOverlaySchemas = CacheBuilder.newBuilder()
            .maximumSize(512)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();
    private final Cache<Long, CachedOverlaySchema> liveOverlaySchemas = CacheBuilder.newBuilder()
            .maximumSize(2048)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();
    private final Cache<Long, ParquetArchiveReader.ArchivedRow> recentColdBaseById = CacheBuilder.newBuilder()
            .maximumSize(4096)
            .expireAfterWrite(2, TimeUnit.MINUTES)
            .build();
    private final Cache<MutatedGenSummaryCountKey, Long> mutatedGenSummaryCounts = CacheBuilder.newBuilder()
            .maximumSize(16_384)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();
    private final ColdPointReadCoalescer coldPointReadCoalescer = new ColdPointReadCoalescer();
    private final OverlayWriteCoalescer overlayWriteCoalescer = new OverlayWriteCoalescer();
    private final Map<Long, Boolean> immutableDataStreamCache = new ConcurrentHashMap<>();
    private final ThreadLocal<Map<Long, IDataStreamInfo>> pinnedDataStreamSchemas =
            new ThreadLocal<>();
    private final java.util.concurrent.atomic.AtomicLong lastMutableReindexEpochMillis =
            new java.util.concurrent.atomic.AtomicLong(System.currentTimeMillis());

    public OscarTieredObsStore(String url, String dbName, String login, String password, String dataStoreName,
                               int idScope, IdProviderType idProviderType, TieredArchiveConfig archiveConfig) {
        super(url, dbName, login, password, dataStoreName, idScope, idProviderType,
                tieredQueryBuilder(dataStoreName, archiveConfig));
        this.archiveConfig = archiveConfig;
        try {
            if (idProviderType != IdProviderType.SEQUENTIAL)
                throw new IllegalArgumentException("OSCAR tiered observations require sequential IDs for race-free archive publication");
            archiveConfig.validate();
            try (java.sql.Connection connection = connectionManager.getConnection()) {
                String[] markerQueries = archiveConfig.isParquetPrimary()
                        ? new String[]{
                        queryBuilder.addArchiveGenerationColumnQuery(),
                        queryBuilder.dropUnarchivedTimeIndexQuery(),
                        queryBuilder.createTieredUnadjudicatedAlarmIndexQuery()}
                        : new String[]{
                        queryBuilder.addArchiveGenerationColumnQuery(),
                        queryBuilder.createUnarchivedTimeIndexQuery(),
                        queryBuilder.createArchiveGenerationIndexQuery(),
                        queryBuilder.createTieredUnadjudicatedAlarmIndexQuery()};
                org.sensorhub.impl.datastore.postgis.utils.PostgisUtils.executeQueries(
                        connection, markerQueries);
            } catch (java.sql.SQLException e) {
                throw new IllegalStateException("Cannot initialize tiered observation markers", e);
            }
            this.archiveCatalog = new ArchiveCatalog(connectionManager::getConnection, dataStoreName, idScope,
                    archiveConfig);
            this.archiveCatalog.migrate();
            if (archiveConfig.enabled && archiveConfig.rootDirectory != null && !archiveConfig.rootDirectory.isBlank()) {
                this.archiveCatalog.backfillGenerationSummariesBatch(Path.of(archiveConfig.rootDirectory), 500);
            }
            this.archiveCatalog.maximumObservationId().ifPresent(archiveMaximum ->
                    lastId.accumulateAndGet(archiveMaximum, Math::max));
            this.archiveExporter = new ArchiveExporter(connectionManager::getConnection, archiveCatalog, archiveConfig,
                    idScope, dataStoreName, dataStoreName + "_datastreams");
            this.archiveLifecycle = new ArchiveLifecycleManager(connectionManager::getConnection, archiveCatalog,
                    archiveConfig, dataStoreName);
            this.archiveReader = new ParquetArchiveReader(Path.of(archiveConfig.rootDirectory), archiveConfig);
            if (archiveConfig.isParquetPrimary()) {
                this.immutableStage = new ImmutableObservationStageManager(
                        connectionManager::getConnection, archiveCatalog, archiveConfig,
                        dataStoreName, dataStoreName + "_datastreams", idScope);
                this.immutableStage.initialize();
            } else {
                this.immutableStage = null;
            }
        } catch (RuntimeException | Error failure) {
            try {
                super.close();
            } catch (RuntimeException cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
            throw failure;
        }
    }

    private static QueryBuilderObsStore tieredQueryBuilder(
            String dataStoreName, TieredArchiveConfig archiveConfig) {
        Objects.requireNonNull(archiveConfig, "archiveConfig");
        // Validate before the superclass opens PostgreSQL or creates a table.
        // A mistyped primary-mode configuration must fail without leaving a
        // partially initialized schema with the wrong index profile.
        archiveConfig.validate();
        return new QueryBuilderObsStore(dataStoreName, !archiveConfig.isParquetPrimary());
    }

    public ArchiveCatalog getArchiveCatalog() {
        return archiveCatalog;
    }

    public TieredArchiveConfig getArchiveConfig() {
        return archiveConfig;
    }

    public boolean isParquetPrimary() {
        return archiveConfig.enabled && archiveConfig.isParquetPrimary();
    }

    /**
     * Pins metadata already loaded by a coordinated datastream removal. Hot
     * observation decoding can then proceed while that metadata store's sole
     * connection remains occupied by the outer hold-barrier transaction.
     */
    <T> T withPinnedDataStreamSchema(long dataStreamId, IDataStreamInfo schema,
                                     Supplier<T> operation) {
        if (schema == null)
            return operation.get();
        Map<Long, IDataStreamInfo> pinned = pinnedDataStreamSchemas.get();
        if (pinned == null) {
            pinned = new HashMap<>();
            pinnedDataStreamSchemas.set(pinned);
        }
        IDataStreamInfo previous = pinned.put(dataStreamId, schema);
        try {
            return operation.get();
        } finally {
            if (previous == null)
                pinned.remove(dataStreamId);
            else
                pinned.put(dataStreamId, previous);
            if (pinned.isEmpty())
                pinnedDataStreamSchemas.remove();
        }
    }

    @Override
    protected IDataStreamInfo getDataStreamInfo(BigId dataStreamId) {
        Map<Long, IDataStreamInfo> schemas = pinnedDataStreamSchemas.get();
        IDataStreamInfo pinned = schemas == null ? null :
                schemas.get(dataStreamId.getIdAsLong());
        return pinned != null ? pinned : super.getDataStreamInfo(dataStreamId);
    }

    /** Visible to health checks and cursor-lifecycle regression tests. */
    public int getOpenArchiveCursorCount() {
        return archiveReader.getOpenCursorCount();
    }

    public void maintainArchiveLifecycle() {
        archiveLifecycle.maintain();
        if (immutableStage != null)
            immutableStage.retireVerifiedPartitions();
        archiveCatalog.backfillGenerationSummariesBatch(Path.of(archiveConfig.rootDirectory), 100);
        archiveCatalog.migrateLegacyOverlaySchemas(250);
        long now = System.currentTimeMillis();
        long previous = lastMutableReindexEpochMillis.get();
        if (now - previous >= MUTABLE_INDEX_REINDEX_INTERVAL.toMillis() &&
                lastMutableReindexEpochMillis.compareAndSet(previous, now)) {
            reindexMutableHotTables();
        }
    }

    public int migrateLegacyOverlaySchemas(int limit) {
        return archiveCatalog.migrateLegacyOverlaySchemas(limit);
    }

    /**
     * Rebuilds B-tree indexes online on the mutable observation parent table
     * and companion datastream metadata table to prevent multi-year index bloat.
     */
    public boolean stageLegacyAllowlistedRowsBatch() {
        return immutableStage != null && immutableStage.stageLegacyAllowlistedRowsBatch();
    }

    public void reindexMutableHotTables() {
        archiveCatalog.reindexMutableTables(getDatastoreName(), getDataStreams().getDatastoreName());
    }

    public void compactDrainedMutableHotTables() {
        archiveCatalog.compactDrainedParentTable(getDatastoreName(), getDataStreams().getDatastoreName());
    }

    /** Physical short-lived stage state exposed for health and regression probes. */
    public List<ImmutableObservationStageManager.PartitionState> getImmutableStagePartitions() {
        return immutableStage == null ? List.of() : immutableStage.listPartitions();
    }

    /** Oldest database receipt still present in the disposable raw stage. */
    public java.util.Optional<Instant> getOldestImmutableStageReceipt() {
        return immutableStage == null
                ? java.util.Optional.empty()
                : immutableStage.oldestReceivedAt();
    }

    public java.util.Optional<ArchiveGeneration> archiveWindow(Instant start, Instant end, String profile) {
        return archiveExporter.exportWindow(start, end, profile);
    }

    /**
     * Natural-key corrections must never rewrite a base row that belongs to a
     * query-visible immutable archive generation. The guarded PostgreSQL
     * upsert leaves that row untouched; {@link #reconcileProtectedObservations}
     * records the logical replacement in the generation-owned overlay within
     * the same database transaction.
     */
    @Override
    protected String preparedObservationUpsertQuery() {
        return queryBuilder.insertTieredObsPreparedQuery();
    }

    @Override
    protected void beforeObservationWrite(Connection connection,
                                          Collection<PreparedObservation> observations)
            throws Exception {
        if (archiveConfig.enabled) {
            archiveCatalog.lockForMutation(connection, observations.stream()
                    .map(observation -> observation.id)
                    .toList());
        }
        if (immutableStage != null) {
            List<ImmutableObservationStageManager.Route> routes = new ArrayList<>(observations.size());
            Map<Long, Boolean> immutableByDataStream = new HashMap<>();
            Map<Long, DataStreamRange> immutableRanges = new HashMap<>();
            for (PreparedObservation observation : observations) {
                Boolean immutable = immutableByDataStream.get(observation.dataStreamId);
                if (immutable == null) {
                    immutable = immutableDataStreamCache.get(observation.dataStreamId);
                    if (immutable == null) {
                        IDataStreamInfo info = getDataStreamInfo(
                                BigId.fromLong(idScope, observation.dataStreamId));
                        if (info == null)
                            throw new IllegalStateException(
                                    "Cannot classify unknown datastream " + observation.dataStreamId);
                        immutable = archiveConfig.outputNames.contains(info.getOutputName());
                        immutableDataStreamCache.putIfAbsent(observation.dataStreamId, immutable);
                    }
                    immutableByDataStream.put(observation.dataStreamId, immutable);
                }
                routes.add(new ImmutableObservationStageManager.Route(
                        observation.dataStreamId, observation.phenomenonTime, immutable));
                if (immutable)
                    immutableRanges.computeIfAbsent(observation.dataStreamId,
                                    ignored -> new DataStreamRange())
                            .include(observation.phenomenonTime, observation.resultTime);
            }
            immutableStage.prepare(connection, routes);
            updateImmutableDataStreamRanges(connection, immutableRanges);
        }
    }

    private void updateImmutableDataStreamRanges(Connection connection,
                                                 Map<Long, DataStreamRange> ranges)
            throws SQLException {
        if (ranges.isEmpty())
            return;
        String sql = "UPDATE " + getDataStreams().getDatastoreName() + " SET " +
                "min_phenomenon_time=LEAST(COALESCE(min_phenomenon_time,?),?)," +
                "max_phenomenon_time=GREATEST(COALESCE(max_phenomenon_time,?),?)," +
                "min_result_time=CASE WHEN ?::timestamp IS NULL THEN min_result_time ELSE " +
                "LEAST(COALESCE(min_result_time,?::timestamp),?::timestamp) END," +
                "max_result_time=CASE WHEN ?::timestamp IS NULL THEN max_result_time ELSE " +
                "GREATEST(COALESCE(max_result_time,?::timestamp),?::timestamp) END " +
                "WHERE id=? AND (" +
                "min_phenomenon_time IS NULL OR ? < min_phenomenon_time OR " +
                "max_phenomenon_time IS NULL OR ? >= max_phenomenon_time + interval '60 seconds' OR " +
                "(?::timestamp IS NOT NULL AND (min_result_time IS NULL OR ?::timestamp < min_result_time)) OR " +
                "(?::timestamp IS NOT NULL AND (max_result_time IS NULL OR ?::timestamp >= max_result_time + interval '60 seconds'))" +
                ")";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            for (Map.Entry<Long, DataStreamRange> entry : ranges.entrySet()) {
                DataStreamRange range = entry.getValue();
                UtcJdbcTime.setInstant(statement, 1, range.minimumPhenomenon);
                UtcJdbcTime.setInstant(statement, 2, range.minimumPhenomenon);
                UtcJdbcTime.setInstant(statement, 3, range.maximumPhenomenon);
                UtcJdbcTime.setInstant(statement, 4, range.maximumPhenomenon);
                bindNullableInstant(statement, 5, range.minimumResult);
                bindNullableInstant(statement, 6, range.minimumResult);
                bindNullableInstant(statement, 7, range.minimumResult);
                bindNullableInstant(statement, 8, range.maximumResult);
                bindNullableInstant(statement, 9, range.maximumResult);
                bindNullableInstant(statement, 10, range.maximumResult);
                statement.setLong(11, entry.getKey());
                UtcJdbcTime.setInstant(statement, 12, range.minimumPhenomenon);
                UtcJdbcTime.setInstant(statement, 13, range.maximumPhenomenon);
                bindNullableInstant(statement, 14, range.minimumResult);
                bindNullableInstant(statement, 15, range.minimumResult);
                bindNullableInstant(statement, 16, range.maximumResult);
                bindNullableInstant(statement, 17, range.maximumResult);
                statement.addBatch();
            }
            statement.executeBatch();
        }
    }

    private static void bindNullableInstant(PreparedStatement statement, int index,
                                            Instant value) throws SQLException {
        if (value == null)
            statement.setNull(index, Types.TIMESTAMP);
        else
            UtcJdbcTime.setInstant(statement, index, value);
    }

    private static final class DataStreamRange {
        private Instant minimumPhenomenon;
        private Instant maximumPhenomenon;
        private Instant minimumResult;
        private Instant maximumResult;

        void include(Instant phenomenon, Instant result) {
            if (phenomenon == null)
                throw new IllegalArgumentException(
                        "Immutable detector observations require a phenomenon time");
            minimumPhenomenon = minimumPhenomenon == null || phenomenon.isBefore(minimumPhenomenon)
                    ? phenomenon : minimumPhenomenon;
            maximumPhenomenon = maximumPhenomenon == null || phenomenon.isAfter(maximumPhenomenon)
                    ? phenomenon : maximumPhenomenon;
            if (result != null) {
                minimumResult = minimumResult == null || result.isBefore(minimumResult)
                        ? result : minimumResult;
                maximumResult = maximumResult == null || result.isAfter(maximumResult)
                        ? result : maximumResult;
            }
        }
    }

    @Override
    protected Instant getStoredPhenomenonTimeHighWater(long dataStreamId) {
        Instant hot = super.getStoredPhenomenonTimeHighWater(dataStreamId);
        if (!archiveConfig.enabled)
            return hot;
        Instant cold = archiveCatalog.latestWindowEnd(dataStreamId).orElse(null);
        if (hot == null)
            return cold;
        return cold == null || hot.isAfter(cold) ? hot : cold;
    }

    @Override
    protected Long findStoredObservationId(PreparedObservation observation) {
        Long hot = super.findStoredObservationId(observation);
        if (hot != null || !archiveConfig.enabled)
            return hot;
        return findArchivedNaturalKey(observation)
                .map(match -> match.observation().id())
                .orElse(null);
    }

    private boolean isImmutableDataStream(long dataStreamId) {
        Boolean immutable = immutableDataStreamCache.get(dataStreamId);
        if (immutable == null) {
            IDataStreamInfo info = getDataStreamInfo(BigId.fromLong(idScope, dataStreamId));
            if (info == null)
                return false;
            immutable = archiveConfig.outputNames.contains(info.getOutputName());
            immutableDataStreamCache.putIfAbsent(dataStreamId, immutable);
        }
        return immutable;
    }

    @Override
    protected Set<Long> reconcileProtectedObservations(Connection connection,
                                                        Collection<PreparedObservation> observations)
            throws Exception {
        if (!archiveConfig.enabled || observations.isEmpty())
            return Set.of();

        Map<Long, PreparedObservation> expectedById = new HashMap<>();
        for (PreparedObservation observation : observations) {
            if (isImmutableDataStream(observation.dataStreamId))
                expectedById.put(observation.id, observation);
        }
        if (expectedById.isEmpty())
            return Set.of();

        List<ProtectedCorrection> corrections = new ArrayList<>();
        Set<Long> protectedHotIds = new HashSet<>();
        boolean primary = archiveConfig.isParquetPrimary();
        String sql = primary
                ? "SELECT observation.id, generation.id, " +
                "COALESCE(overlay_row.result_json, observation.result)::text " +
                "FROM oscar_obs_stage_owner owner " +
                "JOIN oscar_obs_stage_partition stage ON stage.store_key=owner.store_key " +
                "AND stage.id_scope=owner.id_scope AND stage.child_table=owner.child_table " +
                "JOIN oscar_archive_generation generation ON generation.id=owner.generation_id " +
                "AND generation.store_key=owner.store_key AND generation.id_scope=owner.id_scope " +
                "JOIN " + getDatastoreName() + " observation ON observation.id=owner.observation_id " +
                "AND observation.tableoid=to_regclass(stage.child_table) " +
                "LEFT JOIN oscar_obs_overlay overlay_row " +
                "ON overlay_row.store_key=owner.store_key AND overlay_row.id_scope=owner.id_scope " +
                "AND overlay_row.observation_id=owner.observation_id " +
                "AND overlay_row.generation_id=generation.id " +
                "WHERE owner.store_key=? AND owner.id_scope=? " +
                "AND owner.observation_id=ANY(?) AND generation.state IN ('ACTIVE','SEALED')"
                : "SELECT observation.id, generation.id, " +
                "COALESCE(overlay_row.result_json, observation.result)::text " +
                "FROM " + getDatastoreName() + " observation " +
                "JOIN oscar_archive_generation generation " +
                "ON generation.id=observation.archivegenerationid " +
                "LEFT JOIN oscar_obs_overlay overlay_row " +
                "ON overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND overlay_row.observation_id=observation.id " +
                "AND overlay_row.generation_id=generation.id " +
                "WHERE observation.id=ANY(?) AND generation.store_key=? " +
                "AND generation.id_scope=? AND generation.state IN ('ACTIVE','SEALED')";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            int parameter = 1;
            if (primary) {
                statement.setString(parameter++, getDatastoreName());
                statement.setInt(parameter++, idScope);
                statement.setArray(parameter, connection.createArrayOf("bigint",
                        expectedById.keySet().toArray(Long[]::new)));
            } else {
                statement.setString(parameter++, getDatastoreName());
                statement.setInt(parameter++, idScope);
                statement.setArray(parameter++, connection.createArrayOf("bigint",
                        expectedById.keySet().toArray(Long[]::new)));
                statement.setString(parameter++, getDatastoreName());
                statement.setInt(parameter, idScope);
            }
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    PreparedObservation expected = expectedById.get(result.getLong(1));
                    if (expected != null) {
                        protectedHotIds.add(expected.id);
                        if (!jsonEquals(expected.resultJson, result.getString(3))) {
                            corrections.add(new ProtectedCorrection(
                                    result.getObject(2, UUID.class), expected));
                        }
                    }
                }
            }
        }

        // After grace deletion PostgreSQL no longer contains the stable row,
        // so the upsert temporarily inserts it. Resolve the exact immutable
        // Parquet identity, remove that uncommitted duplicate, and apply only
        // a logical correction as an overlay. Exact replay leaves no hot row
        // and creates no overlay.
        Set<Long> candidateDataStreams = new HashSet<>();
        Instant minCandidatePhenomenon = null;
        Instant maxCandidatePhenomenon = null;
        for (PreparedObservation expected : expectedById.values()) {
            if (protectedHotIds.contains(expected.id) || expected.phenomenonTime == null)
                continue;
            candidateDataStreams.add(expected.dataStreamId);
            if (minCandidatePhenomenon == null || expected.phenomenonTime.isBefore(minCandidatePhenomenon))
                minCandidatePhenomenon = expected.phenomenonTime;
            if (maxCandidatePhenomenon == null || expected.phenomenonTime.isAfter(maxCandidatePhenomenon))
                maxCandidatePhenomenon = expected.phenomenonTime;
        }
        if (!candidateDataStreams.isEmpty() && minCandidatePhenomenon != null) {
            Instant batchEnd;
            try {
                batchEnd = maxCandidatePhenomenon.plus(1, ChronoUnit.MICROS);
            } catch (RuntimeException overflow) {
                batchEnd = MAX_ARCHIVE_TIME;
            }
            if (!archiveCatalog.candidates(
                    connection, minCandidatePhenomenon, batchEnd, null, null, null, candidateDataStreams).isEmpty()) {
                for (PreparedObservation expected : expectedById.values()) {
                    if (protectedHotIds.contains(expected.id))
                        continue;
                    java.util.Optional<ArchivedNaturalMatch> archived =
                            findArchivedNaturalKey(connection, expected);
                    if (archived.isEmpty())
                        continue;
                    ArchivedNaturalMatch match = archived.get();
                    if (match.observation().id() != expected.id)
                        throw new IllegalStateException("Archived observation identity resolved after add returned a different ID");
                    protectedHotIds.add(expected.id);
                    deleteTemporaryReplayRow(connection, expected.id);
                    String logicalResult = archiveCatalog.overlayWithGeneration(
                                    connection, expected.id, idScope, "OVERLAY", true)
                            .map(overlay -> overlay.observation().resultJson())
                            .orElse(match.observation().resultJson());
                    if (!jsonEquals(expected.resultJson, logicalResult))
                        corrections.add(new ProtectedCorrection(match.generation().id(), expected));
                }
            }
        }

        Map<Long, String> schemaByDataStream = new HashMap<>();
        for (ProtectedCorrection correction : corrections) {
            PreparedObservation observation = correction.observation();
            String schemaJson = schemaByDataStream.get(observation.dataStreamId);
            if (schemaJson == null) {
                // PostgisDataStreamStoreImpl owns a distinct ConnectionManager;
                // this lookup therefore cannot recursively borrow the
                // observation connection held by this transaction. A linked
                // non-PostgreSQL store likewise does not use this pool.
                IDataStreamInfo schema = getDataStreamInfo(
                        BigId.fromLong(idScope, observation.dataStreamId));
                if (schema == null)
                    throw new IllegalStateException(
                            "Cannot correct archived observation without a live datastream schema");
                schemaJson = SerializerUtils.writeIDataStreamInfoToJson(schema);
                schemaByDataStream.put(observation.dataStreamId, schemaJson);
            }
            archiveCatalog.upsertOverlay(connection, correction.generationId(),
                    new ArchivedObservation(idScope, observation.id, idScope,
                            observation.dataStreamId,
                            observation.foiId == null ? null : idScope, observation.foiId,
                            observation.phenomenonTime, observation.resultTime,
                            observation.resultJson, "OVERLAY"), schemaJson);
        }
        return protectedHotIds;
    }

    private record ProtectedCorrection(UUID generationId, PreparedObservation observation) {}

    private java.util.Optional<ArchivedNaturalMatch> findArchivedNaturalKey(
            PreparedObservation observation) {
        try {
            return findArchivedNaturalKey(null, observation);
        } catch (Exception error) {
            throw new IllegalStateException("Cannot resolve archived observation identity", error);
        }
    }

    private java.util.Optional<ArchivedNaturalMatch> findArchivedNaturalKey(
            Connection transaction, PreparedObservation observation) throws Exception {
        if (observation.phenomenonTime == null)
            return java.util.Optional.empty();
        Instant end;
        try {
            end = observation.phenomenonTime.plus(1, ChronoUnit.MICROS);
        } catch (RuntimeException overflow) {
            end = MAX_ARCHIVE_TIME;
        }
        Set<Long> dataStreams = Set.of(observation.dataStreamId);
        List<ArchiveGeneration> candidates = transaction == null
                ? archiveCatalog.candidates(observation.phenomenonTime, end, null, null, null, dataStreams)
                : archiveCatalog.candidates(transaction, observation.phenomenonTime, end, null, null, null, dataStreams);
        if (candidates.isEmpty())
            return java.util.Optional.empty();
        ObsFilter.Builder filter = new ObsFilter.Builder().withPhenomenonTime(
                new TemporalFilter.Builder().withSingleValue(observation.phenomenonTime).build());
        if (observation.resultTime != null) {
            filter.withResultTime(new TemporalFilter.Builder()
                    .withSingleValue(observation.resultTime).build());
        }
        Set<Long> fois = Set.of(observation.foiId == null ? 0L : observation.foiId);
        try (Stream<ParquetArchiveReader.ArchivedRow> rows = archiveReader.stream(
                candidates, filter.build(), dataStreams, fois, Long.MAX_VALUE)) {
            return rows.filter(row -> {
                        ArchivedObservation archived = row.observation();
                        return archived.dataStreamId() == observation.dataStreamId &&
                                Objects.equals(archived.foiId(), observation.foiId) &&
                                Objects.equals(archived.phenomenonTime(), observation.phenomenonTime) &&
                                Objects.equals(archived.resultTime(), observation.resultTime) &&
                                !isArchiveTombstoned(transaction, archived.id());
                    })
                    .map(row -> new ArchivedNaturalMatch(row.generation(), row.observation()))
                    .findFirst();
        }
    }

    private boolean isArchiveTombstoned(Connection transaction, long observationId) {
        try {
            return transaction == null
                    ? archiveCatalog.isTombstoned(observationId)
                    : archiveCatalog.isTombstoned(transaction, observationId);
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Cannot inspect archive tombstone for observation " + observationId, error);
        }
    }

    private static boolean jsonEquals(String left, String right) {
        return JsonParser.parseString(left).equals(JsonParser.parseString(right));
    }

    private record ArchivedNaturalMatch(ArchiveGeneration generation,
                                        ArchivedObservation observation) {}

    /** Returns the oldest unarchived eligible row, including late arrivals. */
    public java.util.Optional<Instant> nextArchiveWindowStart(Instant cutoff, String profile) {
        try (java.sql.Connection connection = connectionManager.getConnection()) {
            List<String> attachedChildren = archiveConfig.isParquetPrimary()
                    ? ArchiveCatalog.attachedStageChildTables(connection, getDatastoreName(), idScope)
                    : List.of();
            if (archiveConfig.isParquetPrimary() && attachedChildren.isEmpty())
                return java.util.Optional.empty();
            String sql = archiveConfig.isParquetPrimary()
                    ? "SELECT min(o.phenomenontime) FROM " +
                    ArchiveCatalog.stageChildrenFromClause(attachedChildren, "id, phenomenontime") +
                    " WHERE NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                    "WHERE owner.store_key=? AND owner.id_scope=? AND owner.observation_id=o.id) " +
                    "AND o.phenomenontime < ?"
                    : "SELECT min(o.phenomenontime) FROM " + getDatastoreName() +
                    " o JOIN " + getDataStreams().getDatastoreName() + " d ON d.id=o.datastreamid " +
                    "WHERE o.archivegenerationid IS NULL AND o.phenomenontime < ? " +
                    "AND d.data->>'outputName' = ANY (?)";
            try (java.sql.PreparedStatement statement = connection.prepareStatement(sql)) {
                int parameter = 1;
                if (archiveConfig.isParquetPrimary()) {
                    statement.setString(parameter++, getDatastoreName());
                    statement.setInt(parameter++, idScope);
                }
                UtcJdbcTime.setInstant(statement, parameter++, cutoff);
                if (!archiveConfig.isParquetPrimary())
                    statement.setArray(parameter, connection.createArrayOf("text", archiveConfig.outputNames.toArray()));
                try (java.sql.ResultSet result = statement.executeQuery()) {
                    if (!result.next()) return java.util.Optional.empty();
                    Instant value = UtcJdbcTime.getInstant(result, 1);
                    return value == null ? java.util.Optional.empty() : java.util.Optional.of(value);
                }
            }
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("Cannot determine next archive window", e);
        }
    }

    /** Reclaims an exact failed/stale window before normal full-hour work. */
    public java.util.Optional<ArchiveCatalog.RetryableJobWindow> nextRetryableArchiveWindow(
            Instant cutoff, String profile) {
        return archiveCatalog.nextRetryableJobWindow(profile, cutoff);
    }

    /** Marks interrupted in-flight {@code EXPORTING} jobs as {@code FAILED} on module startup. */
    public int reclaimInterruptedExportingJobsOnStartup() {
        return archiveCatalog.reclaimInterruptedExportingJobsOnStartup();
    }

    /**
     * Measures at most {@code rowCap + 1} candidates using the same uncompressed-byte
     * accounting as {@link ArchiveExporter}. If the returned row count is at most the
     * cap, the byte count covers the complete window; once the row cap is exceeded the
     * scheduler must split regardless of the partial byte total.
     */
    public ArchiveCandidateFootprint measureArchiveCandidates(Instant start, Instant end,
                                                               long rowCap) {
        if (start == null || end == null || !start.isBefore(end))
            throw new IllegalArgumentException("Archive candidate window must be non-empty");
        if (rowCap < 1)
            throw new IllegalArgumentException("Archive candidate row cap must be positive");
        try (java.sql.Connection connection = connectionManager.getConnection()) {
            List<String> attachedChildren = archiveConfig.isParquetPrimary()
                    ? ArchiveCatalog.attachedStageChildTables(connection, getDatastoreName(), idScope)
                    : List.of();
            if (archiveConfig.isParquetPrimary() && attachedChildren.isEmpty())
                return new ArchiveCandidateFootprint(0, 0, null);
            String candidateSource = archiveConfig.isParquetPrimary()
                    ? "FROM " + ArchiveCatalog.stageChildrenFromClause(
                    attachedChildren, "id, phenomenontime, result") +
                    " WHERE NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                    "WHERE owner.store_key=? AND owner.id_scope=? AND owner.observation_id=o.id) " +
                    "AND o.phenomenontime >= ? AND o.phenomenontime < ? "
                    : "FROM " + getDatastoreName() + " o JOIN " + getDataStreams().getDatastoreName() +
                    " d ON d.id=o.datastreamid WHERE o.archivegenerationid IS NULL " +
                    "AND o.phenomenontime >= ? AND o.phenomenontime < ? " +
                    "AND d.data->>'outputName' = ANY (?) ";
            String sql = "SELECT count(*), COALESCE(sum(" +
                    ArchiveExporter.ESTIMATED_FIXED_BYTES_PER_ROW + "::bigint + " +
                    "COALESCE(octet_length(convert_to(candidate.result::text, 'UTF8')), 0)), 0), " +
                    "min(candidate.phenomenontime) FROM (SELECT o.result,o.phenomenontime " +
                    candidateSource + "ORDER BY o.phenomenontime,o.id LIMIT ?) candidate";
            try (java.sql.PreparedStatement statement = connection.prepareStatement(sql)) {
                int parameter = 1;
                if (archiveConfig.isParquetPrimary()) {
                    statement.setString(parameter++, getDatastoreName());
                    statement.setInt(parameter++, idScope);
                }
                UtcJdbcTime.setInstant(statement, parameter++, start);
                UtcJdbcTime.setInstant(statement, parameter++, end);
                if (!archiveConfig.isParquetPrimary())
                    statement.setArray(parameter++, connection.createArrayOf("text", archiveConfig.outputNames.toArray()));
                statement.setLong(parameter, Math.addExact(rowCap, 1));
                try (java.sql.ResultSet result = statement.executeQuery()) {
                    if (!result.next())
                        return new ArchiveCandidateFootprint(0, 0, null);
                    BigDecimal measuredBytes = result.getBigDecimal(2);
                    long bytes = measuredBytes.compareTo(BigDecimal.valueOf(Long.MAX_VALUE)) >= 0
                            ? Long.MAX_VALUE : measuredBytes.longValueExact();
                    return new ArchiveCandidateFootprint(result.getLong(1), bytes,
                            UtcJdbcTime.getInstant(result, 3));
                }
            }
        } catch (java.sql.SQLException | ArithmeticException e) {
            throw new IllegalStateException("Cannot size next archive window", e);
        }
    }

    /** Selects the largest deterministic prefix that satisfies both generation caps. */
    public Instant selectArchiveWindowEnd(Instant start, Instant proposedEnd) {
        Instant end = proposedEnd;
        Instant minimumCandidateEnd = null;
        while (true) {
            ArchiveCandidateFootprint footprint = measureArchiveCandidates(
                    start, end, archiveConfig.maximumRowsPerGeneration);
            boolean rowLimitExceeded = footprint.rowCount() > archiveConfig.maximumRowsPerGeneration;
            boolean byteLimitExceeded = footprint.estimatedUncompressedBytes() >
                    archiveConfig.effectiveMaximumUncompressedBytesPerGeneration();
            if (!rowLimitExceeded && !byteLimitExceeded)
                return end;

            if (minimumCandidateEnd == null) {
                Instant earliest = footprint.earliestPhenomenonTime();
                if (earliest == null)
                    throw new IllegalStateException("Oversized archive footprint did not identify its first row");
                // Do not let wall-clock bisection choose an empty prefix when
                // a sparse hour's first observation is in its latter half.
                // PostgreSQL timestamps have microsecond precision, but the
                // documented archive split floor is one whole UTC second.
                minimumCandidateEnd = earliest.truncatedTo(java.time.temporal.ChronoUnit.SECONDS)
                        .plus(MINIMUM_ARCHIVE_WINDOW);
                Instant absoluteMinimumEnd = start.plus(MINIMUM_ARCHIVE_WINDOW);
                if (minimumCandidateEnd.isBefore(absoluteMinimumEnd))
                    minimumCandidateEnd = absoluteMinimumEnd;
                if (minimumCandidateEnd.isAfter(proposedEnd))
                    minimumCandidateEnd = proposedEnd;
            }

            if (Duration.between(start, end).compareTo(MINIMUM_ARCHIVE_WINDOW) <= 0 ||
                    end.equals(minimumCandidateEnd)) {
                throw new IllegalStateException("Archive candidates in the minimum one-second window " +
                        "exceed configured generation limits: rows=" + footprint.rowCount() + "/" +
                        archiveConfig.maximumRowsPerGeneration + ", estimatedBytes=" +
                        footprint.estimatedUncompressedBytes() + "/" +
                        archiveConfig.effectiveMaximumUncompressedBytesPerGeneration());
            }
            Instant bisected = bisectArchiveWindowEnd(start, end);
            end = bisected.isBefore(minimumCandidateEnd) ? minimumCandidateEnd : bisected;
        }
    }

    /** Deterministic UTC window bisection, clamped to the documented one-second minimum. */
    public static Instant bisectArchiveWindowEnd(Instant start, Instant end) {
        if (start == null || end == null || !start.isBefore(end))
            throw new IllegalArgumentException("Archive candidate window must be non-empty");
        Duration width = Duration.between(start, end);
        if (width.compareTo(MINIMUM_ARCHIVE_WINDOW) <= 0)
            throw new IllegalArgumentException("The minimum one-second archive window cannot be bisected");
        Duration half = width.dividedBy(2);
        if (half.compareTo(MINIMUM_ARCHIVE_WINDOW) < 0)
            half = MINIMUM_ARCHIVE_WINDOW;
        return start.plus(half);
    }

    /** Counts at most cap+1 rows for compatibility with existing diagnostics. */
    public long countArchiveCandidates(Instant start, Instant end, long cap) {
        return measureArchiveCandidates(start, end, cap).rowCount();
    }

    public record ArchiveCandidateFootprint(long rowCount, long estimatedUncompressedBytes,
                                            Instant earliestPhenomenonTime) {
        public ArchiveCandidateFootprint {
            if (rowCount < 0 || estimatedUncompressedBytes < 0)
                throw new IllegalArgumentException("Archive candidate footprint must not be negative");
            if ((rowCount == 0) != (earliestPhenomenonTime == null))
                throw new IllegalArgumentException("Archive candidate time must be present exactly when rows are present");
        }
    }

    @Override
    public Stream<Entry<BigId, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields) {
        if (!archiveConfig.enabled) {
            ResolvedRelatedFilter related = resolveRelatedFilter(filter);
            if (!related.matches())
                return Stream.empty();
            ObsFilter unbounded = filter.getLimit() == Long.MAX_VALUE ? related.filter() :
                    ObsFilter.Builder.from(related.filter()).withLimit(Long.MAX_VALUE).build();
            boolean parentTableOnly = isAllMutableOnlyDataStreams(related.dataStreams());
            int initialHotPageSize = adaptiveInitialPageSize(filter.getLimit());
            int mutationBatchSize = adaptiveMutationBatchSize(filter.getLimit());
            Stream<Entry<BigId, IObsData>> hot = super.selectEntries(
                    unbounded, fields, parentTableOnly, initialHotPageSize);
            return limitAndClose(excludeTombstoned(
                    hot, entry -> entry.getKey().getIdAsLong(), mutationBatchSize), filter.getLimit());
        }

        return selectTieredEntries(filter, fields, archiveCatalog.hasObservationMutations());
    }

    private Stream<Entry<BigId, IObsData>> selectTieredEntries(
            ObsFilter filter, Set<ObsField> fields,
            boolean hasMutations) {
        ResolvedRelatedFilter related = resolveRelatedFilter(filter);
        if (!related.matches())
            return Stream.empty();
        Set<Long> dataStreams = related.dataStreams();
        Set<Long> fois = related.fois();
        ObsFilter resolvedFilter = related.filter();
        ArchiveCatalog.MutationCounts mutationCounts = hasMutations
                ? archiveCatalog.countObservationMutationBreakdown(dataStreams)
                : new ArchiveCatalog.MutationCounts(0L, 0L);
        long mutationCount = mutationCounts.total();
        boolean effectiveHasMutations = mutationCount > 0L;
        boolean hasTombstones = mutationCounts.tombstoneCount() > 0L;
        boolean parentTableOnly = isAllMutableOnlyDataStreams(dataStreams);
        boolean latest = hasLatestSelection(filter);
        boolean pushDownLatest = latest && !hasTombstones &&
                queryBuilder.supportsLatestResultPerDataStreamQuery(resolvedFilter);

        // The top N of a union can only contain rows from the top N of either
        // ordered tier. Both PostgreSQL and Parquet evaluate CQL predicates
        // before applying the tier limit, so CQL queries remain eligible for a
        // bounded merge in the common mutation-free case. When sparse mutations
        // exist, at most mutationCount rows can be suppressed by excludeMutated,
        // so saturateAdd(limit, mutationCount) preserves the bounded merge.
        boolean boundedMerge = !latest && filter.getLimit() != Long.MAX_VALUE &&
                filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null;
        // Do not push the caller's final LIMIT into latest-per-stream tiers.
        // The PostgreSQL and Parquet plans already reduce to one row per
        // explicitly selected stream, but they order those rows differently
        // from the final logical union. Applying (for example) LIMIT 1 in each
        // tier could discard a different stream that belongs in the union's
        // final top row. Leaving this small, stream-bounded candidate set
        // unbounded preserves the public ordering/limit contract.
        long tierLimit = boundedMerge
                ? saturateAdd(filter.getLimit(), mutationCount)
                : Long.MAX_VALUE;
        ObsFilter tierFilter = latest && !pushDownLatest ? withoutLatestSelection(resolvedFilter, tierLimit) :
                ObsFilter.Builder.from(resolvedFilter).withLimit(tierLimit).build();
        Comparator<Entry<BigId, IObsData>> comparator = entryComparator(filter);
        int initialHotPageSize = latest
                ? STREAM_FETCH_SIZE
                : adaptiveInitialPageSize(filter.getLimit());
        int mutationBatchSize = latest
                ? MutationFilteringIterator.LOOKUP_BATCH_SIZE
                : adaptiveMutationBatchSize(filter.getLimit());

        // Read complete internal rows even when the caller requested a narrow
        // projection. The union, latest-per-stream reduction, predicates, and
        // ordering all need datastream/time/result fields. Projection is
        // applied only after those semantics are complete.
        Stream<Entry<BigId, IObsData>> hot;
        Stream<Entry<BigId, IObsData>> cold;
        if (parentTableOnly) {
            hot = super.selectEntries(tierFilter, (Set<ObsField>) null,
                    true, initialHotPageSize);
            if (effectiveHasMutations)
                hot = excludeMutated(hot, entry -> entry.getKey().getIdAsLong(), mutationBatchSize);
            cold = Stream.empty();
        } else if (pushDownLatest && dataStreams != null && !dataStreams.isEmpty()) {
            List<Entry<BigId, IObsData>> hotEntries;
            try (Stream<Entry<BigId, IObsData>> hotStream =
                         super.selectEntries(tierFilter, (Set<ObsField>) null,
                                 false, initialHotPageSize)) {
                hotEntries = hotStream.toList();
            }
            hot = effectiveHasMutations
                    ? excludeMutated(hotEntries.stream(), entry -> entry.getKey().getIdAsLong(), mutationBatchSize)
                    : hotEntries.stream();
            cold = streamColdBasesForLatest(tierFilter, tierLimit,
                    dataStreams, fois, hotEntries, effectiveHasMutations);
        } else if (boundedMerge && filter.getLimit() <= STREAM_FETCH_SIZE) {
            List<Entry<BigId, IObsData>> hotEntries;
            try (Stream<Entry<BigId, IObsData>> hotStream =
                         super.selectEntries(tierFilter, (Set<ObsField>) null,
                                 false, initialHotPageSize)) {
                Stream<Entry<BigId, IObsData>> effectiveHot = effectiveHasMutations
                        ? excludeMutated(hotStream, entry -> entry.getKey().getIdAsLong(), mutationBatchSize)
                        : hotStream;
                hotEntries = effectiveHot.limit(filter.getLimit()).toList();
            }
            hot = hotEntries.stream();
            Instant hotBoundaryTime = (filter.getLimit() > 0 && hotEntries.size() >= filter.getLimit())
                    ? hotEntries.get((int) filter.getLimit() - 1).getValue().getPhenomenonTime()
                    : null;
            cold = streamColdBases(tierFilter, filter.getLimit(), tierLimit,
                    dataStreams, fois, effectiveHasMutations, mutationBatchSize, hotBoundaryTime);
        } else {
            hot = super.selectEntries(tierFilter, (Set<ObsField>) null,
                    false, initialHotPageSize);
            if (effectiveHasMutations)
                hot = excludeMutated(hot, entry -> entry.getKey().getIdAsLong(), mutationBatchSize);
            cold = streamColdBases(tierFilter, filter.getLimit(), tierLimit,
                    dataStreams, fois, effectiveHasMutations, mutationBatchSize, null);
        }
        boolean smallPlainRead = !latest && filter.getCQLFilter() == null &&
                filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null &&
                filter.getLimit() != Long.MAX_VALUE;
        int overlayInitialPageSize = smallPlainRead
                ? Math.min(1_000, adaptiveInitialPageSize(filter.getLimit()))
                : 1_000;
        Stream<Entry<BigId, IObsData>> overlays = parentTableOnly
                ? Stream.empty()
                : streamOverlays(tierFilter, dataStreams, fois, effectiveHasMutations, overlayInitialPageSize);

        // Each source is already ordered by phenomenon time and ID. Merge it
        // lazily so cold payloads are never accumulated in a JVM collection.
        // The left side wins equal keys, making the deliberate hot/cold grace
        // overlap deterministic without retaining a global ID set.
        Stream<Entry<BigId, IObsData>> merged = mergeSorted(hot, cold, comparator);
        merged = mergeSorted(merged, overlays, comparator);
        if (latest)
            merged = applyLatestAndOrder(merged, filter);
        Stream<Entry<BigId, IObsData>> limited = limitAndClose(merged, filter.getLimit());
        return fields == null || fields.isEmpty() ? limited :
                limited.map(entry -> Map.<BigId, IObsData>entry(
                        entry.getKey(), project(entry.getValue(), fields)));
    }

    private static boolean hasLatestSelection(ObsFilter filter) {
        return (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
    }

    /** Latest selection is evaluated only after the logical hot/cold union. */
    private static ObsFilter withoutLatestSelection(ObsFilter filter, long limit) {
        ObsFilter.Builder builder = ObsFilter.Builder.from(filter).withLimit(limit);
        if (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) {
            builder.withPhenomenonTime(new TemporalFilter.Builder().withAllTimes()
                    .descendingOrder(filter.getPhenomenonTime().descendingOrder()).build());
        }
        if (filter.getResultTime() != null && filter.getResultTime().isLatestTime()) {
            builder.withResultTime(new TemporalFilter.Builder().withAllTimes()
                    .descendingOrder(filter.getResultTime().descendingOrder()).build());
        }
        return builder.build();
    }

    private java.util.Optional<IObsData> readCold(ObsFilter filter) {
        Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
        Set<Long> fois = resolveFois(filter.getFoiFilter());
        boolean hasMutations = archiveCatalog.hasObservationMutations();
        Comparator<Entry<BigId, IObsData>> comparator = entryComparator(filter);
        try (Stream<Entry<BigId, IObsData>> bases = streamColdBases(filter, Long.MAX_VALUE,
                dataStreams, fois, hasMutations, 32);
             Stream<Entry<BigId, IObsData>> overlays = streamOverlays(filter,
                     dataStreams, fois, hasMutations);
             Stream<Entry<BigId, IObsData>> merged = mergeSorted(bases, overlays, comparator)) {
            return merged.map(Entry::getValue).findFirst();
        }
    }

    private Stream<Entry<BigId, IObsData>> streamColdBasesForLatest(
            ObsFilter filter, long readerLimit, Set<Long> dataStreams, Set<Long> fois,
            List<Entry<BigId, IObsData>> hotEntries, boolean hasMutations) {
        ArchiveBounds bounds = archiveBounds(filter);
        if (!bounds.start().isBefore(bounds.end()))
            return Stream.empty();

        boolean latestResult = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        Instant minResultTime = minResultTimeBound(filter);
        Map<Long, Instant> hotTimeByStream = new HashMap<>();
        for (Entry<BigId, IObsData> entry : hotEntries) {
            long streamId = entry.getValue().getDataStreamID().getIdAsLong();
            if (!dataStreams.contains(streamId))
                continue;
            Instant time = latestResult
                    ? entry.getValue().getResultTime()
                    : entry.getValue().getPhenomenonTime();
            if (time != null)
                hotTimeByStream.merge(streamId, time,
                        (existing, candidate) -> candidate.isAfter(existing) ? candidate : existing);
        }

        Set<Long> hotStreams = hotTimeByStream.keySet();
        Set<Long> missingHotStreams = dataStreams.stream()
                .filter(id -> !hotTimeByStream.containsKey(id))
                .collect(Collectors.toSet());

        Map<UUID, ArchiveGeneration> candidateById = new HashMap<>();
        Set<Long> coldDataStreams = new HashSet<>();
        Set<UUID> neededGenerationIds = new HashSet<>();

        if (!hotStreams.isEmpty()) {
            Instant minHotTime = hotTimeByStream.values().stream()
                    .min(Comparator.naturalOrder())
                    .orElse(null);
            List<ArchiveGeneration> hotCandidates;
            Set<Long> archivedHotStreams = hotStreams;
            if (latestResult && minHotTime != null) {
                hotCandidates = archiveCatalog.candidatesPotentiallyNewerThanResultTime(
                        bounds.start(), bounds.end(), hotStreams, minHotTime);
                if (!hotCandidates.isEmpty() && hotStreams.size() > 1) {
                    archivedHotStreams = archiveCatalog.filterPresentDataStreams(
                            bounds.start(), bounds.end(), hotStreams);
                }
            } else if (!latestResult && minHotTime != null) {
                archivedHotStreams = archiveCatalog.filterPresentDataStreams(
                        bounds.start(), bounds.end(), hotStreams);
                Instant effectiveStart = minHotTime.isAfter(bounds.start()) ? minHotTime : bounds.start();
                hotCandidates = !archivedHotStreams.isEmpty() && effectiveStart.isBefore(bounds.end())
                        ? archiveCatalog.candidates(
                                effectiveStart, bounds.end(), null, null, null, archivedHotStreams, minResultTime)
                        : List.of();
            } else {
                archivedHotStreams = archiveCatalog.filterPresentDataStreams(
                        bounds.start(), bounds.end(), hotStreams);
                hotCandidates = !archivedHotStreams.isEmpty()
                        ? archiveCatalog.candidates(
                                bounds.start(), bounds.end(), null, null, null, archivedHotStreams, minResultTime)
                        : List.of();
            }
            for (ArchiveGeneration generation : hotCandidates)
                candidateById.put(generation.id(), generation);

            boolean inspectedResultTimes = false;
            if (!hotCandidates.isEmpty()) {
                for (Long streamId : archivedHotStreams) {
                    Instant hotTime = hotTimeByStream.get(streamId);
                    if (latestResult) {
                        inspectedResultTimes = true;
                        List<ArchiveGeneration> newer = archiveReader.generationsPotentiallyNewerThanResultTime(
                                hotCandidates, hotTime);
                        if (!newer.isEmpty()) {
                            coldDataStreams.add(streamId);
                            for (ArchiveGeneration generation : newer)
                                neededGenerationIds.add(generation.id());
                        }
                    } else {
                        boolean anyNewer = false;
                        for (ArchiveGeneration generation : hotCandidates) {
                            if (generation.end().isAfter(hotTime)) {
                                anyNewer = true;
                                neededGenerationIds.add(generation.id());
                            }
                        }
                        if (anyNewer)
                            coldDataStreams.add(streamId);
                    }
                }
            }
            if (inspectedResultTimes) {
                for (ArchiveGeneration generation : hotCandidates) {
                    if (generation.maxResultTime() == null) {
                        archiveReader.cachedMaxResultTime(generation).ifPresent(
                                discovered -> archiveCatalog.backfillMaxResultTime(generation.id(), discovered));
                    }
                }
            }
        }

        List<Entry<BigId, IObsData>> cachedColdEntries = new ArrayList<>();
        if (!missingHotStreams.isEmpty()) {
            Set<Long> unresolvedMissingStreams = new HashSet<>(missingHotStreams);
            if (fois == null && minResultTime == null &&
                    bounds.start().equals(MIN_ARCHIVE_TIME) &&
                    bounds.end().equals(MAX_ARCHIVE_TIME)) {
                Map<Long, ArchiveCatalog.ArchivedOverlay> indexedLatest =
                        archiveCatalog.loadVerifiedColdLatestByDataStream(idScope, missingHotStreams);
                for (Map.Entry<Long, ArchiveCatalog.ArchivedOverlay> entry : indexedLatest.entrySet()) {
                    ArchiveCatalog.ArchivedOverlay latestCold = entry.getValue();
                    ArchivedObservation obs = latestCold.observation();
                    IObsData decoded = toObservation(latestCold.generationId(), latestCold.schemaJson(), obs);
                    if (matchesObservation(filter, missingHotStreams, fois, obs.id(), obs.resultJson(), decoded)) {
                        cachedColdEntries.add(Map.entry(BigId.fromLong(idScope, obs.id()), decoded));
                        unresolvedMissingStreams.remove(entry.getKey());
                    }
                }
            }
            if (!unresolvedMissingStreams.isEmpty()) {
                Set<Long> archivedUnresolvedStreams = archiveCatalog.filterPresentDataStreams(
                        bounds.start(), bounds.end(), unresolvedMissingStreams);
                if (!archivedUnresolvedStreams.isEmpty()) {
                    List<ArchiveGeneration> missingCandidates = archiveCatalog.candidates(
                            bounds.start(), bounds.end(), null, null, null, archivedUnresolvedStreams, minResultTime);
                    if (!missingCandidates.isEmpty()) {
                        coldDataStreams.addAll(archivedUnresolvedStreams);
                        for (ArchiveGeneration generation : missingCandidates) {
                            candidateById.put(generation.id(), generation);
                            neededGenerationIds.add(generation.id());
                        }
                    }
                }
            }
        }

        Comparator<Entry<BigId, IObsData>> comparator = entryComparator(filter);
        if (!cachedColdEntries.isEmpty())
            cachedColdEntries.sort(comparator);

        if (coldDataStreams.isEmpty() || neededGenerationIds.isEmpty())
            return cachedColdEntries.stream();

        List<ArchiveGeneration> filteredCandidates = candidateById.values().stream()
                .filter(generation -> neededGenerationIds.contains(generation.id()))
                .sorted(Comparator.comparing(ArchiveGeneration::start)
                        .thenComparing(ArchiveGeneration::end)
                        .thenComparing(ArchiveGeneration::id))
                .toList();
        Stream<Entry<BigId, IObsData>> scannedCold = streamColdBasesFromCandidates(
                filteredCandidates, filter, readerLimit, coldDataStreams, fois, hasMutations,
                MutationFilteringIterator.LOOKUP_BATCH_SIZE);
        return cachedColdEntries.isEmpty()
                ? scannedCold
                : mergeSorted(cachedColdEntries.stream(), scannedCold, comparator);
    }

    private static long saturateAdd(long a, long b) {
        if (a == Long.MAX_VALUE || b <= 0L)
            return a;
        long r = a + b;
        return r < 0L ? Long.MAX_VALUE : r;
    }

    private Stream<Entry<BigId, IObsData>> streamColdBases(
            ObsFilter filter, long readerLimit, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations, int mutationBatchSize) {
        return streamColdBases(filter, readerLimit, readerLimit, dataStreams, fois, hasMutations, mutationBatchSize, null);
    }

    private Stream<Entry<BigId, IObsData>> streamColdBases(
            ObsFilter filter, long callerLimit, long readerLimit, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations, int mutationBatchSize, Instant hotBoundaryTime) {
        ArchiveBounds bounds = archiveBounds(filter, hotBoundaryTime);
        if (!bounds.start().isBefore(bounds.end()))
            return Stream.empty();
        Instant minResultTime = minResultTimeBound(filter);
        Instant maxResultTime = maxResultTimeBound(filter);
        Long oneId = filter.getInternalIDs() != null && filter.getInternalIDs().size() == 1
                ? filter.getInternalIDs().first().getIdAsLong() : null;
        Long minId = filter.getInternalIDs() != null && filter.getInternalIDs().size() > 1
                ? filter.getInternalIDs().first().getIdAsLong() : null;
        Long maxId = filter.getInternalIDs() != null && filter.getInternalIDs().size() > 1
                ? filter.getInternalIDs().last().getIdAsLong() : null;

        if (oneId == null && minId == null && !hasLatestSelection(filter)) {
            boolean descending = filter.getPhenomenonTime() != null &&
                    filter.getPhenomenonTime().descendingOrder();
            JsonCqlEvaluator.SummaryCounterSpec summarySpec =
                    fois == null && filter.getCQLFilter() != null
                            ? JsonCqlEvaluator.summaryCounterSpec(filter.getCQLFilter()).orElse(null)
                            : null;
            String exactEventId = fois == null && filter.getCQLFilter() != null
                    ? JsonCqlEvaluator.extractExactEventIdFilter(filter.getCQLFilter()).orElse(null)
                    : null;
            if (summarySpec != null || exactEventId != null) {
                List<ArchiveGeneration> allCandidates = pruneAndBackfillLegacyResultTimeCandidates(
                        archiveCatalog.candidates(
                                bounds.start(), bounds.end(), null, null, null, dataStreams,
                                minResultTime, maxResultTime),
                        minResultTime);
                if (allCandidates.isEmpty())
                    return Stream.empty();
                ArchiveCatalog.CandidateSelectionPlan plan =
                        archiveCatalog.selectSummaryMatchedCandidates(
                                allCandidates, filter.getPhenomenonTime(), filter.getResultTime(),
                                dataStreams, descending, callerLimit, readerLimit, hasMutations,
                                summarySpec, exactEventId,
                                summarySpec != null && hasMutations
                                        ? (chunk, mutatedByGen) -> countMutatedSummaryBaseMatchesByGeneration(
                                                chunk, mutatedByGen, filter, dataStreams, fois)
                                        : null);
                if (plan.primaryCandidates().isEmpty())
                    return Stream.empty();
                ObsFilter coldStreamFilter = callerLimit == readerLimit
                        ? filter
                        : ObsFilter.Builder.from(filter).withLimit(callerLimit).build();
                if (plan.overflowCandidates().isEmpty()) {
                    return streamColdBasesFromPreprunedCandidates(
                            plan.primaryCandidates(), coldStreamFilter, readerLimit,
                            dataStreams, fois, hasMutations, mutationBatchSize);
                }
                java.util.concurrent.atomic.AtomicLong emitted = new java.util.concurrent.atomic.AtomicLong();
                Supplier<Stream<Entry<BigId, IObsData>>> primarySupplier = () ->
                        streamColdBasesFromPreprunedCandidates(
                                plan.primaryCandidates(), coldStreamFilter, readerLimit,
                                dataStreams, fois, hasMutations, mutationBatchSize)
                                .peek(ignored -> emitted.incrementAndGet());
                Supplier<Stream<Entry<BigId, IObsData>>> overflowSupplier = () -> {
                    long remainingLimit = readerLimit == Long.MAX_VALUE
                            ? Long.MAX_VALUE
                            : readerLimit - emitted.get();
                    if (readerLimit != Long.MAX_VALUE && remainingLimit <= 0)
                        return Stream.empty();
                    return streamColdBasesFromPreprunedCandidates(
                            plan.overflowCandidates(), coldStreamFilter, remainingLimit,
                            dataStreams, fois, hasMutations, mutationBatchSize);
                };
                return Stream.of(primarySupplier, overflowSupplier).flatMap(Supplier::get);
            }

            int initialClusterLimit = readerLimit <= 64
                    ? 16
                    : INITIAL_CANDIDATE_CLUSTER_BATCH;
            ArchiveCatalog.InitialCandidateBatch initialBatch =
                    archiveCatalog.candidatesInitialClusters(
                            bounds.start(), bounds.end(), dataStreams, minResultTime, maxResultTime,
                            descending, initialClusterLimit);
            if (initialBatch.complete()) {
                return streamColdBasesFromCandidates(
                        initialBatch.generations(), filter, readerLimit,
                        dataStreams, fois, hasMutations, mutationBatchSize);
            }
            Set<UUID> initialIds = initialBatch.generations().stream()
                    .map(ArchiveGeneration::id)
                    .collect(Collectors.toSet());
            java.util.concurrent.atomic.AtomicLong emitted = new java.util.concurrent.atomic.AtomicLong();
            Supplier<Stream<Entry<BigId, IObsData>>> initialSupplier = () ->
                    streamColdBasesFromCandidates(
                            initialBatch.generations(), filter, readerLimit,
                            dataStreams, fois, hasMutations, mutationBatchSize)
                            .peek(ignored -> emitted.incrementAndGet());
            Supplier<Stream<Entry<BigId, IObsData>>> remainderSupplier = () -> {
                long remainingLimit = readerLimit == Long.MAX_VALUE
                        ? Long.MAX_VALUE
                        : readerLimit - emitted.get();
                if (readerLimit != Long.MAX_VALUE && remainingLimit <= 0)
                    return Stream.empty();
                List<ArchiveGeneration> remaining = archiveCatalog.candidates(
                                bounds.start(), bounds.end(), null, null, null, dataStreams,
                                minResultTime, maxResultTime)
                        .stream()
                        .filter(generation -> !initialIds.contains(generation.id()))
                        .toList();
                return streamColdBasesFromCandidates(
                        remaining, filter, remainingLimit,
                        dataStreams, fois, hasMutations, mutationBatchSize);
            };
            return Stream.of(initialSupplier, remainderSupplier).flatMap(Supplier::get);
        }

        List<ArchiveGeneration> candidates = archiveCatalog.candidates(
                bounds.start(), bounds.end(), oneId, minId, maxId, dataStreams,
                minResultTime, maxResultTime);
        return streamColdBasesFromCandidates(
                candidates, filter, readerLimit, dataStreams, fois, hasMutations, mutationBatchSize);
    }

    private Stream<Entry<BigId, IObsData>> streamColdBasesFromCandidates(
            List<ArchiveGeneration> candidates, ObsFilter filter, long readerLimit,
            Set<Long> dataStreams, Set<Long> fois, boolean hasMutations, int mutationBatchSize) {
        List<ArchiveGeneration> effectiveCandidates = pruneAndBackfillLegacyResultTimeCandidates(
                candidates, minResultTimeBound(filter));
        if (!effectiveCandidates.isEmpty() && fois == null &&
                (filter.getInternalIDs() == null || filter.getInternalIDs().isEmpty()) &&
                !hasLatestSelection(filter) && filter.getCQLFilter() != null) {
            String summaryExpr = JsonCqlEvaluator.summaryCounterExpression(filter.getCQLFilter()).orElse(null);
            String exactEventId = JsonCqlEvaluator.extractExactEventIdFilter(filter.getCQLFilter()).orElse(null);
            if (summaryExpr != null || exactEventId != null) {
                effectiveCandidates = archiveCatalog.pruneZeroMatchSummaryCandidates(
                        effectiveCandidates, dataStreams, summaryExpr, exactEventId);
            }
        }
        return streamColdBasesFromPreprunedCandidates(
                effectiveCandidates, filter, readerLimit, dataStreams, fois, hasMutations, mutationBatchSize);
    }

    private Stream<Entry<BigId, IObsData>> streamColdBasesFromPreprunedCandidates(
            List<ArchiveGeneration> effectiveCandidates, ObsFilter filter, long readerLimit,
            Set<Long> dataStreams, Set<Long> fois, boolean hasMutations, int mutationBatchSize) {
        if (effectiveCandidates.isEmpty())
            return Stream.empty();
        Stream<ParquetArchiveReader.ArchivedRow> rows = archiveReader.stream(
                effectiveCandidates, filter, dataStreams, fois, readerLimit);
        if (hasMutations)
            rows = excludeMutated(rows, row -> row.observation().id(), mutationBatchSize);
        return rows
                .map(row -> {
                    long id = row.observation().id();
                    IObsData observation = toObservation(row.generation().id(), null, row.observation());
                    return Map.entry(BigId.fromLong(idScope, id), observation);
                })
                .filter(entry -> filter.test(entry.getValue()));
    }

    private List<ArchiveGeneration> pruneAndBackfillLegacyResultTimeCandidates(
            List<ArchiveGeneration> candidates, Instant minResultTime) {
        if (minResultTime == null || candidates.isEmpty())
            return candidates;
        boolean hasNullMaxResultTime = false;
        for (ArchiveGeneration generation : candidates) {
            if (generation.maxResultTime() == null) {
                hasNullMaxResultTime = true;
                break;
            }
        }
        if (!hasNullMaxResultTime)
            return candidates;
        List<ArchiveGeneration> filtered = archiveReader.generationsPotentiallyNewerThanResultTime(
                candidates, minResultTime);
        for (ArchiveGeneration generation : candidates) {
            if (generation.maxResultTime() == null) {
                archiveReader.cachedMaxResultTime(generation).ifPresent(
                        discovered -> archiveCatalog.backfillMaxResultTime(generation.id(), discovered));
            }
        }
        return filtered;
    }

    private Stream<Entry<BigId, IObsData>> streamOverlays(
            ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations) {
        return streamOverlays(filter, dataStreams, fois, hasMutations, 1_000);
    }

    private Stream<Entry<BigId, IObsData>> streamOverlays(
            ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
            boolean hasMutations, int initialPageSize) {
        if (!hasMutations)
            return Stream.empty();
        org.geotools.api.filter.Filter cqlFilter = filter.getCQLFilter();
        boolean canPrecheckCql = cqlFilter != null &&
                JsonCqlEvaluator.canonicalCacheKey(cqlFilter).isPresent();
        java.util.function.Predicate<String> cqlMatcher = canPrecheckCql
                ? JsonCqlEvaluator.compileMatcher(cqlFilter)
                : null;
        return archiveCatalog.streamOverlays(idScope, filter, dataStreams, fois, initialPageSize)
                .flatMap(mutation -> {
                    ArchivedObservation overlay = mutation.observation();
                    if (cqlMatcher != null && !cqlMatcher.test(overlay.resultJson()))
                        return Stream.empty();
                    long id = overlay.id();
                    IObsData observation = toObservation(
                            mutation.generationId(), mutation.schemaJson(), overlay);
                    return matchesObservationWithoutCqlIfPrechecked(
                            filter, dataStreams, fois, id, overlay.resultJson(), observation, cqlMatcher != null)
                            ? Stream.of(Map.entry(BigId.fromLong(idScope, id), observation))
                            : Stream.empty();
                });
    }

    private Set<Long> resolveDataStreams(DataStreamFilter filter) {
        if (filter == null)
            return null;
        // DataStreamFilter's contract makes internal IDs an exact selector:
        // no other filter option is allowed alongside them. Resolve that form
        // without borrowing the datastream pool. This is important when a
        // datastream deletion deliberately keeps its metadata transaction and
        // retention-hold barrier open while removing its observations; a
        // second borrow from the same one-connection pool would deadlock.
        if (filter.getInternalIDs() != null) {
            return filter.getInternalIDs().stream()
                    .filter(id -> id.getScope() == idScope)
                    .map(BigId::getIdAsLong)
                    .collect(Collectors.toCollection(HashSet::new));
        }
        try (Stream<DataStreamKey> keys = dataStreamStore.selectKeys(filter)) {
            return keys.map(key -> key.getInternalID().getIdAsLong()).collect(Collectors.toCollection(HashSet::new));
        }
    }

    private Set<Long> resolveFois(FoiFilter filter) {
        if (filter == null)
            return null;
        // Exact FOI IDs are already fully resolved and do not require a
        // linked metadata store. Preserve the conventional zero representation
        // of BigId.NONE used for observations with a SQL NULL FOI.
        if (hasOnlyInternalFoiIds(filter)) {
            return filter.getInternalIDs().stream()
                    .filter(id -> BigId.NONE.equals(id) || id.getScope() == idScope)
                    .map(id -> BigId.NONE.equals(id) ? 0L : id.getIdAsLong())
                    .collect(Collectors.toCollection(HashSet::new));
        }
        if (foiStore == null)
            throw new IllegalStateException("Cannot evaluate FOI filter before an FOI store is linked");
        Set<Long> ids;
        try (Stream<? extends BigId> keys = foiStore.selectKeys(filter).map(key -> key.getInternalID())) {
            ids = keys.map(BigId::getIdAsLong).collect(Collectors.toCollection(HashSet::new));
        }
        if (filter.getInternalIDs() != null && filter.getInternalIDs().contains(BigId.NONE))
            ids.add(0L); // reader interprets zero as SQL NULL as well
        return ids;
    }

    private static boolean hasOnlyInternalFoiIds(FoiFilter filter) {
        return filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty() &&
                filter.getUniqueIDs() == null && filter.getValidTime() == null &&
                filter.getLocationFilter() == null && filter.getFullTextFilter() == null &&
                filter.getValuePredicate() == null && !filter.includeMembers() &&
                filter.getParentFilter() == null && filter.getObservationFilter() == null &&
                filter.getSampledFeatureFilter() == null;
    }

    /**
     * Resolves relational selectors once and replaces them with exact scoped
     * IDs before querying observations. Besides avoiding repeated metadata
     * joins, this preserves the semantics of current/latest version selectors
     * and prevents multiple temporal versions of a related row from
     * multiplying an observation count.
     */
    private ResolvedRelatedFilter resolveRelatedFilter(ObsFilter filter) {
        Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
        if (dataStreams == null && hasLatestSelection(filter) && dataStreamStore != null) {
            Set<Long> allStreams = resolveDataStreams(new DataStreamFilter.Builder().build());
            if (allStreams != null && !allStreams.isEmpty())
                dataStreams = allStreams;
        }
        Set<Long> fois = resolveFois(filter.getFoiFilter());
        if ((dataStreams != null && dataStreams.isEmpty()) ||
                (fois != null && fois.isEmpty())) {
            return new ResolvedRelatedFilter(filter, dataStreams, fois, false);
        }

        ObsFilter.Builder builder = ObsFilter.Builder.from(filter);
        if (dataStreams != null) {
            builder.withDataStreams(dataStreams.stream()
                    .sorted()
                    .map(id -> BigId.fromLong(idScope, id))
                    .toList());
        }
        if (fois != null) {
            builder.withFois(fois.stream()
                    .sorted()
                    .map(id -> id == 0L ? BigId.NONE : BigId.fromLong(idScope, id))
                    .toList());
        }
        return new ResolvedRelatedFilter(builder.build(), dataStreams, fois, true);
    }

    private ArchiveBounds archiveBounds(ObsFilter filter) {
        return archiveBounds(filter, null);
    }

    private ArchiveBounds archiveBounds(ObsFilter filter, Instant hotBoundaryTime) {
        Instant start = MIN_ARCHIVE_TIME;
        Instant end = MAX_ARCHIVE_TIME;
        if (filter.getPhenomenonTime() != null && !filter.getPhenomenonTime().isLatestTime()) {
            Instant minimum = filter.getPhenomenonTime().getMin();
            Instant maximum = filter.getPhenomenonTime().getMax();
            if (minimum != null && !Instant.MIN.equals(minimum))
                start = minimum;
            if (maximum != null && !Instant.MAX.equals(maximum))
                end = maximum.plusNanos(1);
        }
        if (filter.getCQLFilter() != null) {
            java.util.Optional<JsonCqlEvaluator.CqlTimeBounds> cqlBounds =
                    JsonCqlEvaluator.extractCqlPhenomenonTimeBounds(filter.getCQLFilter());
            if (cqlBounds.isPresent()) {
                Instant cqlMin = cqlBounds.get().minPhenomenonTime();
                Instant cqlMax = cqlBounds.get().maxPhenomenonTime();
                if (cqlMin != null && cqlMin.isAfter(start))
                    start = cqlMin;
                if (cqlMax != null) {
                    Instant cqlEnd = cqlMax.isBefore(MAX_ARCHIVE_TIME.minusSeconds(1))
                            ? cqlMax.plusNanos(1) : MAX_ARCHIVE_TIME;
                    if (cqlEnd.isBefore(end))
                        end = cqlEnd;
                }
            }
        }
        if (hotBoundaryTime != null) {
            boolean descending = filter.getPhenomenonTime() != null &&
                    filter.getPhenomenonTime().descendingOrder();
            if (descending) {
                if (hotBoundaryTime.isAfter(start))
                    start = hotBoundaryTime;
            } else {
                Instant boundaryEnd = hotBoundaryTime.isBefore(MAX_ARCHIVE_TIME.minusSeconds(1))
                        ? hotBoundaryTime.plusNanos(1) : MAX_ARCHIVE_TIME;
                if (boundaryEnd.isBefore(end))
                    end = boundaryEnd;
            }
        }
        return new ArchiveBounds(start, end);
    }

    static Instant minResultTimeBound(ObsFilter filter) {
        if (filter == null || filter.getResultTime() == null || filter.getResultTime().isLatestTime())
            return null;
        Instant minimum = filter.getResultTime().getMin();
        if (minimum == null || Instant.MIN.equals(minimum))
            return null;
        return minimum;
    }

    static Instant maxResultTimeBound(ObsFilter filter) {
        if (filter == null || filter.getResultTime() == null || filter.getResultTime().isLatestTime())
            return null;
        if (filter.getResultTime().isCurrentTime())
            return Instant.now();
        Instant maximum = filter.getResultTime().getMax();
        if (maximum == null || Instant.MAX.equals(maximum))
            return null;
        return maximum;
    }

    private boolean matchesObservation(ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
                                       long id, String resultJson, IObsData observation) {
        return matchesObservationWithoutCqlIfPrechecked(
                filter, dataStreams, fois, id, resultJson, observation, false);
    }

    private boolean matchesObservationWithoutCqlIfPrechecked(
            ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
            long id, String resultJson, IObsData observation, boolean cqlAlreadyVerified) {
        if (filter.getInternalIDs() != null &&
                !filter.getInternalIDs().contains(BigId.fromLong(idScope, id)))
            return false;
        if (dataStreams != null && !dataStreams.contains(observation.getDataStreamID().getIdAsLong()))
            return false;
        if (fois != null) {
            long foi = observation.hasFoi() ? observation.getFoiID().getIdAsLong() : 0L;
            if (!fois.contains(foi))
                return false;
        }
        if (!filter.test(observation))
            return false;
        if (!cqlAlreadyVerified && !JsonCqlEvaluator.matches(filter.getCQLFilter(), resultJson))
            return false;
        return true;
    }

    private boolean matchesLockedObservation(ObsFilter filter, Set<Long> dataStreams,
                                             Set<Long> fois, long id,
                                             LockedLogicalObservation locked) {
        if (!matchesTemporal(filter.getPhenomenonTime(), locked.phenomenonTime()) ||
                !matchesTemporal(filter.getResultTime(), locked.resultTime()))
            return false;
        if (filter.getInternalIDs() != null &&
                !filter.getInternalIDs().contains(BigId.fromLong(idScope, id)))
            return false;
        if (dataStreams != null && !dataStreams.contains(locked.dataStreamId()))
            return false;
        long foi = locked.foiId() == null ? 0L : locked.foiId();
        if (fois != null && !fois.contains(foi))
            return false;
        if (!JsonCqlEvaluator.matches(filter.getCQLFilter(), locked.resultJson()))
            return false;
        if (filter.getPhenomenonLocation() != null || filter.getValuePredicate() != null) {
            if (locked.observation() == null)
                throw new IllegalStateException(
                        "A decoded observation is required for Java-side predicate revalidation");
            if (!filter.testPhenomenonLocation(locked.observation()) ||
                    !filter.testValuePredicate(locked.observation()))
                return false;
        }
        return true;
    }

    private static boolean matchesTemporal(TemporalFilter filter, Instant value) {
        if (filter == null || filter.isLatestTime())
            return true;
        return value != null && filter.getRange().contains(value);
    }

    private static IObsData project(IObsData observation, Set<ObsField> fields) {
        ObsData.Builder builder = new ObsData.Builder();
        if (fields.contains(ObsField.DATASTREAM_ID))
            builder.withDataStream(observation.getDataStreamID());
        if (fields.contains(ObsField.FOI_ID) && observation.hasFoi())
            builder.withFoi(observation.getFoiID());
        // The base PostgreSQL store always includes phenomenon time as the
        // stable result-order field, even for a narrow projection.
        if (observation.getPhenomenonTime() != null)
            builder.withPhenomenonTime(observation.getPhenomenonTime());
        if (fields.contains(ObsField.RESULT_TIME) && observation.getResultTime() != null)
            builder.withResultTime(observation.getResultTime());
        builder.withResult(fields.contains(ObsField.RESULT)
                ? observation.getResult() : new DataBlockByte());
        return builder.build();
    }

    private IObsData toObservation(UUID generationId, String overlaySchemaJson,
                                   ArchivedObservation archived) {
        return toObservation(null, generationId, overlaySchemaJson, archived);
    }

    private IObsData toObservation(Connection transaction, UUID generationId,
                                   String overlaySchemaJson, ArchivedObservation archived) {
        BigId dataStreamId = BigId.fromLong(archived.dataStreamScope(), archived.dataStreamId());
        boolean overlay = "OVERLAY".equals(archived.profile());
        IDataStreamInfo schema = null;
        if (overlay && overlaySchemaJson != null) {
            String schemaHash = ArchiveCatalog.computeSchemaSha256(overlaySchemaJson);
            schema = decodedOverlaySchemas.getIfPresent(schemaHash);
            if (schema == null) {
                try {
                    schema = SerializerUtils.readIDataStreamInfoFromJson(overlaySchemaJson);
                    decodedOverlaySchemas.put(schemaHash, schema);
                } catch (IOException e) {
                    throw new IllegalStateException("Cannot decode archived overlay schema", e);
                }
            }
        }
        if (schema == null && generationId != null) {
            String cacheKey = generationId + ":" + archived.dataStreamId();
            schema = archivedSchemas.get(cacheKey);
            if (schema == null) {
                try {
                    java.util.Optional<String> serialized = transaction == null
                            ? archiveCatalog.schemaJson(generationId, archived.dataStreamId())
                            : archiveCatalog.schemaJson(transaction, generationId, archived.dataStreamId());
                    if (serialized.isPresent()) {
                        IDataStreamInfo decoded = SerializerUtils.readIDataStreamInfoFromJson(serialized.get());
                        IDataStreamInfo existing = archivedSchemas.putIfAbsent(cacheKey, decoded);
                        schema = existing == null ? decoded : existing;
                    }
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot decode archived datastream schema", e);
                }
            }
        }
        if (schema == null)
            throw new IllegalStateException((overlay ? "No live or archived schema for overlaid datastream " :
                    "No generation schema for archived datastream ") + archived.dataStreamId());
        DataBlock result = SerializerUtils.readDataBlockFromJson(schema.getRecordStructure(), archived.resultJson());
        ObsData.Builder builder = new ObsData.Builder().withDataStream(dataStreamId).withResult(result);
        if (archived.foiId() != null)
            builder.withFoi(BigId.fromLong(archived.foiScope(), archived.foiId()));
        if (archived.phenomenonTime() != null)
            builder.withPhenomenonTime(archived.phenomenonTime());
        if (archived.resultTime() != null)
            builder.withResultTime(archived.resultTime());
        return builder.build();
    }

    private static Comparator<Entry<BigId, IObsData>> entryComparator(ObsFilter filter) {
        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();
        Comparator<Instant> timeOrder = descending
                ? Comparator.nullsFirst(Comparator.reverseOrder())
                : Comparator.nullsFirst(Comparator.naturalOrder());
        return Comparator.<Entry<BigId, IObsData>, Instant>comparing(
                        entry -> entry.getValue().getPhenomenonTime(), timeOrder)
                .thenComparingLong(entry -> entry.getKey().getIdAsLong());
    }

    private Stream<Entry<BigId, IObsData>> applyLatestAndOrder(
            Stream<Entry<BigId, IObsData>> entries, ObsFilter filter) {
        boolean latestPhenomenon = filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime();
        boolean latestResult = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        if (!latestPhenomenon && !latestResult)
            return entries;

        // Latest selection is intentionally deferred until after the hot/cold
        // union and sparse mutation layer. State is bounded by the number of
        // datastreams, rather than the number of archived observations.
        Map<BigId, Entry<BigId, IObsData>> latest;
        try (entries) {
            latest = entries.collect(Collectors.toMap(
                    entry -> entry.getValue().getDataStreamID(), Function.identity(), (left, right) -> {
                        Instant leftTime = latestResult ? left.getValue().getResultTime() :
                                left.getValue().getPhenomenonTime();
                        Instant rightTime = latestResult ? right.getValue().getResultTime() :
                                right.getValue().getPhenomenonTime();
                        if (leftTime == null && rightTime == null)
                            return right.getKey().getIdAsLong() > left.getKey().getIdAsLong() ? right : left;
                        if (leftTime == null) return right;
                        if (rightTime == null) return left;
                        int compared = rightTime.compareTo(leftTime);
                        return compared > 0 || (compared == 0 &&
                                right.getKey().getIdAsLong() > left.getKey().getIdAsLong()) ? right : left;
                    }, HashMap::new));
        }
        return latest.values().stream().sorted(entryComparator(filter));
    }

    /**
     * Merges two ordered streams without materializing either side. Equal
     * entries are emitted once and the preferred (left) tier wins. Both
     * streams are closed on exhaustion, errors, and explicit close.
     */
    private static <T> Stream<T> mergeSorted(
            Stream<T> preferred, Stream<T> other, Comparator<? super T> comparator) {
        MergingIterator<T> iterator = new MergingIterator<>(preferred, other, comparator);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    private boolean isAllMutableOnlyDataStreams(Set<Long> dataStreams) {
        if (immutableStage == null || dataStreams == null || dataStreams.isEmpty())
            return false;
        for (Long dsId : dataStreams) {
            Boolean immutable = immutableDataStreamCache.get(dsId);
            if (immutable == null) {
                IDataStreamInfo info = getDataStreamInfo(BigId.fromLong(idScope, dsId));
                if (info == null)
                    return false;
                immutable = archiveConfig.outputNames.contains(info.getOutputName());
                immutableDataStreamCache.putIfAbsent(dsId, immutable);
            }
            if (immutable)
                return false;
        }
        return true;
    }

    private static int adaptiveInitialPageSize(long callerLimit) {
        if (callerLimit == Long.MAX_VALUE)
            return STREAM_FETCH_SIZE;
        if (callerLimit <= 0)
            return 1;
        long padded = callerLimit > STREAM_FETCH_SIZE - 16L
                ? STREAM_FETCH_SIZE
                : Math.max(32L, callerLimit + 16L);
        return (int) Math.min(STREAM_FETCH_SIZE, padded);
    }

    private static int adaptiveMutationBatchSize(long callerLimit) {
        int maxBatch = MutationFilteringIterator.LOOKUP_BATCH_SIZE;
        if (callerLimit == Long.MAX_VALUE)
            return maxBatch;
        if (callerLimit <= 0)
            return 1;
        long padded = callerLimit > maxBatch - 16L
                ? maxBatch
                : Math.max(32L, callerLimit + 16L);
        return (int) Math.min(maxBatch, padded);
    }

    /**
     * Filters base rows through bounded set-based mutation lookups. Only one
     * page of IDs and surviving rows is retained; the source closes on
     * exhaustion, failure, or an explicit short-circuit close.
     */
    private <T> Stream<T> excludeMutated(Stream<T> source, ToLongFunction<T> idExtractor,
                                         int initialBatchSize) {
        MutationFilteringIterator<T> iterator = new MutationFilteringIterator<>(
                source, idExtractor, archiveCatalog::mutatedObservationIds, initialBatchSize);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    private <T> Stream<T> excludeTombstoned(Stream<T> source, ToLongFunction<T> idExtractor,
                                            int initialBatchSize) {
        MutationFilteringIterator<T> iterator = new MutationFilteringIterator<>(
                source, idExtractor, archiveCatalog::tombstonedObservationIds, initialBatchSize);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    /** Closes the upstream stream as soon as its externally visible limit is reached. */
    private static <T> Stream<T> limitAndClose(Stream<T> source, long limit) {
        if (limit == Long.MAX_VALUE)
            return source;
        if (limit < 0) {
            source.close();
            throw new IllegalArgumentException("Observation query limit cannot be negative");
        }
        ClosingLimitIterator<T> iterator = new ClosingLimitIterator<>(source, limit);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
    }

    private static final class MergingIterator<T> implements Iterator<T>, AutoCloseable {
        private final Stream<T> leftStream;
        private final Stream<T> rightStream;
        private final Iterator<T> left;
        private final Iterator<T> right;
        private final Comparator<? super T> comparator;
        private final AtomicBoolean closed = new AtomicBoolean();
        private T leftValue;
        private T rightValue;
        private boolean leftReady;
        private boolean rightReady;
        private boolean leftEnded;
        private boolean rightEnded;

        MergingIterator(Stream<T> leftStream, Stream<T> rightStream,
                        Comparator<? super T> comparator) {
            this.leftStream = leftStream;
            this.rightStream = rightStream;
            this.left = leftStream.iterator();
            this.right = rightStream.iterator();
            this.comparator = comparator;
        }

        @Override
        public boolean hasNext() {
            if (closed.get())
                return false;
            try {
                loadLeft();
                loadRight();
                boolean available = leftReady || rightReady;
                if (!available)
                    close();
                return available;
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            if (!leftReady) {
                T value = rightValue;
                rightValue = null;
                rightReady = false;
                return value;
            }
            if (!rightReady) {
                T value = leftValue;
                leftValue = null;
                leftReady = false;
                return value;
            }

            int compared = comparator.compare(leftValue, rightValue);
            if (compared <= 0) {
                T value = leftValue;
                leftValue = null;
                leftReady = false;
                if (compared == 0) {
                    // The left (preferred) tier wins the overlap.
                    rightValue = null;
                    rightReady = false;
                }
                return value;
            }
            T value = rightValue;
            rightValue = null;
            rightReady = false;
            return value;
        }

        private void loadLeft() {
            if (!leftReady && !leftEnded) {
                if (left.hasNext()) {
                    leftValue = left.next();
                    leftReady = true;
                } else {
                    leftEnded = true;
                }
            }
        }

        private void loadRight() {
            if (!rightReady && !rightEnded) {
                if (right.hasNext()) {
                    rightValue = right.next();
                    rightReady = true;
                } else {
                    rightEnded = true;
                }
            }
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            try {
                leftStream.close();
            } finally {
                rightStream.close();
            }
            leftValue = null;
            rightValue = null;
            leftReady = false;
            rightReady = false;
        }
    }

    private final class MutationFilteringIterator<T> implements Iterator<T>, AutoCloseable {
        static final int LOOKUP_BATCH_SIZE = 1_000;

        private final Stream<T> source;
        private final Iterator<T> sourceIterator;
        private final ToLongFunction<T> idExtractor;
        private final Function<Collection<Long>, Set<Long>> exclusionLookup;
        private final ArrayDeque<T> ready = new ArrayDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private int currentBatchSize;
        private boolean sourceEnded;

        MutationFilteringIterator(Stream<T> source, ToLongFunction<T> idExtractor,
                                  Function<Collection<Long>, Set<Long>> exclusionLookup,
                                  int initialBatchSize) {
            this.source = source;
            this.sourceIterator = source.iterator();
            this.idExtractor = idExtractor;
            this.exclusionLookup = exclusionLookup;
            this.currentBatchSize = Math.min(LOOKUP_BATCH_SIZE, Math.max(1, initialBatchSize));
        }

        @Override
        public boolean hasNext() {
            if (closed.get())
                return false;
            try {
                while (ready.isEmpty() && !sourceEnded)
                    fillPage();
                if (ready.isEmpty())
                    close();
                return !ready.isEmpty();
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        private void fillPage() {
            int batchTarget = currentBatchSize;
            List<T> page = new ArrayList<>(batchTarget);
            List<Long> ids = new ArrayList<>(batchTarget);
            while (page.size() < batchTarget && sourceIterator.hasNext()) {
                T value = sourceIterator.next();
                page.add(value);
                ids.add(idExtractor.applyAsLong(value));
            }
            if (page.size() < batchTarget) {
                sourceEnded = true;
            } else if (currentBatchSize < LOOKUP_BATCH_SIZE) {
                currentBatchSize = (int) Math.min((long) LOOKUP_BATCH_SIZE, (long) currentBatchSize * 2L);
            }
            if (ids.isEmpty())
                return;
            Set<Long> mutated = exclusionLookup.apply(ids);
            for (int i = 0; i < page.size(); i++) {
                if (!mutated.contains(ids.get(i)))
                    ready.addLast(page.get(i));
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return ready.removeFirst();
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            ready.clear();
            source.close();
        }
    }

    private static final class ClosingLimitIterator<T> implements Iterator<T>, AutoCloseable {
        private final Stream<T> source;
        private final Iterator<T> iterator;
        private final long limit;
        private final AtomicBoolean closed = new AtomicBoolean();
        private long emitted;

        ClosingLimitIterator(Stream<T> source, long limit) {
            this.source = source;
            this.iterator = source.iterator();
            this.limit = limit;
            if (limit == 0)
                close();
        }

        @Override
        public boolean hasNext() {
            if (closed.get() || emitted >= limit)
                return false;
            try {
                boolean available = iterator.hasNext();
                if (!available)
                    close();
                return available;
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        @Override
        public T next() {
            if (!hasNext())
                throw new NoSuchElementException();
            T value = iterator.next();
            emitted++;
            if (emitted >= limit)
                close();
            return value;
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true))
                source.close();
        }
    }

    private boolean supportsMetadataScan(ObsFilter filter) {
        return filter.getCQLFilter() == null && filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null && filter.getLimit() == Long.MAX_VALUE &&
                (filter.getPhenomenonTime() == null ||
                        (!filter.getPhenomenonTime().isLatestTime() && !filter.getPhenomenonTime().isCurrentTime())) &&
                (filter.getResultTime() == null ||
                        (!filter.getResultTime().isLatestTime() && !filter.getResultTime().isCurrentTime()));
    }

    private MetadataScanPlan prepareMetadataScan(ObsFilter filter) {
        ResolvedRelatedFilter related = resolveRelatedFilter(filter);
        if (!related.matches())
            return new MetadataScanPlan(filter, related.dataStreams(), related.fois(), List.of(), false);
        ObsFilter resolvedFilter = related.filter();
        if (isAllMutableOnlyDataStreams(related.dataStreams()))
            return new MetadataScanPlan(resolvedFilter, related.dataStreams(), related.fois(), List.of(), true);
        ArchiveBounds bounds = archiveBounds(resolvedFilter);
        Instant minResultTime = minResultTimeBound(resolvedFilter);
        Instant maxResultTime = maxResultTimeBound(resolvedFilter);
        Long oneId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() == 1
                ? resolvedFilter.getInternalIDs().first().getIdAsLong() : null;
        Long minId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() > 1
                ? resolvedFilter.getInternalIDs().first().getIdAsLong() : null;
        Long maxId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() > 1
                ? resolvedFilter.getInternalIDs().last().getIdAsLong() : null;
        List<ArchiveGeneration> candidates = pruneAndBackfillLegacyResultTimeCandidates(
                archiveCatalog.candidates(
                        bounds.start(), bounds.end(), oneId, minId, maxId,
                        related.dataStreams(), minResultTime, maxResultTime),
                minResultTime);
        return new MetadataScanPlan(resolvedFilter, related.dataStreams(), related.fois(), candidates, true);
    }

    /**
     * Emits the logical hot/cold union as signed metadata contributions.
     * Positive/negative pairs remove the intentional grace overlap without
     * retaining millions of IDs. Sparse overlays/tombstones are then applied
     * as a correction layer. includeRange is false for rows whose base time
     * was superseded, so reported min/max ranges remain exact.
     */
    private void scanMetadata(MetadataScanPlan plan, MetadataVisitor visitor) {
        if (!plan.matches())
            return;
        boolean parentTableOnly = isAllMutableOnlyDataStreams(plan.dataStreams());
        scanHotMetadata(plan.filter(), parentTableOnly, hot -> {
            visitor.visit(hot.metadata(), 1, true);
            if (hot.coldOverlap())
                visitor.visit(hot.metadata(), -1, false);
        });

        archiveReader.forEachMetadata(plan.candidates(), plan.filter(), plan.dataStreams(), plan.fois(),
                Long.MAX_VALUE, row -> {
                    ObservationMetadata metadata = fromArchived(row);
                    visitor.visit(metadata, 1, true);
                });
    }

    private void scanHotMetadata(ObsFilter filter, boolean parentTableOnly, Consumer<HotMetadata> consumer) {
        ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        Set<ObsField> fields = Set.of(ObsField.DATASTREAM_ID, ObsField.FOI_ID,
                ObsField.PHENOMENON_TIME, ObsField.RESULT_TIME);
        // Metadata aggregation is order-independent. Avoid forcing PostgreSQL
        // to sort the complete hot relation before the streaming aggregate, and
        // project tableoid/archivegenerationid directly in the single table scan
        // instead of self-joining the partitioned obs hierarchy back onto itself.
        boolean useStageOwnership = archiveConfig.isParquetPrimary() && !parentTableOnly;
        String markerColumn = useStageOwnership
                ? "tableoid"
                : "archivegenerationid";
        ParameterizedQuery selected = queryBuilder.createParameterizedUnorderedSelectEntriesQuery(
                unbounded, fields, parentTableOnly, markerColumn);
        String escapedTable = getDatastoreName().replace("'", "''");
        String ownership = useStageOwnership
                ? "owner.generation_id"
                : "matched.archivegenerationid";
        String stageOwnership = useStageOwnership
                ? " LEFT JOIN (SELECT child_table, to_regclass(child_table) AS child_oid " +
                "FROM oscar_obs_stage_partition WHERE store_key='" + escapedTable + "' " +
                "AND id_scope=" + idScope + ") stage ON stage.child_oid=matched.tableoid " +
                "LEFT JOIN oscar_obs_stage_owner owner ON owner.store_key='" + escapedTable + "' " +
                "AND owner.id_scope=" + idScope + " AND owner.child_table=stage.child_table " +
                "AND owner.observation_id=matched.id"
                : "";
        String sql = "SELECT matched.id, matched.datastreamid, matched.foiid, matched.phenomenontime, " +
                "matched.resulttime, EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                "WHERE generation.id=" + ownership + " " +
                "AND generation.store_key='" + escapedTable + "' " +
                "AND generation.id_scope=" + idScope + " " +
                "AND generation.state IN ('ACTIVE','SEALED')) AS cold_overlap " +
                "FROM (" + selected.sql() + ") matched" +
                stageOwnership;
        try (Connection connection = connectionManager.getConnection()) {
            connection.setReadOnly(true);
            connection.setAutoCommit(false); // PostgreSQL cursor fetch requires a transaction
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                selected.bind(statement);
                statement.setFetchSize(STREAM_FETCH_SIZE);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        Long foi = result.getObject(3, Long.class);
                        Instant phenomenon = UtcJdbcTime.getInstant(result, 4);
                        Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                        consumer.accept(new HotMetadata(new ObservationMetadata(
                                result.getLong(1), idScope, result.getLong(2),
                                foi == null ? null : idScope, foi,
                                phenomenon,
                                resultTime), result.getBoolean(6)));
                    }
                }
            }
            connection.rollback();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot scan hot observation metadata", e);
        }
    }

    private ObservationMetadata fromArchived(ParquetArchiveReader.ArchivedMetadataRow row) {
        return new ObservationMetadata(row.id(), row.dataStreamScope(), row.dataStreamId(),
                row.foiScope(), row.foiId(), row.phenomenonTime(), row.resultTime());
    }

    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query) {
        ObsFilter filter = query.getObsFilter() == null ? selectAllFilter() : query.getObsFilter();
        StatsAccumulator accumulator = new StatsAccumulator(query, filter);

        if (archiveConfig.enabled && supportsMetadataScan(filter) &&
                !archiveCatalog.hasObservationMutations()) {
            MetadataScanPlan plan = prepareMetadataScan(filter);
            scanMetadata(plan, accumulator::accept);
            accumulator.prepareHistogram();
            if (query.getHistogramBinSize() != null)
                scanMetadata(plan, accumulator::acceptHistogram);
        } else {
            // Arbitrary value/spatial predicates and latest/current selection
            // require normal OSH observation semantics. Reduce each payload
            // immediately rather than retaining an unbounded cold result set.
            try (Stream<Entry<BigId, IObsData>> entries = selectEntries(filter)) {
                entries.map(entry -> fromObservation(entry.getKey(), entry.getValue()))
                        .forEach(metadata -> accumulator.accept(metadata, 1, true));
            }
            accumulator.prepareHistogram();
            if (query.getHistogramBinSize() != null) {
                // A histogram needs the first pass's exact range. Re-read the
                // lazy union for the second pass; this trades bounded I/O for
                // bounded heap instead of retaining every observation.
                try (Stream<Entry<BigId, IObsData>> entries = selectEntries(filter)) {
                    entries.map(entry -> fromObservation(entry.getKey(), entry.getValue()))
                            .forEach(metadata -> accumulator.acceptHistogram(metadata, 1, false));
                }
            }
        }
        return accumulator.results().stream().limit(query.getLimit());
    }

    private ObservationMetadata fromObservation(BigId id, IObsData observation) {
        return new ObservationMetadata(id.getIdAsLong(), observation.getDataStreamID().getScope(),
                observation.getDataStreamID().getIdAsLong(),
                observation.hasFoi() ? observation.getFoiID().getScope() : null,
                observation.hasFoi() ? observation.getFoiID().getIdAsLong() : null,
                observation.getPhenomenonTime(), observation.getResultTime());
    }

    private final class StatsAccumulator {
        private static final int MAX_HISTOGRAM_BINS = 5_000_000;

        private final ObsStatsQuery query;
        private final ObsFilter filter;
        private final Map<StatsKey, StatsBucket> buckets = new HashMap<>();
        private long histogramSeconds;

        StatsAccumulator(ObsStatsQuery query, ObsFilter filter) {
            this.query = query;
            this.filter = filter;
        }

        void accept(ObservationMetadata metadata, long delta, boolean includeRange) {
            StatsKey key = new StatsKey(metadata.dataStreamId(),
                    query.isAggregateFois() ? null : metadata.foiId());
            StatsBucket bucket = buckets.computeIfAbsent(key,
                    ignored -> new StatsBucket(metadata.dataStreamScope(), metadata.foiScope()));
            bucket.count = Math.addExact(bucket.count, delta);
            if (includeRange)
                bucket.includeRange(metadata.phenomenonTime(), metadata.resultTime());
        }

        void prepareHistogram() {
            buckets.entrySet().removeIf(entry -> {
                if (entry.getValue().count < 0)
                    throw new IllegalStateException("Negative tiered statistics count for " + entry.getKey());
                return entry.getValue().count == 0;
            });
            Duration size = query.getHistogramBinSize();
            if (size == null)
                return;
            histogramSeconds = size.getSeconds();
            if (histogramSeconds <= 0)
                throw new IllegalArgumentException("Observation histogram bin size must be at least one second");

            Instant requestedStart = boundedMinimum(filter);
            Instant requestedEnd = boundedMaximum(filter);
            long totalBins = 0;
            for (StatsBucket bucket : buckets.values()) {
                if (bucket.minimumPhenomenon == null)
                    continue;
                bucket.histogramOrigin = requestedStart != null ? requestedStart : bucket.minimumPhenomenon;
                Instant end = requestedEnd != null ? requestedEnd : bucket.maximumPhenomenon;
                if (end.isBefore(bucket.histogramOrigin))
                    continue;
                Duration span = Duration.between(bucket.histogramOrigin, end);
                double spanSeconds = span.getSeconds() + span.getNano() / 1_000_000_000.0;
                long bins = Math.max(1L, (long)Math.ceil(spanSeconds / histogramSeconds));
                if (bins > MAX_HISTOGRAM_BINS)
                    throw new IllegalArgumentException("Observation histogram would require " + bins +
                            " bins; choose a larger bin size");
                totalBins = Math.addExact(totalBins, bins);
                if (totalBins > MAX_HISTOGRAM_BINS)
                    throw new IllegalArgumentException("Observation histograms would require " + totalBins +
                            " total bins; narrow the query or choose a larger bin size");
                bucket.histogram = new long[(int)bins];
            }
        }

        void acceptHistogram(ObservationMetadata metadata, long delta, boolean ignored) {
            StatsKey key = new StatsKey(metadata.dataStreamId(),
                    query.isAggregateFois() ? null : metadata.foiId());
            StatsBucket bucket = buckets.get(key);
            if (bucket == null || bucket.histogram == null || metadata.phenomenonTime() == null)
                return;
            long seconds = Duration.between(bucket.histogramOrigin, metadata.phenomenonTime()).getSeconds();
            if (seconds < 0)
                return;
            long index = seconds / histogramSeconds;
            if (index >= bucket.histogram.length)
                index = bucket.histogram.length - 1L; // inclusive upper query bound
            bucket.histogram[(int)index] = Math.addExact(bucket.histogram[(int)index], delta);
        }

        List<ObsStats> results() {
            List<Map.Entry<StatsKey, StatsBucket>> ordered = buckets.entrySet().stream()
                    .sorted(Comparator.comparingLong((Map.Entry<StatsKey, StatsBucket> entry) ->
                                    entry.getKey().dataStreamId())
                            .thenComparing(entry -> entry.getKey().foiId(),
                                    Comparator.nullsFirst(Comparator.naturalOrder())))
                    .toList();
            List<ObsStats> results = new ArrayList<>(ordered.size());
            for (Map.Entry<StatsKey, StatsBucket> entry : ordered) {
                StatsKey key = entry.getKey();
                StatsBucket bucket = entry.getValue();
                ObsStats.Builder builder = new ObsStats.Builder()
                        .withDataStreamID(BigId.fromLong(bucket.dataStreamScope, key.dataStreamId()))
                        .withTotalObsCount(bucket.count);
                if (!query.isAggregateFois()) {
                    FeatureId foi = key.foiId() == null ? FeatureId.NULL_FEATURE :
                            new FeatureId(BigId.fromLong(
                                    bucket.foiScope == null ? idScope : bucket.foiScope, key.foiId()),
                                    "urn:foi:unknown");
                    builder.withFoiID(foi);
                }
                if (bucket.minimumPhenomenon != null)
                    builder.withPhenomenonTimeRange(timeExtent(bucket.minimumPhenomenon, bucket.maximumPhenomenon));
                if (bucket.minimumResult != null)
                    builder.withResultTimeRange(timeExtent(bucket.minimumResult, bucket.maximumResult));
                if (bucket.minimumPhenomenon == null && bucket.minimumResult == null)
                    throw new IllegalStateException("Observation statistics row has no time value");
                if (bucket.histogram != null) {
                    int[] histogram = new int[bucket.histogram.length];
                    for (int i = 0; i < histogram.length; i++) {
                        if (bucket.histogram[i] < 0)
                            throw new IllegalStateException("Negative tiered observation histogram count");
                        histogram[i] = (int)Math.min(Integer.MAX_VALUE, bucket.histogram[i]);
                    }
                    builder.withObsCountByTime(histogram);
                }
                results.add(builder.build());
            }
            return results;
        }

        private Instant boundedMinimum(ObsFilter filter) {
            if (filter.getPhenomenonTime() == null)
                return null;
            Instant minimum = filter.getPhenomenonTime().getMin();
            return minimum == null || Instant.MIN.equals(minimum) ? null : minimum;
        }

        private Instant boundedMaximum(ObsFilter filter) {
            if (filter.getPhenomenonTime() == null)
                return null;
            Instant maximum = filter.getPhenomenonTime().getMax();
            return maximum == null || Instant.MAX.equals(maximum) ? null : maximum;
        }
    }

    private static final class StatsBucket {
        final int dataStreamScope;
        final Integer foiScope;
        long count;
        Instant minimumPhenomenon;
        Instant maximumPhenomenon;
        Instant minimumResult;
        Instant maximumResult;
        Instant histogramOrigin;
        long[] histogram;

        StatsBucket(int dataStreamScope, Integer foiScope) {
            this.dataStreamScope = dataStreamScope;
            this.foiScope = foiScope;
        }

        void includeRange(Instant phenomenon, Instant result) {
            if (phenomenon != null) {
                if (minimumPhenomenon == null || phenomenon.isBefore(minimumPhenomenon))
                    minimumPhenomenon = phenomenon;
                if (maximumPhenomenon == null || phenomenon.isAfter(maximumPhenomenon))
                    maximumPhenomenon = phenomenon;
            }
            if (result != null) {
                if (minimumResult == null || result.isBefore(minimumResult))
                    minimumResult = result;
                if (maximumResult == null || result.isAfter(maximumResult))
                    maximumResult = result;
            }
        }
    }

    private static TimeExtent timeExtent(Instant minimum, Instant maximum) {
        return minimum.equals(maximum) ? TimeExtent.instant(minimum) : TimeExtent.period(minimum, maximum);
    }

    @FunctionalInterface
    private interface MetadataVisitor {
        void visit(ObservationMetadata metadata, long delta, boolean includeRange);
    }

    private record ArchiveBounds(Instant start, Instant end) {}
    private record HotMetadata(ObservationMetadata metadata, boolean coldOverlap) {}
    private record ObservationMetadata(long id, int dataStreamScope, long dataStreamId,
                                       Integer foiScope, Long foiId, Instant phenomenonTime,
                                       Instant resultTime) {}
    private record StatsKey(long dataStreamId, Long foiId) {}
    private record MetadataScanPlan(ObsFilter filter, Set<Long> dataStreams, Set<Long> fois,
                                    List<ArchiveGeneration> candidates, boolean matches) {}
    private record ResolvedRelatedFilter(ObsFilter filter, Set<Long> dataStreams,
                                         Set<Long> fois, boolean matches) {}

    @Override
    public IObsData get(Object key) {
        if (!(key instanceof BigId id))
            return super.get(key);
        long observationId = id.getIdAsLong();
        if (!archiveConfig.enabled) {
            if (archiveCatalog.isTombstoned(observationId))
                return null;
            return super.get(key);
        }
        ArchiveCatalog.PointLookupStatus pointStatus =
                archiveCatalog.lookupOverlayOrTombstone(observationId, idScope, "OVERLAY");
        if (pointStatus.tombstoned())
            return null;
        if (pointStatus.overlay() != null) {
            ArchiveCatalog.ArchivedOverlay overlay = pointStatus.overlay();
            return toObservation(overlay.generationId(), overlay.schemaJson(), overlay.observation());
        }
        IObsData hot = super.get(key);
        if (hot != null)
            return hot;
        return getColdByPointId(observationId);
    }

    private IObsData getColdByPointId(long observationId) {
        ParquetArchiveReader.ArchivedRow cached = getVerifiedCachedColdBase(observationId);
        if (cached != null) {
            if (archiveCatalog.isGenerationQueryVisible(cached.generation().id())) {
                return toObservation(cached.generation().id(), null, cached.observation());
            }
            recentColdBaseById.invalidate(observationId);
        }
        if (ConnectionManager.hasActiveCancellableOperation()) {
            ObsFilter filter = new ObsFilter.Builder()
                    .withInternalIDs(BigId.fromLong(idScope, observationId))
                    .build();
            return readCold(filter).orElse(null);
        }
        ParquetArchiveReader.ArchivedRow loaded = coldPointReadCoalescer.readOne(observationId);
        if (loaded == null)
            return null;
        return toObservation(loaded.generation().id(), null, loaded.observation());
    }

    private ParquetArchiveReader.ArchivedRow getVerifiedCachedColdBase(long observationId) {
        ParquetArchiveReader.ArchivedRow cached = recentColdBaseById.getIfPresent(observationId);
        if (cached == null)
            return null;
        try {
            archiveReader.verifyGenerationObject(cached.generation());
            return cached;
        } catch (RuntimeException error) {
            recentColdBaseById.invalidate(observationId);
            throw error;
        }
    }

    @Override
    public IObsData put(BigId key, IObsData observation) {
        Objects.requireNonNull(key, "key");
        Objects.requireNonNull(observation, "observation");
        if (!archiveConfig.enabled)
            return super.put(key, observation);
        if (ConnectionManager.hasActiveCancellableOperation())
            return putSingleDirect(key, observation);
        return overlayWriteCoalescer.submit(key, observation);
    }

    @Override
    public void putAll(Map<? extends BigId, ? extends IObsData> map) {
        Objects.requireNonNull(map, "map");
        if (map.isEmpty())
            return;
        if (!archiveConfig.enabled) {
            super.putAll(map);
            return;
        }
        List<PendingPut> all = new ArrayList<>(map.size());
        for (Map.Entry<? extends BigId, ? extends IObsData> entry : map.entrySet()) {
            BigId key = Objects.requireNonNull(entry.getKey(), "key");
            IObsData obs = Objects.requireNonNull(entry.getValue(), "observation");
            all.add(new PendingPut(key, obs, new CompletableFuture<>()));
        }
        for (int offset = 0; offset < all.size(); offset += OVERLAY_WRITE_COALESCER_BATCH) {
            List<PendingPut> slice = all.subList(
                    offset, Math.min(all.size(), offset + OVERLAY_WRITE_COALESCER_BATCH));
            if (slice.size() == 1) {
                putSingleDirect(slice.get(0).key(), slice.get(0).observation());
            } else {
                putBatchDirect(slice);
            }
        }
    }

    private IObsData putSingleDirect(BigId key, IObsData observation) {
        long id = key.getIdAsLong();
        flushPendingIdsForMutation(List.of(id), "update");

        OverlayPayload overlay = prepareOverlay(key, observation);
        ParquetArchiveReader.ArchivedRow preResolvedCold = resolveColdBasesOutsideTransaction(List.of(id)).get(id);
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForMutation(connection, id);
                archiveCatalog.ensureSchemaBlobsPersisted(
                        connection, Map.of(overlay.schemaSha256(), overlay.schemaJson()));
                java.util.Optional<LockedLogicalObservation> locked =
                        resolveSingleLockedObservation(connection, id, preResolvedCold);
                if (locked.isEmpty())
                    throw new UnsupportedOperationException(
                            "Put can only be used to update existing entries");

                LockedLogicalObservation previous = locked.get();
                if (previous.generationId() != null) {
                    // The grace-period hot row is the immutable parity source.
                    // Logical updates live only in the generation-owned sparse
                    // overlay until lifecycle deletion removes that hot base.
                    archiveCatalog.upsertOverlay(connection, previous.generationId(),
                            overlay.observation(), overlay.schemaJson(), overlay.schemaSha256());
                } else {
                    updateLiveObservation(connection, key, observation,
                            overlay.observation().resultJson());
                }
                connection.commit();
                archiveCatalog.recordPersistedSchemaBlobHashes(List.of(overlay.schemaSha256()));
                recentColdBaseById.invalidate(id);
                return previous.observation();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (UnsupportedOperationException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically update tiered observation " + key, error);
        }
    }

    private java.util.Optional<LockedLogicalObservation> resolveSingleLockedObservation(
            Connection connection, long observationId,
            ParquetArchiveReader.ArchivedRow preResolvedCold) throws Exception {
        if (archiveCatalog.isTombstoned(connection, observationId))
            return java.util.Optional.empty();
        java.util.Optional<LockedLogicalObservation> hot =
                lockHotLogicalObservation(connection, observationId);
        if (hot.isPresent())
            return hot;
        java.util.Optional<LockedLogicalObservation> overlay =
                lockColdOverlay(connection, observationId);
        if (overlay.isPresent())
            return overlay;
        if (preResolvedCold != null) {
            Set<UUID> visible = archiveCatalog.activeOrSealedGenerationIds(
                    connection, List.of(preResolvedCold.generation().id()));
            if (visible.contains(preResolvedCold.generation().id())) {
                ArchivedObservation archived = preResolvedCold.observation();
                return java.util.Optional.of(new LockedLogicalObservation(
                        preResolvedCold.generation().id(), archived.resultJson(),
                        archived.phenomenonTime(), archived.resultTime(),
                        archived.dataStreamId(), archived.foiId(),
                        toObservation(connection, preResolvedCold.generation().id(), null, archived)));
            }
            recentColdBaseById.invalidate(observationId);
        }
        return loadLockedColdParquetObservation(connection, observationId);
    }

    private List<IObsData> putBatchDirect(List<PendingPut> batch) {
        List<Long> sortedIds = batch.stream()
                .map(item -> item.key().getIdAsLong())
                .distinct()
                .sorted()
                .toList();
        flushPendingIdsForMutation(sortedIds, "update");

        List<PreparedPut> prepared = new ArrayList<>(batch.size());
        Map<String, String> schemaJsonByHash = new HashMap<>();
        for (PendingPut item : batch) {
            OverlayPayload payload = prepareOverlay(item.key(), item.observation());
            prepared.add(new PreparedPut(item.key(), item.key().getIdAsLong(), item.observation(), payload));
            schemaJsonByHash.putIfAbsent(payload.schemaSha256(), payload.schemaJson());
        }

        Map<Long, ParquetArchiveReader.ArchivedRow> preResolvedColdById =
                resolveColdBasesOutsideTransaction(sortedIds);
        List<BigId> sortedBigIds = sortedIds.stream()
                .map(id -> BigId.fromLong(idScope, id))
                .toList();

        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForMutation(connection, sortedIds);
                archiveCatalog.ensureSchemaBlobsPersisted(connection, schemaJsonByHash);
                Set<Long> tombstoned = archiveCatalog.activeTombstonedObservationIds(connection, sortedIds);
                if (!tombstoned.isEmpty()) {
                    throw new UnsupportedOperationException(
                            "Put can only be used to update existing entries");
                }

                Map<Long, LockedLogicalObservation> hotById = lockHotLogicalObservations(
                        connection, sortedBigIds, false, true, false);
                Map<Long, LockedLogicalObservation> overlayById = lockOverlaysBatch(connection, sortedIds);

                Set<UUID> candidateGenIds = new HashSet<>();
                for (Long id : sortedIds) {
                    if (!hotById.containsKey(id) && !overlayById.containsKey(id)) {
                        ParquetArchiveReader.ArchivedRow coldRow = preResolvedColdById.get(id);
                        if (coldRow != null)
                            candidateGenIds.add(coldRow.generation().id());
                    }
                }
                Set<UUID> visibleColdGens = candidateGenIds.isEmpty()
                        ? Set.of()
                        : archiveCatalog.activeOrSealedGenerationIds(connection, candidateGenIds);

                Map<Long, LockedLogicalObservation> initialById = new HashMap<>(sortedIds.size());
                for (Long id : sortedIds) {
                    LockedLogicalObservation hot = hotById.get(id);
                    if (hot != null) {
                        if (hot.generationId() != null) {
                            LockedLogicalObservation ov = overlayById.get(id);
                            if (ov != null && hot.generationId().equals(ov.generationId())) {
                                initialById.put(id, ov);
                                continue;
                            }
                        }
                        initialById.put(id, hot);
                        continue;
                    }
                    LockedLogicalObservation ov = overlayById.get(id);
                    if (ov != null) {
                        initialById.put(id, ov);
                        continue;
                    }
                    ParquetArchiveReader.ArchivedRow coldRow = preResolvedColdById.get(id);
                    if (coldRow != null && visibleColdGens.contains(coldRow.generation().id())) {
                        ArchivedObservation archived = coldRow.observation();
                        initialById.put(id, new LockedLogicalObservation(
                                coldRow.generation().id(), archived.resultJson(),
                                archived.phenomenonTime(), archived.resultTime(),
                                archived.dataStreamId(), archived.foiId(),
                                toObservation(connection, coldRow.generation().id(), null, archived)));
                        continue;
                    }
                    if (coldRow != null)
                        recentColdBaseById.invalidate(id);
                    java.util.Optional<LockedLogicalObservation> fallback =
                            loadLockedColdParquetObservation(connection, id);
                    if (fallback.isEmpty()) {
                        throw new UnsupportedOperationException(
                                "Put can only be used to update existing entries");
                    }
                    initialById.put(id, fallback.get());
                }

                Map<Long, IObsData> runningPreviousById = new HashMap<>(sortedIds.size());
                for (Map.Entry<Long, LockedLogicalObservation> entry : initialById.entrySet())
                    runningPreviousById.put(entry.getKey(), entry.getValue().observation());

                List<IObsData> returnValues = new ArrayList<>(prepared.size());
                Map<Long, PreparedPut> finalWriteById = new LinkedHashMap<>(sortedIds.size());
                for (PreparedPut item : prepared) {
                    returnValues.add(runningPreviousById.put(item.id(), item.observation()));
                    finalWriteById.put(item.id(), item);
                }

                List<ArchiveCatalog.OverlayBatchItem> overlayBatchItems = new ArrayList<>(finalWriteById.size());
                for (Long id : sortedIds) {
                    PreparedPut finalItem = finalWriteById.get(id);
                    if (finalItem == null)
                        continue;
                    LockedLogicalObservation baseLocked = initialById.get(id);
                    if (baseLocked.generationId() != null) {
                        overlayBatchItems.add(new ArchiveCatalog.OverlayBatchItem(
                                baseLocked.generationId(),
                                finalItem.payload().observation(),
                                finalItem.payload().schemaJson(),
                                finalItem.payload().schemaSha256()));
                    } else {
                        updateLiveObservation(connection, finalItem.key(), finalItem.observation(),
                                finalItem.payload().observation().resultJson());
                    }
                }
                if (!overlayBatchItems.isEmpty())
                    archiveCatalog.upsertOverlaysBatch(connection, overlayBatchItems);

                connection.commit();
                archiveCatalog.recordPersistedSchemaBlobHashes(schemaJsonByHash.keySet());
                recentColdBaseById.invalidateAll(sortedIds);
                return returnValues;
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (UnsupportedOperationException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically update tiered observation batch", error);
        }
    }

    private Map<Long, ParquetArchiveReader.ArchivedRow> resolveColdBasesOutsideTransaction(
            List<Long> sortedIds) {
        if (sortedIds.isEmpty())
            return Map.of();
        Map<Long, ParquetArchiveReader.ArchivedRow> resolved = new HashMap<>(sortedIds.size());
        List<Long> uncachedCandidateIds = new ArrayList<>();
        for (Long id : sortedIds) {
            ParquetArchiveReader.ArchivedRow cached = getVerifiedCachedColdBase(id);
            if (cached != null)
                resolved.put(id, cached);
            else
                uncachedCandidateIds.add(id);
        }
        if (uncachedCandidateIds.isEmpty())
            return resolved;

        // Probe only IDs that have neither a hot base row nor an existing overlay
        // so live/already-overlaid updates never touch DuckDB.
        Set<Long> hotOrOverlayIds = findExistingHotOrOverlayIds(uncachedCandidateIds);
        List<Long> coldMissIds = new ArrayList<>();
        for (Long id : uncachedCandidateIds) {
            if (!hotOrOverlayIds.contains(id))
                coldMissIds.add(id);
        }
        if (coldMissIds.isEmpty())
            return resolved;

        if (ConnectionManager.hasActiveCancellableOperation()) {
            resolved.putAll(readColdPointBatchDirect(coldMissIds));
        } else {
            resolved.putAll(coldPointReadCoalescer.readMany(coldMissIds));
        }
        return resolved;
    }

    private Set<Long> findExistingHotOrOverlayIds(List<Long> observationIds) {
        if (observationIds.isEmpty())
            return Set.of();
        String sql = "WITH requested(id) AS (SELECT unnest(?::bigint[])) " +
                "SELECT r.id FROM requested r WHERE " +
                "EXISTS (SELECT 1 FROM " + getDatastoreName() + " o WHERE o.id=r.id) " +
                "OR EXISTS (SELECT 1 FROM oscar_obs_overlay ov " +
                "JOIN oscar_archive_generation g ON g.id=ov.generation_id " +
                "WHERE ov.store_key=? AND ov.id_scope=? AND ov.observation_id=r.id " +
                "AND g.store_key=ov.store_key AND g.id_scope=ov.id_scope " +
                "AND g.state IN ('ACTIVE','SEALED'))";
        Set<Long> present = new HashSet<>();
        try (Connection connection = connectionManager.getConnection()) {
            connection.setReadOnly(true);
            java.sql.Array ids = connection.createArrayOf("bigint", observationIds.toArray(Long[]::new));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, ids);
                statement.setString(2, getDatastoreName());
                statement.setInt(3, idScope);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next())
                        present.add(result.getLong(1));
                }
            } finally {
                ids.free();
            }
            return present;
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot inspect hot/overlay presence before update", error);
        }
    }

    private Map<Long, ParquetArchiveReader.ArchivedRow> readColdPointBatchDirect(
            Collection<Long> observationIds) {
        if (observationIds == null || observationIds.isEmpty())
            return Map.of();
        List<Long> distinctIds = observationIds.stream().distinct().sorted().toList();
        List<ArchiveGeneration> generations = archiveCatalog.candidatesForObservationIds(distinctIds);
        if (generations.isEmpty())
            return Map.of();
        ObsFilter filter = new ObsFilter.Builder()
                .withInternalIDs(distinctIds.stream()
                        .map(id -> BigId.fromLong(idScope, id))
                        .toList())
                .build();
        List<ParquetArchiveReader.ArchivedRow> rows =
                archiveReader.read(generations, filter, null, null, Long.MAX_VALUE);
        if (rows.isEmpty())
            return Map.of();
        Map<Long, ParquetArchiveReader.ArchivedRow> byId = new HashMap<>(rows.size());
        for (ParquetArchiveReader.ArchivedRow row : rows) {
            long id = row.observation().id();
            byId.put(id, row);
            recentColdBaseById.put(id, row);
        }
        return byId;
    }

    private OverlayPayload prepareOverlay(BigId key, IObsData observation) {
        BigId dsBigId = observation.getDataStreamID();
        IDataStreamInfo schema = getDataStreamInfo(dsBigId);
        if (schema == null)
            throw new IllegalStateException("Cannot mutate archived observation without a live datastream schema");
        String resultJson = SerializerUtils.writeDataBlockToJson(
                schema.getRecordStructure(), schema.getRecordEncoding(), observation.getResult());
        long dsId = dsBigId.getIdAsLong();
        CachedOverlaySchema cachedSchema = liveOverlaySchemas.getIfPresent(dsId);
        if (cachedSchema == null || cachedSchema.sourceRef() != schema) {
            try {
                String schemaJson = SerializerUtils.writeIDataStreamInfoToJson(schema);
                String schemaSha256 = ArchiveCatalog.computeSchemaSha256(schemaJson);
                cachedSchema = new CachedOverlaySchema(schema, schemaJson, schemaSha256);
                liveOverlaySchemas.put(dsId, cachedSchema);
            } catch (IOException e) {
                throw new IllegalStateException("Cannot snapshot archived overlay datastream schema", e);
            }
        }
        return new OverlayPayload(new ArchivedObservation(
                idScope, key.getIdAsLong(), dsBigId.getScope(), dsId,
                observation.hasFoi() ? observation.getFoiID().getScope() : null,
                observation.hasFoi() ? observation.getFoiID().getIdAsLong() : null,
                observation.getPhenomenonTime(), observation.getResultTime(), resultJson, "OVERLAY"),
                cachedSchema.schemaJson(),
                cachedSchema.schemaSha256());
    }

    private void updateLiveObservation(Connection connection, BigId key, IObsData observation,
                                       String resultJson) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                queryBuilder.updateByIdQuery() + " AND " +
                        unownedLivePredicate(getDatastoreName()))) {
            statement.setLong(1, observation.getDataStreamID().getIdAsLong());
            if (observation.hasFoi())
                statement.setLong(2, observation.getFoiID().getIdAsLong());
            else
                statement.setNull(2, Types.BIGINT);
            if (observation.getPhenomenonTime() == null)
                statement.setNull(3, Types.TIMESTAMP);
            else
                UtcJdbcTime.setInstant(statement, 3, observation.getPhenomenonTime());
            if (observation.getResultTime() == null)
                statement.setNull(4, Types.TIMESTAMP);
            else
                UtcJdbcTime.setInstant(statement, 4, observation.getResultTime());
            PGobject json = new PGobject();
            json.setType("json");
            json.setValue(resultJson);
            statement.setObject(5, json);
            statement.setLong(6, key.getIdAsLong());
            if (statement.executeUpdate() != 1)
                throw new IllegalStateException(
                        "Live observation became archive-owned during update: " + key);
        }
    }

    private java.util.Optional<UUID> findArchiveGeneration(Connection connection,
                                                            long observationId) throws Exception {
        List<ArchiveGeneration> candidates = archiveCatalog.candidates(
                connection, MIN_ARCHIVE_TIME, MAX_ARCHIVE_TIME, observationId);
        if (candidates.isEmpty())
            return java.util.Optional.empty();
        ObsFilter filter = new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(idScope, observationId)).build();
        UUID[] generation = {null};
        archiveReader.forEachMetadata(candidates, filter, null, null, 1,
                row -> generation[0] = row.generation().id());
        return java.util.Optional.ofNullable(generation[0]);
    }

    @Override
    public IObsData remove(Object key) {
        if (!(key instanceof BigId id))
            return super.remove(key);
        flushPendingIdsForMutation(List.of(id.getIdAsLong()), "remove");
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                long observationId = id.getIdAsLong();
                archiveCatalog.lockForMutation(connection, observationId);
                ArchiveCatalog.ActiveRemovalHolds holds = archiveCatalog.lockActiveRemovalHolds(
                        connection, List.of(observationId));
                java.util.Optional<LockedLogicalObservation> locked = archiveConfig.enabled
                        ? loadLockedLogicalObservation(connection, observationId)
                        : lockHotLogicalObservation(connection, observationId, false);
                if (locked.isEmpty()) {
                    connection.commit();
                    recentColdBaseById.invalidate(observationId);
                    return null;
                }

                LockedLogicalObservation previous = locked.get();
                ArchiveCatalog.requireRemovalAllowed(
                        holds, observationId, previous.generationId());
                if (previous.generationId() != null) {
                    // Keep the immutable hot base through parity and grace
                    // deletion; the exact-generation tombstone is the logical
                    // deletion.
                    archiveCatalog.tombstoneIfAbsent(connection, observationId,
                            previous.generationId(), "Removed through IObsStore",
                            "USER_OR_SYSTEM_DELETE", holds);
                    if (!archiveConfig.enabled) {
                        try (PreparedStatement delete = connection.prepareStatement(
                                "DELETE FROM " + getDatastoreName() +
                                        " WHERE id=? AND archivegenerationid=?")) {
                            delete.setLong(1, observationId);
                            delete.setObject(2, previous.generationId());
                            if (delete.executeUpdate() != 1)
                                throw new IllegalStateException(
                                        "Visible hot archive row changed during disabled-mode removal: " + id);
                        }
                    }
                } else {
                    try (PreparedStatement delete = connection.prepareStatement(
                            "DELETE FROM " + getDatastoreName() +
                                    " WHERE id=? AND " +
                                    unownedLivePredicate(getDatastoreName()))) {
                        delete.setLong(1, observationId);
                        if (delete.executeUpdate() != 1)
                            throw new IllegalStateException(
                                    "Live observation became archive-owned during removal: " + id);
                    }
                }
                connection.commit();
                recentColdBaseById.invalidate(observationId);
                return previous.observation();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (ArchiveCatalog.RetentionHoldException error) {
            throw error;
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically remove tiered observation " + id, error);
        }
    }

    /**
     * The inherited table-wide DELETE bypasses observation/generation holds.
     * Callers that need a logical purge must use {@link #removeEntries(ObsFilter)};
     * that path revalidates every bounded batch under the mutation and hold locks
     * and deliberately leaves protected rows queryable.
     */
    @Override
    public void clear() {
        throw new UnsupportedOperationException(
                "Tiered observation stores cannot be cleared directly; use hold-aware removeEntries");
    }

    /**
     * Dropping only the hot table would bypass retention holds and strand the
     * archive catalog. Physical teardown is therefore not a datastore API for
     * a tiered store; deployment removal must use the audited storage tooling.
     */
    @Override
    public void drop() {
        throw new UnsupportedOperationException(
                "Tiered observation stores cannot be dropped through the datastore API");
    }

    @Override
    public void backup(java.io.OutputStream output) {
        throw new UnsupportedOperationException(
                "Tiered observation stores require the coordinated OSCAR storage backup");
    }

    @Override
    public void restore(java.io.InputStream input) {
        throw new UnsupportedOperationException(
                "Tiered observation stores require the coordinated OSCAR storage restore");
    }

    private void flushPendingForMutation(String operation) {
        try {
            commit();
        } catch (DataStoreException error) {
            throw new IllegalStateException(
                    "Cannot flush observations before tiered " + operation, error);
        }
    }

    private void flushPendingIdsForMutation(Collection<Long> observationIds, String operation) {
        try {
            flushPendingForIds(observationIds);
        } catch (DataStoreException error) {
            throw new IllegalStateException(
                    "Cannot flush observations before tiered " + operation, error);
        }
    }

    @Override
    public long removeEntries(ObsFilter filter) {
        if ((filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime())) {
            throw new UnsupportedOperationException("Latest-time filters do not support REMOVE operations");
        }
        if (filter.getLimit() == 0)
            return 0;

        if (filter.getLimit() == Long.MAX_VALUE &&
                filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty()) {
            flushPendingForMutation("removeEntries");
            List<BigId> explicitIds = filter.getInternalIDs().stream()
                    .filter(Objects::nonNull)
                    .filter(id -> id.getScope() == idScope)
                    .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                    .distinct()
                    .toList();
            long removed = 0;
            for (int offset = 0; offset < explicitIds.size(); offset += REMOVE_BATCH_SIZE) {
                List<BigId> page = explicitIds.subList(
                        offset, Math.min(explicitIds.size(), offset + REMOVE_BATCH_SIZE));
                beforeRemoveBatchRevalidation(List.copyOf(page));
                RemovalBatchResult result = removeRevalidatedBatch(filter, page, page.size());
                removed = Math.addExact(removed, result.removed());
            }
            return removed;
        }

        long removed = 0;
        List<Long> previousStalledBatch = null;
        while (removed < filter.getLimit()) {
            long remaining = filter.getLimit() == Long.MAX_VALUE
                    ? REMOVE_BATCH_SIZE : filter.getLimit() - removed;
            long batchLimit = Math.min(REMOVE_BATCH_SIZE, remaining);
            ObsFilter candidateFilter = ObsFilter.Builder.from(filter)
                    .withLimit(batchLimit)
                    .build();
            List<BigId> selected;
            try (Stream<BigId> stream = selectKeys(candidateFilter)) {
                selected = stream.toList();
            }
            if (selected.isEmpty())
                break;

            // Test subclasses use this hook to reproduce a mutation in the
            // former select-then-delete race window. Production does nothing.
            beforeRemoveBatchRevalidation(List.copyOf(selected));
            RemovalBatchResult result = removeRevalidatedBatch(filter, selected, batchLimit);
            long batchRemoved = result.removed();
            removed = Math.addExact(removed, batchRemoved);

            // Once a hold appears in a leading page, repeatedly growing the
            // SELECT prefix makes a large held generation quadratic and keeps
            // every held ID boxed in heap. Snapshot the remaining logical
            // candidates once into a bounded private scratch file instead.
            // Revalidation still happens in fixed transactions, so holds
            // created or released after the snapshot retain their exact
            // mutation semantics.
            if (!result.heldObservationIds().isEmpty() && removed < filter.getLimit())
                return removeFromCandidateSnapshot(filter, removed);

            // A short page whose every logical row was removed proves that
            // the selection was exhausted at this operation's linearization
            // point. Avoid a redundant post-tombstone tier merge (and another
            // full archive scan) merely to rediscover an empty result.
            if (batchRemoved == selected.size() && selected.size() < batchLimit)
                break;

            List<Long> batchIds = selected.stream()
                    .map(BigId::getIdAsLong)
                    .distinct()
                    .sorted()
                    .toList();
            if (batchRemoved == 0 && batchIds.equals(previousStalledBatch)) {
                throw new IllegalStateException(
                        "Tiered observation removal made no progress after predicate revalidation");
            }
            previousStalledBatch = batchRemoved == 0 ? batchIds : null;
        }
        return removed;
    }

    /**
     * Makes a single O(n) pass over candidates after a hold blocks normal
     * page-and-delete progress. The binary scratch representation is one long
     * per candidate, is bounded by the configured DuckDB scratch allowance,
     * and is deleted on every success or failure path. No observation or
     * archive cursor remains open while mutation transactions are acquired.
     */
    private long removeFromCandidateSnapshot(ObsFilter filter, long alreadyRemoved) {
        ObsFilter unbounded = ObsFilter.Builder.from(filter)
                .withLimit(Long.MAX_VALUE)
                .build();
        try (RemovalCandidateSpool spool = RemovalCandidateSpool.create(archiveConfig)) {
            try (Stream<BigId> stream = selectKeys(unbounded)) {
                Iterator<BigId> candidates = stream.iterator();
                while (candidates.hasNext())
                    spool.append(candidates.next().getIdAsLong());
            }
            spool.finishWriting();

            long removed = alreadyRemoved;
            while (removed < filter.getLimit()) {
                long remaining = filter.getLimit() == Long.MAX_VALUE
                        ? REMOVE_BATCH_SIZE : filter.getLimit() - removed;
                // Candidate progress must not shrink with the remaining delete
                // allowance. Otherwise a limit of one behind a large held
                // prefix degenerates into one transaction per protected row.
                // Revalidate fixed pages, but stop mutation exactly at the
                // caller's remaining logical limit.
                List<BigId> candidates = spool.readBatch(REMOVE_BATCH_SIZE, idScope);
                if (candidates.isEmpty())
                    break;
                beforeRemoveBatchRevalidation(List.copyOf(candidates));
                RemovalBatchResult result = removeRevalidatedBatch(filter, candidates, remaining);
                removed = Math.addExact(removed, result.removed());
            }
            return removed;
        } catch (IOException error) {
            throw new IllegalStateException(
                    "Cannot create or clean up the bounded removal candidate snapshot", error);
        }
    }

    private static final class RemovalCandidateSpool implements AutoCloseable {
        private static final long MEBIBYTE = 1024L * 1024L;
        private static final Set<PosixFilePermission> PRIVATE_DIRECTORY_PERMISSIONS =
                PosixFilePermissions.fromString("rwx------");
        private static final Set<PosixFilePermission> PRIVATE_FILE_PERMISSIONS =
                PosixFilePermissions.fromString("rw-------");
        private static final Semaphore ADMISSION = new Semaphore(1, true);

        private final Path path;
        private final long maximumEntries;
        private DataOutputStream output;
        private DataInputStream input;
        private long entries;

        private RemovalCandidateSpool(Path path, long maximumEntries,
                                      DataOutputStream output) {
            this.path = path;
            this.maximumEntries = maximumEntries;
            this.output = output;
        }

        static RemovalCandidateSpool create(TieredArchiveConfig config) throws IOException {
            try {
                if (!ADMISSION.tryAcquire(
                        config.duckDbSessionAcquireTimeoutSeconds, TimeUnit.SECONDS)) {
                    throw new IllegalStateException(
                            "Removal candidate scratch is busy after " +
                                    config.duckDbSessionAcquireTimeoutSeconds + " seconds");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "Interrupted while waiting for removal candidate scratch", interrupted);
            }

            Path file = null;
            try {
                Path root = preparePrivateRoot(Path.of(config.duckDbTemporaryDirectory)
                        .toAbsolutePath().normalize());
                if (supportsPosix(root)) {
                    file = Files.createTempFile(root, "remove-candidates-", ".bin",
                            PosixFilePermissions.asFileAttribute(PRIVATE_FILE_PERMISSIONS));
                } else {
                    file = Files.createTempFile(root, "remove-candidates-", ".bin");
                }
                BasicFileAttributes attributes = Files.readAttributes(
                        file, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                if (!attributes.isRegularFile() || attributes.isSymbolicLink() ||
                        !file.toRealPath().getParent().equals(root)) {
                    throw new IllegalStateException(
                            "Removal candidate scratch creation was redirected");
                }
                long maximumBytes;
                try {
                    maximumBytes = Math.multiplyExact(
                            config.duckDbMaximumTemporaryMegabytes, MEBIBYTE);
                } catch (ArithmeticException overflow) {
                    maximumBytes = Long.MAX_VALUE;
                }
                DataOutputStream output = new DataOutputStream(new BufferedOutputStream(
                        Files.newOutputStream(file, StandardOpenOption.WRITE,
                                StandardOpenOption.TRUNCATE_EXISTING,
                                LinkOption.NOFOLLOW_LINKS)));
                return new RemovalCandidateSpool(file, maximumBytes / Long.BYTES, output);
            } catch (IOException | RuntimeException | Error failure) {
                if (file != null) {
                    try {
                        Files.deleteIfExists(file);
                    } catch (IOException cleanup) {
                        failure.addSuppressed(cleanup);
                    }
                }
                ADMISSION.release();
                throw failure;
            }
        }

        void append(long observationId) throws IOException {
            if (output == null)
                throw new IllegalStateException("Removal candidate snapshot is already sealed");
            if (entries >= maximumEntries) {
                throw new IllegalStateException(
                        "Removal candidate snapshot exceeds the configured DuckDB temporary limit");
            }
            output.writeLong(observationId);
            entries++;
        }

        void finishWriting() throws IOException {
            if (output == null)
                return;
            output.close();
            output = null;
            input = new DataInputStream(new BufferedInputStream(
                    Files.newInputStream(path, StandardOpenOption.READ,
                            LinkOption.NOFOLLOW_LINKS)));
        }

        List<BigId> readBatch(int maximum, int scope) throws IOException {
            if (input == null)
                throw new IllegalStateException("Removal candidate snapshot is not sealed");
            List<BigId> batch = new ArrayList<>(maximum);
            while (batch.size() < maximum) {
                try {
                    batch.add(BigId.fromLong(scope, input.readLong()));
                } catch (EOFException exhausted) {
                    break;
                }
            }
            return batch;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                if (output != null)
                    output.close();
            } catch (IOException error) {
                failure = error;
            }
            try {
                if (input != null)
                    input.close();
            } catch (IOException error) {
                if (failure == null)
                    failure = error;
                else
                    failure.addSuppressed(error);
            }
            try {
                try {
                    Files.delete(path);
                } catch (IOException error) {
                    if (failure == null)
                        failure = error;
                    else
                        failure.addSuppressed(error);
                }
            } finally {
                ADMISSION.release();
            }
            if (failure != null)
                throw failure;
        }

        private static Path preparePrivateRoot(Path root) throws IOException {
            Path ancestor = root;
            while (ancestor != null && Files.notExists(ancestor, LinkOption.NOFOLLOW_LINKS))
                ancestor = ancestor.getParent();
            if (ancestor == null)
                throw new IllegalStateException(
                        "Removal candidate scratch has no existing filesystem ancestor");
            if (supportsPosix(ancestor)) {
                Files.createDirectories(root,
                        PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY_PERMISSIONS));
            } else {
                Files.createDirectories(root);
            }
            BasicFileAttributes attributes = Files.readAttributes(
                    root, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attributes.isDirectory() || attributes.isSymbolicLink())
                throw new IllegalStateException(
                        "Removal candidate scratch root is not a real directory: " + root);
            if (supportsPosix(root) && !Files.getPosixFilePermissions(
                    root, LinkOption.NOFOLLOW_LINKS).equals(PRIVATE_DIRECTORY_PERMISSIONS)) {
                throw new IllegalStateException(
                        "Removal candidate scratch root must have POSIX mode 0700: " + root);
            }
            return root.toRealPath();
        }

        private static boolean supportsPosix(Path path) {
            return Files.getFileAttributeView(path, PosixFileAttributeView.class,
                    LinkOption.NOFOLLOW_LINKS) != null;
        }
    }

    /** Visible only to deterministic concurrency tests. */
    protected void beforeRemoveBatchRevalidation(List<BigId> candidateIds) {
        // no-op
    }

    /** Invoked after mutation locks are held, for deterministic race tests. */
    protected void afterRemoveBatchLocked(List<BigId> candidateIds) {
        // no-op
    }

    private RemovalBatchResult removeRevalidatedBatch(ObsFilter filter, List<BigId> candidates,
                                                       long maximumRemovals) {
        if (maximumRemovals <= 0)
            return new RemovalBatchResult(0, Set.of());
        List<BigId> ordered = candidates.stream()
                .distinct()
                .sorted(Comparator.comparingLong(BigId::getIdAsLong))
                .toList();
        try (Connection connection = connectionManager.getConnection()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForMutation(connection, ordered.stream()
                        .map(BigId::getIdAsLong)
                        .toList());
                afterRemoveBatchLocked(ordered);
                ArchiveCatalog.ActiveRemovalHolds holds = archiveCatalog.lockActiveRemovalHolds(
                        connection, ordered.stream().map(BigId::getIdAsLong).toList());
                // Resolve relational predicates again after the observation
                // locks instead of trusting the earlier candidate page. The
                // PostgreSQL datastream and FOI stores own independent pools,
                // so these lookups cannot recursively borrow this observation
                // connection. Their metadata transactions remain independent;
                // archive/observation mutation atomicity is provided by the
                // store and per-ID locks above, not by a cross-store snapshot.
                Set<Long> dataStreams = resolveDataStreams(filter.getDataStreamFilter());
                Set<Long> fois = resolveFois(filter.getFoiFilter());
                boolean parentTableOnly = isAllMutableOnlyDataStreams(dataStreams);
                Map<Long, LockedLogicalObservation> hotById = lockHotLogicalObservations(
                        connection, ordered, archiveConfig.enabled,
                        filter.getValuePredicate() != null || filter.getPhenomenonLocation() != null,
                        parentTableOnly);
                long removed = 0;
                List<Long> liveDeletes = new ArrayList<>();
                Set<Long> heldObservationIds = new java.util.HashSet<>();
                for (BigId key : ordered) {
                    if (removed + liveDeletes.size() >= maximumRemovals)
                        break;
                    long id = key.getIdAsLong();
                    LockedLogicalObservation current = hotById.get(id);
                    if (current != null) {
                        if (!matchesLockedObservation(filter, dataStreams, fois, id, current)) {
                            continue;
                        }
                        if (holds.blocks(id, current.generationId())) {
                            heldObservationIds.add(id);
                            continue;
                        }
                        if (current.generationId() != null) {
                            boolean tombstoned = archiveCatalog.tombstoneIfAbsent(connection, id,
                                    current.generationId(), "Removed through IObsStore filter",
                                    "USER_OR_SYSTEM_DELETE", holds);
                            if (!archiveConfig.enabled) {
                                try (PreparedStatement delete = connection.prepareStatement(
                                        "DELETE FROM " + getDatastoreName() +
                                                " WHERE id=? AND archivegenerationid=?")) {
                                    delete.setLong(1, id);
                                    delete.setObject(2, current.generationId());
                                    if (delete.executeUpdate() == 1)
                                        removed = Math.addExact(removed, 1L);
                                }
                            } else if (tombstoned) {
                                removed = Math.addExact(removed, 1L);
                            }
                        } else {
                            liveDeletes.add(id);
                        }
                        continue;
                    }

                    // Disabled mode is deliberately hot-only. A row that was
                    // not visible in the selected hot tier must not be
                    // resolved or tombstoned through an overlay/Parquet base.
                    if (!archiveConfig.enabled || parentTableOnly)
                        continue;

                    // Once hot rows have aged out, an overlay is the only
                    // mutable representation. Lock and recheck it just like a
                    // hot row. An unmodified Parquet base is immutable, so its
                    // original candidate match remains valid.
                    java.util.Optional<LockedLogicalObservation> overlay =
                            lockColdOverlay(connection, id);
                    if (overlay.isPresent()) {
                        LockedLogicalObservation overlayCurrent = overlay.get();
                        if (!matchesLockedObservation(filter, dataStreams, fois, id, overlayCurrent)) {
                            continue;
                        }
                        if (holds.blocks(id, overlayCurrent.generationId())) {
                            heldObservationIds.add(id);
                            continue;
                        }
                        if (archiveCatalog.tombstoneIfAbsent(connection, id,
                                overlayCurrent.generationId(), "Removed through IObsStore filter",
                                "USER_OR_SYSTEM_DELETE", holds)) {
                            removed = Math.addExact(removed, 1L);
                        }
                        continue;
                    }

                    java.util.Optional<UUID> generation = findArchiveGeneration(connection, id);
                    if (generation.isPresent() && holds.blocks(id, generation.get())) {
                        heldObservationIds.add(id);
                    } else if (generation.isPresent() && archiveCatalog.tombstoneIfAbsent(
                            connection, id, generation.get(), "Removed through IObsStore filter",
                            "USER_OR_SYSTEM_DELETE", holds)) {
                        removed = Math.addExact(removed, 1L);
                    }
                }
                if (!liveDeletes.isEmpty()) {
                    long deleted = deleteLockedLiveRows(connection, liveDeletes, parentTableOnly);
                    if (deleted != liveDeletes.size()) {
                        throw new IllegalStateException(
                                "Live observation ownership changed while its rows were locked");
                    }
                    removed = Math.addExact(removed, deleted);
                }
                connection.commit();
                recentColdBaseById.invalidateAll(
                        ordered.stream().map(BigId::getIdAsLong).toList());
                return new RemovalBatchResult(removed, Set.copyOf(heldObservationIds));
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot atomically remove tiered observations", error);
        }
    }

    /**
     * Locks and decodes a complete candidate page in one PostgreSQL round
     * trip. Generation-owned overlays remain sparse, so only those exceptional
     * rows need an additional overlay lookup.
     */
    private Map<Long, LockedLogicalObservation> lockHotLogicalObservations(
            Connection connection, List<BigId> candidates, boolean resolveOverlay,
            boolean decodeObservation, boolean parentTableOnly) throws Exception {
        List<Long> ids = candidates.stream()
                .map(BigId::getIdAsLong)
                .distinct()
                .sorted()
                .toList();
        if (ids.isEmpty())
            return Map.of();

        List<LockedHotRow> rows = new ArrayList<>(ids.size());
        java.sql.Array sqlIds = connection.createArrayOf("bigint", ids.toArray(Long[]::new));
        String lockSql = (archiveConfig.isParquetPrimary() && !parentTableOnly)
                ? "SELECT observation.id,observation.datastreamid,observation.foiid," +
                "observation.phenomenontime,observation.resulttime,observation.result::text," +
                "COALESCE(owner.generation_id,observation.archivegenerationid) FROM " +
                getDatastoreName() + " observation " +
                "LEFT JOIN oscar_obs_stage_partition stage ON stage.store_key='" +
                getDatastoreName() + "' AND stage.id_scope=" + idScope +
                " AND observation.tableoid=to_regclass(stage.child_table) " +
                "LEFT JOIN oscar_obs_stage_owner owner ON owner.store_key='" +
                getDatastoreName() + "' AND owner.id_scope=" + idScope +
                " AND owner.child_table=stage.child_table AND owner.observation_id=observation.id " +
                "WHERE observation.id=ANY (?::bigint[]) ORDER BY observation.id FOR UPDATE OF observation"
                : "SELECT observation.id,observation.datastreamid,observation.foiid," +
                "observation.phenomenontime,observation.resulttime,observation.result::text," +
                "observation.archivegenerationid FROM " +
                (parentTableOnly ? "ONLY " : "") + getDatastoreName() + " observation " +
                "WHERE observation.id=ANY (?::bigint[]) ORDER BY observation.id FOR UPDATE OF observation";
        try (PreparedStatement statement = connection.prepareStatement(lockSql)) {
            statement.setArray(1, sqlIds);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    rows.add(new LockedHotRow(
                            result.getLong(1), result.getLong(2), result.getObject(3, Long.class),
                            UtcJdbcTime.getInstant(result, 4), UtcJdbcTime.getInstant(result, 5),
                            result.getString(6), result.getObject(7, UUID.class)));
                }
            }
        } finally {
            sqlIds.free();
        }

        Map<Long, LockedLogicalObservation> locked = new HashMap<>(rows.size());
        for (LockedHotRow row : rows) {
            if (resolveOverlay && row.generationId() != null) {
                java.util.Optional<LockedLogicalObservation> overlay =
                        lockOverlay(connection, row.observationId(), row.generationId());
                if (overlay.isPresent()) {
                    locked.put(row.observationId(), overlay.get());
                    continue;
                }
            }

            ArchivedObservation archived = new ArchivedObservation(
                    idScope, row.observationId(), idScope, row.dataStreamId(),
                    row.foiId() == null ? null : idScope, row.foiId(),
                    row.phenomenonTime(), row.resultTime(), row.resultJson(), "HOT");
            IObsData observation = null;
            if (decodeObservation) {
                observation = row.generationId() == null
                        ? decodeLiveObservation(archived)
                        : toObservation(connection, row.generationId(), null, archived);
            }
            locked.put(row.observationId(), new LockedLogicalObservation(
                    row.generationId(), row.resultJson(), row.phenomenonTime(),
                    row.resultTime(), row.dataStreamId(), row.foiId(), observation));
        }
        return locked;
    }

    /** Deletes already locked live rows with one guarded, auditable statement. */
    private long deleteLockedLiveRows(Connection connection, List<Long> observationIds,
                                      boolean parentTableOnly)
            throws SQLException {
        java.sql.Array ids = connection.createArrayOf(
                "bigint", observationIds.toArray(Long[]::new));
        long deleted = 0;
        String deleteSql = parentTableOnly
                ? "DELETE FROM ONLY " + getDatastoreName() +
                " WHERE id=ANY (?::bigint[]) AND archivegenerationid IS NULL RETURNING id"
                : "DELETE FROM " + getDatastoreName() +
                " WHERE id=ANY (?::bigint[]) AND " +
                unownedLivePredicate(getDatastoreName()) + " RETURNING id";
        try (PreparedStatement statement = connection.prepareStatement(deleteSql)) {
            statement.setArray(1, ids);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    deleted++;
            }
        } finally {
            ids.free();
        }
        return deleted;
    }

    /**
     * Guards in-place changes to live rows. Primary mode records exact archive
     * ownership in the compact staging sidecar rather than rewriting each raw
     * observation tuple with an archive marker.
     */
    private String unownedLivePredicate(String observationReference) {
        String marker = observationReference + ".archivegenerationid IS NULL";
        if (!archiveConfig.isParquetPrimary())
            return marker;
        String store = getDatastoreName().replace("'", "''");
        return marker + " AND NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                "WHERE owner.store_key='" + store + "' AND owner.id_scope=" + idScope +
                " AND owner.observation_id=" + observationReference + ".id)";
    }

    /**
     * Removes only the transient row created by replaying an observation whose
     * authoritative owner is already in a detached staging child. Global ID
     * ownership must continue to protect the old child, so this exceptional
     * cleanup is deliberately scoped to an attached child with no owner for
     * that same physical child. Every other mutation path uses the global
     * ownership predicate above.
     */
    private void deleteTemporaryReplayRow(Connection connection, long observationId)
            throws SQLException {
        String sql = archiveConfig.isParquetPrimary()
                ? "DELETE FROM " + getDatastoreName() + " observation " +
                "USING oscar_obs_stage_partition stage " +
                "WHERE observation.id=? AND stage.store_key=? AND stage.id_scope=? " +
                "AND stage.state='ATTACHED' " +
                "AND observation.tableoid=to_regclass(stage.child_table) " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                "WHERE owner.store_key=stage.store_key AND owner.id_scope=stage.id_scope " +
                "AND owner.child_table=stage.child_table " +
                "AND owner.observation_id=observation.id)"
                : "DELETE FROM " + getDatastoreName() +
                " observation WHERE observation.id=? " +
                "AND observation.archivegenerationid IS NULL";
        try (PreparedStatement delete = connection.prepareStatement(sql)) {
            delete.setLong(1, observationId);
            if (archiveConfig.isParquetPrimary()) {
                delete.setString(2, getDatastoreName());
                delete.setInt(3, idScope);
            }
            int deleted = delete.executeUpdate();
            if (deleted != 1)
                throw new IllegalStateException(
                        "Expected exactly one transient attached replay row for observation " +
                                observationId + ", deleted " + deleted);
        }
    }

    private java.util.Optional<LockedLogicalObservation> lockHotLogicalObservation(
            Connection connection, long observationId) throws Exception {
        return lockHotLogicalObservation(connection, observationId, true);
    }

    private java.util.Optional<LockedLogicalObservation> lockHotLogicalObservation(
            Connection connection, long observationId, boolean resolveOverlay) throws Exception {
        String lockSql = archiveConfig.isParquetPrimary()
                ? "SELECT observation.datastreamid,observation.foiid,observation.phenomenontime," +
                "observation.resulttime,observation.result::text," +
                "COALESCE(owner.generation_id,observation.archivegenerationid) FROM " +
                getDatastoreName() + " observation " +
                "LEFT JOIN oscar_obs_stage_partition stage ON stage.store_key='" +
                getDatastoreName() + "' AND stage.id_scope=" + idScope +
                " AND observation.tableoid=to_regclass(stage.child_table) " +
                "LEFT JOIN oscar_obs_stage_owner owner ON owner.store_key='" +
                getDatastoreName() + "' AND owner.id_scope=" + idScope +
                " AND owner.child_table=stage.child_table AND owner.observation_id=observation.id " +
                "WHERE observation.id=? FOR UPDATE OF observation"
                : "SELECT observation.datastreamid,observation.foiid,observation.phenomenontime," +
                "observation.resulttime,observation.result::text,observation.archivegenerationid " +
                "FROM " + getDatastoreName() + " observation WHERE observation.id=? " +
                "FOR UPDATE OF observation";
        try (PreparedStatement statement = connection.prepareStatement(lockSql)) {
            statement.setLong(1, observationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return java.util.Optional.empty();
                long dataStreamId = result.getLong(1);
                Long foiId = result.getObject(2, Long.class);
                Instant phenomenonTime = UtcJdbcTime.getInstant(result, 3);
                Instant resultTime = UtcJdbcTime.getInstant(result, 4);
                String resultJson = result.getString(5);
                UUID generationId = result.getObject(6, UUID.class);

                if (resolveOverlay && generationId != null) {
                    java.util.Optional<LockedLogicalObservation> overlay =
                            lockOverlay(connection, observationId, generationId);
                    if (overlay.isPresent())
                        return overlay;
                }

                ArchivedObservation archived = new ArchivedObservation(
                        idScope, observationId, idScope, dataStreamId,
                        foiId == null ? null : idScope, foiId,
                        phenomenonTime, resultTime, resultJson, "HOT");
                IObsData observation = generationId == null
                        ? decodeLiveObservation(archived)
                        : toObservation(connection, generationId, null, archived);
                return java.util.Optional.of(new LockedLogicalObservation(
                        generationId, resultJson, phenomenonTime, resultTime,
                        dataStreamId, foiId, observation));
            }
        }
    }

    private java.util.Optional<LockedLogicalObservation> lockColdOverlay(
            Connection connection, long observationId) throws Exception {
        return lockOverlay(connection, observationId, null);
    }

    private java.util.Optional<LockedLogicalObservation> lockOverlay(
            Connection connection, long observationId, UUID expectedGeneration) throws Exception {
        String generationClause = expectedGeneration == null ? "" : " AND overlay_row.generation_id=?";
        String sql = "SELECT overlay_row.generation_id, overlay_row.datastream_id, overlay_row.foi_id, " +
                "overlay_row.phenomenon_time, overlay_row.result_time, overlay_row.result_json::text, " +
                "COALESCE(overlay_row.schema_json, overlay_schema_blob.schema_json)::text " +
                "FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? AND overlay_row.observation_id=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')" + generationClause +
                " FOR UPDATE OF overlay_row";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, getDatastoreName());
            statement.setInt(2, idScope);
            statement.setLong(3, observationId);
            if (expectedGeneration != null)
                statement.setObject(4, expectedGeneration);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return java.util.Optional.empty();
                UUID generationId = result.getObject(1, UUID.class);
                long dataStreamId = result.getLong(2);
                Long foiId = result.getObject(3, Long.class);
                Instant phenomenonTime = UtcJdbcTime.getInstant(result, 4);
                Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                String resultJson = result.getString(6);
                String schemaJson = result.getString(7);
                ArchivedObservation archived = new ArchivedObservation(
                        idScope, observationId, idScope, dataStreamId,
                        foiId == null ? null : idScope, foiId,
                        phenomenonTime, resultTime, resultJson, "OVERLAY");
                return java.util.Optional.of(new LockedLogicalObservation(
                        generationId, resultJson, phenomenonTime, resultTime,
                        dataStreamId, foiId,
                        toObservation(connection, generationId, schemaJson, archived)));
            }
        }
    }

    private Map<Long, LockedLogicalObservation> lockOverlaysBatch(
            Connection connection, List<Long> sortedIds) throws Exception {
        if (sortedIds.isEmpty())
            return Map.of();
        String sql = "SELECT overlay_row.observation_id, overlay_row.generation_id, " +
                "overlay_row.datastream_id, overlay_row.foi_id, overlay_row.phenomenon_time, " +
                "overlay_row.result_time, overlay_row.result_json::text, " +
                "COALESCE(overlay_row.schema_json, overlay_schema_blob.schema_json)::text " +
                "FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND overlay_row.observation_id=ANY (?::bigint[]) " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED') " +
                "ORDER BY overlay_row.observation_id ASC FOR UPDATE OF overlay_row";
        Map<Long, LockedLogicalObservation> locked = new HashMap<>();
        java.sql.Array ids = connection.createArrayOf("bigint", sortedIds.toArray(Long[]::new));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, getDatastoreName());
            statement.setInt(2, idScope);
            statement.setArray(3, ids);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    long obsId = result.getLong(1);
                    UUID generationId = result.getObject(2, UUID.class);
                    long dataStreamId = result.getLong(3);
                    Long foiId = result.getObject(4, Long.class);
                    Instant phenomenonTime = UtcJdbcTime.getInstant(result, 5);
                    Instant resultTime = UtcJdbcTime.getInstant(result, 6);
                    String resultJson = result.getString(7);
                    String schemaJson = result.getString(8);
                    ArchivedObservation archived = new ArchivedObservation(
                            idScope, obsId, idScope, dataStreamId,
                            foiId == null ? null : idScope, foiId,
                            phenomenonTime, resultTime, resultJson, "OVERLAY");
                    locked.put(obsId, new LockedLogicalObservation(
                            generationId, resultJson, phenomenonTime, resultTime,
                            dataStreamId, foiId,
                            toObservation(connection, generationId, schemaJson, archived)));
                }
            }
        } finally {
            ids.free();
        }
        return locked;
    }

    private java.util.Optional<LockedLogicalObservation> loadLockedLogicalObservation(
            Connection connection, long observationId) throws Exception {
        if (archiveCatalog.isTombstoned(connection, observationId))
            return java.util.Optional.empty();

        java.util.Optional<LockedLogicalObservation> hot =
                lockHotLogicalObservation(connection, observationId);
        if (hot.isPresent())
            return hot;

        java.util.Optional<LockedLogicalObservation> overlay =
                lockColdOverlay(connection, observationId);
        if (overlay.isPresent())
            return overlay;

        return loadLockedColdParquetObservation(connection, observationId);
    }

    private java.util.Optional<LockedLogicalObservation> loadLockedColdParquetObservation(
            Connection connection, long observationId) throws Exception {
        List<ArchiveGeneration> generations = archiveCatalog.candidates(
                connection, MIN_ARCHIVE_TIME, MAX_ARCHIVE_TIME, observationId);
        if (generations.isEmpty())
            return java.util.Optional.empty();
        ObsFilter filter = new ObsFilter.Builder()
                .withInternalIDs(BigId.fromLong(idScope, observationId))
                .build();
        List<ParquetArchiveReader.ArchivedRow> rows =
                archiveReader.read(generations, filter, null, null, 1);
        if (rows.isEmpty())
            return java.util.Optional.empty();
        ParquetArchiveReader.ArchivedRow row = rows.get(0);
        ArchivedObservation archived = row.observation();
        return java.util.Optional.of(new LockedLogicalObservation(
                row.generation().id(), archived.resultJson(),
                archived.phenomenonTime(), archived.resultTime(),
                archived.dataStreamId(), archived.foiId(),
                toObservation(connection, row.generation().id(), null, archived)));
    }

    private IObsData decodeLiveObservation(ArchivedObservation archived) {
        BigId dataStreamId = BigId.fromLong(idScope, archived.dataStreamId());
        IDataStreamInfo schema = getDataStreamInfo(dataStreamId);
        if (schema == null)
            throw new IllegalStateException(
                    "Cannot revalidate observation without datastream schema " + archived.dataStreamId());
        DataBlock result = SerializerUtils.readDataBlockFromJson(
                schema.getRecordStructure(), archived.resultJson());
        ObsData.Builder builder = new ObsData.Builder()
                .withDataStream(dataStreamId)
                .withResult(result);
        if (archived.foiId() != null)
            builder.withFoi(BigId.fromLong(idScope, archived.foiId()));
        if (archived.phenomenonTime() != null)
            builder.withPhenomenonTime(archived.phenomenonTime());
        if (archived.resultTime() != null)
            builder.withResultTime(archived.resultTime());
        return builder.build();
    }

    private final class ColdPointReadCoalescer {
        private static final int MAX_WORKERS = 2;
        private static final long COALESCE_WAIT_NS = TimeUnit.MILLISECONDS.toNanos(4);

        private final Object lock = new Object();
        private final ArrayDeque<PendingColdRead> queue = new ArrayDeque<>();
        private int activeWorkers;

        ParquetArchiveReader.ArchivedRow readOne(long observationId) {
            return readMany(List.of(observationId)).get(observationId);
        }

        Map<Long, ParquetArchiveReader.ArchivedRow> readMany(Collection<Long> observationIds) {
            if (observationIds == null || observationIds.isEmpty())
                return Map.of();
            CompletableFuture<Map<Long, ParquetArchiveReader.ArchivedRow>> future = new CompletableFuture<>();
            List<Long> ids = observationIds.stream().distinct().toList();
            PendingColdRead request = new PendingColdRead(ids, future);
            boolean becomeWorker = false;
            synchronized (lock) {
                queue.addLast(request);
                if (activeWorkers < MAX_WORKERS) {
                    activeWorkers++;
                    becomeWorker = true;
                } else {
                    lock.notifyAll();
                }
            }
            if (becomeWorker)
                runWorkerLoop(future);
            return awaitFuture(future, "Interrupted while reading archived observation");
        }

        private void runWorkerLoop(CompletableFuture<?> ownFuture) {
            boolean registered = true;
            try {
                while (true) {
                    List<PendingColdRead> drained = new ArrayList<>();
                    Set<Long> distinctIds = new HashSet<>();
                    synchronized (lock) {
                        if (!queue.isEmpty() && queue.size() < 4 && !ownFuture.isDone()) {
                            long deadline = System.nanoTime() + COALESCE_WAIT_NS;
                            while (queue.size() < 8) {
                                long remainingNs = deadline - System.nanoTime();
                                if (remainingNs <= 0L)
                                    break;
                                long ms = remainingNs / 1_000_000L;
                                int ns = (int) (remainingNs % 1_000_000L);
                                try {
                                    lock.wait(ms, ns);
                                } catch (InterruptedException interrupted) {
                                    Thread.currentThread().interrupt();
                                    break;
                                }
                            }
                        }
                        while (!queue.isEmpty()) {
                            PendingColdRead next = queue.peekFirst();
                            if (!drained.isEmpty() &&
                                    distinctIds.size() + next.ids().size() > POINT_READ_COALESCER_BATCH) {
                                break;
                            }
                            queue.removeFirst();
                            drained.add(next);
                            distinctIds.addAll(next.ids());
                            if (distinctIds.size() >= POINT_READ_COALESCER_BATCH)
                                break;
                        }
                        if (drained.isEmpty()) {
                            activeWorkers--;
                            registered = false;
                            return;
                        }
                    }
                    try {
                        Map<Long, ParquetArchiveReader.ArchivedRow> resolved = new HashMap<>();
                        List<Long> stillMissing = new ArrayList<>();
                        for (Long id : distinctIds) {
                            ParquetArchiveReader.ArchivedRow cached = getVerifiedCachedColdBase(id);
                            if (cached != null && archiveCatalog.isGenerationQueryVisible(cached.generation().id()))
                                resolved.put(id, cached);
                            else
                                stillMissing.add(id);
                        }
                        if (!stillMissing.isEmpty())
                            resolved.putAll(readColdPointBatchDirect(stillMissing));
                        for (PendingColdRead item : drained) {
                            Map<Long, ParquetArchiveReader.ArchivedRow> sub = new HashMap<>();
                            for (Long id : item.ids()) {
                                ParquetArchiveReader.ArchivedRow row = resolved.get(id);
                                if (row != null)
                                    sub.put(id, row);
                            }
                            item.future().complete(sub);
                        }
                    } catch (Throwable failure) {
                        for (PendingColdRead item : drained)
                            item.future().completeExceptionally(failure);
                    }
                    if (ownFuture.isDone()) {
                        synchronized (lock) {
                            if (queue.isEmpty()) {
                                activeWorkers--;
                                registered = false;
                                return;
                            }
                        }
                    }
                }
            } finally {
                if (registered) {
                    synchronized (lock) {
                        activeWorkers--;
                    }
                }
            }
        }
    }

    private final class OverlayWriteCoalescer {
        private final Object lock = new Object();
        private final ArrayDeque<PendingPut> queue = new ArrayDeque<>();
        private boolean workerActive;

        IObsData submit(BigId key, IObsData observation) {
            CompletableFuture<IObsData> future = new CompletableFuture<>();
            PendingPut request = new PendingPut(key, observation, future);
            boolean becomeWorker = false;
            synchronized (lock) {
                queue.addLast(request);
                if (!workerActive) {
                    workerActive = true;
                    becomeWorker = true;
                }
            }
            if (becomeWorker)
                runWorkerLoop();
            return awaitFuture(future, "Interrupted while updating tiered observation " + key);
        }

        private void runWorkerLoop() {
            while (true) {
                List<PendingPut> batch = new ArrayList<>();
                synchronized (lock) {
                    while (!queue.isEmpty() && batch.size() < OVERLAY_WRITE_COALESCER_BATCH) {
                        batch.add(queue.removeFirst());
                    }
                    if (batch.isEmpty()) {
                        workerActive = false;
                        return;
                    }
                }
                if (batch.size() == 1) {
                    PendingPut single = batch.get(0);
                    try {
                        single.future().complete(putSingleDirect(single.key(), single.observation()));
                    } catch (Throwable failure) {
                        single.future().completeExceptionally(failure);
                    }
                    continue;
                }
                try {
                    List<IObsData> results = putBatchDirect(batch);
                    for (int i = 0; i < batch.size(); i++)
                        batch.get(i).future().complete(results.get(i));
                } catch (Throwable batchFailure) {
                    // Isolate per-caller validation or missing-key failures so a
                    // bad update cannot fail unrelated concurrent callers.
                    for (PendingPut item : batch) {
                        try {
                            item.future().complete(putSingleDirect(item.key(), item.observation()));
                        } catch (Throwable individualFailure) {
                            item.future().completeExceptionally(individualFailure);
                        }
                    }
                }
            }
        }
    }

    private static <T> T awaitFuture(CompletableFuture<T> future, String interruptMessage) {
        try {
            return future.get();
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptMessage, interrupted);
        } catch (ExecutionException execution) {
            Throwable cause = execution.getCause();
            if (cause instanceof RuntimeException runtime)
                throw runtime;
            if (cause instanceof Error err)
                throw err;
            throw new IllegalStateException(interruptMessage, cause);
        }
    }

    private record PendingColdRead(
            List<Long> ids,
            CompletableFuture<Map<Long, ParquetArchiveReader.ArchivedRow>> future) {}
    private record PendingPut(
            BigId key, IObsData observation, CompletableFuture<IObsData> future) {}
    private record PreparedPut(
            BigId key, long id, IObsData observation, OverlayPayload payload) {}
    private record CachedOverlaySchema(
            IDataStreamInfo sourceRef, String schemaJson, String schemaSha256) {}
    private record LockedLogicalObservation(UUID generationId, String resultJson,
                                            Instant phenomenonTime, Instant resultTime,
                                            long dataStreamId, Long foiId,
                                            IObsData observation) {}
    private record LockedHotRow(long observationId, long dataStreamId, Long foiId,
                                Instant phenomenonTime, Instant resultTime,
                                String resultJson, UUID generationId) {}
    private record RemovalBatchResult(long removed, Set<Long> heldObservationIds) {}
    private record OverlayPayload(ArchivedObservation observation, String schemaJson, String schemaSha256) {}

    @Override
    public long countMatchingEntries(ObsFilter filter) {
        if (!archiveConfig.enabled) {
            ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
            if (supportsDirectTieredCount(unbounded)) {
                ResolvedRelatedFilter related = resolveRelatedFilter(unbounded);
                if (!related.matches())
                    return 0;
                boolean parentTableOnly = isAllMutableOnlyDataStreams(related.dataStreams());
                String tbl = getDatastoreName();
                String escapedTbl = tbl.replace("'", "''");
                String excludeTombstoned = "NOT EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                        "WHERE tombstone.subject_type='OBSERVATION' AND tombstone.store_key='" + escapedTbl +
                        "' AND tombstone.id_scope=" + idScope +
                        " AND tombstone.subject_id=" + tbl + ".id::text)";
                long count = countHotRows(related.filter(), parentTableOnly, excludeTombstoned);
                return filter.getLimit() == Long.MAX_VALUE ? count : Math.min(count, filter.getLimit());
            }
            try (Stream<Entry<BigId, IObsData>> stream = selectEntries(filter)) {
                return stream.count();
            }
        }
        if (isUnfiltered(filter))
            return getNumRecords();
        ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        if (supportsDirectTieredCount(unbounded)) {
            boolean hasVisibleCold = archiveCatalog.hasQueryVisibleGenerations();
            boolean hasMutations = archiveCatalog.hasObservationMutations();
            long count = countDirectTiered(unbounded, hasVisibleCold, hasMutations);
            return filter.getLimit() == Long.MAX_VALUE ? count : Math.min(count, filter.getLimit());
        }
        try (Stream<Entry<BigId, IObsData>> stream = selectEntries(filter)) {
            return stream.count();
        }
    }

    private boolean supportsDirectTieredCount(ObsFilter filter) {
        return filter.getValuePredicate() == null &&
                filter.getPhenomenonLocation() == null &&
                filter.getLimit() == Long.MAX_VALUE &&
                (filter.getPhenomenonTime() == null ||
                        (!filter.getPhenomenonTime().isLatestTime() && !filter.getPhenomenonTime().isCurrentTime())) &&
                (filter.getResultTime() == null ||
                        (!filter.getResultTime().isLatestTime() && !filter.getResultTime().isCurrentTime()));
    }

    private long countDirectTiered(ObsFilter unbounded, boolean hasVisibleCold, boolean hasMutations) {
        ResolvedRelatedFilter related = resolveRelatedFilter(unbounded);
        if (!related.matches())
            return 0L;
        ObsFilter resolvedFilter = related.filter();
        Set<Long> dataStreams = related.dataStreams();
        Set<Long> fois = related.fois();
        if (hasMutations)
            hasMutations = archiveCatalog.countObservationMutations(dataStreams) > 0L;
        boolean parentTableOnly = isAllMutableOnlyDataStreams(dataStreams);

        if (!hasVisibleCold && !hasMutations) {
            if (parentTableOnly && immutableStage != null &&
                    immutableStage.supportsFastMutableStreamCount(resolvedFilter, dataStreams, fois)) {
                return immutableStage.countMutableStreamObservations(resolvedFilter, dataStreams);
            }
            return countHotRows(resolvedFilter, parentTableOnly, null);
        }

        long hotUnownedCount;
        if (!hasMutations && parentTableOnly && immutableStage != null &&
                immutableStage.supportsFastMutableStreamCount(resolvedFilter, dataStreams, fois)) {
            hotUnownedCount = immutableStage.countMutableStreamObservations(resolvedFilter, dataStreams);
        } else {
            hotUnownedCount = countHotRows(
                    resolvedFilter,
                    parentTableOnly,
                    buildUnownedHotCountPredicate(parentTableOnly, hasMutations));
        }

        long coldUnmutatedCount = 0L;
        if (hasVisibleCold && !parentTableOnly) {
            ArchiveBounds bounds = archiveBounds(resolvedFilter);
            Instant minResultTime = minResultTimeBound(resolvedFilter);
            Instant maxResultTime = maxResultTimeBound(resolvedFilter);
            Long oneId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() == 1
                    ? resolvedFilter.getInternalIDs().first().getIdAsLong() : null;
            Long minId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() > 1
                    ? resolvedFilter.getInternalIDs().first().getIdAsLong() : null;
            Long maxId = resolvedFilter.getInternalIDs() != null && resolvedFilter.getInternalIDs().size() > 1
                    ? resolvedFilter.getInternalIDs().last().getIdAsLong() : null;
            List<ArchiveGeneration> candidates = pruneAndBackfillLegacyResultTimeCandidates(
                    archiveCatalog.candidates(
                            bounds.start(), bounds.end(), oneId, minId, maxId,
                            dataStreams, minResultTime, maxResultTime),
                    minResultTime);
            if (!candidates.isEmpty()) {
                Map<UUID, Set<Long>> mutatedByGen = hasMutations
                        ? archiveCatalog.mutatedObservationIdsByGeneration(
                                candidates.stream().map(ArchiveGeneration::id).toList())
                        : Map.of();
                Set<Long> allMutatedIds = mutatedByGen.isEmpty()
                        ? Set.of()
                        : mutatedByGen.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
                List<ArchiveGeneration> parquetCandidates = candidates;
                long summarizedColdCount = 0L;
                if (fois == null && oneId == null && minId == null && maxId == null &&
                        (resolvedFilter.getResultTime() == null || !resolvedFilter.getResultTime().isLatestTime())) {
                    JsonCqlEvaluator.SummaryCounterSpec summarySpec = JsonCqlEvaluator.summaryCounterSpec(
                            resolvedFilter.getCQLFilter()).orElse(null);
                    JsonCqlEvaluator.TimeContainedSummarySpec timeContainedSpec =
                            JsonCqlEvaluator.timeContainedSummarySpec(resolvedFilter.getCQLFilter()).orElse(null);
                    if (summarySpec != null || timeContainedSpec != null) {
                        ArchiveCatalog.ColdSummaryPlan summaryPlan = archiveCatalog.summarizeColdCandidates(
                                candidates, resolvedFilter.getPhenomenonTime(),
                                resolvedFilter.getResultTime(),
                                dataStreams, mutatedByGen.keySet(), summarySpec, timeContainedSpec);
                        long mutatedSummaryMatches = summaryPlan.exactMutatedCandidates().isEmpty()
                                ? 0L
                                : countMutatedSummaryBaseMatches(
                                        summaryPlan.exactMutatedCandidates(),
                                        mutatedByGen,
                                        resolvedFilter,
                                        dataStreams,
                                        fois);
                        summarizedColdCount = Math.subtractExact(summaryPlan.summarizedCount(), mutatedSummaryMatches);
                        parquetCandidates = summaryPlan.remainingCandidates();
                    }
                }
                long scannedColdCount = 0L;
                if (!parquetCandidates.isEmpty()) {
                    if (resolvedFilter.getCQLFilter() == null) {
                        long rawCold = archiveReader.count(parquetCandidates, resolvedFilter, dataStreams, fois);
                        long mutatedMatchingCold = 0L;
                        if (!mutatedByGen.isEmpty() && rawCold > 0) {
                            List<ArchiveGeneration> mutatedGens = parquetCandidates.stream()
                                    .filter(generation -> mutatedByGen.containsKey(generation.id()))
                                    .toList();
                            if (!mutatedGens.isEmpty()) {
                                mutatedMatchingCold = archiveReader.countMatchingIds(
                                        mutatedGens, idScope, allMutatedIds, resolvedFilter, dataStreams, fois);
                            }
                        }
                        scannedColdCount = Math.subtractExact(rawCold, mutatedMatchingCold);
                    } else {
                        scannedColdCount = archiveReader.countCql(
                                parquetCandidates, resolvedFilter, dataStreams, fois, allMutatedIds);
                    }
                }
                coldUnmutatedCount = Math.addExact(summarizedColdCount, scannedColdCount);
            }
        }

        long overlayCount = hasMutations
                ? archiveCatalog.countMatchingOverlays(resolvedFilter, dataStreams, fois)
                : 0L;
        long total = Math.addExact(Math.addExact(hotUnownedCount, coldUnmutatedCount), overlayCount);
        if (total < 0)
            throw new IllegalStateException("Negative tiered observation count");
        return total;
    }

    private record MutatedGenSummaryCountKey(
            UUID generationId,
            String generationSha256,
            Set<Long> mutatedIds,
            Set<Long> dataStreams,
            org.geotools.api.filter.Filter cqlFilter) {}

    private long countMutatedSummaryBaseMatches(
            List<ArchiveGeneration> exactMutatedCandidates,
            Map<UUID, Set<Long>> mutatedByGen,
            ObsFilter resolvedFilter,
            Set<Long> dataStreams,
            Set<Long> fois) {
        long total = 0L;
        for (Long matched : countMutatedSummaryBaseMatchesByGeneration(
                exactMutatedCandidates, mutatedByGen, resolvedFilter, dataStreams, fois).values()) {
            total = Math.addExact(total, matched);
        }
        return total;
    }

    private Map<UUID, Long> countMutatedSummaryBaseMatchesByGeneration(
            List<ArchiveGeneration> exactMutatedCandidates,
            Map<UUID, Set<Long>> mutatedByGen,
            ObsFilter resolvedFilter,
            Set<Long> dataStreams,
            Set<Long> fois) {
        if (exactMutatedCandidates == null || exactMutatedCandidates.isEmpty())
            return Map.of();
        Set<Long> immutableStreams = dataStreams == null ? null : Set.copyOf(dataStreams);
        org.geotools.api.filter.Filter cqlFilter = resolvedFilter.getCQLFilter();
        Map<UUID, Long> byGeneration = new HashMap<>(exactMutatedCandidates.size());
        List<ArchiveGeneration> misses = new ArrayList<>();
        Map<UUID, MutatedGenSummaryCountKey> missKeys = new HashMap<>();
        Map<UUID, Set<Long>> missTargets = new HashMap<>();
        for (ArchiveGeneration generation : exactMutatedCandidates) {
            Set<Long> genMutated = mutatedByGen.get(generation.id());
            if (genMutated == null || genMutated.isEmpty())
                continue;
            archiveReader.verifyGenerationObject(generation);
            Set<Long> immutableMutated = Set.copyOf(genMutated);
            MutatedGenSummaryCountKey key = new MutatedGenSummaryCountKey(
                    generation.id(), generation.sha256(), immutableMutated, immutableStreams, cqlFilter);
            Long cached = mutatedGenSummaryCounts.getIfPresent(key);
            if (cached != null) {
                byGeneration.put(generation.id(), cached);
            } else {
                misses.add(generation);
                missKeys.put(generation.id(), key);
                missTargets.put(generation.id(), immutableMutated);
            }
        }
        if (!misses.isEmpty()) {
            Map<UUID, Long> computed = archiveReader.countMatchingIdsCqlByGeneration(
                    misses, missTargets, resolvedFilter, dataStreams, fois);
            for (ArchiveGeneration generation : misses) {
                long matched = computed.getOrDefault(generation.id(), 0L);
                mutatedGenSummaryCounts.put(missKeys.get(generation.id()), matched);
                byGeneration.put(generation.id(), matched);
            }
        }
        return byGeneration;
    }

    private String buildUnownedHotCountPredicate(boolean parentTableOnly, boolean hasMutations) {
        String tbl = getDatastoreName();
        String escapedTbl = tbl.replace("'", "''");
        StringBuilder clause = new StringBuilder("(")
                .append(tbl).append(".archivegenerationid IS NULL OR NOT EXISTS (" +
                        "SELECT 1 FROM oscar_archive_generation g WHERE g.id=")
                .append(tbl).append(".archivegenerationid AND g.store_key='")
                .append(escapedTbl).append("' AND g.id_scope=").append(idScope)
                .append(" AND g.state IN ('ACTIVE','SEALED')))");
        if (archiveConfig.isParquetPrimary() && !parentTableOnly) {
            clause.append(" AND NOT EXISTS (SELECT 1 FROM oscar_obs_stage_owner owner " +
                            "JOIN oscar_archive_generation g ON g.id=owner.generation_id " +
                            "WHERE owner.store_key='").append(escapedTbl)
                    .append("' AND owner.id_scope=").append(idScope)
                    .append(" AND owner.observation_id=").append(tbl).append(".id " +
                            "AND g.store_key=owner.store_key AND g.id_scope=owner.id_scope " +
                            "AND g.state IN ('ACTIVE','SEALED'))");
        }
        if (hasMutations) {
            clause.append(" AND NOT EXISTS (SELECT 1 FROM oscar_obs_overlay overlay_row " +
                            "JOIN oscar_archive_generation g ON g.id=overlay_row.generation_id " +
                            "WHERE overlay_row.observation_id=").append(tbl).append(".id " +
                            "AND overlay_row.store_key='").append(escapedTbl)
                    .append("' AND overlay_row.id_scope=").append(idScope)
                    .append(" AND g.store_key=overlay_row.store_key AND g.id_scope=overlay_row.id_scope " +
                            "AND g.state IN ('ACTIVE','SEALED')) " +
                            "AND NOT EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                            "WHERE tombstone.subject_type='OBSERVATION' " +
                            "AND tombstone.store_key='").append(escapedTbl)
                    .append("' AND tombstone.id_scope=").append(idScope)
                    .append(" AND tombstone.subject_id=").append(tbl).append(".id::text " +
                            "AND tombstone.generation_id IS NOT NULL " +
                            "AND EXISTS (SELECT 1 FROM oscar_archive_generation g " +
                            "WHERE g.id=tombstone.generation_id AND g.store_key=tombstone.store_key " +
                            "AND g.id_scope=tombstone.id_scope AND g.state IN ('ACTIVE','SEALED')))");
        }
        return clause.toString();
    }

    private long countHotRows(ObsFilter filter, boolean parentTableOnly, String extraWhereClause) {
        ParameterizedQuery query = queryBuilder.createParameterizedSelectEntriesCountQuery(
                filter, parentTableOnly, extraWhereClause);
        try (Connection connection = this.connectionManager.getConnection()) {
            connection.setReadOnly(true);
            try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
                query.bind(statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    return resultSet.next() ? resultSet.getLong(1) : 0L;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException("Cannot count hot observations", e);
        }
    }

    private boolean isUnfiltered(ObsFilter filter) {
        return filter.getInternalIDs() == null && filter.getPhenomenonTime() == null &&
                filter.getResultTime() == null && filter.getPhenomenonLocation() == null &&
                filter.getDataStreamFilter() == null && filter.getFoiFilter() == null &&
                filter.getValuePredicate() == null && filter.getCQLFilter() == null &&
                filter.getLimit() == Long.MAX_VALUE;
    }

    @Override
    public long getNumRecords() {
        if (!archiveConfig.enabled)
            return countMatchingEntries(selectAllFilter());
        return archiveCatalog.countTieredRecords(getDatastoreName());
    }
}
