package org.sensorhub.impl.datastore.postgis.archive;

import com.google.common.collect.BoundType;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Reads only catalog-authorized Parquet objects and validates each object before first use. */
public final class ParquetArchiveReader {
    private static final long VERIFICATION_TTL_NANOS = java.time.Duration.ofMinutes(30).toNanos();
    private static final int VERIFICATION_LOCK_STRIPES = 64;
    private static final String METADATA_COLUMNS =
            "filename, id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, profile";
    private static final String FULL_COLUMNS =
            "filename, id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, result_json, profile";

    private final Path root;
    private final TieredArchiveConfig config;
    private final Map<Path, VerificationStamp> verifiedObjects = new ConcurrentHashMap<>();
    private final Object[] verificationLocks = createVerificationLocks();
    private final Map<String, java.util.Optional<Instant>> maxResultTimeBySha256 = new ConcurrentHashMap<>();
    private final AtomicInteger openCursors = new AtomicInteger();
    private volatile DirectReadFaultInjector directReadFaultInjector;

    @FunctionalInterface
    interface DirectReadFaultInjector {
        void beforeDirectRead(Set<String> filenames) throws SQLException;
    }

    void setDirectReadFaultInjectorForTesting(DirectReadFaultInjector injector) {
        this.directReadFaultInjector = injector;
    }

    private static Object[] createVerificationLocks() {
        Object[] locks = new Object[VERIFICATION_LOCK_STRIPES];
        for (int i = 0; i < locks.length; i++)
            locks[i] = new Object();
        return locks;
    }

    public ParquetArchiveReader(Path root) {
        this(root, defaultConfig(root));
    }

    public ParquetArchiveReader(Path root, TieredArchiveConfig config) {
        this.root = root.toAbsolutePath().normalize();
        this.config = Objects.requireNonNull(config, "config");
    }

    private static TieredArchiveConfig defaultConfig(Path root) {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toAbsolutePath().normalize().toString();
        Path normalizedRoot = root.toAbsolutePath().normalize();
        config.duckDbTemporaryDirectory = normalizedRoot.resolveSibling(
                normalizedRoot.getFileName() + ".duckdb-test-tmp").toString();
        return config;
    }

    public List<ArchivedRow> read(ArchiveGeneration generation, ObsFilter filter,
                                  Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        return read(List.of(generation), filter, allowedDataStreams, allowedFois, Long.MAX_VALUE);
    }

    /** Executes one DuckDB query over an explicit catalog-authorized file list. */
    public List<ArchivedRow> read(List<ArchiveGeneration> generations, ObsFilter filter,
                                  Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit) {
        try (Stream<ArchivedRow> rows = stream(generations, filter, allowedDataStreams, allowedFois, limit)) {
            return rows.toList();
        }
    }

    /**
     * Lazily streams one globally ordered DuckDB result set over the explicit,
     * catalog-authorized object list. The cursor closes at exhaustion and is
     * also attached to {@link Stream#close()} for short-circuiting callers.
     */
    public Stream<ArchivedRow> stream(List<ArchiveGeneration> generations, ObsFilter filter,
                                      Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit) {
        if (limit < 0)
            throw new IllegalArgumentException("Archive query limit cannot be negative");
        if (limit == 0 || generations.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return Stream.empty();

        boolean latestSelection = (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
        boolean javaCql = filter.getCQLFilter() != null;
        if (!latestSelection && !javaCql && (limit != Long.MAX_VALUE || generations.size() > 1))
            return streamWindowClusters(generations, filter, allowedDataStreams, allowedFois, limit);
        if (latestSelection && generations.size() > 1 && !javaCql &&
                filter.getValuePredicate() == null && filter.getPhenomenonLocation() == null)
            return streamLatestAcrossWindowClusters(generations, filter, allowedDataStreams, allowedFois, limit);

        // CQL is evaluated against the decoded JSON map. It must run before
        // latest-per-stream selection and before LIMIT or matching rows can be
        // silently lost. Structured predicates remain pushed into Parquet.
        boolean javaLatest = javaCql && latestSelection;
        Stream<ArchivedRow> rows;
        if (generations.size() > 1) {
            rows = streamWindowClusters(generations, filter, allowedDataStreams, allowedFois, Long.MAX_VALUE);
        } else {
            AuthorizedObjects objects = authorize(generations);
            if (objects.byFilename().isEmpty())
                return Stream.empty();
            rows = openRowStream(objects, auth -> queryPlan(auth, filter, allowedDataStreams, allowedFois,
                    true, !javaCql, javaCql ? Long.MAX_VALUE : limit), generations.size());
        }
        if (javaCql)
            rows = rows.filter(row -> matchesCql(filter, row.observation().resultJson()));

        if (javaLatest) {
            Map<Long, ArchivedRow> latest = new HashMap<>();
            try (Stream<ArchivedRow> latestRows = rows) {
                latestRows.forEach(row -> latest.merge(row.observation().dataStreamId(), row,
                        (previous, candidate) -> later(candidate, previous,
                                filter.getResultTime() != null && filter.getResultTime().isLatestTime())
                                ? candidate : previous));
            }
            Stream<ArchivedRow> reduced = latest.values().stream().sorted(rowComparator(filter));
            return limit == Long.MAX_VALUE ? reduced : reduced.limit(limit);
        }
        return (javaCql || generations.size() > 1) && limit != Long.MAX_VALUE ? limitAndClose(rows, limit) : rows;
    }

    /**
     * Opens and verifies only the time-window cluster needed by a query.
     * Revisions and genuinely overlapping windows stay in one DuckDB query;
     * disjoint windows are streamed sequentially so DuckDB memory stays bounded
     * by a single window cluster regardless of multi-year archive depth.
     */
    private Stream<ArchivedRow> streamWindowClusters(
            List<ArchiveGeneration> generations, ObsFilter filter,
            Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit) {
        List<List<ArchiveGeneration>> clusters = windowClusters(generations);
        if (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().descendingOrder())
            java.util.Collections.reverse(clusters);
        if (limit == Long.MAX_VALUE && filter.getCQLFilter() != null && clusters.size() > 1 &&
                JsonCqlEvaluator.hasSelectiveDuckDbPrefilter(filter.getCQLFilter())) {
            clusters = batchOrderedClusters(clusters, MAX_CQL_PAYLOAD_BATCH);
        }
        WindowClusterCursor cursor = new WindowClusterCursor(
                clusters.iterator(), filter, allowedDataStreams, allowedFois, limit);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(cursor::close);
    }

    /**
     * Evaluates latest-per-stream queries across disjoint window clusters from
     * newest to oldest so that once every requested datastream has a non-null
     * timestamp in a newer cluster, older clusters do not need to be scanned.
     */
    private Stream<ArchivedRow> streamLatestAcrossWindowClusters(
            List<ArchiveGeneration> generations, ObsFilter filter,
            Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit) {
        List<List<ArchiveGeneration>> clusters = windowClusters(generations);
        if (clusters.size() <= 1) {
            AuthorizedObjects objects = authorize(generations);
            if (objects.byFilename().isEmpty())
                return Stream.empty();
            return openRowStream(objects, auth -> queryPlan(auth, filter, allowedDataStreams, allowedFois,
                    true, true, limit), generations.size());
        }

        boolean latestResult = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        Map<Long, ArchivedRow> bestByStream = new HashMap<>();
        Set<Long> remainingNonNullPhenomenonStreams = allowedDataStreams != null
                ? new java.util.HashSet<>(allowedDataStreams)
                : null;

        for (int index = clusters.size() - 1; index >= 0; index--) {
            List<ArchiveGeneration> cluster = clusters.get(index);
            Set<Long> clusterStreams;
            List<ArchiveGeneration> clusterGenerations;
            if (allowedDataStreams == null) {
                clusterStreams = null;
                clusterGenerations = cluster;
            } else if (!latestResult) {
                if (remainingNonNullPhenomenonStreams.isEmpty())
                    break;
                clusterStreams = new java.util.HashSet<>(remainingNonNullPhenomenonStreams);
                clusterGenerations = cluster;
            } else {
                clusterStreams = new java.util.HashSet<>();
                Set<UUID> neededGenerationIds = new java.util.HashSet<>();
                for (Long dataStreamId : allowedDataStreams) {
                    ArchivedRow best = bestByStream.get(dataStreamId);
                    if (best == null || best.observation().resultTime() == null) {
                        clusterStreams.add(dataStreamId);
                        for (ArchiveGeneration generation : cluster)
                            neededGenerationIds.add(generation.id());
                    } else {
                        List<ArchiveGeneration> newer = generationsPotentiallyNewerThanResultTime(
                                cluster, best.observation().resultTime());
                        if (!newer.isEmpty()) {
                            clusterStreams.add(dataStreamId);
                            for (ArchiveGeneration generation : newer)
                                neededGenerationIds.add(generation.id());
                        }
                    }
                }
                if (clusterStreams.isEmpty() || neededGenerationIds.isEmpty())
                    continue;
                clusterGenerations = cluster.stream()
                        .filter(generation -> neededGenerationIds.contains(generation.id()))
                        .toList();
            }

            AuthorizedObjects objects = authorize(clusterGenerations);
            if (objects.byFilename().isEmpty())
                continue;
            final Set<Long> effectiveClusterStreams = clusterStreams;
            try (Stream<ArchivedRow> clusterRows = openRowStream(objects,
                    auth -> queryPlan(auth, filter, effectiveClusterStreams, allowedFois,
                            true, true, Long.MAX_VALUE),
                    clusterGenerations.size())) {
                clusterRows.forEach(row -> {
                    long streamId = row.observation().dataStreamId();
                    bestByStream.merge(streamId, row,
                            (previous, candidate) -> later(candidate, previous, latestResult)
                                    ? candidate : previous);
                    if (!latestResult && remainingNonNullPhenomenonStreams != null &&
                            row.observation().phenomenonTime() != null)
                        remainingNonNullPhenomenonStreams.remove(streamId);
                });
            }
        }

        Stream<ArchivedRow> ordered = bestByStream.values().stream().sorted(rowComparator(filter));
        return limit == Long.MAX_VALUE ? ordered : ordered.limit(limit);
    }

    private static final int MAX_RESULT_TIME_METADATA_BATCH = 64;
    private static final int MAX_CQL_PAYLOAD_BATCH = 64;
    private static final int LOCAL_STAGE_COPY_RETRIES = 3;

    /**
     * Returns only those generations whose maximum {@code result_time} is at
     * least {@code minResultTime}, using catalog-persisted {@code max_result_time}
     * (or cached Parquet metadata per immutable generation SHA-256 for legacy
     * generations) so hot-satisfied {@code resultTime=latest} queries avoid
     * scanning historical Parquet payloads.
     */
    public List<ArchiveGeneration> generationsPotentiallyNewerThanResultTime(
            List<ArchiveGeneration> generations, Instant minResultTime) {
        if (generations.isEmpty() || minResultTime == null)
            return generations;
        List<ArchiveGeneration> uncached = new ArrayList<>();
        for (ArchiveGeneration generation : generations) {
            if (generation.maxResultTime() != null) {
                maxResultTimeBySha256.putIfAbsent(
                        generation.sha256(), java.util.Optional.of(generation.maxResultTime()));
            } else if (!maxResultTimeBySha256.containsKey(generation.sha256())) {
                uncached.add(generation);
            }
        }
        for (int offset = 0; offset < uncached.size(); offset += MAX_RESULT_TIME_METADATA_BATCH) {
            int end = Math.min(uncached.size(), offset + MAX_RESULT_TIME_METADATA_BATCH);
            populateMaxResultTimes(uncached.subList(offset, end));
        }
        return generations.stream()
                .filter(generation -> {
                    Instant catalogMax = generation.maxResultTime();
                    if (catalogMax != null)
                        return !catalogMax.isBefore(minResultTime);
                    return maxResultTimeBySha256
                            .getOrDefault(generation.sha256(), java.util.Optional.empty())
                            .map(maxResult -> !maxResult.isBefore(minResultTime))
                            .orElse(false);
                })
                .toList();
    }

    public java.util.Optional<Instant> cachedMaxResultTime(ArchiveGeneration generation) {
        if (generation == null)
            return java.util.Optional.empty();
        if (generation.maxResultTime() != null)
            return java.util.Optional.of(generation.maxResultTime());
        return maxResultTimeBySha256.getOrDefault(generation.sha256(), java.util.Optional.empty());
    }

    private void populateMaxResultTimes(List<ArchiveGeneration> uncached) {
        AuthorizedObjects objects = authorize(uncached);
        if (objects.byFilename().isEmpty())
            return;
        Map<String, java.util.Optional<Instant>> discovered = new HashMap<>();
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "metadata-max-result")) {
            try {
                executeMaxResultTimesQuery(duckSession, objects, true, discovered);
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                discovered.clear();
                executeMaxResultTimesQuery(duckSession, staged, false, discovered);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot inspect max result_time from " + uncached.size() +
                    " archive generation(s)", e);
        }
        for (ArchiveGeneration generation : uncached) {
            maxResultTimeBySha256.putIfAbsent(
                    generation.sha256(),
                    discovered.getOrDefault(generation.sha256(), java.util.Optional.empty()));
        }
    }

    private void executeMaxResultTimesQuery(DuckDbResourcePolicy.Session duckSession,
                                            AuthorizedObjects objects,
                                            boolean directRead,
                                            Map<String, java.util.Optional<Instant>> discovered) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        String fileList = objects.byFilename().keySet().stream()
                .map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        String sql = "SELECT filename, max(COALESCE(result_time, phenomenon_time)) FROM read_parquet([" + fileList +
                "], filename=true) GROUP BY filename";
        try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                     duckSession.connection().prepareStatement(sql), "DuckDB max result time metadata query");
             ResultSet result = statement.executeQuery()) {
            while (nextCancellable(result)) {
                ArchiveGeneration generation = generationForFilename(objects, result.getString(1));
                Instant maxResult = UtcJdbcTime.getDuckDbInstant(result, 2);
                discovered.put(generation.sha256(), java.util.Optional.ofNullable(maxResult));
            }
        }
    }

    private static List<List<ArchiveGeneration>> windowClusters(List<ArchiveGeneration> generations) {
        List<ArchiveGeneration> ordered = generations.stream()
                .sorted(Comparator.comparing(ArchiveGeneration::start)
                        .thenComparing(ArchiveGeneration::end)
                        .thenComparing(ArchiveGeneration::id))
                .toList();
        List<List<ArchiveGeneration>> clusters = new ArrayList<>();
        Instant clusterEnd = null;
        for (ArchiveGeneration generation : ordered) {
            if (clusters.isEmpty() || !generation.start().isBefore(clusterEnd)) {
                clusters.add(new ArrayList<>());
                clusterEnd = generation.end();
            } else if (generation.end().isAfter(clusterEnd)) {
                clusterEnd = generation.end();
            }
            clusters.get(clusters.size() - 1).add(generation);
        }
        return clusters;
    }

    private Stream<ArchivedRow> openRowStream(AuthorizedObjects objects,
                                              Function<AuthorizedObjects, QueryPlan> planFactory,
                                              int generationCount) {
        DuckDbResourcePolicy.Session duckSession = null;
        PreparedStatement statement = null;
        ResultSet result = null;
        try {
            duckSession = DuckDbResourcePolicy.open(config, "cold-query");
            AuthorizedObjects activeObjects = objects;
            try {
                maybeInjectDirectReadFault(objects);
                QueryPlan plan = planFactory.apply(objects);
                statement = ConnectionManager.trackCancellableStatement(
                        duckSession.connection().prepareStatement(plan.sql()), "DuckDB cold query");
                bind(statement, plan.parameters());
                result = statement.executeQuery();
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                closeAndSuppress(result, directError);
                closeAndSuppress(statement, directError);
                result = null;
                statement = null;
                activeObjects = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                QueryPlan stagedPlan = planFactory.apply(activeObjects);
                statement = ConnectionManager.trackCancellableStatement(
                        duckSession.connection().prepareStatement(stagedPlan.sql()), "DuckDB cold query");
                bind(statement, stagedPlan.parameters());
                result = statement.executeQuery();
            }
            RowCursor cursor = new RowCursor(duckSession, statement, result, activeObjects);
            openCursors.incrementAndGet();
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                            Spliterator.ORDERED | Spliterator.NONNULL), false)
                    .onClose(cursor::close);
        } catch (Exception error) {
            closeAndSuppress(result, error);
            closeAndSuppress(statement, error);
            closeAndSuppress(duckSession, error);
            throw new IllegalStateException("Cannot query " + generationCount + " archive generation(s)", error);
        }
    }

    /** Visible for health metrics and resource-lifecycle regression tests. */
    public int getOpenCursorCount() {
        return openCursors.get();
    }

    /**
     * Streams only archive metadata. No observation result JSON is read or
     * decoded, which keeps count/statistics scans bounded by Parquet metadata
     * columns rather than payload size.
     */
    public void forEachMetadata(List<ArchiveGeneration> generations, ObsFilter filter,
                                Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                long limit, Consumer<ArchivedMetadataRow> consumer) {
        if (filter.getCQLFilter() != null || filter.getValuePredicate() != null ||
                filter.getPhenomenonLocation() != null)
            throw new IllegalArgumentException("Payload and spatial predicates require full observation decoding");
        boolean latestSelection = (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
        if (!latestSelection && limit == Long.MAX_VALUE && generations.size() > MAX_RESULT_TIME_METADATA_BATCH) {
            for (List<ArchiveGeneration> batch : batchWindowClusters(generations, MAX_RESULT_TIME_METADATA_BATCH))
                forEachMetadataSingleBatch(batch, filter, allowedDataStreams, allowedFois, Long.MAX_VALUE, consumer);
            return;
        }
        forEachMetadataSingleBatch(generations, filter, allowedDataStreams, allowedFois, limit, consumer);
    }

    private void forEachMetadataSingleBatch(List<ArchiveGeneration> generations, ObsFilter filter,
                                            Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                            long limit, Consumer<ArchivedMetadataRow> consumer) {
        AuthorizedObjects objects = authorize(generations);
        if (objects.byFilename().isEmpty())
            return;
        Function<AuthorizedObjects, QueryPlan> planFactory = auth -> queryPlan(
                auth, filter, allowedDataStreams, allowedFois, false, true, limit,
                limit != Long.MAX_VALUE);
        boolean[] emittedAny = {false};
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "metadata-query")) {
            try {
                executeForEachMetadataPlan(duckSession, planFactory.apply(objects), objects, true, consumer, emittedAny);
            } catch (Exception directError) {
                if (emittedAny[0] || !isTransientReadMemoryError(directError))
                    throw directError;
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                executeForEachMetadataPlan(duckSession, planFactory.apply(staged), staged, false, consumer, emittedAny);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot scan metadata from " + generations.size() +
                    " archive generation(s)", e);
        }
    }

    private void executeForEachMetadataPlan(DuckDbResourcePolicy.Session duckSession,
                                            QueryPlan plan,
                                            AuthorizedObjects objects,
                                            boolean directRead,
                                            Consumer<ArchivedMetadataRow> consumer,
                                            boolean[] emittedAny) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                     duckSession.connection().prepareStatement(plan.sql()), "DuckDB metadata query")) {
            bind(statement, plan.parameters());
            try (ResultSet result = statement.executeQuery()) {
                while (nextCancellable(result)) {
                    ArchiveGeneration generation = generationForFilename(objects, result.getString(1));
                    Integer foiScope = result.getObject(6, Integer.class);
                    Long foiId = result.getObject(7, Long.class);
                    Instant phenomenon = UtcJdbcTime.getDuckDbInstant(result, 8);
                    Instant resultTime = UtcJdbcTime.getDuckDbInstant(result, 9);
                    emittedAny[0] = true;
                    consumer.accept(new ArchivedMetadataRow(generation,
                            result.getInt(2), result.getLong(3), result.getInt(4), result.getLong(5),
                            foiScope, foiId, phenomenon,
                            resultTime, result.getString(10)));
                }
            }
        }
    }

    /** Counts structured matches entirely inside DuckDB without materializing payloads. */
    public long count(List<ArchiveGeneration> generations, ObsFilter filter,
                      Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        if (filter.getCQLFilter() != null || filter.getValuePredicate() != null ||
                filter.getPhenomenonLocation() != null)
            throw new IllegalArgumentException("Payload and spatial predicates require full observation decoding");
        if (generations.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return 0;
        boolean latestSelection = (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
        if (!latestSelection && generations.size() > MAX_RESULT_TIME_METADATA_BATCH) {
            long total = 0;
            for (List<ArchiveGeneration> batch : batchWindowClusters(generations, MAX_RESULT_TIME_METADATA_BATCH)) {
                total += countSingleBatch(batch, filter, allowedDataStreams, allowedFois);
                if (filter.getLimit() != Long.MAX_VALUE && total >= filter.getLimit())
                    return filter.getLimit();
            }
            return total;
        }
        return countSingleBatch(generations, filter, allowedDataStreams, allowedFois);
    }

    /**
     * Counts how many specific observation IDs in {@code targetIds} also match
     * the structured {@code filter} inside {@code generations}, used to subtract
     * sparse overlaid/tombstoned base rows from a metadata-only cold count.
     */
    public long countMatchingIds(List<ArchiveGeneration> generations, int idScope,
                                 Set<Long> targetIds, ObsFilter filter,
                                 Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        if (generations.isEmpty() || targetIds == null || targetIds.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return 0;
        List<Long> effectiveIds;
        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty()) {
            effectiveIds = filter.getInternalIDs().stream()
                    .filter(id -> id.getScope() == idScope && targetIds.contains(id.getIdAsLong()))
                    .map(BigId::getIdAsLong)
                    .sorted()
                    .toList();
            if (effectiveIds.isEmpty())
                return 0;
        } else {
            effectiveIds = targetIds.stream().sorted().toList();
        }

        long total = 0;
        final int idBatchSize = 1_000;
        for (int offset = 0; offset < effectiveIds.size(); offset += idBatchSize) {
            List<BigId> chunk = effectiveIds.subList(offset, Math.min(effectiveIds.size(), offset + idBatchSize))
                    .stream()
                    .map(id -> BigId.fromLong(idScope, id))
                    .toList();
            ObsFilter chunkFilter = ObsFilter.Builder.from(filter)
                    .withInternalIDs(chunk)
                    .withLimit(Long.MAX_VALUE)
                    .build();
            total = Math.addExact(total, count(generations, chunkFilter, allowedDataStreams, allowedFois));
        }
        return total;
    }

    /**
     * Counts cold archive observations matching structured + CQL predicates
     * without constructing SWE {@code DataBlock} instances or sorting rows.
     */
    public long countCql(List<ArchiveGeneration> generations, ObsFilter filter,
                         Set<Long> allowedDataStreams, Set<Long> allowedFois,
                         Set<Long> excludedIds) {
        if (filter.getValuePredicate() != null || filter.getPhenomenonLocation() != null)
            throw new IllegalArgumentException("Value and spatial predicates require full observation decoding");
        if ((filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime()))
            throw new IllegalArgumentException("Latest selection requires per-stream reduction");
        if (generations.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return 0;

        long total = 0;
        Set<Long> exclusions = excludedIds == null ? Set.of() : excludedIds;
        for (List<ArchiveGeneration> batch : batchWindowClusters(generations, MAX_CQL_PAYLOAD_BATCH)) {
            total = Math.addExact(total, countCqlSingleBatch(
                    batch, filter, allowedDataStreams, allowedFois, exclusions));
            if (filter.getLimit() != Long.MAX_VALUE && total >= filter.getLimit())
                return filter.getLimit();
        }
        return total;
    }

    private long countCqlSingleBatch(List<ArchiveGeneration> generations, ObsFilter filter,
                                     Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                     Set<Long> excludedIds) {
        AuthorizedObjects objects = authorize(generations);
        if (objects.byFilename().isEmpty())
            return 0;
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "cold-cql-count")) {
            try {
                return executeCountCqlPlan(
                        duckSession, cqlCountPlan(objects, filter, allowedDataStreams, allowedFois),
                        objects, filter, excludedIds, true);
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                return executeCountCqlPlan(
                        duckSession, cqlCountPlan(staged, filter, allowedDataStreams, allowedFois),
                        staged, filter, excludedIds, false);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot count CQL matches from " + generations.size() +
                    " archive generation(s)", e);
        }
    }

    private QueryPlan cqlCountPlan(AuthorizedObjects objects, ObsFilter filter,
                                   Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        String fileList = objects.byFilename().keySet().stream()
                .map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        StringBuilder sql = new StringBuilder("SELECT id_value, result_json FROM read_parquet([")
                .append(fileList).append("]) WHERE true");
        List<Object> parameters = new ArrayList<>();
        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty())
            appendIn(sql, "id_value", filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList(), parameters);
        if (allowedDataStreams != null)
            appendIn(sql, "datastream_id", allowedDataStreams, parameters);
        if (allowedFois != null) {
            Set<Long> explicitFois = allowedFois.stream().filter(id -> id != 0L).collect(Collectors.toSet());
            if (allowedFois.contains(0L)) {
                sql.append(" AND (foi_id IS NULL");
                if (!explicitFois.isEmpty()) {
                    sql.append(" OR foi_id IN (");
                    appendPlaceholders(sql, explicitFois.size());
                    sql.append(')');
                    parameters.addAll(explicitFois);
                }
                sql.append(')');
            } else {
                appendIn(sql, "foi_id", explicitFois, parameters);
            }
        }
        appendTime(sql, "phenomenon_time", filter.getPhenomenonTime(), parameters);
        appendTime(sql, "result_time", filter.getResultTime(), parameters);
        if (filter.getCQLFilter() != null)
            JsonCqlEvaluator.appendDuckDbPrefilter(filter.getCQLFilter(), "result_json", sql, parameters);
        return new QueryPlan(sql.toString(), parameters);
    }

    private long executeCountCqlPlan(DuckDbResourcePolicy.Session duckSession,
                                     QueryPlan plan,
                                     AuthorizedObjects objects,
                                     ObsFilter filter,
                                     Set<Long> excludedIds,
                                     boolean directRead) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        long matched = 0;
        try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                     duckSession.connection().prepareStatement(plan.sql()), "DuckDB cold CQL count")) {
            bind(statement, plan.parameters());
            try (ResultSet result = statement.executeQuery()) {
                while (nextCancellable(result)) {
                    long id = result.getLong(1);
                    if (excludedIds.contains(id))
                        continue;
                    if (matchesCql(filter, result.getString(2))) {
                        matched = Math.addExact(matched, 1L);
                        if (filter.getLimit() != Long.MAX_VALUE && matched >= filter.getLimit())
                            return filter.getLimit();
                    }
                }
            }
            return matched;
        }
    }

    private long countSingleBatch(List<ArchiveGeneration> generations, ObsFilter filter,
                                  Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        AuthorizedObjects objects = authorize(generations);
        if (objects.byFilename().isEmpty())
            return 0;
        ObsFilter unbounded = ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        Function<AuthorizedObjects, QueryPlan> planFactory = auth -> queryPlan(
                auth, unbounded, allowedDataStreams, allowedFois, false, true, Long.MAX_VALUE, false);
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "cold-count")) {
            try {
                return executeCountPlan(duckSession, planFactory.apply(objects), objects, filter, true);
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                return executeCountPlan(duckSession, planFactory.apply(staged), staged, filter, false);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot count " + generations.size() + " archive generation(s)", e);
        }
    }

    private long executeCountPlan(DuckDbResourcePolicy.Session duckSession,
                                  QueryPlan plan,
                                  AuthorizedObjects objects,
                                  ObsFilter filter,
                                  boolean directRead) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                     duckSession.connection().prepareStatement(
                             "SELECT count(*) FROM (" + plan.sql() + ") archive_matches"),
                     "DuckDB cold count")) {
            bind(statement, plan.parameters());
            try (ResultSet result = statement.executeQuery()) {
                long count = nextCancellable(result) ? result.getLong(1) : 0;
                return filter.getLimit() == Long.MAX_VALUE ? count : Math.min(count, filter.getLimit());
            }
        }
    }

    private void maybeInjectDirectReadFault(AuthorizedObjects objects) throws SQLException {
        DirectReadFaultInjector injector = directReadFaultInjector;
        if (injector != null)
            injector.beforeDirectRead(objects.byFilename().keySet());
    }

    static boolean isTransientReadMemoryError(Throwable error) {
        int depth = 0;
        for (Throwable current = error; current != null && depth < 16; current = current.getCause(), depth++) {
            String message = current.getMessage();
            if (message == null)
                continue;
            String normalized = message.toLowerCase(Locale.ROOT);
            if (normalized.contains("cannot allocate memory") &&
                    (normalized.contains("could not read from file") || normalized.contains("io error"))) {
                return true;
            }
        }
        return false;
    }

    /**
     * Copies already-authorized Parquet objects into the bounded DuckDB session's
     * private local temporary directory using 16 KiB stream reads (safe on WSL2
     * 9p drvFs where multi-megabyte {@code pread} calls can fail with {@code ENOMEM})
     * and re-verifies each object's SHA-256 digest during staging.
     */
    private AuthorizedObjects stageAuthorizedObjectsLocally(AuthorizedObjects objects, Path operationDirectory)
            throws Exception {
        Path normalizedOperationDir = operationDirectory.toAbsolutePath().normalize();
        Path stageDir = normalizedOperationDir.resolve("staged-" + UUID.randomUUID()).normalize();
        if (!stageDir.getParent().equals(normalizedOperationDir))
            throw new IllegalStateException("Unsafe local Parquet stage directory: " + stageDir);
        Files.createDirectory(stageDir);
        Map<String, ArchiveGeneration> staged = new LinkedHashMap<>();
        int index = 0;
        for (ArchiveGeneration generation : objects.byFilename().values()) {
            ConnectionManager.throwIfCancellableOperationCancelled();
            Path source = resolveObject(generation.objectPath());
            Path target = stageDir.resolve("object-" + (index++) + "-" + generation.id() + ".parquet").normalize();
            if (!target.getParent().equals(stageDir))
                throw new IllegalStateException("Unsafe staged Parquet path: " + target);
            copyAndVerifyParquet(source, target, generation.sha256());
            staged.put(canonicalFilename(target), generation);
        }
        return new AuthorizedObjects(staged);
    }

    private static void copyAndVerifyParquet(Path source, Path target, String expectedSha256) throws Exception {
        for (int attempt = 1; ; attempt++) {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
                 var output = Files.newOutputStream(
                         target,
                         StandardOpenOption.CREATE,
                         StandardOpenOption.TRUNCATE_EXISTING,
                         StandardOpenOption.WRITE,
                         LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[ArchiveExporter.SHA256_READ_BUFFER_BYTES];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                        output.write(buffer, 0, read);
                    }
                }
                String actualSha256 = HexFormat.of().formatHex(digest.digest());
                if (!expectedSha256.equals(actualSha256)) {
                    Files.deleteIfExists(target);
                    throw new IllegalStateException("Archive checksum mismatch while staging object: " + source);
                }
                return;
            } catch (IOException error) {
                Files.deleteIfExists(target);
                if (attempt >= LOCAL_STAGE_COPY_RETRIES ||
                        !ArchiveExporter.isTransientAllocationIoError(error)) {
                    throw error;
                }
                Thread.sleep(20L * attempt);
            }
        }
    }

    private static List<List<ArchiveGeneration>> batchWindowClusters(
            List<ArchiveGeneration> generations, int targetBatchSize) {
        return batchOrderedClusters(windowClusters(generations), targetBatchSize);
    }

    private static List<List<ArchiveGeneration>> batchOrderedClusters(
            List<List<ArchiveGeneration>> orderedClusters, int targetBatchSize) {
        List<List<ArchiveGeneration>> batches = new ArrayList<>();
        List<ArchiveGeneration> current = new ArrayList<>();
        for (List<ArchiveGeneration> cluster : orderedClusters) {
            if (!current.isEmpty() && current.size() + cluster.size() > targetBatchSize) {
                batches.add(current);
                current = new ArrayList<>();
            }
            current.addAll(cluster);
        }
        if (!current.isEmpty())
            batches.add(current);
        return batches;
    }

    private AuthorizedObjects authorize(List<ArchiveGeneration> generations) {
        Map<String, ArchiveGeneration> objects = new LinkedHashMap<>();
        for (ArchiveGeneration generation : generations) {
            Path object = resolveObject(generation.objectPath());
            verifyOnce(object, generation.sha256());
            String filename = canonicalFilename(object);
            ArchiveGeneration previous = objects.putIfAbsent(filename, generation);
            if (previous != null && !previous.id().equals(generation.id()))
                throw new IllegalStateException("Multiple archive generations reference the same object: " + object);
        }
        return new AuthorizedObjects(objects);
    }

    private QueryPlan queryPlan(AuthorizedObjects objects, ObsFilter filter,
                                Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                boolean includeResult, boolean applyLatest, long limit) {
        return queryPlan(objects, filter, allowedDataStreams, allowedFois,
                includeResult, applyLatest, limit, true);
    }

    private QueryPlan queryPlan(AuthorizedObjects objects, ObsFilter filter,
                                Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                boolean includeResult, boolean applyLatest, long limit,
                                boolean ordered) {
        String columns = includeResult ? FULL_COLUMNS : METADATA_COLUMNS;
        String fileList = objects.byFilename().keySet().stream().map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        StringBuilder source = new StringBuilder("SELECT ").append(columns)
                .append(" FROM read_parquet([").append(fileList).append("], filename=true) WHERE true");
        List<Object> parameters = new ArrayList<>();

        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty())
            appendIn(source, "id_value", filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList(), parameters);
        if (allowedDataStreams != null)
            appendIn(source, "datastream_id", allowedDataStreams, parameters);
        if (allowedFois != null) {
            Set<Long> explicitFois = allowedFois.stream().filter(id -> id != 0L).collect(Collectors.toSet());
            if (allowedFois.contains(0L)) {
                source.append(" AND (foi_id IS NULL");
                if (!explicitFois.isEmpty()) {
                    source.append(" OR foi_id IN (");
                    appendPlaceholders(source, explicitFois.size());
                    source.append(')');
                    parameters.addAll(explicitFois);
                }
                source.append(')');
            } else {
                appendIn(source, "foi_id", explicitFois, parameters);
            }
        }
        appendTime(source, "phenomenon_time", filter.getPhenomenonTime(), parameters);
        appendTime(source, "result_time", filter.getResultTime(), parameters);
        if (includeResult && filter.getCQLFilter() != null)
            JsonCqlEvaluator.appendDuckDbPrefilter(filter.getCQLFilter(), "result_json", source, parameters);

        boolean latestPhenomenon = applyLatest && filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().isLatestTime();
        boolean latestResult = applyLatest && filter.getResultTime() != null && filter.getResultTime().isLatestTime();
        StringBuilder sql;
        if (latestPhenomenon || latestResult) {
            String latestColumn = latestResult ? "result_time" : "phenomenon_time";
            sql = new StringBuilder("SELECT ").append(columns).append(" FROM (SELECT ").append(columns)
                    .append(", row_number() OVER (PARTITION BY datastream_id ORDER BY ")
                    .append(latestColumn).append(" DESC NULLS LAST, id_value DESC) AS archive_rank FROM (")
                    .append(source).append(") filtered) ranked WHERE archive_rank=1");
        } else {
            sql = source;
        }
        if (ordered) {
            sql.append(" ORDER BY phenomenon_time ")
                    .append(filter.getPhenomenonTime() != null && filter.getPhenomenonTime().descendingOrder()
                            ? "DESC" : "ASC")
                    .append(" NULLS FIRST, id_value");
        }
        if (limit != Long.MAX_VALUE) {
            if (limit < 0)
                throw new IllegalArgumentException("Archive query limit cannot be negative");
            sql.append(" LIMIT ?");
            parameters.add(limit);
        }
        return new QueryPlan(sql.toString(), parameters);
    }

    private static void bind(PreparedStatement statement, List<Object> parameters) throws Exception {
        for (int i = 0; i < parameters.size(); i++) {
            Object value = parameters.get(i);
            if (value instanceof Instant instant)
                UtcJdbcTime.setDuckDbInstant(statement, i + 1, instant);
            else
                statement.setObject(i + 1, value);
        }
    }

    private static boolean later(ArchivedRow candidate, ArchivedRow previous, boolean resultTime) {
        Instant candidateTime = resultTime ? candidate.observation().resultTime() : candidate.observation().phenomenonTime();
        Instant previousTime = resultTime ? previous.observation().resultTime() : previous.observation().phenomenonTime();
        if (candidateTime == null)
            return false;
        if (previousTime == null)
            return true;
        int compared = candidateTime.compareTo(previousTime);
        return compared > 0 || (compared == 0 && candidate.observation().id() > previous.observation().id());
    }

    private static Comparator<ArchivedRow> rowComparator(ObsFilter filter) {
        boolean descending = filter.getPhenomenonTime() != null && filter.getPhenomenonTime().descendingOrder();
        Comparator<Instant> timeOrder = descending ? Comparator.nullsFirst(Comparator.reverseOrder()) :
                Comparator.nullsFirst(Comparator.naturalOrder());
        Comparator<ArchivedRow> comparator = Comparator.comparing(
                row -> row.observation().phenomenonTime(), timeOrder);
        return comparator.thenComparingLong(row -> row.observation().id());
    }

    private static boolean matchesCql(ObsFilter filter, String resultJson) {
        return JsonCqlEvaluator.matches(filter.getCQLFilter(), resultJson);
    }

    private ArchiveGeneration generationForFilename(AuthorizedObjects objects, String returnedFilename) {
        String filename = canonicalFilename(Path.of(returnedFilename));
        ArchiveGeneration generation = objects.byFilename().get(filename);
        if (generation == null)
            throw new IllegalStateException("DuckDB returned an object outside the authorized file list: " + returnedFilename);
        return generation;
    }

    private static String canonicalFilename(Path path) {
        return path.toAbsolutePath().normalize().toString();
    }

    private Path resolveObject(Path relative) {
        if (relative.isAbsolute())
            throw new IllegalStateException("Archive catalog contains a non-portable absolute object path");
        Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root))
            throw new IllegalStateException("Archive object escapes configured root");
        try {
            Path realRoot = root.toRealPath();
            Path current = root;
            for (Path component : root.relativize(resolved)) {
                current = current.resolve(component);
                if (Files.isSymbolicLink(current))
                    throw new IllegalStateException("Archive object path contains a symbolic link: " + current);
            }
            // Every component beneath the configured root was explicitly
            // rejected if it was a symlink. Follow only aliases in the root
            // itself (for example macOS /tmp -> /private/tmp) so both paths
            // are compared in the same canonical namespace.
            Path realObject = resolved.toRealPath();
            if (!realObject.startsWith(realRoot))
                throw new IllegalStateException("Archive object resolves outside configured root");
            return realObject;
        } catch (IllegalStateException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve archive object " + resolved, e);
        }
    }

    private void verifyOnce(Path object, String expectedSha256) {
        try {
            BasicFileAttributes before = readAttributes(object);
            long now = System.nanoTime();
            VerificationStamp cached = verifiedObjects.get(object);
            if (cached != null && cached.matches(expectedSha256, before, now))
                return;
            Object lock = verificationLocks[object.hashCode() & (VERIFICATION_LOCK_STRIPES - 1)];
            synchronized (lock) {
                before = readAttributes(object);
                now = System.nanoTime();
                cached = verifiedObjects.get(object);
                if (cached != null && cached.matches(expectedSha256, before, now))
                    return;
                String actual = ArchiveExporter.sha256(object);
                if (!expectedSha256.equals(actual))
                    throw new IllegalStateException("Archive checksum mismatch: " + object);
                BasicFileAttributes after = readAttributes(object);
                if (!sameObject(before, after))
                    throw new IllegalStateException("Archive object changed while its checksum was being verified: " + object);
                verifiedObjects.put(object, VerificationStamp.from(expectedSha256, after, now));
            }
        } catch (Exception e) {
            verifiedObjects.remove(object);
            throw new IllegalStateException("Cannot verify archive object " + object, e);
        }
    }

    private static BasicFileAttributes readAttributes(Path object) throws Exception {
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

    private static void appendTime(StringBuilder sql, String column, TemporalFilter filter, List<Object> parameters) {
        // Current time is a concrete instant captured by TemporalFilter on
        // first access and must be pushed to cold storage just like hot SQL.
        if (filter == null || filter.isLatestTime())
            return;
        Instant minimum = filter.getMin();
        Instant maximum = filter.getMax();
        if (minimum != null && !Instant.MIN.equals(minimum)) {
            String operator = filter.getRange().lowerBoundType() == BoundType.CLOSED ? ">=" : ">";
            sql.append(" AND ").append(column).append(' ').append(operator).append(" ?");
            parameters.add(minimum);
        }
        if (maximum != null && !Instant.MAX.equals(maximum)) {
            String operator = filter.getRange().upperBoundType() == BoundType.CLOSED ? "<=" : "<";
            sql.append(" AND ").append(column).append(' ').append(operator).append(" ?");
            parameters.add(maximum);
        }
    }

    private static void appendIn(StringBuilder sql, String column, Collection<Long> values, List<Object> parameters) {
        if (values.isEmpty()) {
            sql.append(" AND false");
            return;
        }
        sql.append(" AND ").append(column).append(" IN (");
        appendPlaceholders(sql, values.size());
        sql.append(')');
        parameters.addAll(values);
    }

    private static void appendPlaceholders(StringBuilder sql, int count) {
        for (int i = 0; i < count; i++) {
            if (i > 0)
                sql.append(',');
            sql.append('?');
        }
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static void closeAndSuppress(AutoCloseable closeable, Throwable primary) {
        if (closeable == null)
            return;
        try {
            closeable.close();
        } catch (Throwable cleanupError) {
            if (cleanupError != primary)
                primary.addSuppressed(cleanupError);
        }
    }

    /**
     * Checks cancellation on both sides of a raw DuckDB result-set advance.
     * Statement.cancel() remains responsible for interrupting a next() call
     * that is actively executing inside the driver; the second checkpoint
     * covers results that DuckDB materialized before cancellation arrived.
     */
    private static boolean nextCancellable(ResultSet result) throws SQLException {
        ConnectionManager.throwIfCancellableOperationCancelled();
        boolean available = result.next();
        ConnectionManager.throwIfCancellableOperationCancelled();
        return available;
    }

    private static Throwable closeAndCollect(AutoCloseable closeable, Throwable failure) {
        if (closeable == null)
            return failure;
        try {
            closeable.close();
        } catch (Throwable cleanupError) {
            if (failure == null)
                return cleanupError;
            if (cleanupError != failure)
                failure.addSuppressed(cleanupError);
        }
        return failure;
    }

    /**
     * Unlike {@link Stream#limit(long)}, this wrapper closes a live DuckDB
     * cursor immediately after returning the last requested row. It also
     * closes on source exhaustion, failure, and an explicit stream close.
     */
    private static <T> Stream<T> limitAndClose(Stream<T> source, long limit) {
        ClosingLimitIterator<T> iterator = new ClosingLimitIterator<>(source, limit);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(iterator::close);
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

    private final class WindowClusterCursor implements Iterator<ArchivedRow>, AutoCloseable {
        private final Iterator<List<ArchiveGeneration>> clusters;
        private final ObsFilter filter;
        private final Set<Long> allowedDataStreams;
        private final Set<Long> allowedFois;
        private final AtomicBoolean closed = new AtomicBoolean();
        private Stream<ArchivedRow> currentStream;
        private Iterator<ArchivedRow> currentRows;
        private long remaining;

        WindowClusterCursor(Iterator<List<ArchiveGeneration>> clusters, ObsFilter filter,
                            Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit) {
            this.clusters = clusters;
            this.filter = filter;
            this.allowedDataStreams = allowedDataStreams;
            this.allowedFois = allowedFois;
            this.remaining = limit;
        }

        @Override
        public boolean hasNext() {
            if (closed.get() || remaining == 0)
                return false;
            try {
                while (true) {
                    if (currentRows != null && currentRows.hasNext())
                        return true;
                    closeCurrent();
                    if (!clusters.hasNext()) {
                        close();
                        return false;
                    }
                    List<ArchiveGeneration> cluster = clusters.next();
                    AuthorizedObjects objects = authorize(cluster);
                    boolean javaCql = filter.getCQLFilter() != null;
                    final long clusterLimit = javaCql ? Long.MAX_VALUE : remaining;
                    currentStream = openRowStream(objects,
                            auth -> queryPlan(auth, filter, allowedDataStreams, allowedFois,
                                    true, !javaCql, clusterLimit),
                            cluster.size());
                    currentRows = currentStream.iterator();
                }
            } catch (RuntimeException | Error error) {
                close();
                throw error;
            }
        }

        @Override
        public ArchivedRow next() {
            if (!hasNext())
                throw new NoSuchElementException();
            ArchivedRow value = currentRows.next();
            if (remaining != Long.MAX_VALUE) {
                remaining--;
                if (remaining == 0)
                    close();
            }
            return value;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            closeCurrent();
        }

        private void closeCurrent() {
            if (currentStream != null)
                currentStream.close();
            currentStream = null;
            currentRows = null;
        }
    }

    private final class RowCursor implements Iterator<ArchivedRow>, AutoCloseable {
        private final DuckDbResourcePolicy.Session duckSession;
        private final PreparedStatement statement;
        private final ResultSet result;
        private final AuthorizedObjects objects;
        private final AtomicBoolean closed = new AtomicBoolean();
        private ArchivedRow next;
        private boolean prefetched;

        RowCursor(DuckDbResourcePolicy.Session duckSession, PreparedStatement statement, ResultSet result,
                  AuthorizedObjects objects) {
            this.duckSession = duckSession;
            this.statement = statement;
            this.result = result;
            this.objects = objects;
        }

        @Override
        public boolean hasNext() {
            if (prefetched)
                return true;
            if (closed.get())
                return false;
            try {
                if (nextCancellable(result)) {
                    ArchiveGeneration generation = generationForFilename(objects, result.getString(1));
                    Integer foiScope = result.getObject(6, Integer.class);
                    Long foiId = result.getObject(7, Long.class);
                    next = new ArchivedRow(generation, new ArchivedObservation(
                            result.getInt(2), result.getLong(3), result.getInt(4), result.getLong(5),
                            foiScope, foiId, UtcJdbcTime.getDuckDbInstant(result, 8),
                            UtcJdbcTime.getDuckDbInstant(result, 9), result.getString(10), result.getString(11)));
                    prefetched = true;
                    return true;
                }
            } catch (Exception error) {
                closeAndSuppress(this, error);
                throw new IllegalStateException("Cannot decode archived observation", error);
            }
            close();
            return false;
        }

        @Override
        public ArchivedRow next() {
            if (!hasNext())
                throw new NoSuchElementException();
            ArchivedRow value = next;
            next = null;
            prefetched = false;
            return value;
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            Throwable failure = null;
            try {
                failure = closeAndCollect(result, failure);
                failure = closeAndCollect(statement, failure);
                failure = closeAndCollect(duckSession, failure);
            } finally {
                openCursors.decrementAndGet();
                next = null;
                prefetched = false;
            }
            if (failure != null)
                throw new IllegalStateException("Cannot close archive query resources", failure);
        }
    }

    private record QueryPlan(String sql, List<Object> parameters) {}
    private record AuthorizedObjects(Map<String, ArchiveGeneration> byFilename) {}
    private record VerificationStamp(String expectedSha256, long size,
                                     java.nio.file.attribute.FileTime lastModified,
                                     Object fileKey, long verifiedAtNanos) {
        static VerificationStamp from(String expectedSha256, BasicFileAttributes attributes,
                                      long verifiedAtNanos) {
            return new VerificationStamp(expectedSha256, attributes.size(),
                    attributes.lastModifiedTime(), attributes.fileKey(), verifiedAtNanos);
        }

        boolean matches(String expected, BasicFileAttributes attributes, long now) {
            long age = now - verifiedAtNanos;
            return expectedSha256.equals(expected) && size == attributes.size() &&
                    lastModified.equals(attributes.lastModifiedTime()) &&
                    Objects.equals(fileKey, attributes.fileKey()) && age >= 0 &&
                    age <= VERIFICATION_TTL_NANOS;
        }
    }

    public record ArchivedRow(ArchiveGeneration generation, ArchivedObservation observation) {}

    public record ArchivedMetadataRow(ArchiveGeneration generation, int idScope, long id,
                                      int dataStreamScope, long dataStreamId,
                                      Integer foiScope, Long foiId, Instant phenomenonTime,
                                      Instant resultTime, String profile) {}
}
