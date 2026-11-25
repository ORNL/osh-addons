package org.sensorhub.impl.datastore.postgis.archive;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.BoundType;
import org.geotools.api.filter.Filter;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Reads only catalog-authorized Parquet objects and validates each object before first use. */
public final class ParquetArchiveReader {
    private static final long VERIFICATION_TTL_NANOS = java.time.Duration.ofMinutes(30).toNanos();
    private static final int VERIFICATION_LOCK_STRIPES = 64;
    private static final int AUTHORIZE_PARALLELISM = 8;
    private static final int MAX_CQL_COUNT_CACHE_ENTRIES = 65_536;
    private static final AtomicInteger AUTHORIZE_THREAD_COUNTER = new AtomicInteger();
    private static final ExecutorService AUTHORIZE_EXECUTOR = Executors.newFixedThreadPool(
            AUTHORIZE_PARALLELISM, runnable -> {
                Thread thread = new Thread(
                        runnable, "oscar-archive-authorize-" + AUTHORIZE_THREAD_COUNTER.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            });
    private static final String METADATA_COLUMNS =
            "filename, id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, profile";
    private static final String FULL_COLUMNS =
            "filename, id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, result_json, profile";

    /**
     * Per-generation base row and alarm summary counters used to prune
     * zero-match generations in memory before opening a DuckDB session.
     */
    public record GenerationSummary(
            boolean populated,
            long rowCount,
            long gammaAlarmCount,
            long neutronAlarmCount,
            long gammaNeutronAlarmCount,
            long gammaTrueCount,
            long neutronTrueCount) {

        public GenerationSummary(long rowCount, long gammaAlarmCount, long neutronAlarmCount) {
            this(true, rowCount, gammaAlarmCount, neutronAlarmCount, 0L,
                    Math.max(0L, gammaAlarmCount), Math.max(0L, neutronAlarmCount));
        }

        public GenerationSummary(
                long rowCount,
                long gammaAlarmCount,
                long neutronAlarmCount,
                long gammaNeutronAlarmCount) {
            this(true, rowCount, gammaAlarmCount, neutronAlarmCount, gammaNeutronAlarmCount,
                    saturateNonNegativeAdd(gammaAlarmCount, gammaNeutronAlarmCount),
                    saturateNonNegativeAdd(neutronAlarmCount, gammaNeutronAlarmCount));
        }

        private static long saturateNonNegativeAdd(long a, long b) {
            long x = Math.max(0L, a);
            long y = Math.max(0L, b);
            long sum = x + y;
            return sum < 0L ? Long.MAX_VALUE : sum;
        }
    }

    private record CqlGenerationCountKey(
            String sha256,
            String streamsKey,
            String foisKey,
            String cqlKey,
            boolean nonNullPhenomenonRequired,
            boolean nonNullResultTimeRequired,
            String excludedIdsDigest) {}

    static final long MIN_LOCAL_9P_CACHE_MAX_BYTES = 2_147_483_648L;
    static final long MAX_LOCAL_9P_CACHE_MAX_BYTES = 8_589_934_592L;
    static final long LOW_DISK_FLOOR_LOCAL_9P_CACHE_BYTES = 268_435_456L;
    static final long LOW_DISK_THRESHOLD_BYTES = 16L * 1024L * 1024L * 1024L;
    static final String PROP_LOCAL_9P_CACHE_MAX_BYTES = "oscar.archive.local9pCacheMaxBytes";
    static final String ENV_LOCAL_9P_CACHE_MAX_BYTES = "OSCAR_LOCAL_9P_CACHE_MAX_BYTES";
    private static final Path DEFAULT_STATE_9P_CACHE_DIR = Path.of("/var/lib/oscar/.cache/oscar-9p-parquet-cache");
    private static final Path DEFAULT_LOCAL_9P_CACHE_DIR = Path.of("/var/tmp/oscar-9p-parquet-cache");
    private static final Pattern SHA256_HEX_PATTERN = Pattern.compile("^[0-9a-f]{64}$");
    private static final Set<PosixFilePermission> PRIVATE_CACHE_DIR_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> PRIVATE_CACHE_FILE_PERMISSIONS =
            PosixFilePermissions.fromString("rw-------");
    private static final Map<Path, java.util.Optional<Local9pParquetCache>> SHARED_LOCAL_9P_CACHES =
            new ConcurrentHashMap<>();

    private final Path root;
    private final TieredArchiveConfig config;
    private final Map<Path, VerificationStamp> verifiedObjects = new ConcurrentHashMap<>();
    private final Object[] verificationLocks = createVerificationLocks();
    private final Map<String, java.util.Optional<Instant>> maxResultTimeBySha256 = new ConcurrentHashMap<>();
    private final Cache<CqlGenerationCountKey, Long> cqlGenerationCounts = CacheBuilder.newBuilder()
            .maximumSize(MAX_CQL_COUNT_CACHE_ENTRIES)
            .build();
    private final AtomicLong cqlCountDuckDbSessionOpens = new AtomicLong();
    private final AtomicLong cqlCountCacheHits = new AtomicLong();
    private final AtomicLong cqlCountZeroMatchPrunedGenerations = new AtomicLong();
    private final AtomicLong cqlCountScannedGenerations = new AtomicLong();
    private final AtomicInteger openCursors = new AtomicInteger();
    private volatile DirectReadFaultInjector directReadFaultInjector;
    private volatile Boolean archiveMountIs9p;
    private volatile Boolean forceProactiveLocalStagingForTesting;
    private volatile Local9pParquetCache localStageCacheForTesting;
    private volatile java.util.Optional<Local9pParquetCache> resolvedSharedLocal9pCache;

    @FunctionalInterface
    interface DirectReadFaultInjector {
        void beforeDirectRead(Set<String> filenames) throws SQLException;
    }

    void setDirectReadFaultInjectorForTesting(DirectReadFaultInjector injector) {
        this.directReadFaultInjector = injector;
    }

    void setForceProactiveLocalStagingForTesting(Boolean force) {
        this.forceProactiveLocalStagingForTesting = force;
    }

    void setLocalStageCacheForTesting(Path cacheDir, long maxBytes) throws IOException {
        this.localStageCacheForTesting = cacheDir == null
                ? null
                : Local9pParquetCache.open(cacheDir, maxBytes, true);
    }

    long localStageCacheCopyCountForTesting() {
        Local9pParquetCache cache = localStageCacheForTesting;
        return cache == null ? 0L : cache.copyCount.get();
    }

    long localStageCachePinHitCountForTesting() {
        Local9pParquetCache cache = localStageCacheForTesting;
        return cache == null ? 0L : cache.pinHitCount.get();
    }

    long localStageCacheTrackedBytesForTesting() {
        Local9pParquetCache cache = localStageCacheForTesting;
        return cache == null ? 0L : cache.trackedBytes();
    }

    public long cqlCountDuckDbSessionOpensForTesting() {
        return cqlCountDuckDbSessionOpens.get();
    }

    public long cqlCountCacheHitsForTesting() {
        return cqlCountCacheHits.get();
    }

    public long cqlCountZeroMatchPrunedGenerationsForTesting() {
        return cqlCountZeroMatchPrunedGenerations.get();
    }

    public long cqlCountScannedGenerationsForTesting() {
        return cqlCountScannedGenerations.get();
    }

    static boolean is9pFileSystemType(String fsType) {
        if (fsType == null)
            return false;
        String normalized = fsType.trim().toLowerCase(Locale.ROOT);
        return "9p".equals(normalized) || "v9fs".equals(normalized) || "drvfs".equals(normalized);
    }

    static boolean isVolatileMemoryFileSystemType(String fsType) {
        if (fsType == null)
            return false;
        String normalized = fsType.trim().toLowerCase(Locale.ROOT);
        return "tmpfs".equals(normalized) || "ramfs".equals(normalized);
    }

    void setPersistentLocalStageCacheForTesting(Path cacheDir, long maxBytes) throws IOException {
        this.localStageCacheForTesting = cacheDir == null
                ? null
                : Local9pParquetCache.open(cacheDir, maxBytes, false);
    }

    static List<Path> candidateLocal9pCacheDirectories(TieredArchiveConfig cfg) {
        Set<Path> candidates = new LinkedHashSet<>();
        if (cfg != null && cfg.duckDbTemporaryDirectory != null && !cfg.duckDbTemporaryDirectory.isBlank()) {
            try {
                Path duckTmp = Path.of(cfg.duckDbTemporaryDirectory).toAbsolutePath().normalize();
                Path duckParent = duckTmp.getParent();
                if (duckParent != null) {
                    candidates.add(duckParent.resolve(".cache").resolve("oscar-9p-parquet-cache").normalize());
                }
            } catch (Exception ignored) {
            }
        }
        candidates.add(DEFAULT_STATE_9P_CACHE_DIR.toAbsolutePath().normalize());
        candidates.add(DEFAULT_LOCAL_9P_CACHE_DIR.toAbsolutePath().normalize());
        return List.copyOf(candidates);
    }

    private Local9pParquetCache activeLocalStageCache() {
        if (directReadFaultInjector != null)
            return null;
        Local9pParquetCache testCache = localStageCacheForTesting;
        if (testCache != null)
            return testCache;
        if (forceProactiveLocalStagingForTesting != null)
            return null;
        return isArchiveOn9pMount() ? resolveSharedLocal9pCache() : null;
    }

    private Local9pParquetCache resolveSharedLocal9pCache() {
        java.util.Optional<Local9pParquetCache> existing = resolvedSharedLocal9pCache;
        if (existing != null)
            return existing.orElse(null);
        synchronized (this) {
            existing = resolvedSharedLocal9pCache;
            if (existing != null)
                return existing.orElse(null);
            Path duckTmp = null;
            if (config.duckDbTemporaryDirectory != null && !config.duckDbTemporaryDirectory.isBlank()) {
                try {
                    duckTmp = Path.of(config.duckDbTemporaryDirectory).toAbsolutePath().normalize();
                } catch (Exception ignored) {
                }
            }
            Local9pParquetCache found = null;
            for (Path candidate : candidateLocal9pCacheDirectories(config)) {
                if (pathsOverlap(candidate, root) || (duckTmp != null && pathsOverlap(candidate, duckTmp)))
                    continue;
                Path ancestor = nearestExistingAncestor(candidate);
                if (ancestor == null)
                    continue;
                try {
                    if (!Files.isDirectory(ancestor, LinkOption.NOFOLLOW_LINKS) || !Files.isWritable(ancestor))
                        continue;
                    String fsType = Files.getFileStore(ancestor).type();
                    if (is9pFileSystemType(fsType) || isVolatileMemoryFileSystemType(fsType))
                        continue;
                    java.util.Optional<Local9pParquetCache> opened = SHARED_LOCAL_9P_CACHES.computeIfAbsent(
                            candidate,
                            dir -> {
                                try {
                                    long maxBytes = resolveLocal9pCacheMaxBytes(dir, root);
                                    return java.util.Optional.of(
                                            Local9pParquetCache.open(dir, maxBytes, false));
                                } catch (Exception ignored) {
                                    return java.util.Optional.empty();
                                }
                            });
                    if (opened.isPresent()) {
                        found = opened.get();
                        break;
                    }
                } catch (Exception ignored) {
                }
            }
            resolvedSharedLocal9pCache = java.util.Optional.ofNullable(found);
            return found;
        }
    }

    static long resolveLocal9pCacheMaxBytes(Path cacheDir, Path archiveRoot) {
        String explicit = System.getProperty(PROP_LOCAL_9P_CACHE_MAX_BYTES);
        if (explicit == null || explicit.isBlank()) {
            explicit = System.getenv(ENV_LOCAL_9P_CACHE_MAX_BYTES);
        }
        long minUsable = Long.MAX_VALUE;
        for (Path target : List.of(cacheDir, archiveRoot)) {
            Path ancestor = Files.exists(target, LinkOption.NOFOLLOW_LINKS) ? target : nearestExistingAncestor(target);
            if (ancestor == null)
                continue;
            try {
                long usable = Files.getFileStore(ancestor).getUsableSpace();
                if (usable > 0L && usable < minUsable) {
                    minUsable = usable;
                }
            } catch (Exception ignored) {
            }
        }
        return computeLocal9pCacheMaxBytes(minUsable, explicit);
    }

    public static long computeLocal9pCacheMaxBytes(long minUsableBytes, String explicitOverride) {
        if (explicitOverride != null && !explicitOverride.isBlank()) {
            try {
                long parsed = Long.parseLong(explicitOverride.trim());
                if (parsed > 0L) {
                    return parsed;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        if (minUsableBytes <= 0L || minUsableBytes == Long.MAX_VALUE) {
            return MIN_LOCAL_9P_CACHE_MAX_BYTES;
        }
        if (minUsableBytes < LOW_DISK_THRESHOLD_BYTES) {
            return Math.max(LOW_DISK_FLOOR_LOCAL_9P_CACHE_BYTES, minUsableBytes / 8L);
        }
        long proportional = minUsableBytes / 16L;
        return Math.max(MIN_LOCAL_9P_CACHE_MAX_BYTES, Math.min(MAX_LOCAL_9P_CACHE_MAX_BYTES, proportional));
    }

    private static boolean pathsOverlap(Path first, Path second) {
        return first.equals(second) || first.startsWith(second) || second.startsWith(first);
    }

    private static Path nearestExistingAncestor(Path target) {
        Path current = target.getParent();
        while (current != null && !Files.exists(current, LinkOption.NOFOLLOW_LINKS))
            current = current.getParent();
        return current;
    }

    private boolean shouldProactivelyStage(AuthorizedObjects objects, Path operationDirectory) {
        if (objects.byFilename().isEmpty() || directReadFaultInjector != null)
            return false;
        if (localStageCacheForTesting != null)
            return true;
        Boolean forced = forceProactiveLocalStagingForTesting;
        if (forced != null)
            return forced;
        return isArchiveOn9pMount() &&
                (activeLocalStageCache() != null || !isPathOn9pMount(operationDirectory));
    }

    private boolean isArchiveOn9pMount() {
        Boolean cached = archiveMountIs9p;
        if (cached != null)
            return cached;
        boolean detected = isPathOn9pMount(resolveRealRoot());
        archiveMountIs9p = detected;
        return detected;
    }

    private static boolean isPathOn9pMount(Path path) {
        try {
            return is9pFileSystemType(Files.getFileStore(path).type());
        } catch (Exception ignored) {
            return false;
        }
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
        return stream(generations, filter, allowedDataStreams, allowedFois, limit, null);
    }

    public Stream<ArchivedRow> stream(List<ArchiveGeneration> generations, ObsFilter filter,
                                      Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit,
                                      Function<Collection<UUID>, Set<Long>> mutatedIdsByClusterLookup) {
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
        if (latestSelection && !javaCql &&
                filter.getValuePredicate() == null && filter.getPhenomenonLocation() == null &&
                (generations.size() > 1 || mutatedIdsByClusterLookup != null))
            return streamLatestAcrossWindowClusters(
                    generations, filter, allowedDataStreams, allowedFois, limit, mutatedIdsByClusterLookup);

        // CQL is evaluated against the decoded JSON map. It must run before
        // latest-per-stream selection and before LIMIT or matching rows can be
        // silently lost. Structured predicates remain pushed into Parquet.
        boolean javaLatest = javaCql && latestSelection;
        Predicate<String> cqlMatcher = javaCql ? JsonCqlEvaluator.compileMatcher(filter.getCQLFilter()) : null;
        Stream<ArchivedRow> rows;
        if (generations.size() > 1) {
            long callerHintLimit = javaLatest ? Long.MAX_VALUE : Math.min(limit, filter.getLimit());
            rows = streamWindowClusters(generations, filter, allowedDataStreams, allowedFois,
                    Long.MAX_VALUE, callerHintLimit, javaLatest ? null : cqlMatcher);
            if (javaLatest && cqlMatcher != null) {
                rows = rows.filter(row -> cqlMatcher.test(row.observation().resultJson()));
            }
        } else {
            AuthorizedObjects objects = authorize(generations);
            if (objects.byFilename().isEmpty())
                return Stream.empty();
            rows = openRowStream(objects, auth -> queryPlan(auth, filter, allowedDataStreams, allowedFois,
                    true, !javaCql, javaCql ? Long.MAX_VALUE : limit), generations.size(),
                    queryOperationName(filter));
            if (cqlMatcher != null) {
                rows = rows.filter(row -> cqlMatcher.test(row.observation().resultJson()));
            }
        }

        if (javaLatest) {
            Set<Long> mutatedIds = resolveClusterMutatedIds(generations, mutatedIdsByClusterLookup);
            Map<Long, ArchivedRow> latest = new HashMap<>();
            try (Stream<ArchivedRow> latestRows = rows) {
                latestRows.forEach(row -> {
                    if (!mutatedIds.isEmpty() && mutatedIds.contains(row.observation().id()))
                        return;
                    latest.merge(row.observation().dataStreamId(), row,
                            (previous, candidate) -> later(candidate, previous,
                                    filter.getResultTime() != null && filter.getResultTime().isLatestTime())
                                    ? candidate : previous);
                });
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
        return streamWindowClusters(generations, filter, allowedDataStreams, allowedFois, limit, limit, null);
    }

    private Stream<ArchivedRow> streamWindowClusters(
            List<ArchiveGeneration> generations, ObsFilter filter,
            Set<Long> allowedDataStreams, Set<Long> allowedFois,
            long cursorLimit, long callerHintLimit, Predicate<String> cqlMatcher) {
        List<List<ArchiveGeneration>> clusters = windowClusters(generations);
        if (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().descendingOrder())
            java.util.Collections.reverse(clusters);
        boolean explicitIdLookup = filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty();
        boolean selectiveColdScan = explicitIdLookup || (filter.getCQLFilter() != null &&
                JsonCqlEvaluator.hasSelectiveDuckDbPrefilter(filter.getCQLFilter()));
        if (selectiveColdScan && clusters.size() > 1) {
            int leadSingleClusters = callerHintLimit <= 64 ? 2 : 0;
            clusters = batchOrderedClustersWithLeadProbe(clusters, leadSingleClusters, MAX_CQL_PAYLOAD_BATCH);
        } else if (filter.getCQLFilter() == null && clusters.size() > 4 &&
                ((cursorLimit >= 2 && cursorLimit <= 64) ||
                 (cursorLimit == Long.MAX_VALUE && allowedDataStreams != null && allowedDataStreams.size() <= 256))) {
            clusters = batchOrderedClusters(clusters, 32);
        }
        WindowClusterCursor cursor = new WindowClusterCursor(
                clusters.iterator(), filter, allowedDataStreams, allowedFois, cursorLimit, cqlMatcher);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(cursor::close);
    }

    /**
     * Evaluates latest-per-stream queries across disjoint window clusters from
     * newest to oldest so that once every requested datastream has a non-null
     * timestamp in a newer cluster, older clusters do not need to be scanned.
     */
    private static Set<Long> resolveClusterMutatedIds(
            List<ArchiveGeneration> clusterGenerations,
            Function<Collection<UUID>, Set<Long>> mutatedIdsByClusterLookup) {
        if (mutatedIdsByClusterLookup == null || clusterGenerations.isEmpty())
            return Set.of();
        List<UUID> ids = clusterGenerations.stream().map(ArchiveGeneration::id).toList();
        Set<Long> resolved = mutatedIdsByClusterLookup.apply(ids);
        return resolved == null ? Set.of() : resolved;
    }

    private Stream<ArchivedRow> streamLatestAcrossWindowClusters(
            List<ArchiveGeneration> generations, ObsFilter filter,
            Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit,
            Function<Collection<UUID>, Set<Long>> mutatedIdsByClusterLookup) {
        List<List<ArchiveGeneration>> clusters = windowClusters(generations);
        if (clusters.size() <= 1) {
            Set<Long> singleClusterMutated = resolveClusterMutatedIds(generations, mutatedIdsByClusterLookup);
            if (singleClusterMutated.size() <= MAX_INLINE_MUTATION_EXCLUSIONS) {
                AuthorizedObjects objects = authorize(generations);
                if (objects.byFilename().isEmpty())
                    return Stream.empty();
                return openRowStream(objects, auth -> queryPlan(auth, filter, allowedDataStreams, allowedFois,
                        singleClusterMutated, true, true, limit, true), generations.size());
            }
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
            final Set<Long> clusterMutatedIds = resolveClusterMutatedIds(
                    clusterGenerations, mutatedIdsByClusterLookup);
            final boolean pushDownMutated = clusterMutatedIds.size() <= MAX_INLINE_MUTATION_EXCLUSIONS;
            try (Stream<ArchivedRow> clusterRows = openRowStream(objects,
                    auth -> queryPlan(auth, filter, effectiveClusterStreams, allowedFois,
                            pushDownMutated ? clusterMutatedIds : Set.of(),
                            true, pushDownMutated, Long.MAX_VALUE, pushDownMutated),
                    clusterGenerations.size())) {
                clusterRows.forEach(row -> {
                    if (!pushDownMutated && clusterMutatedIds.contains(row.observation().id()))
                        return;
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

    private static final int MAX_INLINE_MUTATION_EXCLUSIONS = 4_096;
    private static final int MAX_INLINE_CQL_AGGREGATE_EXCLUSIONS = 64;
    private static final int MAX_RESULT_TIME_METADATA_BATCH = 64;
    private static final int MAX_CQL_PAYLOAD_BATCH = 16;
    private static final int MAX_CQL_AGGREGATE_BATCH = 16;
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
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    executeMaxResultTimesQuery(duckSession, staged, false, discovered);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            } else {
                try {
                    executeMaxResultTimesQuery(duckSession, objects, true, discovered);
                } catch (Exception directError) {
                    if (!isTransientReadMemoryError(directError))
                        throw directError;
                    AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                    try {
                        discovered.clear();
                        executeMaxResultTimesQuery(duckSession, staged, false, discovered);
                    } finally {
                        cleanupStagedBatchObjects(staged);
                    }
                }
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

    private static String queryOperationName(ObsFilter filter) {
        return filter != null
                && filter.getInternalIDs() != null
                && !filter.getInternalIDs().isEmpty()
                && filter.getCQLFilter() == null
                ? "point-query"
                : "cold-query";
    }

    private Stream<ArchivedRow> openRowStream(AuthorizedObjects objects,
                                              Function<AuthorizedObjects, QueryPlan> planFactory,
                                              int generationCount) {
        return openRowStream(objects, planFactory, generationCount, "cold-query");
    }

    private Stream<ArchivedRow> openRowStream(AuthorizedObjects objects,
                                              Function<AuthorizedObjects, QueryPlan> planFactory,
                                              int generationCount,
                                              String operationName) {
        DuckDbResourcePolicy.Session duckSession = null;
        PreparedStatement statement = null;
        ResultSet result = null;
        AuthorizedObjects stagedObjects = null;
        try {
            duckSession = DuckDbResourcePolicy.open(config, operationName);
            AuthorizedObjects activeObjects = objects;
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                stagedObjects = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                activeObjects = stagedObjects;
                QueryPlan stagedPlan = planFactory.apply(activeObjects);
                statement = ConnectionManager.trackCancellableStatement(
                        duckSession.connection().prepareStatement(stagedPlan.sql()), "DuckDB cold query");
                bind(statement, stagedPlan.parameters());
                result = statement.executeQuery();
            } else {
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
                    stagedObjects = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                    activeObjects = stagedObjects;
                    QueryPlan stagedPlan = planFactory.apply(activeObjects);
                    statement = ConnectionManager.trackCancellableStatement(
                            duckSession.connection().prepareStatement(stagedPlan.sql()), "DuckDB cold query");
                    bind(statement, stagedPlan.parameters());
                    result = statement.executeQuery();
                }
            }
            RowCursor cursor = new RowCursor(duckSession, statement, result, activeObjects, stagedObjects);
            openCursors.incrementAndGet();
            return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                            Spliterator.ORDERED | Spliterator.NONNULL), false)
                    .onClose(cursor::close);
        } catch (Exception | Error error) {
            closeAndSuppress(result, error);
            closeAndSuppress(statement, error);
            if (stagedObjects != null)
                cleanupStagedBatchObjects(stagedObjects);
            closeAndSuppress(duckSession, error);
            if (error instanceof Error fatal)
                throw fatal;
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
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    executeForEachMetadataPlan(
                            duckSession, planFactory.apply(staged), staged, false, consumer, emittedAny);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            } else {
                try {
                    executeForEachMetadataPlan(
                            duckSession, planFactory.apply(objects), objects, true, consumer, emittedAny);
                } catch (Exception directError) {
                    if (emittedAny[0] || !isTransientReadMemoryError(directError))
                        throw directError;
                    AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                    try {
                        executeForEachMetadataPlan(
                                duckSession, planFactory.apply(staged), staged, false, consumer, emittedAny);
                    } finally {
                        cleanupStagedBatchObjects(staged);
                    }
                }
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
        return countCql(generations, filter, allowedDataStreams, allowedFois, excludedIds, null, null);
    }

    public long countCql(List<ArchiveGeneration> generations, ObsFilter filter,
                         Set<Long> allowedDataStreams, Set<Long> allowedFois,
                         Set<Long> excludedIds,
                         Map<UUID, GenerationSummary> summariesByGeneration) {
        return countCql(generations, filter, allowedDataStreams, allowedFois,
                excludedIds, null, summariesByGeneration);
    }

    public long countCql(List<ArchiveGeneration> generations, ObsFilter filter,
                         Set<Long> allowedDataStreams, Set<Long> allowedFois,
                         Set<Long> excludedIds,
                         Map<UUID, Set<Long>> excludedIdsByGeneration,
                         Map<UUID, GenerationSummary> summariesByGeneration) {
        if (filter.getValuePredicate() != null || filter.getPhenomenonLocation() != null)
            throw new IllegalArgumentException("Value and spatial predicates require full observation decoding");
        if ((filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime()))
            throw new IllegalArgumentException("Latest selection requires per-stream reduction");
        if (generations.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return 0;

        Set<Long> exclusions;
        if (excludedIds != null && !excludedIds.isEmpty()) {
            exclusions = excludedIds;
        } else if (excludedIdsByGeneration != null && !excludedIdsByGeneration.isEmpty()) {
            exclusions = generations.stream()
                    .flatMap(g -> excludedIdsByGeneration.getOrDefault(g.id(), Set.of()).stream())
                    .collect(Collectors.toSet());
        } else {
            exclusions = Set.of();
        }
        if (filter.getLimit() == Long.MAX_VALUE) {
            return countCqlUnboundedWithCache(
                    generations, filter, allowedDataStreams, allowedFois,
                    exclusions, excludedIdsByGeneration, summariesByGeneration);
        }

        JsonCqlEvaluator.ExactDuckDbPredicate exactPredicate = filter.getCQLFilter() != null
                ? JsonCqlEvaluator.tryBuildExactDuckDbPredicate(filter.getCQLFilter(), "result_json").orElse(null)
                : null;
        NavigableSet<Long> allSortedExclusions = exclusions.isEmpty()
                ? Collections.emptyNavigableSet()
                : new TreeSet<>(exclusions);

        long total = 0;
        DuckDbResourcePolicy.Session duckSession = null;
        try {
            int batchSize = exactPredicate != null ? MAX_CQL_AGGREGATE_BATCH : MAX_CQL_PAYLOAD_BATCH;
            for (List<ArchiveGeneration> batch : batchWindowClusters(generations, batchSize)) {
                throwIfInterruptedOrCancelled();
                AuthorizedObjects objects = authorize(batch);
                if (objects.byFilename().isEmpty())
                    continue;
                Map<String, ArchiveGeneration> unprunedMap = new LinkedHashMap<>(objects.byFilename().size() * 2);
                for (Map.Entry<String, ArchiveGeneration> entry : objects.byFilename().entrySet()) {
                    ArchiveGeneration gen = entry.getValue();
                    if (isGuaranteedZeroMatch(gen, filter.getCQLFilter(), summariesByGeneration)) {
                        cqlCountZeroMatchPrunedGenerations.incrementAndGet();
                        continue;
                    }
                    unprunedMap.put(entry.getKey(), gen);
                }
                if (unprunedMap.isEmpty())
                    continue;
                AuthorizedObjects activeBatch = unprunedMap.size() == objects.byFilename().size()
                        ? objects
                        : new AuthorizedObjects(unprunedMap, objects.stampsByGenId());
                if (duckSession == null) {
                    duckSession = DuckDbResourcePolicy.open(config, "cold-cql-count");
                    cqlCountDuckDbSessionOpens.incrementAndGet();
                }
                cqlCountScannedGenerations.addAndGet(unprunedMap.size());
                List<Long> batchSortedExclusions = resolveBatchSortedExclusions(
                        unprunedMap.values(), allSortedExclusions, excludedIdsByGeneration);
                total = Math.addExact(total, countCqlAuthorizedBatch(
                        duckSession, activeBatch, unprunedMap.size(), filter,
                        allowedDataStreams, allowedFois, exclusions, batchSortedExclusions,
                        exactPredicate, null));
                if (total >= filter.getLimit()) {
                    duckSession.close();
                    duckSession = null;
                    return filter.getLimit();
                }
                if (duckSession.shouldYield()) {
                    duckSession.close();
                    duckSession = null;
                }
            }
            if (duckSession != null) {
                duckSession.close();
                duckSession = null;
            }
            return total;
        } catch (IllegalStateException | IllegalArgumentException e) {
            closeAndSuppress(duckSession, e);
            throw e;
        } catch (Exception e) {
            closeAndSuppress(duckSession, e);
            throw new IllegalStateException("Cannot count CQL matches from " + generations.size() +
                    " archive generation(s)", e);
        } catch (Error err) {
            closeAndSuppress(duckSession, err);
            throw err;
        }
    }

    private long countCqlUnboundedWithCache(
            List<ArchiveGeneration> generations,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois,
            Set<Long> exclusions,
            Map<UUID, Set<Long>> excludedIdsByGeneration,
            Map<UUID, GenerationSummary> summariesByGeneration) {
        throwIfInterruptedOrCancelled();
        AuthorizedObjects allAuthorized = authorize(generations);
        if (allAuthorized.byFilename().isEmpty())
            return 0;

        boolean cacheAllowed = directReadFaultInjector == null &&
                (filter.getInternalIDs() == null || filter.getInternalIDs().isEmpty());
        String cqlKey = cacheAllowed
                ? JsonCqlEvaluator.canonicalCacheKey(filter.getCQLFilter()).orElse(null)
                : null;
        if (cqlKey == null)
            cacheAllowed = false;

        String streamsKey = cacheAllowed ? canonicalIdSetKey(allowedDataStreams) : null;
        String foisKey = cacheAllowed ? canonicalIdSetKey(allowedFois) : null;
        boolean nonNullPhenomenonRequired = filter.getPhenomenonTime() != null;
        boolean nonNullResultTimeRequired = filter.getResultTime() != null;
        NavigableSet<Long> excludedNav = cacheAllowed && excludedIdsByGeneration == null && !exclusions.isEmpty()
                ? new TreeSet<>(exclusions)
                : null;

        Map<UUID, String> filenameByGenId = new HashMap<>(allAuthorized.byFilename().size() * 2);
        for (Map.Entry<String, ArchiveGeneration> entry : allAuthorized.byFilename().entrySet()) {
            filenameByGenId.put(entry.getValue().id(), entry.getKey());
        }

        long total = 0;
        List<ArchiveGeneration> uncached = new ArrayList<>(generations.size());
        Map<UUID, CqlGenerationCountKey> cacheKeyByGenId = cacheAllowed ? new HashMap<>() : Map.of();

        for (ArchiveGeneration generation : generations) {
            if (!filenameByGenId.containsKey(generation.id()))
                continue;
            if (cacheAllowed && isEligibleForCqlCountCache(generation, filter)) {
                String excludedIdsDigest = resolveGenerationExcludedIdsDigest(
                        generation, excludedIdsByGeneration, excludedNav);
                CqlGenerationCountKey key = new CqlGenerationCountKey(
                        generation.sha256(), streamsKey, foisKey, cqlKey,
                        nonNullPhenomenonRequired, nonNullResultTimeRequired, excludedIdsDigest);
                Long cached = cqlGenerationCounts.getIfPresent(key);
                if (cached != null) {
                    cqlCountCacheHits.incrementAndGet();
                    total = Math.addExact(total, cached);
                    continue;
                }
                if (isGuaranteedZeroMatch(generation, filter.getCQLFilter(), summariesByGeneration)) {
                    cqlCountZeroMatchPrunedGenerations.incrementAndGet();
                    cqlGenerationCounts.put(key, 0L);
                    continue;
                }
                cacheKeyByGenId.put(generation.id(), key);
                uncached.add(generation);
                continue;
            }
            if (isGuaranteedZeroMatch(generation, filter.getCQLFilter(), summariesByGeneration)) {
                cqlCountZeroMatchPrunedGenerations.incrementAndGet();
                continue;
            }
            uncached.add(generation);
        }

        if (uncached.isEmpty())
            return total;

        JsonCqlEvaluator.ExactDuckDbPredicate exactPredicate = filter.getCQLFilter() != null
                ? JsonCqlEvaluator.tryBuildExactDuckDbPredicate(filter.getCQLFilter(), "result_json").orElse(null)
                : null;
        NavigableSet<Long> allSortedExclusions = excludedNav != null
                ? excludedNav
                : (exclusions.isEmpty() ? Collections.emptyNavigableSet() : new TreeSet<>(exclusions));
        int batchSize = exactPredicate != null ? MAX_CQL_AGGREGATE_BATCH : MAX_CQL_PAYLOAD_BATCH;

        DuckDbResourcePolicy.Session duckSession = null;
        try {
            for (List<ArchiveGeneration> batch : batchWindowClusters(uncached, batchSize)) {
                throwIfInterruptedOrCancelled();
                Map<String, ArchiveGeneration> batchMap = new LinkedHashMap<>(batch.size() * 2);
                for (ArchiveGeneration generation : batch) {
                    CqlGenerationCountKey key = cacheKeyByGenId.get(generation.id());
                    if (key != null) {
                        Long concurrentHit = cqlGenerationCounts.getIfPresent(key);
                        if (concurrentHit != null) {
                            cqlCountCacheHits.incrementAndGet();
                            total = Math.addExact(total, concurrentHit);
                            continue;
                        }
                    }
                    String filename = filenameByGenId.get(generation.id());
                    if (filename != null)
                        batchMap.put(filename, generation);
                }
                if (batchMap.isEmpty())
                    continue;
                if (duckSession == null) {
                    duckSession = DuckDbResourcePolicy.open(config, "cold-cql-count");
                    cqlCountDuckDbSessionOpens.incrementAndGet();
                    if (!cacheKeyByGenId.isEmpty()) {
                        Iterator<Map.Entry<String, ArchiveGeneration>> recheck = batchMap.entrySet().iterator();
                        while (recheck.hasNext()) {
                            ArchiveGeneration gen = recheck.next().getValue();
                            CqlGenerationCountKey key = cacheKeyByGenId.get(gen.id());
                            if (key != null) {
                                Long afterAcquireHit = cqlGenerationCounts.getIfPresent(key);
                                if (afterAcquireHit != null) {
                                    cqlCountCacheHits.incrementAndGet();
                                    total = Math.addExact(total, afterAcquireHit);
                                    recheck.remove();
                                }
                            }
                        }
                        if (batchMap.isEmpty())
                            continue;
                    }
                }
                cqlCountScannedGenerations.addAndGet(batchMap.size());
                Map<UUID, Long> perGenCounts = cacheKeyByGenId.isEmpty() ? null : new HashMap<>(batchMap.size() * 2);
                AuthorizedObjects batchObjects = new AuthorizedObjects(batchMap, allAuthorized.stampsByGenId());
                List<Long> batchSortedExclusions = resolveBatchSortedExclusions(
                        batchMap.values(), allSortedExclusions, excludedIdsByGeneration);
                long batchTotal = countCqlAuthorizedBatch(
                        duckSession, batchObjects, batchMap.size(), filter, allowedDataStreams, allowedFois,
                        exclusions, batchSortedExclusions, exactPredicate, perGenCounts);
                total = Math.addExact(total, batchTotal);
                if (perGenCounts != null) {
                    for (ArchiveGeneration generation : batchMap.values()) {
                        CqlGenerationCountKey key = cacheKeyByGenId.get(generation.id());
                        if (key != null) {
                            cqlGenerationCounts.put(key, perGenCounts.getOrDefault(generation.id(), 0L));
                        }
                    }
                }
                if (duckSession.shouldYield()) {
                    duckSession.close();
                    duckSession = null;
                }
            }
            if (duckSession != null) {
                duckSession.close();
                duckSession = null;
            }
            return total;
        } catch (IllegalStateException | IllegalArgumentException e) {
            closeAndSuppress(duckSession, e);
            throw e;
        } catch (Exception e) {
            closeAndSuppress(duckSession, e);
            throw new IllegalStateException("Cannot count CQL matches from " + uncached.size() +
                    " archive generation(s)", e);
        } catch (Error err) {
            closeAndSuppress(duckSession, err);
            throw err;
        }
    }

    private static boolean isGuaranteedZeroMatch(
            ArchiveGeneration generation,
            Filter cqlFilter,
            Map<UUID, GenerationSummary> summariesByGeneration) {
        if (generation.rowCount() <= 0L || cqlFilter == Filter.EXCLUDE)
            return true;
        if (cqlFilter == null || summariesByGeneration == null)
            return false;
        GenerationSummary summary = summariesByGeneration.get(generation.id());
        return summary != null && JsonCqlEvaluator.isGuaranteedZeroMatchBySummary(cqlFilter, summary);
    }

    private static String resolveGenerationExcludedIdsDigest(
            ArchiveGeneration generation,
            Map<UUID, Set<Long>> excludedIdsByGeneration,
            NavigableSet<Long> excludedNav) {
        if (excludedIdsByGeneration != null) {
            Set<Long> genIds = excludedIdsByGeneration.get(generation.id());
            return computeExcludedIdsDigest(genIds);
        }
        if (excludedNav == null || generation.rowCount() <= 0L ||
                generation.minimumId() > generation.maximumId()) {
            return "";
        }
        NavigableSet<Long> slice = excludedNav.subSet(
                generation.minimumId(), true, generation.maximumId(), true);
        return computeSortedExcludedIdsDigest(slice);
    }

    public static String computeExcludedIdsDigest(Collection<Long> excludedIds) {
        if (excludedIds == null || excludedIds.isEmpty())
            return "";
        if (excludedIds instanceof NavigableSet<Long> sorted && sorted.comparator() == null)
            return computeSortedExcludedIdsDigest(sorted);
        return computeSortedExcludedIdsDigest(new TreeSet<>(excludedIds));
    }

    static String computeSortedExcludedIdsDigest(NavigableSet<Long> sortedExcludedIds) {
        if (sortedExcludedIds == null || sortedExcludedIds.isEmpty())
            return "";
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] buf = new byte[8];
            for (long id : sortedExcludedIds) {
                buf[0] = (byte) (id >>> 56);
                buf[1] = (byte) (id >>> 48);
                buf[2] = (byte) (id >>> 40);
                buf[3] = (byte) (id >>> 32);
                buf[4] = (byte) (id >>> 24);
                buf[5] = (byte) (id >>> 16);
                buf[6] = (byte) (id >>> 8);
                buf[7] = (byte) id;
                digest.update(buf);
            }
            return HexFormat.of().formatHex(digest.digest());
        } catch (java.security.NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }

    private static String canonicalIdSetKey(Set<Long> ids) {
        if (ids == null)
            return "*";
        if (ids.isEmpty())
            return "";
        return ids.stream().sorted().map(String::valueOf).collect(Collectors.joining(","));
    }

    private static boolean isEligibleForCqlCountCache(
            ArchiveGeneration generation, ObsFilter filter) {
        if (!generation.parityVerified() || generation.start() == null || generation.end() == null)
            return false;
        TemporalFilter phenFilter = filter.getPhenomenonTime();
        if (phenFilter != null) {
            if (phenFilter.isLatestTime() || phenFilter.isCurrentTime())
                return false;
            Instant min = phenFilter.getMin();
            if (min != null && !Instant.MIN.equals(min)) {
                boolean lowerClosed = phenFilter.getRange().lowerBoundType() == BoundType.CLOSED;
                if (lowerClosed ? generation.start().isBefore(min) : !generation.start().isAfter(min))
                    return false;
            }
            Instant max = phenFilter.getMax();
            if (max != null && !Instant.MAX.equals(max)) {
                if (generation.end().isAfter(max))
                    return false;
            }
        }
        TemporalFilter resFilter = filter.getResultTime();
        if (resFilter != null) {
            if (resFilter.isLatestTime() || resFilter.isCurrentTime())
                return false;
            Instant minResult = generation.minResultTime();
            Instant maxResult = generation.maxResultTime();
            if (minResult == null || maxResult == null)
                return false;
            Instant min = resFilter.getMin();
            if (min != null && !Instant.MIN.equals(min)) {
                boolean lowerClosed = resFilter.getRange().lowerBoundType() == BoundType.CLOSED;
                if (lowerClosed ? minResult.isBefore(min) : !minResult.isAfter(min))
                    return false;
            }
            Instant max = resFilter.getMax();
            if (max != null && !Instant.MAX.equals(max)) {
                boolean upperClosed = resFilter.getRange().upperBoundType() == BoundType.CLOSED;
                if (upperClosed ? maxResult.isAfter(max) : !maxResult.isBefore(max))
                    return false;
            }
        }
        return true;
    }

    /**
     * Counts base cold archive observations for an explicit per-generation ID
     * set that match structured + CQL predicates, returning per-generation
     * counts in batched DuckDB scans with {@code id_value} min/max pruning.
     */
    public Map<UUID, Long> countMatchingIdsCqlByGeneration(
            List<ArchiveGeneration> generations,
            Map<UUID, Set<Long>> targetIdsByGeneration,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois) {
        if (filter.getValuePredicate() != null || filter.getPhenomenonLocation() != null)
            throw new IllegalArgumentException("Value and spatial predicates require full observation decoding");
        if (generations.isEmpty() || targetIdsByGeneration == null || targetIdsByGeneration.isEmpty() ||
                (allowedDataStreams != null && allowedDataStreams.isEmpty()) ||
                (allowedFois != null && allowedFois.isEmpty()))
            return Map.of();

        Map<UUID, Long> countsByGen = new HashMap<>();
        for (ArchiveGeneration generation : generations) {
            countsByGen.put(generation.id(), 0L);
        }
        for (List<ArchiveGeneration> batch : batchWindowClusters(generations, MAX_CQL_PAYLOAD_BATCH)) {
            throwIfInterruptedOrCancelled();
            List<Long> sortedIds = batch.stream()
                    .flatMap(gen -> targetIdsByGeneration.getOrDefault(gen.id(), Set.of()).stream())
                    .distinct()
                    .sorted()
                    .toList();
            if (sortedIds.isEmpty())
                continue;
            accumulateMatchingIdsCqlBatch(batch, sortedIds, filter, allowedDataStreams, allowedFois, countsByGen);
        }
        return countsByGen;
    }

    private void accumulateMatchingIdsCqlBatch(
            List<ArchiveGeneration> generations,
            List<Long> sortedTargetIds,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois,
            Map<UUID, Long> countsByGen) {
        AuthorizedObjects objects = authorize(generations);
        if (objects.byFilename().isEmpty())
            return;
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "cold-cql-id-count")) {
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    executeAccumulateMatchingIdsCql(
                            duckSession, staged, sortedTargetIds, filter, allowedDataStreams, allowedFois,
                            countsByGen, false);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            } else {
                try {
                    executeAccumulateMatchingIdsCql(
                            duckSession, objects, sortedTargetIds, filter, allowedDataStreams, allowedFois,
                            countsByGen, true);
                } catch (Exception directError) {
                    if (!isTransientReadMemoryError(directError))
                        throw directError;
                    AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                    try {
                        executeAccumulateMatchingIdsCql(
                                duckSession, staged, sortedTargetIds, filter, allowedDataStreams, allowedFois,
                                countsByGen, false);
                    } finally {
                        cleanupStagedBatchObjects(staged);
                    }
                }
            }
        } catch (IllegalStateException | IllegalArgumentException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot count targeted CQL matches from " + generations.size() +
                    " archive generation(s)", e);
        }
    }

    private void executeAccumulateMatchingIdsCql(
            DuckDbResourcePolicy.Session duckSession,
            AuthorizedObjects objects,
            List<Long> sortedTargetIds,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois,
            Map<UUID, Long> countsByGen,
            boolean directRead) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        Predicate<String> cqlMatcher = filter.getCQLFilter() != null
                ? JsonCqlEvaluator.compileMatcher(filter.getCQLFilter())
                : null;
        Map<UUID, Long> batchCounts = new HashMap<>();
        final int idChunkSize = 2_000;
        for (int offset = 0; offset < sortedTargetIds.size(); offset += idChunkSize) {
            List<Long> chunk = sortedTargetIds.subList(offset, Math.min(sortedTargetIds.size(), offset + idChunkSize));
            QueryPlan plan = cqlTargetIdsCountPlan(objects, chunk, filter, allowedDataStreams, allowedFois);
            try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                         duckSession.connection().prepareStatement(plan.sql()), "DuckDB cold targeted CQL count")) {
                bind(statement, plan.parameters());
                try (ResultSet result = statement.executeQuery()) {
                    while (nextCancellable(result)) {
                        String filename = result.getString(1);
                        String resultJson = cqlMatcher != null ? result.getString(2) : null;
                        if (cqlMatcher == null || cqlMatcher.test(resultJson)) {
                            ArchiveGeneration generation = generationForFilename(objects, filename);
                            batchCounts.merge(generation.id(), 1L, Math::addExact);
                        }
                    }
                }
            }
        }
        for (Map.Entry<UUID, Long> entry : batchCounts.entrySet()) {
            countsByGen.merge(entry.getKey(), entry.getValue(), Math::addExact);
        }
    }

    private QueryPlan cqlTargetIdsCountPlan(
            AuthorizedObjects objects,
            List<Long> sortedChunkIds,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois) {
        String fileList = objects.byFilename().keySet().stream()
                .map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        boolean hasCql = filter.getCQLFilter() != null;
        String selectCols = hasCql ? "filename, result_json" : "filename";
        StringBuilder sql = new StringBuilder("SELECT ").append(selectCols)
                .append(" FROM read_parquet([").append(fileList).append("], filename=true) WHERE id_value >= ? AND id_value <= ?");
        List<Object> parameters = new ArrayList<>();
        parameters.add(sortedChunkIds.get(0));
        parameters.add(sortedChunkIds.get(sortedChunkIds.size() - 1));
        appendIn(sql, "id_value", sortedChunkIds, parameters);
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
        if (hasCql)
            JsonCqlEvaluator.appendDuckDbPrefilter(filter.getCQLFilter(), "result_json", sql, parameters);
        return new QueryPlan(sql.toString(), parameters);
    }

    private static List<Long> resolveBatchSortedExclusions(
            Collection<ArchiveGeneration> batchGenerations,
            NavigableSet<Long> allSortedExclusions,
            Map<UUID, Set<Long>> excludedIdsByGeneration) {
        if (excludedIdsByGeneration != null) {
            NavigableSet<Long> scoped = new TreeSet<>();
            for (ArchiveGeneration gen : batchGenerations) {
                Set<Long> genIds = excludedIdsByGeneration.get(gen.id());
                if (genIds != null && !genIds.isEmpty())
                    scoped.addAll(genIds);
            }
            return scoped.isEmpty() ? List.of() : List.copyOf(scoped);
        }
        if (allSortedExclusions == null || allSortedExclusions.isEmpty())
            return List.of();
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        for (ArchiveGeneration gen : batchGenerations) {
            if (gen.rowCount() <= 0L)
                continue;
            if (gen.minimumId() > gen.maximumId())
                return List.copyOf(allSortedExclusions);
            if (gen.minimumId() < minId)
                minId = gen.minimumId();
            if (gen.maximumId() > maxId)
                maxId = gen.maximumId();
        }
        if (minId > maxId)
            return List.of();
        NavigableSet<Long> slice = allSortedExclusions.subSet(minId, true, maxId, true);
        return slice.isEmpty() ? List.of() : List.copyOf(slice);
    }

    private long countCqlAuthorizedBatch(
            DuckDbResourcePolicy.Session duckSession,
            AuthorizedObjects objects,
            int generationCount,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois,
            Set<Long> excludedIds,
            List<Long> batchSortedExclusions,
            JsonCqlEvaluator.ExactDuckDbPredicate exactPredicate,
            Map<UUID, Long> perGenCounts) {
        boolean useExactAggregate = exactPredicate != null;
        try {
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    return useExactAggregate
                            ? executeExactAggregateCountCqlPlan(
                                    duckSession,
                                    cqlExactAggregateCountPlan(
                                            staged, filter, allowedDataStreams, allowedFois,
                                            batchSortedExclusions, exactPredicate),
                                    staged, filter, batchSortedExclusions, false, perGenCounts)
                            : executeCountCqlPlan(
                                    duckSession, cqlCountPlan(staged, filter, allowedDataStreams, allowedFois),
                                    staged, filter, excludedIds, false, perGenCounts);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            }
            try {
                return useExactAggregate
                        ? executeExactAggregateCountCqlPlan(
                                duckSession,
                                cqlExactAggregateCountPlan(
                                        objects, filter, allowedDataStreams, allowedFois,
                                        batchSortedExclusions, exactPredicate),
                                objects, filter, batchSortedExclusions, true, perGenCounts)
                        : executeCountCqlPlan(
                                duckSession, cqlCountPlan(objects, filter, allowedDataStreams, allowedFois),
                                objects, filter, excludedIds, true, perGenCounts);
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                if (perGenCounts != null)
                    perGenCounts.clear();
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    return useExactAggregate
                            ? executeExactAggregateCountCqlPlan(
                                    duckSession,
                                    cqlExactAggregateCountPlan(
                                            staged, filter, allowedDataStreams, allowedFois,
                                            batchSortedExclusions, exactPredicate),
                                    staged, filter, batchSortedExclusions, false, perGenCounts)
                            : executeCountCqlPlan(
                                    duckSession, cqlCountPlan(staged, filter, allowedDataStreams, allowedFois),
                                    staged, filter, excludedIds, false, perGenCounts);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            }
        } catch (Exception e) {
            if (objects.byFilename().size() > 1 && isDuckDbOutOfMemoryError(e)) {
                throwIfInterruptedOrCancelled();
                List<Map.Entry<String, ArchiveGeneration>> entries = new ArrayList<>(objects.byFilename().entrySet());
                int mid = entries.size() / 2;
                Map<String, ArchiveGeneration> leftMap = new LinkedHashMap<>(mid * 2);
                Map<String, ArchiveGeneration> rightMap = new LinkedHashMap<>((entries.size() - mid) * 2);
                for (int i = 0; i < entries.size(); i++) {
                    Map.Entry<String, ArchiveGeneration> entry = entries.get(i);
                    if (i < mid)
                        leftMap.put(entry.getKey(), entry.getValue());
                    else
                        rightMap.put(entry.getKey(), entry.getValue());
                }
                Map<UUID, Long> leftCounts = perGenCounts != null ? new HashMap<>(mid * 2) : null;
                Map<UUID, Long> rightCounts = perGenCounts != null ? new HashMap<>((entries.size() - mid) * 2) : null;
                long leftTotal = countCqlAuthorizedBatch(
                        duckSession, new AuthorizedObjects(leftMap, objects.stampsByGenId()), leftMap.size(),
                        filter, allowedDataStreams, allowedFois, excludedIds, batchSortedExclusions,
                        exactPredicate, leftCounts);
                long rightTotal = countCqlAuthorizedBatch(
                        duckSession, new AuthorizedObjects(rightMap, objects.stampsByGenId()), rightMap.size(),
                        filter, allowedDataStreams, allowedFois, excludedIds, batchSortedExclusions,
                        exactPredicate, rightCounts);
                if (perGenCounts != null) {
                    perGenCounts.clear();
                    perGenCounts.putAll(leftCounts);
                    perGenCounts.putAll(rightCounts);
                }
                return Math.addExact(leftTotal, rightTotal);
            }
            throw new IllegalStateException("Cannot count CQL matches from " + generationCount +
                    " archive generation(s)", e);
        }
    }

    private static void cleanupStagedBatchObjects(AuthorizedObjects staged) {
        Path parent = null;
        for (String filename : staged.byFilename().keySet()) {
            try {
                Path file = Path.of(filename);
                parent = file.getParent();
                Files.deleteIfExists(file);
            } catch (Exception ignored) {
            }
        }
        if (parent != null) {
            try {
                Files.deleteIfExists(parent);
            } catch (Exception ignored) {
            }
        }
    }

    private QueryPlan cqlExactAggregateCountPlan(
            AuthorizedObjects objects,
            ObsFilter filter,
            Set<Long> allowedDataStreams,
            Set<Long> allowedFois,
            List<Long> batchSortedExclusions,
            JsonCqlEvaluator.ExactDuckDbPredicate exactPredicate) {
        String fileList = objects.byFilename().keySet().stream()
                .map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        boolean useTempExclusions = batchSortedExclusions.size() > MAX_INLINE_MUTATION_EXCLUSIONS;
        StringBuilder sql = new StringBuilder(useTempExclusions
                ? "SELECT sub.filename, COUNT(*) FROM (SELECT filename, id_value FROM read_parquet(["
                : "SELECT filename, COUNT(*) FROM read_parquet([")
                .append(fileList).append("], filename=true) WHERE true");
        List<Object> parameters = new ArrayList<>();
        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty())
            appendIn(sql, "id_value", filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList(), parameters);
        if (!useTempExclusions && !batchSortedExclusions.isEmpty()) {
            appendNotIn(sql, "id_value", batchSortedExclusions, parameters);
        }
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
        if ("FALSE".equals(exactPredicate.sql())) {
            sql.append(" AND FALSE");
        } else if (!"TRUE".equals(exactPredicate.sql())) {
            sql.append(" AND (").append(exactPredicate.sql()).append(')');
            parameters.addAll(exactPredicate.parameters());
        }
        if (useTempExclusions) {
            sql.append(") sub WHERE sub.id_value NOT IN (SELECT id_value FROM oscar_cql_excluded_ids) GROUP BY sub.filename");
        } else {
            sql.append(" GROUP BY filename");
        }
        return new QueryPlan(sql.toString(), parameters);
    }

    private static void populateTempExcludedIds(java.sql.Connection duckConn, List<Long> sortedExcludedIds) throws SQLException {
        try (java.sql.Statement ddl = duckConn.createStatement()) {
            ddl.execute("CREATE TEMP TABLE IF NOT EXISTS oscar_cql_excluded_ids (id_value BIGINT NOT NULL)");
            ddl.execute("DELETE FROM oscar_cql_excluded_ids");
        }
        final int chunkSize = 1_024;
        for (int offset = 0; offset < sortedExcludedIds.size(); offset += chunkSize) {
            int end = Math.min(sortedExcludedIds.size(), offset + chunkSize);
            int count = end - offset;
            StringBuilder insertSql = new StringBuilder("INSERT INTO oscar_cql_excluded_ids VALUES ");
            for (int idx = 0; idx < count; idx++) {
                if (idx > 0)
                    insertSql.append(',');
                insertSql.append("(?)");
            }
            try (PreparedStatement insert = duckConn.prepareStatement(insertSql.toString())) {
                for (int idx = 0; idx < count; idx++) {
                    insert.setLong(idx + 1, sortedExcludedIds.get(offset + idx));
                }
                insert.executeUpdate();
            }
        }
    }

    private static void dropTempExcludedIds(java.sql.Connection duckConn) {
        try (java.sql.Statement cleanup = duckConn.createStatement()) {
            cleanup.execute("DROP TABLE IF EXISTS oscar_cql_excluded_ids");
        } catch (Exception ignored) {
        }
    }

    private long executeExactAggregateCountCqlPlan(
            DuckDbResourcePolicy.Session duckSession,
            QueryPlan plan,
            AuthorizedObjects objects,
            ObsFilter filter,
            List<Long> batchSortedExclusions,
            boolean directRead,
            Map<UUID, Long> perGenCounts) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        boolean usedTempExclusions = batchSortedExclusions.size() > MAX_INLINE_MUTATION_EXCLUSIONS;
        if (usedTempExclusions) {
            populateTempExcludedIds(duckSession.connection(), batchSortedExclusions);
        }
        try {
            long matched = 0;
            try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                         duckSession.connection().prepareStatement(plan.sql()), "DuckDB cold CQL aggregate count")) {
                bind(statement, plan.parameters());
                try (ResultSet result = statement.executeQuery()) {
                    while (nextCancellable(result)) {
                        String filename = result.getString(1);
                        long fileCount = result.getLong(2);
                        if (fileCount <= 0L)
                            continue;
                        if (perGenCounts != null) {
                            ArchiveGeneration generation = generationForFilename(objects, filename);
                            perGenCounts.merge(generation.id(), fileCount, Math::addExact);
                        }
                        matched = Math.addExact(matched, fileCount);
                        if (filter.getLimit() != Long.MAX_VALUE && matched >= filter.getLimit())
                            return filter.getLimit();
                    }
                }
                return matched;
            }
        } finally {
            if (usedTempExclusions) {
                dropTempExcludedIds(duckSession.connection());
            }
        }
    }

    private QueryPlan cqlCountPlan(AuthorizedObjects objects, ObsFilter filter,
                                   Set<Long> allowedDataStreams, Set<Long> allowedFois) {
        String fileList = objects.byFilename().keySet().stream()
                .map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        StringBuilder sql = new StringBuilder("SELECT filename, id_value, result_json FROM read_parquet([")
                .append(fileList).append("], filename=true) WHERE true");
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
                                     boolean directRead,
                                     Map<UUID, Long> perGenCounts) throws Exception {
        if (directRead)
            maybeInjectDirectReadFault(objects);
        Predicate<String> cqlMatcher = JsonCqlEvaluator.compileMatcher(filter.getCQLFilter());
        long matched = 0;
        try (PreparedStatement statement = ConnectionManager.trackCancellableStatement(
                     duckSession.connection().prepareStatement(plan.sql()), "DuckDB cold CQL count")) {
            bind(statement, plan.parameters());
            try (ResultSet result = statement.executeQuery()) {
                while (nextCancellable(result)) {
                    String filename = result.getString(1);
                    long id = result.getLong(2);
                    if (excludedIds.contains(id))
                        continue;
                    if (cqlMatcher.test(result.getString(3))) {
                        if (perGenCounts != null) {
                            ArchiveGeneration generation = generationForFilename(objects, filename);
                            perGenCounts.merge(generation.id(), 1L, Math::addExact);
                        }
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
            if (shouldProactivelyStage(objects, duckSession.operationDirectory())) {
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    return executeCountPlan(duckSession, planFactory.apply(staged), staged, filter, false);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
            }
            try {
                return executeCountPlan(duckSession, planFactory.apply(objects), objects, filter, true);
            } catch (Exception directError) {
                if (!isTransientReadMemoryError(directError))
                    throw directError;
                AuthorizedObjects staged = stageAuthorizedObjectsLocally(objects, duckSession.operationDirectory());
                try {
                    return executeCountPlan(duckSession, planFactory.apply(staged), staged, filter, false);
                } finally {
                    cleanupStagedBatchObjects(staged);
                }
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

    static boolean isDuckDbOutOfMemoryError(Throwable error) {
        int depth = 0;
        for (Throwable current = error; current != null && depth < 16; current = current.getCause(), depth++) {
            String message = current.getMessage();
            if (message == null)
                continue;
            String normalized = message.toLowerCase(Locale.ROOT);
            if (normalized.contains("out of memory error") || normalized.contains("failed to allocate data")) {
                return true;
            }
        }
        return false;
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
     * Copies already-authorized Parquet objects in parallel into the bounded
     * DuckDB session's private local temporary directory using 16 KiB stream
     * reads (safe on WSL2 9p drvFs where multi-megabyte {@code pread} calls can
     * fail with {@code ENOMEM} and where DuckDB's 2-thread random column seeks
     * pay high per-RPC latency) and re-verifies each object's SHA-256 digest
     * during staging.
     */
    private AuthorizedObjects stageAuthorizedObjectsLocally(AuthorizedObjects objects, Path operationDirectory)
            throws Exception {
        int n = objects.byFilename().size();
        if (n == 0)
            return new AuthorizedObjects(Map.of());
        Local9pParquetCache localCache = activeLocalStageCache();
        Path stageDir;
        if (localCache != null) {
            stageDir = localCache.createPinDirectory();
        } else {
            Path normalizedOperationDir = operationDirectory.toAbsolutePath().normalize();
            stageDir = normalizedOperationDir.resolve("staged-" + UUID.randomUUID()).normalize();
            if (!stageDir.getParent().equals(normalizedOperationDir))
                throw new IllegalStateException("Unsafe local Parquet stage directory: " + stageDir);
            Files.createDirectory(stageDir);
        }

        boolean completed = false;
        try {
            List<ArchiveGeneration> generationList = new ArrayList<>(objects.byFilename().values());
            Runnable cancellationCheck = ConnectionManager.captureCancellableOperationCheck();
            cancellationCheck.run();

            String[] stagedFilenames = new String[n];
            List<Integer> missIndices = new ArrayList<>();
            if (localCache != null && !objects.stampsByGenId().isEmpty()) {
                for (int i = 0; i < n; i++) {
                    cancellationCheck.run();
                    ArchiveGeneration generation = generationList.get(i);
                    VerificationStamp stamp = objects.stampsByGenId().get(generation.id());
                    Path target = stageTargetPath(stageDir, i, generation);
                    if (stamp != null && localCache.tryPinCached(generation.sha256(), stamp.size(), target)) {
                        stagedFilenames[i] = canonicalFilename(target);
                    } else {
                        missIndices.add(i);
                    }
                }
                if (missIndices.isEmpty()) {
                    Map<String, ArchiveGeneration> fastStaged = new LinkedHashMap<>(n * 2);
                    for (int i = 0; i < n; i++) {
                        fastStaged.put(stagedFilenames[i], generationList.get(i));
                    }
                    completed = true;
                    return new AuthorizedObjects(fastStaged, objects.stampsByGenId());
                }
            } else {
                for (int i = 0; i < n; i++) {
                    missIndices.add(i);
                }
            }

            Path realRoot = resolveRealRoot();
            if (missIndices.size() == 1) {
                int idx = missIndices.get(0);
                ArchiveGeneration generation = generationList.get(idx);
                Path normalizedRelative = validateNormalizedRelativePath(generation.objectPath());
                verifyRelativeDirectories(realRoot, normalizedRelative);
                ResolvedSourceObject source = resolveLeafObjectForStaging(realRoot, normalizedRelative);
                Path target = stageTargetPath(stageDir, idx, generation);
                stageSingleObject(source, target, generation.sha256(), localCache, cancellationCheck);
                stagedFilenames[idx] = canonicalFilename(target);
            } else {
                int missCount = missIndices.size();
                Path[] normalizedRelatives = new Path[missCount];
                Set<Path> uniqueRelativeDirs = new LinkedHashSet<>();
                for (int m = 0; m < missCount; m++) {
                    int idx = missIndices.get(m);
                    Path normalizedRelative = validateNormalizedRelativePath(generationList.get(idx).objectPath());
                    normalizedRelatives[m] = normalizedRelative;
                    Path parent = normalizedRelative.getParent();
                    if (parent != null) {
                        Path prefix = null;
                        for (Path component : parent) {
                            prefix = prefix == null ? component : prefix.resolve(component);
                            uniqueRelativeDirs.add(prefix);
                        }
                    }
                }

                if (!uniqueRelativeDirs.isEmpty()) {
                    List<Path> dirList = new ArrayList<>(uniqueRelativeDirs);
                    runParallelSlices(dirList.size(), (start, end) -> {
                        for (int i = start; i < end; i++) {
                            cancellationCheck.run();
                            Path relDir = dirList.get(i);
                            if (Files.isSymbolicLink(realRoot.resolve(relDir)))
                                throw new IllegalStateException(
                                        "Archive object path contains a symbolic link: " + root.resolve(relDir));
                        }
                    });
                }

                runParallelSlices(missCount, (start, end) -> {
                    for (int m = start; m < end; m++) {
                        cancellationCheck.run();
                        int idx = missIndices.get(m);
                        ArchiveGeneration generation = generationList.get(idx);
                        ResolvedSourceObject source = resolveLeafObjectForStaging(realRoot, normalizedRelatives[m]);
                        Path target = stageTargetPath(stageDir, idx, generation);
                        try {
                            stageSingleObject(source, target, generation.sha256(), localCache, cancellationCheck);
                        } catch (RuntimeException | Error unchecked) {
                            throw unchecked;
                        } catch (Exception checked) {
                            throw new StagingIoException(checked);
                        }
                        stagedFilenames[idx] = canonicalFilename(target);
                    }
                });
            }

            Map<String, ArchiveGeneration> staged = new LinkedHashMap<>(n * 2);
            for (int i = 0; i < n; i++) {
                staged.put(stagedFilenames[i], generationList.get(i));
            }
            completed = true;
            return new AuthorizedObjects(staged, objects.stampsByGenId());
        } catch (StagingIoException wrapped) {
            if (wrapped.getCause() instanceof Exception checked)
                throw checked;
            throw wrapped;
        } finally {
            if (!completed)
                cleanupStageDirectoryQuietly(stageDir);
        }
    }

    private void stageSingleObject(
            ResolvedSourceObject source,
            Path target,
            String expectedSha256,
            Local9pParquetCache localCache,
            Runnable cancellationCheck) throws Exception {
        if (localCache == null) {
            copyAndVerifyParquet(source.path(), target, expectedSha256, cancellationCheck);
            return;
        }
        long now = System.nanoTime();
        VerificationStamp verified = verifiedObjects.get(source.path());
        if (verified != null && verified.matches(expectedSha256, source.attributes(), now) &&
                localCache.tryPinCached(expectedSha256, source.attributes().size(), target)) {
            return;
        }
        if (localCache.canCache(expectedSha256, source.attributes().size())) {
            Path tempFile = localCache.newTempFile();
            try {
                String actual = copyToTempAndHash(source.path(), tempFile, localCache, cancellationCheck);
                if (!expectedSha256.equals(actual))
                    throw new IllegalStateException("Archive checksum mismatch while staging object: " + source.path());
                BasicFileAttributes after = readAttributes(source.path());
                if (!sameObject(source.attributes(), after)) {
                    throw new IllegalStateException(
                            "Archive object changed while its checksum was being verified: " + source.path());
                }
                localCache.putVerifiedTempFileAndPin(expectedSha256, after.size(), tempFile, target);
                verifiedObjects.put(source.path(), VerificationStamp.from(expectedSha256, after, System.nanoTime()));
                return;
            } finally {
                Files.deleteIfExists(tempFile);
            }
        }
        copyAndVerifyParquet(source.path(), target, expectedSha256, cancellationCheck);
    }

    private record ResolvedSourceObject(Path path, BasicFileAttributes attributes) {}

    private static final class StagingIoException extends RuntimeException {
        StagingIoException(Exception cause) {
            super(cause);
        }
    }

    private static Path stageTargetPath(Path stageDir, int index, ArchiveGeneration generation) {
        Path target = stageDir.resolve("object-" + index + "-" + generation.id() + ".parquet").normalize();
        if (!target.getParent().equals(stageDir))
            throw new IllegalStateException("Unsafe staged Parquet path: " + target);
        return target;
    }

    private ResolvedSourceObject resolveLeafObjectForStaging(Path realRoot, Path normalizedRelative) {
        Path candidate = realRoot.resolve(normalizedRelative).normalize();
        if (!candidate.startsWith(realRoot))
            throw new IllegalStateException("Archive object resolves outside configured root");
        Path logicalPath = root.resolve(normalizedRelative);
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(candidate, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve archive object " + logicalPath, e);
        }
        if (attributes.isSymbolicLink())
            throw new IllegalStateException("Archive object path contains a symbolic link: " + logicalPath);
        if (!attributes.isRegularFile())
            throw new IllegalStateException("Cataloged archive object is not a regular file: " + candidate);
        return new ResolvedSourceObject(candidate, attributes);
    }

    private static void cleanupStageDirectoryQuietly(Path stageDir) {
        if (stageDir == null)
            return;
        try {
            if (Files.exists(stageDir, LinkOption.NOFOLLOW_LINKS)) {
                try (var children = Files.newDirectoryStream(stageDir)) {
                    for (Path child : children) {
                        try {
                            Files.deleteIfExists(child);
                        } catch (Exception ignored) {
                        }
                    }
                }
                Files.deleteIfExists(stageDir);
            }
        } catch (Exception ignored) {
        }
    }

    private static String copyToTempAndHash(
            Path source,
            Path tempTarget,
            Local9pParquetCache localCache,
            Runnable cancellationCheck) throws Exception {
        for (int attempt = 1; ; attempt++) {
            cancellationCheck.run();
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            try (var input = Files.newInputStream(source, LinkOption.NOFOLLOW_LINKS);
                 var output = Files.newOutputStream(
                         tempTarget,
                         StandardOpenOption.CREATE,
                         StandardOpenOption.TRUNCATE_EXISTING,
                         StandardOpenOption.WRITE,
                         LinkOption.NOFOLLOW_LINKS)) {
                byte[] buffer = new byte[ArchiveExporter.SHA256_READ_BUFFER_BYTES];
                int read;
                while ((read = input.read(buffer)) >= 0) {
                    cancellationCheck.run();
                    if (read > 0) {
                        digest.update(buffer, 0, read);
                        output.write(buffer, 0, read);
                    }
                }
                Local9pParquetCache.setPrivateFilePermissionsIfSupported(tempTarget);
                if (localCache != null)
                    localCache.copyCount.incrementAndGet();
                return HexFormat.of().formatHex(digest.digest());
            } catch (IOException error) {
                Files.deleteIfExists(tempTarget);
                if (attempt >= LOCAL_STAGE_COPY_RETRIES ||
                        !ArchiveExporter.isTransientAllocationIoError(error)) {
                    throw error;
                }
                Thread.sleep(20L * attempt);
            } catch (RuntimeException | Error error) {
                try {
                    Files.deleteIfExists(tempTarget);
                } catch (Exception ignored) {
                }
                throw error;
            }
        }
    }

    private static void copyAndVerifyParquet(
            Path source, Path target, String expectedSha256, Runnable cancellationCheck) throws Exception {
        for (int attempt = 1; ; attempt++) {
            cancellationCheck.run();
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
                    cancellationCheck.run();
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
            } catch (RuntimeException | Error error) {
                try {
                    Files.deleteIfExists(target);
                } catch (Exception ignored) {
                }
                throw error;
            }
        }
    }

    private static List<List<ArchiveGeneration>> batchWindowClusters(
            List<ArchiveGeneration> generations, int targetBatchSize) {
        return batchOrderedClusters(windowClusters(generations), targetBatchSize);
    }

    private static List<List<ArchiveGeneration>> batchOrderedClustersWithLeadProbe(
            List<List<ArchiveGeneration>> orderedClusters, int leadSingleClusters, int targetBatchSize) {
        if (leadSingleClusters <= 0)
            return batchOrderedClusters(orderedClusters, targetBatchSize);
        if (orderedClusters.size() <= leadSingleClusters)
            return orderedClusters;
        List<List<ArchiveGeneration>> batches = new ArrayList<>();
        for (int i = 0; i < leadSingleClusters; i++)
            batches.add(orderedClusters.get(i));
        batches.addAll(batchOrderedClusters(
                orderedClusters.subList(leadSingleClusters, orderedClusters.size()), targetBatchSize));
        return batches;
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
        int n = generations.size();
        if (n == 0)
            return new AuthorizedObjects(Map.of());

        Path realRoot = resolveRealRoot();
        if (n == 1) {
            ArchiveGeneration generation = generations.get(0);
            Path normalizedRelative = validateNormalizedRelativePath(generation.objectPath());
            verifyRelativeDirectories(realRoot, normalizedRelative);
            Path object = verifyLeafObject(realRoot, normalizedRelative, generation.sha256());
            VerificationStamp stamp = verifiedObjects.get(object);
            return new AuthorizedObjects(
                    Map.of(canonicalFilename(object), generation),
                    stamp != null ? Map.of(generation.id(), stamp) : Map.of());
        }

        Path[] normalizedRelatives = new Path[n];
        Set<Path> uniqueRelativeDirs = new LinkedHashSet<>();
        for (int i = 0; i < n; i++) {
            Path normalizedRelative = validateNormalizedRelativePath(generations.get(i).objectPath());
            normalizedRelatives[i] = normalizedRelative;
            Path parent = normalizedRelative.getParent();
            if (parent != null) {
                Path prefix = null;
                for (Path component : parent) {
                    prefix = prefix == null ? component : prefix.resolve(component);
                    uniqueRelativeDirs.add(prefix);
                }
            }
        }

        Runnable cancellationCheck = ConnectionManager.captureCancellableOperationCheck();
        if (!uniqueRelativeDirs.isEmpty()) {
            List<Path> dirList = new ArrayList<>(uniqueRelativeDirs);
            runParallelSlices(dirList.size(), (start, end) -> {
                for (int i = start; i < end; i++) {
                    cancellationCheck.run();
                    Path relDir = dirList.get(i);
                    if (Files.isSymbolicLink(realRoot.resolve(relDir)))
                        throw new IllegalStateException(
                                "Archive object path contains a symbolic link: " + root.resolve(relDir));
                }
            });
        }

        String[] filenames = new String[n];
        VerificationStamp[] stamps = new VerificationStamp[n];
        runParallelSlices(n, (start, end) -> {
            for (int i = start; i < end; i++) {
                cancellationCheck.run();
                Path object = verifyLeafObject(
                        realRoot, normalizedRelatives[i], generations.get(i).sha256());
                filenames[i] = canonicalFilename(object);
                stamps[i] = verifiedObjects.get(object);
            }
        });

        Map<String, ArchiveGeneration> objects = new LinkedHashMap<>(n * 2);
        Map<UUID, VerificationStamp> stampsByGenId = new HashMap<>(n * 2);
        for (int i = 0; i < n; i++) {
            ArchiveGeneration generation = generations.get(i);
            String filename = filenames[i];
            ArchiveGeneration previous = objects.putIfAbsent(filename, generation);
            if (previous != null && !previous.id().equals(generation.id()))
                throw new IllegalStateException("Multiple archive generations reference the same object: " + filename);
            if (stamps[i] != null)
                stampsByGenId.put(generation.id(), stamps[i]);
        }
        return new AuthorizedObjects(objects, stampsByGenId);
    }

    @FunctionalInterface
    private interface SliceTask {
        void runSlice(int startInclusive, int endExclusive);
    }

    private static void runParallelSlices(int totalItems, SliceTask task) {
        if (totalItems <= 1) {
            task.runSlice(0, totalItems);
            return;
        }
        int slices = Math.min(AUTHORIZE_PARALLELISM, totalItems);
        List<Future<?>> futures = new ArrayList<>(slices - 1);
        for (int s = 1; s < slices; s++) {
            final int sliceStart = (int) (((long) s * totalItems) / slices);
            final int sliceEnd = (int) (((long) (s + 1) * totalItems) / slices);
            futures.add(AUTHORIZE_EXECUTOR.submit(() -> task.runSlice(sliceStart, sliceEnd)));
        }
        Throwable failure = null;
        try {
            int firstEnd = (int) ((long) totalItems / slices);
            task.runSlice(0, firstEnd);
        } catch (Throwable error) {
            failure = error;
        }
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException execError) {
                if (failure == null)
                    failure = execError.getCause() != null ? execError.getCause() : execError;
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                for (Future<?> remaining : futures)
                    remaining.cancel(true);
                if (failure == null) {
                    java.util.concurrent.CancellationException cancelled =
                            new java.util.concurrent.CancellationException("Interrupted while authorizing archive objects");
                    cancelled.initCause(interrupted);
                    failure = cancelled;
                }
            }
        }
        if (failure instanceof RuntimeException runtimeError)
            throw runtimeError;
        if (failure instanceof Error fatalError)
            throw fatalError;
        if (failure != null)
            throw new IllegalStateException("Cannot authorize archive objects", failure);
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
        return queryPlan(objects, filter, allowedDataStreams, allowedFois, Set.of(),
                includeResult, applyLatest, limit, ordered);
    }

    private QueryPlan queryPlan(AuthorizedObjects objects, ObsFilter filter,
                                Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                Set<Long> excludedObservationIds,
                                boolean includeResult, boolean applyLatest, long limit,
                                boolean ordered) {
        return queryPlan(objects, filter, allowedDataStreams, allowedFois, excludedObservationIds,
                includeResult, applyLatest, limit, ordered, null);
    }

    private QueryPlan queryPlan(AuthorizedObjects objects, ObsFilter filter,
                                Set<Long> allowedDataStreams, Set<Long> allowedFois,
                                Set<Long> excludedObservationIds,
                                boolean includeResult, boolean applyLatest, long limit,
                                boolean ordered,
                                JsonCqlEvaluator.ExactDuckDbPredicate exactCqlPredicate) {
        String columns = includeResult ? FULL_COLUMNS : METADATA_COLUMNS;
        String fileList = objects.byFilename().keySet().stream().map(path -> "'" + sqlLiteral(path) + "'")
                .collect(Collectors.joining(","));
        StringBuilder source = new StringBuilder("SELECT ").append(columns)
                .append(" FROM read_parquet([").append(fileList).append("], filename=true) WHERE true");
        List<Object> parameters = new ArrayList<>();

        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty())
            appendIn(source, "id_value", filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList(), parameters);
        if (excludedObservationIds != null && !excludedObservationIds.isEmpty())
            appendNotIn(source, "id_value", excludedObservationIds.stream().sorted().toList(), parameters);
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
        if (includeResult && filter.getCQLFilter() != null) {
            if (exactCqlPredicate != null) {
                if ("FALSE".equals(exactCqlPredicate.sql())) {
                    source.append(" AND FALSE");
                } else if (!"TRUE".equals(exactCqlPredicate.sql())) {
                    source.append(" AND (").append(exactCqlPredicate.sql()).append(')');
                    parameters.addAll(exactCqlPredicate.parameters());
                }
            } else {
                JsonCqlEvaluator.appendDuckDbPrefilter(filter.getCQLFilter(), "result_json", source, parameters);
            }
        }

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

    private Path resolveRealRoot() {
        try {
            return root.toRealPath();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve configured archive root " + root, e);
        }
    }

    private Path validateNormalizedRelativePath(Path relative) {
        if (relative.isAbsolute())
            throw new IllegalStateException("Archive catalog contains a non-portable absolute object path");
        Path resolved = root.resolve(relative).normalize();
        if (!resolved.startsWith(root))
            throw new IllegalStateException("Archive object escapes configured root");
        Path normalizedRelative = root.relativize(resolved);
        if (normalizedRelative.toString().isEmpty())
            throw new IllegalStateException("Cataloged archive object is not a regular file: " + resolved);
        return normalizedRelative;
    }

    private void verifyRelativeDirectories(Path realRoot, Path normalizedRelative) {
        Path parent = normalizedRelative.getParent();
        if (parent == null)
            return;
        Path currentReal = realRoot;
        Path currentLogical = root;
        for (Path component : parent) {
            currentReal = currentReal.resolve(component);
            currentLogical = currentLogical.resolve(component);
            if (Files.isSymbolicLink(currentReal))
                throw new IllegalStateException("Archive object path contains a symbolic link: " + currentLogical);
        }
    }

    private Path verifyLeafObject(Path realRoot, Path normalizedRelative, String expectedSha256) {
        return verifyLeafObject(realRoot, normalizedRelative, expectedSha256, true);
    }

    private Path verifyLeafObject(
            Path realRoot,
            Path normalizedRelative,
            String expectedSha256,
            boolean allowAdoptedLocalCache) {
        Path candidate = realRoot.resolve(normalizedRelative).normalize();
        if (!candidate.startsWith(realRoot))
            throw new IllegalStateException("Archive object resolves outside configured root");
        Path logicalPath = root.resolve(normalizedRelative);
        BasicFileAttributes before;
        try {
            before = Files.readAttributes(candidate, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (Exception e) {
            verifiedObjects.remove(candidate);
            throw new IllegalStateException("Cannot resolve archive object " + logicalPath, e);
        }
        if (before.isSymbolicLink()) {
            verifiedObjects.remove(candidate);
            throw new IllegalStateException("Archive object path contains a symbolic link: " + logicalPath);
        }
        if (!before.isRegularFile()) {
            verifiedObjects.remove(candidate);
            throw new IllegalStateException("Cataloged archive object is not a regular file: " + candidate);
        }
        verifyOnceWithAttributes(candidate, expectedSha256, before, allowAdoptedLocalCache);
        return candidate;
    }

    private Path resolveObject(Path relative) {
        Path normalizedRelative = validateNormalizedRelativePath(relative);
        Path realRoot = resolveRealRoot();
        verifyRelativeDirectories(realRoot, normalizedRelative);
        Path candidate = realRoot.resolve(normalizedRelative).normalize();
        if (!candidate.startsWith(realRoot))
            throw new IllegalStateException("Archive object resolves outside configured root");
        Path logicalPath = root.resolve(normalizedRelative);
        BasicFileAttributes attributes;
        try {
            attributes = Files.readAttributes(candidate, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve archive object " + logicalPath, e);
        }
        if (attributes.isSymbolicLink())
            throw new IllegalStateException("Archive object path contains a symbolic link: " + logicalPath);
        if (!attributes.isRegularFile())
            throw new IllegalStateException("Cataloged archive object is not a regular file: " + candidate);
        return candidate;
    }

    public void verifyGenerationObject(ArchiveGeneration generation) {
        Objects.requireNonNull(generation, "generation");
        Path realRoot = resolveRealRoot();
        Path normalizedRelative = validateNormalizedRelativePath(generation.objectPath());
        verifyRelativeDirectories(realRoot, normalizedRelative);
        verifyLeafObject(realRoot, normalizedRelative, generation.sha256(), false);
    }

    private void verifyOnceWithAttributes(
            Path object,
            String expectedSha256,
            BasicFileAttributes initialBefore,
            boolean allowAdoptedLocalCache) {
        try {
            long now = System.nanoTime();
            VerificationStamp cached = verifiedObjects.get(object);
            if (cached != null && cached.matches(expectedSha256, initialBefore, now))
                return;
            Object lock = verificationLocks[object.hashCode() & (VERIFICATION_LOCK_STRIPES - 1)];
            synchronized (lock) {
                BasicFileAttributes before = readAttributes(object);
                now = System.nanoTime();
                cached = verifiedObjects.get(object);
                if (cached != null && cached.matches(expectedSha256, before, now))
                    return;
                Local9pParquetCache localCache = activeLocalStageCache();
                if (localCache != null && localCache.canCache(expectedSha256, before.size())) {
                    if (allowAdoptedLocalCache &&
                            localCache.verifyCachedObjectOnLocalDisk(expectedSha256, before.size())) {
                        verifiedObjects.put(object, VerificationStamp.from(expectedSha256, before, now));
                        return;
                    }
                    Runnable cancellationCheck = ConnectionManager.captureCancellableOperationCheck();
                    Path tempFile = localCache.newTempFile();
                    try {
                        String actual = copyToTempAndHash(object, tempFile, localCache, cancellationCheck);
                        if (!expectedSha256.equals(actual))
                            throw new IllegalStateException("Archive checksum mismatch: " + object);
                        BasicFileAttributes after = readAttributes(object);
                        if (!sameObject(before, after))
                            throw new IllegalStateException(
                                    "Archive object changed while its checksum was being verified: " + object);
                        localCache.putVerifiedTempFile(expectedSha256, after.size(), tempFile);
                        verifiedObjects.put(object, VerificationStamp.from(expectedSha256, after, now));
                    } finally {
                        Files.deleteIfExists(tempFile);
                    }
                } else {
                    String actual = ArchiveExporter.sha256(object);
                    if (!expectedSha256.equals(actual))
                        throw new IllegalStateException("Archive checksum mismatch: " + object);
                    BasicFileAttributes after = readAttributes(object);
                    if (!sameObject(before, after))
                        throw new IllegalStateException(
                                "Archive object changed while its checksum was being verified: " + object);
                    verifiedObjects.put(object, VerificationStamp.from(expectedSha256, after, now));
                }
            }
        } catch (Exception e) {
            verifiedObjects.remove(object);
            throw new IllegalStateException("Cannot verify archive object " + object, e);
        }
    }

    /**
     * Bounded, private native-filesystem cache of SHA-256-verified immutable
     * Parquet objects for WSL2 {@code 9p}/{@code drvfs} bind mounts.
     *
     * <p>During cold verification ({@link #verifyOnceWithAttributes}), each
     * authorized Parquet object is streamed once in 16 KiB chunks onto native
     * container disk while computing its SHA-256 digest. Subsequent DuckDB
     * queries hard-link verified objects into ephemeral {@code pin-<uuid>}
     * directories on the same filesystem so concurrent LRU eviction never
     * unlinks an active DuckDB file handle and {@code duckdb-tmp} remains empty
     * at rest.
     */
    private static final class Local9pParquetCache {
        private final Path rootDir;
        private final Path objectsDir;
        private final Path pinsDir;
        private final Path tmpDir;
        private final long maxBytes;
        private final Object lock = new Object();
        private final LinkedHashMap<String, Long> lruSizes = new LinkedHashMap<>(512, 0.75f, true);
        private final Set<String> digestVerifiedInProcess = new LinkedHashSet<>();
        private long trackedBytes;
        final AtomicLong copyCount = new AtomicLong();
        final AtomicLong pinHitCount = new AtomicLong();

        private Local9pParquetCache(Path rootDir, Path objectsDir, Path pinsDir, Path tmpDir, long maxBytes) {
            this.rootDir = rootDir;
            this.objectsDir = objectsDir;
            this.pinsDir = pinsDir;
            this.tmpDir = tmpDir;
            this.maxBytes = maxBytes;
        }

        static Local9pParquetCache open(Path dir, long maxBytes, boolean resetOnOpen) throws IOException {
            if (maxBytes <= 0)
                throw new IllegalArgumentException("Local Parquet cache maxBytes must be positive");
            Path normalizedRoot = dir.toAbsolutePath().normalize();
            ensurePrivateDirectory(normalizedRoot);
            Path realRoot = normalizedRoot.toRealPath(LinkOption.NOFOLLOW_LINKS);
            ensurePrivateDirectory(realRoot);
            Path objectsDir = realRoot.resolve("objects");
            Path pinsDir = realRoot.resolve("pins");
            Path tmpDir = realRoot.resolve("tmp");
            ensurePrivateDirectory(objectsDir);
            ensurePrivateDirectory(pinsDir);
            ensurePrivateDirectory(tmpDir);
            sweepDirectoryChildren(pinsDir);
            sweepDirectoryChildren(tmpDir);
            if (resetOnOpen) {
                sweepDirectoryChildren(objectsDir);
                return new Local9pParquetCache(realRoot, objectsDir, pinsDir, tmpDir, maxBytes);
            }
            Local9pParquetCache cache = new Local9pParquetCache(realRoot, objectsDir, pinsDir, tmpDir, maxBytes);
            cache.adoptExistingObjects();
            return cache;
        }

        private void adoptExistingObjects() {
            record CandidateCachedFile(String sha256, long size, long lastModifiedMillis) {}
            List<CandidateCachedFile> retained = new ArrayList<>();
            try (var entries = Files.newDirectoryStream(objectsDir)) {
                for (Path entry : entries) {
                    try {
                        BasicFileAttributes attrs = Files.readAttributes(
                                entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                        String fileName = entry.getFileName().toString();
                        if (!attrs.isRegularFile() || attrs.isSymbolicLink() || !fileName.endsWith(".parquet")) {
                            if (attrs.isDirectory() && !attrs.isSymbolicLink())
                                sweepDirectoryChildren(entry);
                            Files.deleteIfExists(entry);
                            continue;
                        }
                        String sha = fileName.substring(0, fileName.length() - ".parquet".length());
                        if (!canCache(sha, attrs.size())) {
                            Files.deleteIfExists(entry);
                            continue;
                        }
                        retained.add(new CandidateCachedFile(
                                sha, attrs.size(), attrs.lastModifiedTime().toMillis()));
                    } catch (IOException ignored) {
                    }
                }
            } catch (IOException ignored) {
                return;
            }
            retained.sort(Comparator.comparingLong(CandidateCachedFile::lastModifiedMillis));
            synchronized (lock) {
                for (CandidateCachedFile item : retained) {
                    lruSizes.put(item.sha256(), item.size());
                    trackedBytes += item.size();
                }
                evictOverflowUnderLock(null);
            }
        }

        boolean canCache(String expectedSha256, long sizeBytes) {
            return expectedSha256 != null &&
                    sizeBytes >= 0 &&
                    sizeBytes <= maxBytes &&
                    SHA256_HEX_PATTERN.matcher(expectedSha256).matches();
        }

        Path newTempFile() {
            Path temp = tmpDir.resolve("verify-" + UUID.randomUUID() + ".tmp").normalize();
            if (!temp.getParent().equals(tmpDir))
                throw new IllegalStateException("Unsafe temporary cache file path: " + temp);
            return temp;
        }

        Path createPinDirectory() throws IOException {
            Path pinDir = pinsDir.resolve("pin-" + UUID.randomUUID()).normalize();
            if (!pinDir.getParent().equals(pinsDir))
                throw new IllegalStateException("Unsafe local Parquet pin directory: " + pinDir);
            if (supportsPosix(pinsDir)) {
                Files.createDirectory(pinDir, PosixFilePermissions.asFileAttribute(PRIVATE_CACHE_DIR_PERMISSIONS));
            } else {
                Files.createDirectory(pinDir);
            }
            return pinDir;
        }

        boolean verifyCachedObjectOnLocalDisk(String sha256, long expectedSize) {
            if (!canCache(sha256, expectedSize))
                return false;
            Path verifyPin = null;
            try {
                BasicFileAttributes beforeAttrs;
                synchronized (lock) {
                    Long recordedSize = lruSizes.get(sha256);
                    if (recordedSize == null || recordedSize != expectedSize)
                        return false;
                    Path cachedFile = objectPath(sha256);
                    try {
                        beforeAttrs = Files.readAttributes(cachedFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    } catch (IOException missing) {
                        trackedBytes = Math.max(0L, trackedBytes - recordedSize);
                        lruSizes.remove(sha256);
                        digestVerifiedInProcess.remove(sha256);
                        return false;
                    }
                    if (!beforeAttrs.isRegularFile() || beforeAttrs.isSymbolicLink() || beforeAttrs.size() != expectedSize) {
                        Files.deleteIfExists(cachedFile);
                        trackedBytes = Math.max(0L, trackedBytes - recordedSize);
                        lruSizes.remove(sha256);
                        digestVerifiedInProcess.remove(sha256);
                        return false;
                    }
                    if (digestVerifiedInProcess.contains(sha256))
                        return true;
                    verifyPin = newTempFile();
                    linkOrCopy(cachedFile, verifyPin);
                }
                String actual = ArchiveExporter.sha256(verifyPin);
                if (!sha256.equals(actual)) {
                    synchronized (lock) {
                        Long removed = lruSizes.remove(sha256);
                        if (removed != null)
                            trackedBytes = Math.max(0L, trackedBytes - removed);
                        digestVerifiedInProcess.remove(sha256);
                        Files.deleteIfExists(objectPath(sha256));
                    }
                    return false;
                }
                synchronized (lock) {
                    Long currentSize = lruSizes.get(sha256);
                    if (currentSize != null && currentSize == expectedSize) {
                        digestVerifiedInProcess.add(sha256);
                        return true;
                    }
                    return false;
                }
            } catch (Exception ignored) {
                return false;
            } finally {
                if (verifyPin != null) {
                    try {
                        Files.deleteIfExists(verifyPin);
                    } catch (IOException ignored) {
                    }
                }
            }
        }

        boolean tryPinCached(String sha256, long expectedSize, Path pinTarget) throws IOException {
            if (!canCache(sha256, expectedSize))
                return false;
            synchronized (lock) {
                Long recordedSize = lruSizes.get(sha256);
                Path cachedFile = objectPath(sha256);
                if (recordedSize == null || recordedSize != expectedSize)
                    return false;
                BasicFileAttributes attrs;
                try {
                    attrs = Files.readAttributes(cachedFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                } catch (IOException missing) {
                    trackedBytes = Math.max(0L, trackedBytes - recordedSize);
                    lruSizes.remove(sha256);
                    digestVerifiedInProcess.remove(sha256);
                    return false;
                }
                if (!attrs.isRegularFile() || attrs.isSymbolicLink() || attrs.size() != expectedSize) {
                    Files.deleteIfExists(cachedFile);
                    trackedBytes = Math.max(0L, trackedBytes - recordedSize);
                    lruSizes.remove(sha256);
                    digestVerifiedInProcess.remove(sha256);
                    return false;
                }
                linkOrCopy(cachedFile, pinTarget);
                pinHitCount.incrementAndGet();
                return true;
            }
        }

        void putVerifiedTempFile(String sha256, long sizeBytes, Path tempFile) throws IOException {
            if (!canCache(sha256, sizeBytes))
                return;
            synchronized (lock) {
                storeVerifiedTempUnderLock(sha256, sizeBytes, tempFile);
                evictOverflowUnderLock(null);
            }
        }

        void putVerifiedTempFileAndPin(String sha256, long sizeBytes, Path tempFile, Path pinTarget)
                throws IOException {
            synchronized (lock) {
                Path cachedFile = storeVerifiedTempUnderLock(sha256, sizeBytes, tempFile);
                linkOrCopy(cachedFile, pinTarget);
                evictOverflowUnderLock(sha256);
            }
        }

        long trackedBytes() {
            synchronized (lock) {
                return trackedBytes;
            }
        }

        private Path storeVerifiedTempUnderLock(String sha256, long sizeBytes, Path tempFile) throws IOException {
            Path cachedFile = objectPath(sha256);
            Long previous = lruSizes.remove(sha256);
            if (previous != null)
                trackedBytes = Math.max(0L, trackedBytes - previous);
            try {
                Files.move(tempFile, cachedFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicUnsupported) {
                Files.move(tempFile, cachedFile, StandardCopyOption.REPLACE_EXISTING);
            }
            setPrivateFilePermissionsIfSupported(cachedFile);
            lruSizes.put(sha256, sizeBytes);
            digestVerifiedInProcess.add(sha256);
            trackedBytes += sizeBytes;
            return cachedFile;
        }

        private void evictOverflowUnderLock(String protectedSha256) {
            Iterator<Map.Entry<String, Long>> it = lruSizes.entrySet().iterator();
            while (trackedBytes > maxBytes && it.hasNext()) {
                Map.Entry<String, Long> eldest = it.next();
                if (protectedSha256 != null && protectedSha256.equals(eldest.getKey()))
                    continue;
                Path victim = objectPath(eldest.getKey());
                try {
                    Files.deleteIfExists(victim);
                } catch (IOException ignored) {
                }
                trackedBytes = Math.max(0L, trackedBytes - eldest.getValue());
                digestVerifiedInProcess.remove(eldest.getKey());
                it.remove();
            }
        }

        private Path objectPath(String sha256) {
            Path path = objectsDir.resolve(sha256 + ".parquet").normalize();
            if (!path.getParent().equals(objectsDir))
                throw new IllegalStateException("Unsafe cached Parquet path: " + path);
            return path;
        }

        private static void linkOrCopy(Path source, Path target) throws IOException {
            Files.deleteIfExists(target);
            try {
                Files.createLink(target, source);
            } catch (UnsupportedOperationException | IOException linkFailed) {
                Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
                setPrivateFilePermissionsIfSupported(target);
            }
        }

        private static void ensurePrivateDirectory(Path dir) throws IOException {
            if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS)) {
                Path ancestor = dir.getParent();
                while (ancestor != null && !Files.exists(ancestor, LinkOption.NOFOLLOW_LINKS))
                    ancestor = ancestor.getParent();
                if (ancestor != null && supportsPosix(ancestor)) {
                    Files.createDirectories(dir, PosixFilePermissions.asFileAttribute(PRIVATE_CACHE_DIR_PERMISSIONS));
                } else {
                    Files.createDirectories(dir);
                }
            }
            BasicFileAttributes attrs = Files.readAttributes(dir, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
            if (!attrs.isDirectory() || attrs.isSymbolicLink())
                throw new IllegalStateException("Local Parquet stage cache directory is not a regular directory: " + dir);
            if (supportsPosix(dir))
                Files.setPosixFilePermissions(dir, PRIVATE_CACHE_DIR_PERMISSIONS);
        }

        static void setPrivateFilePermissionsIfSupported(Path file) {
            try {
                if (supportsPosix(file))
                    Files.setPosixFilePermissions(file, PRIVATE_CACHE_FILE_PERMISSIONS);
            } catch (IOException ignored) {
            }
        }

        private static boolean supportsPosix(Path path) {
            return Files.getFileAttributeView(path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
        }

        private static void sweepDirectoryChildren(Path dir) {
            try (var entries = Files.newDirectoryStream(dir)) {
                for (Path entry : entries) {
                    BasicFileAttributes attrs = Files.readAttributes(
                            entry, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                    if (attrs.isDirectory() && !attrs.isSymbolicLink())
                        sweepDirectoryChildren(entry);
                    Files.deleteIfExists(entry);
                }
            } catch (IOException ignored) {
            }
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

    private static void appendNotIn(StringBuilder sql, String column, Collection<Long> values, List<Object> parameters) {
        if (values == null || values.isEmpty())
            return;
        sql.append(" AND ").append(column).append(" NOT IN (");
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

    private static void throwIfInterruptedOrCancelled() {
        ConnectionManager.throwIfCancellableOperationCancelled();
        if (Thread.currentThread().isInterrupted())
            throw new java.util.concurrent.CancellationException("Archive query thread was interrupted");
    }

    /**
     * Checks cancellation on both sides of a raw DuckDB result-set advance.
     * Statement.cancel() remains responsible for interrupting a next() call
     * that is actively executing inside the driver; the second checkpoint
     * covers results that DuckDB materialized before cancellation arrived.
     */
    private static boolean nextCancellable(ResultSet result) throws SQLException {
        throwIfInterruptedOrCancelled();
        boolean available = result.next();
        throwIfInterruptedOrCancelled();
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
        private final Predicate<String> cqlMatcher;
        private final JsonCqlEvaluator.ExactDuckDbPredicate exactCqlPredicate;
        private final boolean zeroCacheAllowed;
        private final String cqlCacheKey;
        private final String streamsCacheKey;
        private final String foisCacheKey;
        private final boolean nonNullPhenomenonRequired;
        private final boolean nonNullResultTimeRequired;
        private final AtomicBoolean closed = new AtomicBoolean();

        private DuckDbResourcePolicy.Session sharedSession;
        private boolean sessionCountedInOpenCursors;
        private PreparedStatement currentStatement;
        private ResultSet currentResult;
        private AuthorizedObjects currentActiveObjects;
        private AuthorizedObjects currentStagedObjects;
        private List<ArchiveGeneration> currentBatchGenerations;
        private Set<UUID> currentBatchMatchedGenIds;
        private long currentBatchLimit = Long.MAX_VALUE;
        private long currentBatchEmitted;
        private ArchivedRow prefetchedNext;
        private boolean hasPrefetched;
        private long remaining;

        WindowClusterCursor(Iterator<List<ArchiveGeneration>> clusters, ObsFilter filter,
                            Set<Long> allowedDataStreams, Set<Long> allowedFois, long limit,
                            Predicate<String> cqlMatcher) {
            this.clusters = clusters;
            this.filter = filter;
            this.allowedDataStreams = allowedDataStreams;
            this.allowedFois = allowedFois;
            this.remaining = limit;
            boolean hasLatest = (filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                    (filter.getResultTime() != null && filter.getResultTime().isLatestTime());
            this.exactCqlPredicate = (!hasLatest && cqlMatcher != null && filter.getCQLFilter() != null)
                    ? JsonCqlEvaluator.tryBuildExactDuckDbPredicate(filter.getCQLFilter(), "result_json").orElse(null)
                    : null;
            this.cqlMatcher = this.exactCqlPredicate != null ? null : cqlMatcher;
            boolean allowCache = (cqlMatcher != null || this.exactCqlPredicate != null) &&
                    directReadFaultInjector == null &&
                    (filter.getInternalIDs() == null || filter.getInternalIDs().isEmpty());
            String resolvedKey = allowCache
                    ? JsonCqlEvaluator.canonicalCacheKey(filter.getCQLFilter()).orElse(null)
                    : null;
            this.zeroCacheAllowed = resolvedKey != null;
            this.cqlCacheKey = resolvedKey;
            this.streamsCacheKey = zeroCacheAllowed ? canonicalIdSetKey(allowedDataStreams) : null;
            this.foisCacheKey = zeroCacheAllowed ? canonicalIdSetKey(allowedFois) : null;
            this.nonNullPhenomenonRequired = filter.getPhenomenonTime() != null;
            this.nonNullResultTimeRequired = filter.getResultTime() != null;
        }

        private CqlGenerationCountKey cacheKeyFor(ArchiveGeneration generation) {
            return new CqlGenerationCountKey(
                    generation.sha256(),
                    streamsCacheKey,
                    foisCacheKey,
                    cqlCacheKey,
                    nonNullPhenomenonRequired,
                    nonNullResultTimeRequired,
                    "");
        }

        @Override
        public boolean hasNext() {
            if (hasPrefetched)
                return true;
            if (closed.get() || remaining == 0)
                return false;
            try {
                while (true) {
                    if (currentResult != null) {
                        while (nextCancellable(currentResult)) {
                            String resultJson = currentResult.getString(10);
                            if (cqlMatcher != null && !cqlMatcher.test(resultJson)) {
                                continue;
                            }
                            currentBatchEmitted++;
                            ArchiveGeneration generation = generationForFilename(
                                    currentActiveObjects, currentResult.getString(1));
                            if (currentBatchMatchedGenIds != null) {
                                currentBatchMatchedGenIds.add(generation.id());
                            }
                            Integer foiScope = currentResult.getObject(6, Integer.class);
                            Long foiId = currentResult.getObject(7, Long.class);
                            prefetchedNext = new ArchivedRow(generation, new ArchivedObservation(
                                    currentResult.getInt(2), currentResult.getLong(3),
                                    currentResult.getInt(4), currentResult.getLong(5),
                                    foiScope, foiId,
                                    UtcJdbcTime.getDuckDbInstant(currentResult, 8),
                                    UtcJdbcTime.getDuckDbInstant(currentResult, 9),
                                    resultJson, currentResult.getString(11)));
                            hasPrefetched = true;
                            return true;
                        }
                        if (currentBatchLimit == Long.MAX_VALUE || currentBatchEmitted < currentBatchLimit) {
                            recordBatchZeroMatches();
                        }
                        closeCurrentBatch();
                    }
                    if (!clusters.hasNext()) {
                        close();
                        return false;
                    }
                    List<ArchiveGeneration> cluster = clusters.next();
                    AuthorizedObjects objects = authorize(cluster);
                    if (objects.byFilename().isEmpty())
                        continue;
                    List<ArchiveGeneration> activeGenerations = cluster;
                    if (zeroCacheAllowed) {
                        Map<String, ArchiveGeneration> uncachedMap = new LinkedHashMap<>(objects.byFilename().size() * 2);
                        List<ArchiveGeneration> uncachedGens = new ArrayList<>(cluster.size());
                        for (Map.Entry<String, ArchiveGeneration> entry : objects.byFilename().entrySet()) {
                            ArchiveGeneration gen = entry.getValue();
                            if (gen.rowCount() <= 0L || (gen.parityVerified() &&
                                    Long.valueOf(0L).equals(cqlGenerationCounts.getIfPresent(cacheKeyFor(gen))))) {
                                continue;
                            }
                            uncachedMap.put(entry.getKey(), gen);
                            uncachedGens.add(gen);
                        }
                        if (uncachedMap.isEmpty())
                            continue;
                        if (uncachedMap.size() != objects.byFilename().size()) {
                            objects = new AuthorizedObjects(uncachedMap, objects.stampsByGenId());
                            activeGenerations = uncachedGens;
                        }
                    }
                    openNextBatch(objects, activeGenerations);
                }
            } catch (RuntimeException | Error error) {
                closeAndSuppress(this, error);
                throw error;
            } catch (Exception checked) {
                closeAndSuppress(this, checked);
                throw new IllegalStateException("Cannot decode archived observation", checked);
            }
        }

        private void openNextBatch(AuthorizedObjects objects, List<ArchiveGeneration> activeGenerations) {
            throwIfInterruptedOrCancelled();
            boolean hasCql = filter.getCQLFilter() != null;
            final long clusterLimit = (hasCql && exactCqlPredicate == null) ? Long.MAX_VALUE : remaining;
            Function<AuthorizedObjects, QueryPlan> planFactory = auth -> queryPlan(
                    auth, filter, allowedDataStreams, allowedFois, Set.of(),
                    true, !hasCql, clusterLimit, true, exactCqlPredicate);
            PreparedStatement statement = null;
            ResultSet result = null;
            AuthorizedObjects stagedObjects = null;
            try {
                if (sharedSession == null) {
                    sharedSession = DuckDbResourcePolicy.open(config, queryOperationName(filter));
                    openCursors.incrementAndGet();
                    sessionCountedInOpenCursors = true;
                }
                AuthorizedObjects activeObjects = objects;
                if (shouldProactivelyStage(objects, sharedSession.operationDirectory())) {
                    stagedObjects = stageAuthorizedObjectsLocally(objects, sharedSession.operationDirectory());
                    activeObjects = stagedObjects;
                    QueryPlan stagedPlan = planFactory.apply(activeObjects);
                    statement = ConnectionManager.trackCancellableStatement(
                            sharedSession.connection().prepareStatement(stagedPlan.sql()), "DuckDB cold query");
                    bind(statement, stagedPlan.parameters());
                    result = statement.executeQuery();
                } else {
                    try {
                        maybeInjectDirectReadFault(objects);
                        QueryPlan plan = planFactory.apply(objects);
                        statement = ConnectionManager.trackCancellableStatement(
                                sharedSession.connection().prepareStatement(plan.sql()), "DuckDB cold query");
                        bind(statement, plan.parameters());
                        result = statement.executeQuery();
                    } catch (Exception directError) {
                        if (!isTransientReadMemoryError(directError))
                            throw directError;
                        closeAndSuppress(result, directError);
                        closeAndSuppress(statement, directError);
                        result = null;
                        statement = null;
                        stagedObjects = stageAuthorizedObjectsLocally(objects, sharedSession.operationDirectory());
                        activeObjects = stagedObjects;
                        QueryPlan stagedPlan = planFactory.apply(activeObjects);
                        statement = ConnectionManager.trackCancellableStatement(
                                sharedSession.connection().prepareStatement(stagedPlan.sql()), "DuckDB cold query");
                        bind(statement, stagedPlan.parameters());
                        result = statement.executeQuery();
                    }
                }
                currentStatement = statement;
                currentResult = result;
                currentActiveObjects = activeObjects;
                currentStagedObjects = stagedObjects;
                currentBatchGenerations = zeroCacheAllowed ? activeGenerations : null;
                currentBatchMatchedGenIds = zeroCacheAllowed ? new java.util.HashSet<>() : null;
                currentBatchLimit = clusterLimit;
                currentBatchEmitted = 0;
            } catch (Exception | Error error) {
                closeAndSuppress(result, error);
                closeAndSuppress(statement, error);
                if (stagedObjects != null)
                    cleanupStagedBatchObjects(stagedObjects);
                if (error instanceof Error fatal)
                    throw fatal;
                throw new IllegalStateException(
                        "Cannot query " + activeGenerations.size() + " archive generation(s)", error);
            }
        }

        private void recordBatchZeroMatches() {
            if (!zeroCacheAllowed || currentBatchGenerations == null || currentBatchMatchedGenIds == null)
                return;
            for (ArchiveGeneration generation : currentBatchGenerations) {
                if (!currentBatchMatchedGenIds.contains(generation.id()) &&
                        isEligibleForCqlCountCache(generation, filter)) {
                    cqlGenerationCounts.put(cacheKeyFor(generation), 0L);
                }
            }
        }

        @Override
        public ArchivedRow next() {
            if (!hasNext())
                throw new NoSuchElementException();
            ArchivedRow value = prefetchedNext;
            prefetchedNext = null;
            hasPrefetched = false;
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
            Throwable failure = null;
            try {
                failure = closeCurrentBatchAndCollect(failure);
                failure = closeAndCollect(sharedSession, failure);
            } finally {
                if (sessionCountedInOpenCursors) {
                    openCursors.decrementAndGet();
                    sessionCountedInOpenCursors = false;
                }
                sharedSession = null;
                prefetchedNext = null;
                hasPrefetched = false;
            }
            if (failure != null)
                throw new IllegalStateException("Cannot close archive query resources", failure);
        }

        private void closeCurrentBatch() {
            Throwable failure = closeCurrentBatchAndCollect(null);
            if (failure != null)
                throw new IllegalStateException("Cannot close archive batch resources", failure);
        }

        private Throwable closeCurrentBatchAndCollect(Throwable failure) {
            failure = closeAndCollect(currentResult, failure);
            failure = closeAndCollect(currentStatement, failure);
            if (currentStagedObjects != null)
                cleanupStagedBatchObjects(currentStagedObjects);
            currentResult = null;
            currentStatement = null;
            currentActiveObjects = null;
            currentStagedObjects = null;
            currentBatchGenerations = null;
            currentBatchMatchedGenIds = null;
            return failure;
        }
    }

    private final class RowCursor implements Iterator<ArchivedRow>, AutoCloseable {
        private final DuckDbResourcePolicy.Session duckSession;
        private final PreparedStatement statement;
        private final ResultSet result;
        private final AuthorizedObjects objects;
        private final AuthorizedObjects stagedObjects;
        private final AtomicBoolean closed = new AtomicBoolean();
        private ArchivedRow next;
        private boolean prefetched;

        RowCursor(DuckDbResourcePolicy.Session duckSession, PreparedStatement statement, ResultSet result,
                  AuthorizedObjects objects, AuthorizedObjects stagedObjects) {
            this.duckSession = duckSession;
            this.statement = statement;
            this.result = result;
            this.objects = objects;
            this.stagedObjects = stagedObjects;
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
                if (stagedObjects != null)
                    cleanupStagedBatchObjects(stagedObjects);
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
    private record AuthorizedObjects(
            Map<String, ArchiveGeneration> byFilename,
            Map<UUID, VerificationStamp> stampsByGenId) {
        AuthorizedObjects(Map<String, ArchiveGeneration> byFilename) {
            this(byFilename, Map.of());
        }
    }
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
