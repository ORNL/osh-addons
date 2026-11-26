package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.duckdb.DuckDBAppender;
import org.duckdb.DuckDBConnection;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Multi-tier LSM Parquet V2 compaction, settled-overlay folding, monthly
 * consolidation, periodic defragmentation, and reader-pin-safe superseded
 * generation cleanup.
 */
public final class ArchiveCompactionManager {
    private static final Logger LOG = LoggerFactory.getLogger(ArchiveCompactionManager.class);

    public static final int DEFAULT_MAX_UTC_DAYS_PER_CYCLE = 4;
    public static final int DEFAULT_MAX_MONTHLY_JOBS_PER_CYCLE = 1;
    public static final int MONTHLY_DEFRAG_OVERLAY_THRESHOLD = 64;

    private static final int DAILY_EVENTS_ROW_GROUP_SIZE = 8_192;
    private static final int MONTHLY_EVENTS_ROW_GROUP_SIZE = 16_384;
    private static final int DAILY_TELEMETRY_ROW_GROUP_SIZE = 65_536;
    private static final long MAX_PROJECTED_EVENT_PART_UNCOMPRESSED_BYTES = 16L * 1024L * 1024L;
    private static final long MAX_COMPACTION_CYCLE_NANOS = TimeUnit.SECONDS.toNanos(40);
    private static final int SOURCE_READ_CHUNK_ROWS = 60_000;

    private final ArchiveCatalog catalog;
    private final ParquetArchiveReader archiveReader;
    private final TieredArchiveConfig config;
    private final int idScope;
    private final String observationTable;
    private final String dataStreamTable;
    private final Path root;
    private final Consumer<Collection<UUID>> retiredGenerationHook;
    private volatile boolean lastJobDeferredBusy;

    public record CompactionCycleSummary(
            int daysCompacted,
            int monthsConsolidated,
            int monthlyGenerationsDefragmented,
            int deletingGenerationsDrained) {}

    public ArchiveCompactionManager(
            ArchiveCatalog catalog,
            ParquetArchiveReader archiveReader,
            TieredArchiveConfig config,
            int idScope,
            String observationTable,
            String dataStreamTable,
            Consumer<Collection<UUID>> retiredGenerationHook) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.archiveReader = Objects.requireNonNull(archiveReader, "archiveReader");
        this.config = Objects.requireNonNull(config, "config");
        this.idScope = idScope;
        this.observationTable = ArchiveExporter.safeIdentifier(observationTable);
        this.dataStreamTable = ArchiveExporter.safeIdentifier(dataStreamTable);
        this.root = Path.of(config.rootDirectory).toAbsolutePath().normalize();
        this.retiredGenerationHook = retiredGenerationHook;
    }

    public static boolean hasInteractiveDuckDbWaiters() {
        return DuckDbResourcePolicy.hasInteractiveWaiters();
    }

    public CompactionCycleSummary runCompactionCycle(Instant now) {
        return runCompactionCycle(
                now,
                DEFAULT_MAX_UTC_DAYS_PER_CYCLE,
                DEFAULT_MAX_MONTHLY_JOBS_PER_CYCLE);
    }

    public CompactionCycleSummary runCompactionCycle(
            Instant now, int maxUtcDays, int maxMonthlyJobs) {
        Objects.requireNonNull(now, "now");
        long cycleStartNanos = System.nanoTime();
        lastJobDeferredBusy = false;
        int drainedBefore = drainDeletingGenerations(now);
        int daysCompacted = 0;
        int monthsConsolidated = 0;
        int defragmented = 0;

        if (maxUtcDays > 0) {
            List<ArchiveCatalog.CompactionWindowCandidate> eligibleDays =
                    catalog.findEligibleUtcDaysForCompaction(now, observationTable, maxUtcDays);
            for (ArchiveCatalog.CompactionWindowCandidate day : eligibleDays) {
                if (daysCompacted > 0 &&
                        (DuckDbResourcePolicy.hasInteractiveWaiters() ||
                         System.nanoTime() - cycleStartNanos >= MAX_COMPACTION_CYCLE_NANOS))
                    break;
                if (compactUtcDay(day, now))
                    daysCompacted++;
                else if (lastJobDeferredBusy)
                    break;
            }
        }

        if (!lastJobDeferredBusy &&
                maxMonthlyJobs > 0 &&
                !DuckDbResourcePolicy.hasInteractiveWaiters()) {
            List<ArchiveCatalog.CompactionWindowCandidate> eligibleMonths =
                    catalog.findEligibleUtcMonthsForConsolidation(now, observationTable, maxMonthlyJobs);
            for (ArchiveCatalog.CompactionWindowCandidate month : eligibleMonths) {
                if (monthsConsolidated > 0 && DuckDbResourcePolicy.hasInteractiveWaiters())
                    break;
                if (consolidateUtcMonth(month, now))
                    monthsConsolidated++;
                else if (lastJobDeferredBusy)
                    break;
            }
            int remainingMonthlyBudget = maxMonthlyJobs - monthsConsolidated;
            if (!lastJobDeferredBusy &&
                    remainingMonthlyBudget > 0 &&
                    !DuckDbResourcePolicy.hasInteractiveWaiters()) {
                List<ArchiveCatalog.CompactionWindowCandidate> defragCandidates =
                        catalog.findEligibleMonthlyGenerationsForDefrag(
                                now, observationTable, MONTHLY_DEFRAG_OVERLAY_THRESHOLD, remainingMonthlyBudget);
                for (ArchiveCatalog.CompactionWindowCandidate candidate : defragCandidates) {
                    if (defragmented > 0 && DuckDbResourcePolicy.hasInteractiveWaiters())
                        break;
                    if (defragmentMonthlyGeneration(candidate, now))
                        defragmented++;
                    else if (lastJobDeferredBusy)
                        break;
                }
            }
        }

        int drainedAfter = drainDeletingGenerations(now);
        return new CompactionCycleSummary(
                daysCompacted,
                monthsConsolidated,
                defragmented,
                drainedBefore + drainedAfter);
    }

    public boolean compactUtcDay(ArchiveCatalog.CompactionWindowCandidate dayCandidate, Instant now) {
        List<ArchiveGeneration> sources = dayCandidate.sourceGenerations();
        if (sources.isEmpty())
            return false;
        List<UUID> sourceIds = sources.stream().map(ArchiveGeneration::id).toList();
        ArchiveCatalog.CompactionSchemaCatalog schemaCatalog =
                catalog.loadCompactionSchemas(sourceIds, dataStreamTable);
        Set<Long> eventStreams = new HashSet<>();
        Set<Long> telemetryStreams = new HashSet<>();
        for (Long dsId : schemaCatalog.schemasByStreamId().keySet()) {
            String outName = schemaCatalog.outputNameByStreamId().get(dsId);
            if (TieredArchiveConfig.isEventOutputName(outName))
                eventStreams.add(dsId);
            else
                telemetryStreams.add(dsId);
        }

        Instant settledOverlayCutoff = now.minus(2, ChronoUnit.HOURS);
        Map<Long, ArchiveCatalog.FoldedOverlayCandidate> settledOverlays =
                catalog.loadSettledOverlaysForFolding(
                        sourceIds, settledOverlayCutoff, dayCandidate.windowStart(), dayCandidate.windowEnd());
        Set<Long> preReferencedIds = catalog.loadReferencedObservationIdsOnGenerations(sourceIds);

        LocalDate partitionDate = dayCandidate.windowStart().atZone(ZoneOffset.UTC).toLocalDate();
        List<ProfileTargetSpec> specs = new ArrayList<>(2);
        if (!eventStreams.isEmpty()) {
            specs.add(new ProfileTargetSpec(
                    TieredArchiveConfig.PROFILE_DAILY_EVENTS,
                    eventStreams,
                    partitionDate,
                    true,
                    DAILY_EVENTS_ROW_GROUP_SIZE));
        }
        if (!telemetryStreams.isEmpty()) {
            specs.add(new ProfileTargetSpec(
                    TieredArchiveConfig.PROFILE_DAILY_TELEMETRY,
                    telemetryStreams,
                    partitionDate,
                    false,
                    DAILY_TELEMETRY_ROW_GROUP_SIZE));
        }
        return executeCompactionJob(
                dayCandidate.windowStart(),
                dayCandidate.windowEnd(),
                sources,
                schemaCatalog,
                settledOverlays,
                preReferencedIds,
                specs,
                now);
    }

    public boolean consolidateUtcMonth(ArchiveCatalog.CompactionWindowCandidate monthCandidate, Instant now) {
        return consolidateOrDefragMonthly(monthCandidate, now);
    }

    public boolean defragmentMonthlyGeneration(ArchiveCatalog.CompactionWindowCandidate candidate, Instant now) {
        return consolidateOrDefragMonthly(candidate, now);
    }

    private boolean consolidateOrDefragMonthly(
            ArchiveCatalog.CompactionWindowCandidate candidate, Instant now) {
        List<ArchiveGeneration> sources = candidate.sourceGenerations();
        if (sources.isEmpty())
            return false;
        List<UUID> sourceIds = sources.stream().map(ArchiveGeneration::id).toList();
        ArchiveCatalog.CompactionSchemaCatalog schemaCatalog =
                catalog.loadCompactionSchemas(sourceIds, dataStreamTable);
        Instant settledOverlayCutoff = now.minus(7, ChronoUnit.DAYS);
        Map<Long, ArchiveCatalog.FoldedOverlayCandidate> settledOverlays =
                catalog.loadSettledOverlaysForFolding(
                        sourceIds, settledOverlayCutoff, candidate.windowStart(), candidate.windowEnd());
        Set<Long> preReferencedIds = catalog.loadReferencedObservationIdsOnGenerations(sourceIds);

        LocalDate monthFirstDate = candidate.windowStart().atZone(ZoneOffset.UTC).toLocalDate().withDayOfMonth(1);
        List<ProfileTargetSpec> specs = List.of(new ProfileTargetSpec(
                TieredArchiveConfig.PROFILE_MONTHLY_EVENTS,
                null,
                monthFirstDate,
                true,
                MONTHLY_EVENTS_ROW_GROUP_SIZE));
        return executeCompactionJob(
                candidate.windowStart(),
                candidate.windowEnd(),
                sources,
                schemaCatalog,
                settledOverlays,
                preReferencedIds,
                specs,
                now);
    }

    private record ProfileTargetSpec(
            String targetProfile,
            Set<Long> allowedStreamIds,
            LocalDate directoryDate,
            boolean projectedEventColumns,
            int rowGroupSize) {}

    private record StagedRow(
            int idScope,
            long id,
            int dataStreamScope,
            long dataStreamId,
            Integer foiScope,
            Long foiId,
            Instant phenomenonTime,
            Instant resultTime,
            String resultJson) {}

    private static final class PendingPart {
        final UUID generationId;
        final String targetProfile;
        final Path directory;
        final Path relativeParquet;
        final Path partialParquet;
        final Path finalParquet;
        final Path relativeSchemas;
        final Path partialSchemas;
        final Path finalSchemas;
        final Path relativeManifest;
        final Path partialManifest;
        final Path finalManifest;
        final List<Path> createdFiles;
        final String parquetSha256;
        final long parquetBytes;
        final long rowCount;
        final long minId;
        final long maxId;
        final Instant minPhenomenonTime;
        final Instant maxPhenomenonTime;
        final Instant minResultTime;
        final Instant maxResultTime;
        final Map<Long, StreamStatsAccumulator> streamAccumulators;
        final Map<Long, ArchiveCatalog.StreamLatestCandidate> latestByStream;
        final Map<String, Long> eventLocators;
        final Map<Long, PendingRowOwner> referencedRowOwners;
        Instant assignedWindowStart;
        Instant assignedWindowEnd;

        PendingPart(
                UUID generationId,
                String targetProfile,
                Path directory,
                Path relativeParquet,
                Path partialParquet,
                Path finalParquet,
                Path relativeSchemas,
                Path partialSchemas,
                Path finalSchemas,
                Path relativeManifest,
                Path partialManifest,
                Path finalManifest,
                List<Path> createdFiles,
                String parquetSha256,
                long parquetBytes,
                PartAccumulator acc) {
            this.generationId = generationId;
            this.targetProfile = targetProfile;
            this.directory = directory;
            this.relativeParquet = relativeParquet;
            this.partialParquet = partialParquet;
            this.finalParquet = finalParquet;
            this.relativeSchemas = relativeSchemas;
            this.partialSchemas = partialSchemas;
            this.finalSchemas = finalSchemas;
            this.relativeManifest = relativeManifest;
            this.partialManifest = partialManifest;
            this.finalManifest = finalManifest;
            this.createdFiles = createdFiles;
            this.parquetSha256 = parquetSha256;
            this.parquetBytes = parquetBytes;
            this.rowCount = acc.rowCount;
            this.minId = acc.minId;
            this.maxId = acc.maxId;
            this.minPhenomenonTime = acc.minPhenomenonTime;
            this.maxPhenomenonTime = acc.maxPhenomenonTime;
            this.minResultTime = acc.minResultTime;
            this.maxResultTime = acc.maxResultTime;
            this.streamAccumulators = acc.streamAccumulators;
            this.latestByStream = acc.latestByStream;
            this.eventLocators = acc.eventLocators;
            this.referencedRowOwners = acc.referencedRowOwners;
        }
    }

    private record PendingRowOwner(long dataStreamId, Long foiId, int summaryMask) {}

    private record PublishedFileBundle(
            ArchiveCatalog.CompactedGenerationActivation activation,
            Path directory,
            Path partialParquet,
            Path finalParquet,
            Path partialSchemas,
            Path finalSchemas,
            Path partialManifest,
            Path finalManifest,
            List<Path> createdFiles) {
        void promotePartialsToFinal() throws Exception {
            ArchiveExporter.atomicMove(partialParquet, finalParquet);
            ArchiveExporter.atomicMove(partialSchemas, finalSchemas);
            ArchiveExporter.atomicMove(partialManifest, finalManifest);
            ArchiveExporter.forceDirectory(directory);
        }
    }

    private boolean executeCompactionJob(
            Instant jobWindowStart,
            Instant jobWindowEnd,
            List<ArchiveGeneration> sources,
            ArchiveCatalog.CompactionSchemaCatalog schemaCatalog,
            Map<Long, ArchiveCatalog.FoldedOverlayCandidate> settledOverlays,
            Set<Long> preReferencedIds,
            List<ProfileTargetSpec> profileSpecs,
            Instant now) {
        List<UUID> sourceIds = sources.stream().map(ArchiveGeneration::id).toList();
        List<Path> publishedFilesToRollback = new ArrayList<>();
        List<PublishedFileBundle> allBundles = new ArrayList<>();
        boolean swapAttempted = false;
        boolean swapCommitted = false;

        try {
            List<ArchiveCatalog.CompactedGenerationActivation> allActivations = new ArrayList<>();
            Map<Long, ArchiveCatalog.CompactedRowOwner> rowOwners = new HashMap<>();
            long totalExpectedRowsAcrossAllProfiles = 0L;
            long totalWrittenRowsAcrossAllProfiles = 0L;

            try (ParquetArchiveReader.PinnedGenerations ignored = archiveReader.pinGenerations(sourceIds)) {
                for (ArchiveGeneration src : sources) {
                    archiveReader.verifyGenerationObject(src);
                    totalExpectedRowsAcrossAllProfiles = Math.addExact(
                            totalExpectedRowsAcrossAllProfiles, src.rowCount());
                }

                Set<Long> trackOwnerIds = new HashSet<>(preReferencedIds);
                trackOwnerIds.addAll(settledOverlays.keySet());

                for (ProfileTargetSpec spec : profileSpecs) {
                    ProfileBuildResult built = buildAndVerifyProfileParts(
                            jobWindowStart,
                            jobWindowEnd,
                            sources,
                            schemaCatalog,
                            settledOverlays,
                            trackOwnerIds,
                            spec,
                            publishedFilesToRollback);
                    totalWrittenRowsAcrossAllProfiles = Math.addExact(
                            totalWrittenRowsAcrossAllProfiles, built.totalRows());
                    allBundles.addAll(built.bundles());
                    allActivations.addAll(built.activations());
                    rowOwners.putAll(built.rowOwners());
                }
            }

            if (totalWrittenRowsAcrossAllProfiles != totalExpectedRowsAcrossAllProfiles ||
                    allActivations.isEmpty()) {
                throw new IllegalStateException("Compaction row count mismatch: expected " +
                        totalExpectedRowsAcrossAllProfiles + " rows across source generations but produced " +
                        totalWrittenRowsAcrossAllProfiles);
            }

            for (PublishedFileBundle bundle : allBundles) {
                bundle.promotePartialsToFinal();
            }

            List<ArchiveGeneration> targetGens = allActivations.stream()
                    .map(ArchiveCatalog.CompactedGenerationActivation::generation)
                    .toList();
            ArchiveCatalog.MissingCompactedRowResolver missingResolver = missingIds ->
                    resolveMissingRowOwnersFromTargets(targetGens, missingIds);

            swapAttempted = true;
            swapCommitted = catalog.activateCompactionSwap(
                    sourceIds,
                    allActivations,
                    settledOverlays,
                    rowOwners,
                    missingResolver,
                    observationTable,
                    now);
            if (!swapCommitted) {
                deletePathsQuietly(publishedFilesToRollback);
                return false;
            }

            notifyRetiredGenerations(sources);
            return true;
        } catch (RuntimeException | Error err) {
            if (!swapCommitted && !anyTargetRegisteredOrUncertain(swapAttempted, allBundles, err))
                deletePathsQuietly(publishedFilesToRollback);
            if (!swapAttempted && isTransientDuckDbBusy(err)) {
                lastJobDeferredBusy = true;
                LOG.info("Deferring archive compaction for window [{}, {}) while DuckDB sessions are busy: {}",
                        jobWindowStart, jobWindowEnd, err.getMessage());
                return false;
            }
            throw err;
        } catch (Exception checked) {
            if (!swapCommitted && !anyTargetRegisteredOrUncertain(swapAttempted, allBundles, checked))
                deletePathsQuietly(publishedFilesToRollback);
            if (!swapAttempted && isTransientDuckDbBusy(checked)) {
                lastJobDeferredBusy = true;
                LOG.info("Deferring archive compaction for window [{}, {}) while DuckDB sessions are busy: {}",
                        jobWindowStart, jobWindowEnd, checked.getMessage());
                return false;
            }
            throw new IllegalStateException("Archive compaction failed for window [" +
                    jobWindowStart + ", " + jobWindowEnd + ")", checked);
        }
    }

    private static boolean isTransientDuckDbBusy(Throwable error) {
        Throwable cursor = error;
        int depth = 0;
        while (cursor != null && depth++ < 8) {
            if (cursor instanceof SQLTransientException)
                return true;
            cursor = cursor.getCause();
        }
        return false;
    }

    private boolean anyTargetRegisteredOrUncertain(
            boolean swapAttempted, List<PublishedFileBundle> bundles, Throwable parentError) {
        if (!swapAttempted || bundles.isEmpty())
            return false;
        try {
            for (PublishedFileBundle bundle : bundles) {
                if (catalog.isGenerationRegistered(bundle.activation().generation().id()))
                    return true;
            }
            return false;
        } catch (Throwable checkErr) {
            parentError.addSuppressed(checkErr);
            return true;
        }
    }

    private record ProfileBuildResult(
            long totalRows,
            List<PublishedFileBundle> bundles,
            List<ArchiveCatalog.CompactedGenerationActivation> activations,
            Map<Long, ArchiveCatalog.CompactedRowOwner> rowOwners) {}

    private ProfileBuildResult buildAndVerifyProfileParts(
            Instant jobWindowStart,
            Instant jobWindowEnd,
            List<ArchiveGeneration> sources,
            ArchiveCatalog.CompactionSchemaCatalog schemaCatalog,
            Map<Long, ArchiveCatalog.FoldedOverlayCandidate> settledOverlays,
            Set<Long> trackOwnerIds,
            ProfileTargetSpec spec,
            List<Path> publishedFilesToRollback) throws Exception {
        if (spec.allowedStreamIds() != null && spec.allowedStreamIds().isEmpty()) {
            return new ProfileBuildResult(0L, List.of(), List.of(), Map.of());
        }
        long maxBytesPerPart = spec.projectedEventColumns()
                ? Math.min(config.effectiveMaximumUncompressedBytesPerGeneration(),
                        MAX_PROJECTED_EVENT_PART_UNCOMPRESSED_BYTES)
                : config.effectiveMaximumUncompressedBytesPerGeneration();
        long maxRowsPerPart = Math.min(config.maximumRowsPerGeneration, 250_000L);

        MultisetRowDigest sourceDigest = new MultisetRowDigest();
        LongIdTracker sourceIdsSeen = new LongIdTracker();
        long sourceProfileRows = 0L;
        List<PendingPart> pendingParts = new ArrayList<>();
        PartAccumulator currentPart = new PartAccumulator();
        List<StagedRow> currentPartRows = new ArrayList<>();

        String streamFilterClause = null;
        if (spec.allowedStreamIds() != null) {
            streamFilterClause = spec.allowedStreamIds().stream()
                    .sorted()
                    .map(String::valueOf)
                    .collect(Collectors.joining(","));
        }

        Path realRoot = root.toRealPath();
        for (int sIdx = 0; sIdx < sources.size(); sIdx++) {
            ArchiveGeneration src = sources.get(sIdx);
            if (spec.allowedStreamIds() != null && schemaCatalog.streamIdsByGeneration() != null) {
                Set<Long> genStreams = schemaCatalog.streamIdsByGeneration().get(src.id());
                if (genStreams != null && !genStreams.isEmpty() &&
                        Collections.disjoint(genStreams, spec.allowedStreamIds())) {
                    continue;
                }
            }
            Path srcFile = realRoot.resolve(src.objectPath().normalize()).normalize();
            long offsetInSource = 0L;
            boolean sourceExhausted = false;

            while (!sourceExhausted) {
                List<StagedRow> chunkRows = new ArrayList<>();
                boolean stoppedOnChunkBytes = false;

                // Short-lived read session per source chunk so activeBackgroundCount never nests
                // and foreground queries never wait behind multi-file compaction reads.
                try (DuckDbResourcePolicy.Session readSession =
                             DuckDbResourcePolicy.open(config, "archive-compaction")) {
                    Path opDir = readSession.operationDirectory();
                    Path stagedSrc = ArchiveExporter.stageToLocalDirectory(
                            srcFile, opDir, "src-" + sIdx + "-" + offsetInSource + "-" + src.id() + ".parquet");
                    try {
                        StringBuilder readSql = new StringBuilder(
                                "SELECT id_scope, id_value, datastream_scope, datastream_id, " +
                                        "foi_scope, foi_id, phenomenon_time, result_time, result_json " +
                                        "FROM read_parquet('")
                                .append(ArchiveExporter.sqlLiteral(stagedSrc.toString()))
                                .append("', union_by_name=true)");
                        if (streamFilterClause != null) {
                            readSql.append(" WHERE datastream_id IN (").append(streamFilterClause).append(')');
                        }
                        readSql.append(" ORDER BY phenomenon_time ASC NULLS FIRST, result_time ASC NULLS FIRST, id_value ASC")
                                .append(" LIMIT ").append(SOURCE_READ_CHUNK_ROWS)
                                .append(" OFFSET ").append(offsetInSource);

                        long chunkBytes = 0L;
                        long rowsReadThisPass = 0L;
                        try (Statement readStmt = readSession.connection().createStatement();
                             ResultSet rs = readStmt.executeQuery(readSql.toString())) {
                            while (rs.next()) {
                                int rowIdScope = rs.getInt(1);
                                long obsId = rs.getLong(2);
                                int dsScope = rs.getInt(3);
                                long dsId = rs.getLong(4);
                                Integer foiScope = rs.getObject(5, Integer.class);
                                Long foiId = rs.getObject(6, Long.class);
                                Instant phenTime = UtcJdbcTime.getDuckDbInstant(rs, 7);
                                Instant resTime = UtcJdbcTime.getDuckDbInstant(rs, 8);
                                String resultJson = rs.getString(9);
                                chunkRows.add(new StagedRow(
                                        rowIdScope, obsId, dsScope, dsId, foiScope, foiId, phenTime, resTime, resultJson));
                                rowsReadThisPass++;
                                chunkBytes = Math.addExact(
                                        chunkBytes, ArchiveExporter.estimateUncompressedRowBytes(resultJson));
                                if (chunkBytes >= maxBytesPerPart && rowsReadThisPass < SOURCE_READ_CHUNK_ROWS) {
                                    stoppedOnChunkBytes = true;
                                    break;
                                }
                            }
                        }
                        offsetInSource += rowsReadThisPass;
                        if (!stoppedOnChunkBytes && rowsReadThisPass < SOURCE_READ_CHUNK_ROWS) {
                            sourceExhausted = true;
                        }
                    } finally {
                        Files.deleteIfExists(stagedSrc);
                    }
                }

                for (StagedRow raw : chunkRows) {
                    int rowIdScope = raw.idScope();
                    long obsId = raw.id();
                    int dsScope = raw.dataStreamScope();
                    long dsId = raw.dataStreamId();
                    Integer foiScope = raw.foiScope();
                    Long foiId = raw.foiId();
                    Instant phenTime = raw.phenomenonTime();
                    Instant resTime = raw.resultTime();
                    String resultJson = raw.resultJson();

                    ArchiveCatalog.FoldedOverlayCandidate folded = settledOverlays.get(obsId);
                    if (folded != null) {
                        dsId = folded.dataStreamId();
                        foiId = folded.foiId();
                        foiScope = foiId == null ? null : rowIdScope;
                        phenTime = folded.phenomenonTime();
                        resTime = folded.resultTime();
                        resultJson = folded.resultJson();
                    }

                    long estBytes = ArchiveExporter.estimateUncompressedRowBytes(resultJson);
                    boolean wouldExceed = currentPart.rowCount > 0 &&
                            (currentPart.rowCount + 1 > maxRowsPerPart ||
                             currentPart.estimatedBytes + estBytes > maxBytesPerPart);
                    boolean timestampBoundary = currentPart.lastPhenomenonTime == null ||
                            !Objects.equals(currentPart.lastPhenomenonTime, phenTime);
                    boolean hardCapReached = currentPart.rowCount >= maxRowsPerPart ||
                            currentPart.estimatedBytes + estBytes > Math.multiplyExact(maxBytesPerPart, 2L);

                    if (wouldExceed && (timestampBoundary || hardCapReached)) {
                        PendingPart written = writeAndStagePartialParquet(
                                currentPartRows, spec, currentPart, publishedFilesToRollback);
                        pendingParts.add(written);
                        currentPartRows.clear();
                        currentPart = new PartAccumulator();
                    }

                    StagedRow effective = new StagedRow(
                            rowIdScope, obsId, dsScope, dsId, foiScope, foiId, phenTime, resTime, resultJson);
                    currentPartRows.add(effective);

                    byte[] rowSha = computeSingleRowSha256(
                            obsId, dsId, foiId, phenTime, resTime, resultJson);
                    sourceDigest.observe(rowSha);
                    sourceIdsSeen.add(obsId);

                    currentPart.observe(effective, estBytes, trackOwnerIds.contains(obsId));
                    sourceProfileRows++;
                }
            }
        }

        if (currentPart.rowCount > 0) {
            PendingPart written = writeAndStagePartialParquet(
                    currentPartRows, spec, currentPart, publishedFilesToRollback);
            pendingParts.add(written);
            currentPartRows.clear();
        }

        if (sourceProfileRows == 0L) {
            return new ProfileBuildResult(0L, List.of(), List.of(), Map.of());
        }

        sourceIdsSeen.verifyNoDuplicates();
        String expectedHexDigest = sourceDigest.finishHex();
        assignPartWindows(pendingParts, jobWindowStart, jobWindowEnd);

        List<PublishedFileBundle> bundles = new ArrayList<>(pendingParts.size());
        for (PendingPart part : pendingParts) {
            bundles.add(publishPartMetadataFiles(part, spec, schemaCatalog, expectedHexDigest));
        }

        verifyPublishedPartsParity(
                bundles,
                spec,
                sourceProfileRows,
                expectedHexDigest);

        List<ArchiveCatalog.CompactedGenerationActivation> activations = new ArrayList<>(bundles.size());
        Map<Long, ArchiveCatalog.CompactedRowOwner> owners = new HashMap<>();
        for (int i = 0; i < pendingParts.size(); i++) {
            PendingPart part = pendingParts.get(i);
            ArchiveCatalog.CompactedGenerationActivation act = bundles.get(i).activation();
            activations.add(act);
            for (Map.Entry<Long, PendingRowOwner> e : part.referencedRowOwners.entrySet()) {
                PendingRowOwner p = e.getValue();
                owners.put(e.getKey(), new ArchiveCatalog.CompactedRowOwner(
                        act.generation().id(),
                        act.generation().sha256(),
                        act.generation().profile(),
                        p.dataStreamId(),
                        p.foiId(),
                        p.summaryMask()));
            }
        }
        return new ProfileBuildResult(sourceProfileRows, bundles, activations, owners);
    }

    private static final int DUCKDB_INSERT_BATCH_SIZE = 250;

    private static void assignPartWindows(
            List<PendingPart> parts, Instant jobWindowStart, Instant jobWindowEnd) {
        if (parts.isEmpty())
            return;
        if (parts.size() == 1) {
            parts.get(0).assignedWindowStart = jobWindowStart;
            parts.get(0).assignedWindowEnd = jobWindowEnd;
            return;
        }
        boolean strictlyIncreasingCuts = true;
        for (int i = 0; i < parts.size() - 1; i++) {
            Instant prevMax = parts.get(i).maxPhenomenonTime;
            Instant nextMin = parts.get(i + 1).minPhenomenonTime;
            if (prevMax == null || nextMin == null || !prevMax.isBefore(nextMin)) {
                strictlyIncreasingCuts = false;
                break;
            }
        }
        if (!strictlyIncreasingCuts) {
            for (PendingPart part : parts) {
                part.assignedWindowStart = jobWindowStart;
                part.assignedWindowEnd = jobWindowEnd;
            }
            return;
        }
        Instant cursor = jobWindowStart;
        for (int i = 0; i < parts.size(); i++) {
            PendingPart part = parts.get(i);
            part.assignedWindowStart = cursor;
            if (i == parts.size() - 1) {
                part.assignedWindowEnd = jobWindowEnd;
            } else {
                Instant cut = parts.get(i + 1).minPhenomenonTime;
                part.assignedWindowEnd = cut;
                cursor = cut;
            }
        }
    }

    private PendingPart writeAndStagePartialParquet(
            List<StagedRow> partRows,
            ProfileTargetSpec spec,
            PartAccumulator acc,
            List<Path> rollbackPaths) throws Exception {
        Path directory = root.resolve("datasets/observations/date=" + spec.directoryDate())
                .resolve("profile=" + ArchiveExporter.safePathComponent(spec.targetProfile()));
        ArchivePathPolicy.prepareDirectory(root, directory);

        UUID genId = UUID.randomUUID();
        Path relativeParquet = root.relativize(directory.resolve("generation-" + genId + ".parquet"));
        Path finalParquet = root.resolve(relativeParquet);
        Path partialParquet = finalParquet.resolveSibling(finalParquet.getFileName() + ".partial");

        Path relativeSchemas = root.relativize(directory.resolve("generation-" + genId + ".schemas.json.gz"));
        Path finalSchemas = root.resolve(relativeSchemas);
        Path partialSchemas = finalSchemas.resolveSibling(finalSchemas.getFileName() + ".partial");

        Path relativeManifest = root.relativize(directory.resolve("generation-" + genId + ".manifest.json"));
        Path finalManifest = root.resolve(relativeManifest);
        Path partialManifest = finalManifest.resolveSibling(finalManifest.getFileName() + ".partial");

        ArchivePathPolicy.prepareWriteTarget(root, partialParquet);
        ArchivePathPolicy.prepareWriteTarget(root, partialSchemas);
        ArchivePathPolicy.prepareWriteTarget(root, partialManifest);
        ArchivePathPolicy.prepareWriteTarget(root, finalParquet);
        ArchivePathPolicy.prepareWriteTarget(root, finalSchemas);
        ArchivePathPolicy.prepareWriteTarget(root, finalManifest);

        List<Path> created = List.of(
                finalManifest, finalSchemas, finalParquet,
                partialManifest, partialSchemas, partialParquet);
        rollbackPaths.addAll(created);

        // Dedicated short-lived DuckDB session per bounded part so native BufferManager
        // memory is 100% released when the part closes (never retaining deleted row-group blocks).
        try (DuckDbResourcePolicy.Session writeSession =
                     DuckDbResourcePolicy.open(config, "archive-compaction")) {
            Path opDir = writeSession.operationDirectory();
            Path tempOut = opDir.resolve("part-" + genId + ".parquet").normalize();
            if (!tempOut.getParent().equals(opDir.normalize()))
                throw new IllegalStateException("Compaction temporary Parquet path escaped operation directory");

            boolean projected = spec.projectedEventColumns();
            try (Statement ddl = writeSession.connection().createStatement()) {
                if (projected) {
                    ddl.execute("CREATE TABLE compaction_part (" +
                            "id_scope INTEGER NOT NULL, id_value BIGINT NOT NULL, " +
                            "datastream_scope INTEGER NOT NULL, datastream_id BIGINT NOT NULL, " +
                            "foi_scope INTEGER, foi_id BIGINT, " +
                            "phenomenon_time TIMESTAMP, result_time TIMESTAMP, result_json VARCHAR, " +
                            "ocr_norm VARCHAR, occ_obs_id VARCHAR, event_id VARCHAR, " +
                            "adj_code INTEGER, adj_count INTEGER, " +
                            "gamma_alarm BOOLEAN, neutron_alarm BOOLEAN, " +
                            "max_gamma DOUBLE, max_neutron DOUBLE)");
                } else {
                    ddl.execute("CREATE TABLE compaction_part (" +
                            "id_scope INTEGER NOT NULL, id_value BIGINT NOT NULL, " +
                            "datastream_scope INTEGER NOT NULL, datastream_id BIGINT NOT NULL, " +
                            "foi_scope INTEGER, foi_id BIGINT, " +
                            "phenomenon_time TIMESTAMP, result_time TIMESTAMP, result_json VARCHAR)");
                }
            }

            appendStagedRowsNative(writeSession.connection(), partRows, projected);

            String copySql = buildV2CopySql(spec, tempOut);
            try (Statement stmt = writeSession.connection().createStatement()) {
                stmt.execute(copySql);
            }

            Files.deleteIfExists(partialParquet);
            ArchiveExporter.copyBounded(tempOut, partialParquet);
            Files.deleteIfExists(tempOut);
        }

        ArchiveExporter.forceFile(partialParquet);
        String parquetSha256 = ArchiveExporter.sha256(partialParquet);
        long parquetBytes = Files.size(partialParquet);

        return new PendingPart(
                genId,
                spec.targetProfile(),
                directory,
                relativeParquet,
                partialParquet,
                finalParquet,
                relativeSchemas,
                partialSchemas,
                finalSchemas,
                relativeManifest,
                partialManifest,
                finalManifest,
                created,
                parquetSha256,
                parquetBytes,
                acc);
    }

    private PublishedFileBundle publishPartMetadataFiles(
            PendingPart part,
            ProfileTargetSpec spec,
            ArchiveCatalog.CompactionSchemaCatalog schemaCatalog,
            String aggregateDigestHex) throws Exception {
        List<ArchiveCatalog.SchemaSnapshot> partSchemas = part.streamAccumulators.keySet().stream()
                .sorted()
                .map(dsId -> {
                    ArchiveCatalog.SchemaSnapshot s = schemaCatalog.schemasByStreamId().get(dsId);
                    if (s == null)
                        throw new IllegalStateException("Missing datastream schema for stream " + dsId);
                    return s;
                })
                .toList();

        Files.deleteIfExists(part.partialSchemas);
        ArchiveExporter.writeSchemaSnapshot(part.partialSchemas, partSchemas);
        ArchiveExporter.forceFile(part.partialSchemas);
        String schemaSha256 = ArchiveExporter.sha256(part.partialSchemas);
        long schemaBytes = Files.size(part.partialSchemas);

        ArchiveGeneration generation = new ArchiveGeneration(
                part.generationId,
                part.targetProfile,
                part.assignedWindowStart,
                part.assignedWindowEnd,
                part.relativeParquet,
                part.parquetSha256,
                part.rowCount,
                part.minId,
                part.maxId,
                "SEALED",
                true,
                part.maxResultTime,
                part.minResultTime);

        List<String> manifestCols = spec.projectedEventColumns()
                ? ArchiveExporter.V2_EVENT_MANIFEST_COLUMNS
                : ArchiveExporter.V1_MANIFEST_COLUMNS;
        Files.deleteIfExists(part.partialManifest);
        ArchiveExporter.writeManifest(
                part.partialManifest,
                idScope,
                2,
                manifestCols,
                generation,
                part.parquetBytes,
                partSchemas,
                part.relativeSchemas,
                schemaSha256,
                schemaBytes);
        ArchiveExporter.forceFile(part.partialManifest);
        String manifestSha256 = ArchiveExporter.sha256(part.partialManifest);
        long manifestBytes = Files.size(part.partialManifest);

        Map<Long, ArchiveCatalog.StreamSummaryCounters> summaries = new HashMap<>(part.streamAccumulators.size() * 2);
        for (Map.Entry<Long, StreamStatsAccumulator> entry : part.streamAccumulators.entrySet()) {
            summaries.put(entry.getKey(), entry.getValue().finish());
        }
        List<ArchiveCatalog.EventLocatorCandidate> locators = part.eventLocators.entrySet().stream()
                .map(e -> new ArchiveCatalog.EventLocatorCandidate(e.getKey(), e.getValue()))
                .toList();

        String parityDetail = "Compacted Parquet V2 parity verified sha256=" + aggregateDigestHex;
        ArchiveCatalog.CompactedGenerationActivation activation =
                new ArchiveCatalog.CompactedGenerationActivation(
                        generation,
                        part.relativeManifest,
                        part.parquetBytes,
                        manifestSha256,
                        manifestBytes,
                        part.relativeSchemas,
                        schemaSha256,
                        schemaBytes,
                        partSchemas,
                        summaries,
                        List.copyOf(part.latestByStream.values()),
                        locators,
                        parityDetail);
        return new PublishedFileBundle(
                activation,
                part.directory,
                part.partialParquet,
                part.finalParquet,
                part.partialSchemas,
                part.finalSchemas,
                part.partialManifest,
                part.finalManifest,
                part.createdFiles);
    }

    private void verifyPublishedPartsParity(
            List<PublishedFileBundle> bundles,
            ProfileTargetSpec spec,
            long expectedTotalRows,
            String expectedAggregateDigest) throws Exception {
        MultisetRowDigest targetDigest = new MultisetRowDigest();
        LongIdTracker targetIdsSeen = new LongIdTracker();
        long actualRows = 0L;

        for (int i = 0; i < bundles.size(); i++) {
            PublishedFileBundle bundle = bundles.get(i);
            ArchiveGeneration gen = bundle.activation().generation();
            Path diskParquet = bundle.partialParquet();
            String diskSha = ArchiveExporter.sha256(diskParquet);
            if (!gen.sha256().equals(diskSha)) {
                throw new IOException("Compacted Parquet SHA-256 mismatch on " + diskParquet);
            }

            try (DuckDbResourcePolicy.Session verifySession =
                         DuckDbResourcePolicy.open(config, "compaction-verify")) {
                Path opDir = verifySession.operationDirectory();
                Path localVerify = ArchiveExporter.stageToLocalDirectory(
                        diskParquet, opDir, "verify-" + i + "-" + gen.id() + ".parquet");
                try {
                    if (spec.projectedEventColumns()) {
                        verifyProjectedColumnsPresent(verifySession.connection(), localVerify);
                    }
                    String sql = "SELECT id_value, datastream_id, foi_id, phenomenon_time, result_time, " +
                            "result_json, archive_version, profile FROM read_parquet('" +
                            ArchiveExporter.sqlLiteral(localVerify.toString()) + "')";
                    long partRows = 0L;
                    long partMinId = Long.MAX_VALUE;
                    long partMaxId = Long.MIN_VALUE;
                    try (Statement read = verifySession.connection().createStatement();
                         ResultSet rs = read.executeQuery(sql)) {
                        while (rs.next()) {
                            long obsId = rs.getLong(1);
                            long dsId = rs.getLong(2);
                            Long foiId = rs.getObject(3, Long.class);
                            Instant phenTime = UtcJdbcTime.getDuckDbInstant(rs, 4);
                            Instant resTime = UtcJdbcTime.getDuckDbInstant(rs, 5);
                            String resultJson = rs.getString(6);
                            int archiveVersion = rs.getInt(7);
                            String profile = rs.getString(8);
                            if (archiveVersion != 2 || !gen.profile().equals(profile)) {
                                throw new IOException("Compacted Parquet archive_version/profile mismatch on " + gen.id());
                            }
                            byte[] rowSha = computeSingleRowSha256(
                                    obsId, dsId, foiId, phenTime, resTime, resultJson);
                            targetDigest.observe(rowSha);
                            targetIdsSeen.add(obsId);
                            partRows++;
                            partMinId = Math.min(partMinId, obsId);
                            partMaxId = Math.max(partMaxId, obsId);
                        }
                    }
                    if (partRows != gen.rowCount() || partMinId != gen.minimumId() || partMaxId != gen.maximumId()) {
                        throw new IOException("Compacted Parquet count/min/max mismatch on " + gen.id());
                    }
                    actualRows = Math.addExact(actualRows, partRows);
                } finally {
                    Files.deleteIfExists(localVerify);
                }
            }
        }

        targetIdsSeen.verifyNoDuplicates();
        if (actualRows != expectedTotalRows) {
            throw new IOException("Compacted Parquet total row mismatch: expected " +
                    expectedTotalRows + " actual " + actualRows);
        }
        String actualDigest = targetDigest.finishHex();
        if (!expectedAggregateDigest.equals(actualDigest)) {
            throw new IOException("Compacted Parquet SHA-256 row-digest parity mismatch: expected " +
                    expectedAggregateDigest + " actual " + actualDigest);
        }
    }

    private static void verifyProjectedColumnsPresent(
            java.sql.Connection duckConn, Path localParquet) throws Exception {
        Set<String> cols = new HashSet<>();
        String sql = "SELECT name FROM parquet_schema('" +
                ArchiveExporter.sqlLiteral(localParquet.toString()) + "')";
        try (Statement stmt = duckConn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                cols.add(rs.getString(1));
            }
        }
        List<String> required = List.of(
                "ocr_norm", "occ_obs_id", "event_id", "adj_code", "adj_count",
                "gamma_alarm", "neutron_alarm", "max_gamma", "max_neutron");
        if (!cols.containsAll(required)) {
            throw new IOException("Compacted V2 event Parquet is missing projected columns: " + cols);
        }
    }

    private String buildV2CopySql(ProfileTargetSpec spec, Path tempOut) {
        String profileLit = ArchiveExporter.sqlLiteral(spec.targetProfile());
        if (spec.projectedEventColumns()) {
            // Projected scalar columns are pre-extracted once in Java before insertion
            // so DuckDB never instantiates 29 per-expression 8 MiB yyjson arenas during ORDER BY.
            return "COPY (" +
                    "SELECT id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, result_json, " +
                    "2 AS archive_version, '" + profileLit + "' AS profile, " +
                    "ocr_norm, occ_obs_id, event_id, adj_code, adj_count, " +
                    "gamma_alarm, neutron_alarm, max_gamma, max_neutron " +
                    "FROM compaction_part " +
                    "ORDER BY datastream_id, phenomenon_time, result_time, id_value" +
                    ") TO '" + ArchiveExporter.sqlLiteral(tempOut.toString()) + "' (" +
                    "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION ZSTD, " +
                    "COMPRESSION_LEVEL " + config.zstdCompressionLevel + ", " +
                    "ROW_GROUP_SIZE " + spec.rowGroupSize() + ", " +
                    "BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.01)";
        }
        return "COPY (" +
                "SELECT id_scope, id_value, datastream_scope, datastream_id, " +
                "foi_scope, foi_id, phenomenon_time, result_time, result_json, " +
                "2 AS archive_version, '" + profileLit + "' AS profile " +
                "FROM compaction_part " +
                "ORDER BY datastream_id, phenomenon_time, result_time, id_value" +
                ") TO '" + ArchiveExporter.sqlLiteral(tempOut.toString()) + "' (" +
                "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION ZSTD, " +
                "COMPRESSION_LEVEL " + config.zstdCompressionLevel + ", " +
                "ROW_GROUP_SIZE " + spec.rowGroupSize() + ")";
    }

    private record ProjectedEventValues(
            String ocrNorm,
            String occObsId,
            String eventId,
            Integer adjCode,
            Integer adjCount,
            Boolean gammaAlarm,
            Boolean neutronAlarm,
            Double maxGamma,
            Double maxNeutron) {
        static final ProjectedEventValues EMPTY = new ProjectedEventValues(
                null, null, null, null, null, null, null, null, null);

        static ProjectedEventValues extract(String resultJson) {
            if (resultJson == null || resultJson.isBlank())
                return EMPTY;
            try {
                JsonElement parsed = JsonParser.parseString(resultJson);
                if (!parsed.isJsonObject())
                    return EMPTY;
                JsonObject obj = parsed.getAsJsonObject();
                String ocrNorm = extractJsonString(obj, "normalizedValue");
                String occObsId = extractJsonString(obj, "occupancyObsId");
                String eventId = extractJsonString(obj, "eventId");
                Integer adjCode = extractJsonInteger(obj.get("adjudicationCode"));
                Integer adjCount = extractAdjudicatedCount(obj);
                Boolean gammaAlarm = extractJsonBoolean(obj, "gammaAlarm");
                Boolean neutronAlarm = extractJsonBoolean(obj, "neutronAlarm");
                Double maxGamma = extractJsonDouble(obj.get("maxGammaCount"));
                Double maxNeutron = extractJsonDouble(obj.get("maxNeutronCount"));
                return new ProjectedEventValues(
                        ocrNorm, occObsId, eventId, adjCode, adjCount,
                        gammaAlarm, neutronAlarm, maxGamma, maxNeutron);
            } catch (RuntimeException ignored) {
                return EMPTY;
            }
        }

        private static String extractJsonString(JsonObject obj, String key) {
            JsonElement el = obj.get(key);
            if (el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isString())
                return el.getAsString();
            return null;
        }

        private static Boolean extractJsonBoolean(JsonObject obj, String key) {
            JsonElement el = obj.get(key);
            if (el != null && el.isJsonPrimitive() && el.getAsJsonPrimitive().isBoolean())
                return el.getAsBoolean();
            return null;
        }

        private static Integer extractJsonInteger(JsonElement el) {
            if (el == null || !el.isJsonPrimitive() || !el.getAsJsonPrimitive().isNumber())
                return null;
            try {
                double d = el.getAsDouble();
                if (!Double.isFinite(d))
                    return null;
                long rounded = Math.round(d);
                if (rounded < Integer.MIN_VALUE || rounded > Integer.MAX_VALUE)
                    return null;
                return (int) rounded;
            } catch (RuntimeException ignored) {
                return null;
            }
        }

        private static Integer extractAdjudicatedCount(JsonObject obj) {
            JsonElement cnt = obj.get("adjudicatedIdsCount");
            if (cnt != null && !cnt.isJsonNull()) {
                return extractJsonInteger(cnt);
            }
            JsonElement arr = obj.get("adjudicatedIds");
            if (arr != null && arr.isJsonArray()) {
                return arr.getAsJsonArray().size();
            }
            return null;
        }

        private static Double extractJsonDouble(JsonElement el) {
            if (el == null || !el.isJsonPrimitive() || !el.getAsJsonPrimitive().isNumber())
                return null;
            try {
                double d = el.getAsDouble();
                return Double.isFinite(d) ? d : null;
            } catch (RuntimeException ignored) {
                return null;
            }
        }
    }

    private static void appendStagedRowsNative(
            java.sql.Connection duckConn, List<StagedRow> rows, boolean projected) throws Exception {
        if (rows.isEmpty())
            return;
        DuckDBConnection nativeConn = duckConn.unwrap(DuckDBConnection.class);
        try (DuckDBAppender appender = nativeConn.createAppender(
                DuckDBConnection.DEFAULT_SCHEMA, "compaction_part")) {
            for (StagedRow row : rows) {
                appender.beginRow();
                appender.append(row.idScope());
                appender.append(row.id());
                appender.append(row.dataStreamScope());
                appender.append(row.dataStreamId());
                if (row.foiId() == null) {
                    appender.appendNull();
                    appender.appendNull();
                } else {
                    appender.append(row.foiScope() != null ? row.foiScope() : row.idScope());
                    appender.append(row.foiId().longValue());
                }
                if (row.phenomenonTime() == null)
                    appender.appendNull();
                else
                    appender.append(LocalDateTime.ofInstant(row.phenomenonTime(), ZoneOffset.UTC));
                if (row.resultTime() == null)
                    appender.appendNull();
                else
                    appender.append(LocalDateTime.ofInstant(row.resultTime(), ZoneOffset.UTC));
                appender.append(row.resultJson());
                if (projected) {
                    ProjectedEventValues ev = ProjectedEventValues.extract(row.resultJson());
                    appender.append(ev.ocrNorm());
                    appender.append(ev.occObsId());
                    appender.append(ev.eventId());
                    appender.append(ev.adjCode());
                    appender.append(ev.adjCount());
                    appender.append(ev.gammaAlarm());
                    appender.append(ev.neutronAlarm());
                    appender.append(ev.maxGamma());
                    appender.append(ev.maxNeutron());
                }
                appender.endRow();
            }
        }
    }

    /**
     * Order-independent 576-bit homomorphic multiset digest (MSet-Add-Hash +
     * MSet-XOR-Hash over 256-bit per-row SHA-256 digests) requiring 72 bytes of
     * JVM memory instead of DuckDB temporary hash tables.
     */
    private static final class MultisetRowDigest {
        private long count;
        private long sum0;
        private long sum1;
        private long sum2;
        private long sum3;
        private long xor0;
        private long xor1;
        private long xor2;
        private long xor3;

        void observe(byte[] sha256) {
            long w0 = readLongBE(sha256, 0);
            long w1 = readLongBE(sha256, 8);
            long w2 = readLongBE(sha256, 16);
            long w3 = readLongBE(sha256, 24);
            count++;
            sum0 += w0;
            sum1 += w1;
            sum2 += w2;
            sum3 += w3;
            xor0 ^= w0;
            xor1 ^= w1;
            xor2 ^= w2;
            xor3 ^= w3;
        }

        String finishHex() throws Exception {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            ByteBuffer buf = ByteBuffer.allocate(72);
            buf.putLong(count);
            buf.putLong(sum0);
            buf.putLong(sum1);
            buf.putLong(sum2);
            buf.putLong(sum3);
            buf.putLong(xor0);
            buf.putLong(xor1);
            buf.putLong(xor2);
            buf.putLong(xor3);
            return HexFormat.of().formatHex(md.digest(buf.array()));
        }

        private static long readLongBE(byte[] b, int off) {
            return ((long) b[off] << 56)
                    | (((long) b[off + 1] & 0xFFL) << 48)
                    | (((long) b[off + 2] & 0xFFL) << 40)
                    | (((long) b[off + 3] & 0xFFL) << 32)
                    | (((long) b[off + 4] & 0xFFL) << 24)
                    | (((long) b[off + 5] & 0xFFL) << 16)
                    | (((long) b[off + 6] & 0xFFL) << 8)
                    | ((long) b[off + 7] & 0xFFL);
        }
    }

    private static final class LongIdTracker {
        private long[] ids = new long[1024];
        private int size;

        void add(long id) {
            if (size == ids.length) {
                ids = Arrays.copyOf(ids, Math.multiplyExact(ids.length, 2));
            }
            ids[size++] = id;
        }

        void verifyNoDuplicates() {
            if (size <= 1)
                return;
            Arrays.sort(ids, 0, size);
            for (int i = 1; i < size; i++) {
                if (ids[i] == ids[i - 1]) {
                    throw new IllegalStateException(
                            "Duplicate observation ID detected during compaction parity: " + ids[i]);
                }
            }
        }
    }

    private static byte[] computeSingleRowSha256(
            long id,
            long dataStreamId,
            Long foiId,
            Instant phenomenonTime,
            Instant resultTime,
            String resultJson) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        ByteBuffer header = ByteBuffer.allocate(8 + 8 + 1 + 8 + 1 + 8 + 4 + 1 + 8 + 4);
        header.putLong(id);
        header.putLong(dataStreamId);
        if (foiId == null) {
            header.put((byte) 0);
            header.putLong(0L);
        } else {
            header.put((byte) 1);
            header.putLong(foiId);
        }
        if (phenomenonTime == null) {
            header.put((byte) 0);
            header.putLong(0L);
            header.putInt(0);
        } else {
            header.put((byte) 1);
            header.putLong(phenomenonTime.getEpochSecond());
            header.putInt(phenomenonTime.getNano());
        }
        if (resultTime == null) {
            header.put((byte) 0);
            header.putLong(0L);
            header.putInt(0);
        } else {
            header.put((byte) 1);
            header.putLong(resultTime.getEpochSecond());
            header.putInt(resultTime.getNano());
        }
        digest.update(header.array());
        if (resultJson != null) {
            byte[] utf8 = resultJson.getBytes(StandardCharsets.UTF_8);
            ByteBuffer len = ByteBuffer.allocate(4);
            len.putInt(utf8.length);
            digest.update(len.array());
            digest.update(utf8);
        } else {
            ByteBuffer len = ByteBuffer.allocate(4);
            len.putInt(-1);
            digest.update(len.array());
        }
        return digest.digest();
    }

    private Map<Long, ArchiveCatalog.CompactedRowOwner> resolveMissingRowOwnersFromTargets(
            List<ArchiveGeneration> targetGens, Set<Long> missingIds) throws Exception {
        if (missingIds == null || missingIds.isEmpty() || targetGens.isEmpty())
            return Map.of();
        Map<Long, ArchiveCatalog.CompactedRowOwner> resolved = new HashMap<>(missingIds.size() * 2);
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "compaction-verify")) {
            Path opDir = duckSession.operationDirectory();
            String idList = missingIds.stream().sorted().map(String::valueOf).collect(Collectors.joining(","));
            for (int i = 0; i < targetGens.size(); i++) {
                ArchiveGeneration gen = targetGens.get(i);
                if (missingIds.stream().noneMatch(id -> id >= gen.minimumId() && id <= gen.maximumId()))
                    continue;
                Path staged = ArchiveExporter.stageToLocalDirectory(
                        root.resolve(gen.objectPath()), opDir, "resolve-" + i + ".parquet");
                try (Statement stmt = duckSession.connection().createStatement();
                     ResultSet rs = stmt.executeQuery(
                             "SELECT id_value, datastream_id, foi_id, result_json FROM read_parquet('" +
                                     ArchiveExporter.sqlLiteral(staged.toString()) +
                                     "', union_by_name=true) WHERE id_value IN (" + idList + ")")) {
                    while (rs.next()) {
                        long obsId = rs.getLong(1);
                        long dsId = rs.getLong(2);
                        Long foiId = rs.getObject(3, Long.class);
                        String json = rs.getString(4);
                        resolved.put(obsId, new ArchiveCatalog.CompactedRowOwner(
                                gen.id(),
                                gen.sha256(),
                                gen.profile(),
                                dsId,
                                foiId,
                                JsonCqlEvaluator.computeSummaryMask(json)));
                    }
                } finally {
                    Files.deleteIfExists(staged);
                }
            }
        }
        return resolved;
    }

    public int drainDeletingGenerations(Instant now) {
        Objects.requireNonNull(now, "now");
        Instant unlinkCutoff = now.minus(Duration.ofMinutes(Math.max(0, config.compactionUnlinkGraceMinutes)));
        List<ArchiveCatalog.DeletingGenerationRecord> candidates =
                catalog.listDrainableDeletingGenerations(unlinkCutoff);
        if (candidates.isEmpty())
            return 0;

        int finalized = 0;
        List<UUID> retiredIds = new ArrayList<>();
        Set<Path> allParentDirs = new LinkedHashSet<>();
        for (ArchiveCatalog.DeletingGenerationRecord rec : candidates) {
            if (archiveReader.activePinCount(rec.generationId()) > 0)
                continue;

            archiveReader.invalidateGenerations(
                    rec.objectRelativePaths(),
                    rec.sha256() != null ? List.of(rec.sha256()) : List.of());

            // Unlink physical files BEFORE deleting DELETING catalog rows so filesystem audit stays 100% clean.
            boolean allUnlinked = true;
            for (Path rel : rec.objectRelativePaths()) {
                try {
                    Path target = resolveSafeRegularDeleteTarget(rel);
                    if (target != null) {
                        if (target.getParent() != null)
                            allParentDirs.add(target.getParent());
                        Files.deleteIfExists(target);
                    }
                } catch (IOException | RuntimeException ioError) {
                    allUnlinked = false;
                }
            }
            if (!allUnlinked)
                continue;

            if (catalog.finalizeDeletedGeneration(rec.generationId(), rec.jobId(), now)) {
                retiredIds.add(rec.generationId());
                finalized++;
            }
        }
        if (!allParentDirs.isEmpty()) {
            pruneEmptyProfileAndDateDirectories(allParentDirs);
        }
        if (!retiredIds.isEmpty() && retiredGenerationHook != null) {
            retiredGenerationHook.accept(retiredIds);
        }
        return finalized;
    }

    private void notifyRetiredGenerations(List<ArchiveGeneration> sources) {
        List<Path> paths = sources.stream().map(ArchiveGeneration::objectPath).toList();
        List<String> shas = sources.stream().map(ArchiveGeneration::sha256).filter(Objects::nonNull).toList();
        archiveReader.invalidateGenerations(paths, shas);
        if (retiredGenerationHook != null) {
            retiredGenerationHook.accept(sources.stream().map(ArchiveGeneration::id).toList());
        }
    }

    private Path resolveSafeRegularDeleteTarget(Path relative) throws IOException {
        if (relative == null || relative.isAbsolute())
            return null;
        Path normalized = root.resolve(relative).normalize();
        if (!normalized.startsWith(root) || normalized.equals(root))
            throw new IOException("Refusing to unlink path outside archive root: " + relative);
        if (!Files.exists(normalized, LinkOption.NOFOLLOW_LINKS))
            return normalized;
        BasicFileAttributes attrs = Files.readAttributes(
                normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (attrs.isSymbolicLink() || !attrs.isRegularFile())
            throw new IOException("Refusing to unlink non-regular archive entry: " + normalized);
        return normalized;
    }

    private void pruneEmptyProfileAndDateDirectories(Set<Path> profileDirs) {
        Path observationsDir = root.resolve("datasets/observations").normalize();
        for (Path profileDir : profileDirs) {
            if (profileDir == null)
                continue;
            Path normProfile = profileDir.toAbsolutePath().normalize();
            if (!normProfile.startsWith(observationsDir) || normProfile.equals(observationsDir))
                continue;
            String name = normProfile.getFileName() != null ? normProfile.getFileName().toString() : "";
            if (name.startsWith("profile=")) {
                deleteDirectoryIfEmpty(normProfile);
                Path dateDir = normProfile.getParent();
                if (dateDir != null && !dateDir.equals(observationsDir) &&
                        dateDir.getFileName() != null &&
                        dateDir.getFileName().toString().startsWith("date=")) {
                    deleteDirectoryIfEmpty(dateDir);
                }
            }
        }
    }

    private static void deleteDirectoryIfEmpty(Path dir) {
        try {
            if (!Files.exists(dir, LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(dir))
                return;
            // Use direct rmdir (Files.deleteIfExists) without opening a DirectoryStream so
            // Windows WSL 2 9p mounts never allocate readdir handles on non-empty directories.
            Files.deleteIfExists(dir);
        } catch (DirectoryNotEmptyException ignored) {
        } catch (IOException | RuntimeException ignored) {
        }
    }

    private static void deletePathsQuietly(List<Path> paths) {
        for (Path p : paths) {
            try {
                if (p != null && !Files.isSymbolicLink(p))
                    Files.deleteIfExists(p);
            } catch (IOException ignored) {
            }
        }
    }

    private static final class PartAccumulator {
        long rowCount;
        long estimatedBytes;
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        Instant minPhenomenonTime;
        Instant maxPhenomenonTime;
        Instant lastPhenomenonTime;
        Instant minResultTime;
        Instant maxResultTime;
        final Map<Long, StreamStatsAccumulator> streamAccumulators = new HashMap<>();
        final Map<Long, ArchiveCatalog.StreamLatestCandidate> latestByStream = new HashMap<>();
        final Map<String, Long> eventLocators = new HashMap<>();
        final Map<Long, PendingRowOwner> referencedRowOwners = new HashMap<>();

        void observe(StagedRow row, long estBytes, boolean trackOwner) {
            rowCount++;
            estimatedBytes = Math.addExact(estimatedBytes, estBytes);
            minId = Math.min(minId, row.id());
            maxId = Math.max(maxId, row.id());
            lastPhenomenonTime = row.phenomenonTime();
            if (row.phenomenonTime() != null) {
                if (minPhenomenonTime == null || row.phenomenonTime().isBefore(minPhenomenonTime))
                    minPhenomenonTime = row.phenomenonTime();
                if (maxPhenomenonTime == null || row.phenomenonTime().isAfter(maxPhenomenonTime))
                    maxPhenomenonTime = row.phenomenonTime();
            }
            Instant effectiveResult = row.resultTime() != null ? row.resultTime() : row.phenomenonTime();
            if (effectiveResult != null) {
                if (minResultTime == null || effectiveResult.isBefore(minResultTime))
                    minResultTime = effectiveResult;
                if (maxResultTime == null || effectiveResult.isAfter(maxResultTime))
                    maxResultTime = effectiveResult;
            }

            StreamStatsAccumulator streamStats = streamAccumulators.computeIfAbsent(
                    row.dataStreamId(), ignored -> new StreamStatsAccumulator());
            int mask = streamStats.observeRow(row.id(), row.resultJson(), eventLocators);
            if (trackOwner) {
                referencedRowOwners.put(row.id(), new PendingRowOwner(row.dataStreamId(), row.foiId(), mask));
            }

            latestByStream.merge(
                    row.dataStreamId(),
                    new ArchiveCatalog.StreamLatestCandidate(
                            row.dataStreamId(),
                            row.id(),
                            row.foiId(),
                            row.phenomenonTime(),
                            row.resultTime(),
                            row.resultJson()),
                    PartAccumulator::pickNewerLatest);
        }

        private static ArchiveCatalog.StreamLatestCandidate pickNewerLatest(
                ArchiveCatalog.StreamLatestCandidate prev,
                ArchiveCatalog.StreamLatestCandidate cand) {
            if (prev.phenomenonTime() == null && cand.phenomenonTime() == null) {
                return cand.observationId() >= prev.observationId() ? cand : prev;
            }
            if (prev.phenomenonTime() == null)
                return cand;
            if (cand.phenomenonTime() == null)
                return prev;
            int cmp = cand.phenomenonTime().compareTo(prev.phenomenonTime());
            if (cmp != 0)
                return cmp > 0 ? cand : prev;
            Instant prevRes = prev.resultTime();
            Instant candRes = cand.resultTime();
            if (prevRes != null || candRes != null) {
                if (prevRes == null)
                    return cand;
                if (candRes == null)
                    return prev;
                int rCmp = candRes.compareTo(prevRes);
                if (rCmp != 0)
                    return rCmp > 0 ? cand : prev;
            }
            return cand.observationId() >= prev.observationId() ? cand : prev;
        }
    }

    private static final class StreamStatsAccumulator {
        long rowCount;
        long gammaAlarmCount;
        long neutronAlarmCount;
        long gammaNeutronAlarmCount;
        long nonAlarmOccupancyCount;
        long gammaTrueCount;
        long neutronTrueCount;
        long anyAlarmCount;
        long unadjudicatedAlarmCount;
        long adjudicatedAlarmCount;
        long unadjudicatedGammaAlarmCount;
        long unadjudicatedNeutronAlarmCount;
        long unadjudicatedGammaNeutronAlarmCount;
        long adjudicatedGammaAlarmCount;
        long adjudicatedNeutronAlarmCount;
        long adjudicatedGammaNeutronAlarmCount;
        long gammaHighFaultCount;
        long gammaLowFaultCount;
        long neutronFaultCount;
        long extendedOccupancyFaultCount;
        long tamperTrueCount;
        long rs350GammaAlarmCount;
        long rs350NeutronAlarmCount;
        final OcrBloomFilter.Accumulator ocrAccumulator = new OcrBloomFilter.Accumulator();

        int observeRow(long obsId, String resultJson, Map<String, Long> eventLocators) {
            rowCount++;
            if (resultJson == null || resultJson.isBlank())
                return 0;

            boolean maybeSummary = resultJson.indexOf("gammaAlarm") >= 0
                    || resultJson.indexOf("neutronAlarm") >= 0
                    || resultJson.indexOf("alarmState") >= 0
                    || resultJson.indexOf("tamperStatus") >= 0
                    || resultJson.indexOf("derivedData") >= 0
                    || resultJson.indexOf("adjudicatedIds") >= 0
                    || resultJson.indexOf("\\u") >= 0;
            boolean maybeOcr = resultJson.indexOf("normalizedValue") >= 0 || resultJson.indexOf("\\u") >= 0;
            boolean maybeEventId = resultJson.indexOf("eventId") >= 0 || resultJson.indexOf("\\u") >= 0;
            if (!maybeSummary && !maybeOcr && !maybeEventId)
                return 0;

            int mask = maybeSummary ? JsonCqlEvaluator.computeSummaryMask(resultJson) : 0;
            if (maybeSummary) {
                boolean gTrue = (mask & JsonCqlEvaluator.BIT_GAMMA_TRUE) != 0;
                boolean gFalse = (mask & JsonCqlEvaluator.BIT_GAMMA_FALSE) != 0;
                boolean nTrue = (mask & JsonCqlEvaluator.BIT_NEUTRON_TRUE) != 0;
                boolean nFalse = (mask & JsonCqlEvaluator.BIT_NEUTRON_FALSE) != 0;

                if (gTrue && nFalse) gammaAlarmCount++;
                if (gFalse && nTrue) neutronAlarmCount++;
                if (gTrue && nTrue) gammaNeutronAlarmCount++;
                if (gFalse && nFalse) nonAlarmOccupancyCount++;
                if (gTrue) gammaTrueCount++;
                if (nTrue) neutronTrueCount++;

                if (gTrue || nTrue) {
                    anyAlarmCount++;
                    AdjudicationState adjState = classifyAlarmAdjudicationForSummary(resultJson, mask);
                    if (adjState == AdjudicationState.UNADJUDICATED) {
                        unadjudicatedAlarmCount++;
                        if (gTrue && nFalse) unadjudicatedGammaAlarmCount++;
                        if (gFalse && nTrue) unadjudicatedNeutronAlarmCount++;
                        if (gTrue && nTrue) unadjudicatedGammaNeutronAlarmCount++;
                    } else if (adjState == AdjudicationState.ADJUDICATED) {
                        adjudicatedAlarmCount++;
                        if (gTrue && nFalse) adjudicatedGammaAlarmCount++;
                        if (gFalse && nTrue) adjudicatedNeutronAlarmCount++;
                        if (gTrue && nTrue) adjudicatedGammaNeutronAlarmCount++;
                    }
                }
                if ((mask & JsonCqlEvaluator.BIT_FAULT_GAMMA_HIGH) != 0) gammaHighFaultCount++;
                if ((mask & JsonCqlEvaluator.BIT_FAULT_GAMMA_LOW) != 0) gammaLowFaultCount++;
                if ((mask & JsonCqlEvaluator.BIT_FAULT_NEUTRON) != 0) neutronFaultCount++;
                if ((mask & JsonCqlEvaluator.BIT_FAULT_EXT_OCC) != 0) extendedOccupancyFaultCount++;
                if ((mask & JsonCqlEvaluator.BIT_TAMPER_TRUE) != 0) tamperTrueCount++;
                if ((mask & JsonCqlEvaluator.BIT_RS350_GAMMA) != 0) rs350GammaAlarmCount++;
                if ((mask & JsonCqlEvaluator.BIT_RS350_NEUTRON) != 0) rs350NeutronAlarmCount++;
            }

            if (maybeOcr || maybeEventId) {
                try {
                    JsonElement parsed = JsonParser.parseString(resultJson);
                    if (parsed.isJsonObject()) {
                        JsonObject obj = parsed.getAsJsonObject();
                        if (maybeOcr) {
                            JsonElement norm = obj.get("normalizedValue");
                            if (norm != null && norm.isJsonPrimitive() && norm.getAsJsonPrimitive().isString()) {
                                ocrAccumulator.addNormalizedValue(norm.getAsString());
                            }
                        }
                        if (maybeEventId) {
                            JsonElement ev = obj.get("eventId");
                            if (ev != null && ev.isJsonPrimitive() && ev.getAsJsonPrimitive().isString()) {
                                String evId = ev.getAsString();
                                if (!evId.isEmpty() && evId.length() <= 256) {
                                    eventLocators.merge(evId, obsId, Math::max);
                                }
                            }
                        }
                    }
                } catch (RuntimeException ignored) {
                }
            }
            return mask;
        }

        private enum AdjudicationState {
            UNADJUDICATED,
            ADJUDICATED,
            NEITHER
        }

        private static AdjudicationState classifyAlarmAdjudicationForSummary(String resultJson, int mask) {
            if ((mask & JsonCqlEvaluator.BIT_ADJ_ZERO) != 0)
                return AdjudicationState.UNADJUDICATED;
            if ((mask & JsonCqlEvaluator.BIT_ADJ_GT_ZERO) != 0)
                return AdjudicationState.ADJUDICATED;
            try {
                JsonElement parsed = JsonParser.parseString(resultJson);
                if (!parsed.isJsonObject())
                    return AdjudicationState.NEITHER;
                JsonObject obj = parsed.getAsJsonObject();
                JsonElement arr = obj.get("adjudicatedIds");
                if (arr != null && !arr.isJsonNull()) {
                    if (arr.isJsonArray()) {
                        return arr.getAsJsonArray().isEmpty()
                                ? AdjudicationState.UNADJUDICATED
                                : AdjudicationState.ADJUDICATED;
                    }
                    return AdjudicationState.NEITHER;
                }
                JsonElement cnt = obj.get("adjudicatedIdsCount");
                if (cnt == null || cnt.isJsonNull())
                    return AdjudicationState.UNADJUDICATED;
                String cntStr = cnt.isJsonPrimitive() ? cnt.getAsString() : "";
                return "0".equals(cntStr) ? AdjudicationState.UNADJUDICATED : AdjudicationState.ADJUDICATED;
            } catch (RuntimeException ignored) {
                return AdjudicationState.NEITHER;
            }
        }

        ArchiveCatalog.StreamSummaryCounters finish() {
            return new ArchiveCatalog.StreamSummaryCounters(
                    rowCount,
                    gammaAlarmCount,
                    neutronAlarmCount,
                    gammaNeutronAlarmCount,
                    nonAlarmOccupancyCount,
                    gammaTrueCount,
                    neutronTrueCount,
                    anyAlarmCount,
                    unadjudicatedAlarmCount,
                    adjudicatedAlarmCount,
                    unadjudicatedGammaAlarmCount,
                    unadjudicatedNeutronAlarmCount,
                    unadjudicatedGammaNeutronAlarmCount,
                    adjudicatedGammaAlarmCount,
                    adjudicatedNeutronAlarmCount,
                    adjudicatedGammaNeutronAlarmCount,
                    gammaHighFaultCount,
                    gammaLowFaultCount,
                    neutronFaultCount,
                    extendedOccupancyFaultCount,
                    tamperTrueCount,
                    rs350GammaAlarmCount,
                    rs350NeutronAlarmCount,
                    ocrAccumulator.finish());
        }
    }
}
