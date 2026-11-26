package org.sensorhub.impl.datastore.postgis.archive;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
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
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Multi-tier LSM Parquet V2 compaction, settled-overlay folding, monthly
 * consolidation, periodic defragmentation, and reader-pin-safe superseded
 * generation cleanup.
 */
public final class ArchiveCompactionManager {
    public static final int DEFAULT_MAX_UTC_DAYS_PER_CYCLE = 4;
    public static final int DEFAULT_MAX_MONTHLY_JOBS_PER_CYCLE = 1;
    public static final int MONTHLY_DEFRAG_OVERLAY_THRESHOLD = 64;

    private static final int DAILY_EVENTS_ROW_GROUP_SIZE = 8_192;
    private static final int MONTHLY_EVENTS_ROW_GROUP_SIZE = 16_384;
    private static final int DAILY_TELEMETRY_ROW_GROUP_SIZE = 65_536;
    private static final int DUCKDB_BATCH_ROWS = 500;

    private final ArchiveCatalog catalog;
    private final ParquetArchiveReader archiveReader;
    private final TieredArchiveConfig config;
    private final int idScope;
    private final String observationTable;
    private final String dataStreamTable;
    private final Path root;
    private final Consumer<Collection<UUID>> retiredGenerationHook;

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
        int drainedBefore = drainDeletingGenerations(now);
        int daysCompacted = 0;
        int monthsConsolidated = 0;
        int defragmented = 0;

        if (maxUtcDays > 0) {
            List<ArchiveCatalog.CompactionWindowCandidate> eligibleDays =
                    catalog.findEligibleUtcDaysForCompaction(now, observationTable, maxUtcDays);
            for (ArchiveCatalog.CompactionWindowCandidate day : eligibleDays) {
                if (daysCompacted > 0 && DuckDbResourcePolicy.hasInteractiveWaiters())
                    break;
                if (compactUtcDay(day, now))
                    daysCompacted++;
            }
        }

        if (maxMonthlyJobs > 0 && !DuckDbResourcePolicy.hasInteractiveWaiters()) {
            List<ArchiveCatalog.CompactionWindowCandidate> eligibleMonths =
                    catalog.findEligibleUtcMonthsForConsolidation(now, observationTable, maxMonthlyJobs);
            for (ArchiveCatalog.CompactionWindowCandidate month : eligibleMonths) {
                if (monthsConsolidated > 0 && DuckDbResourcePolicy.hasInteractiveWaiters())
                    break;
                if (consolidateUtcMonth(month, now))
                    monthsConsolidated++;
            }
            int remainingMonthlyBudget = maxMonthlyJobs - monthsConsolidated;
            if (remainingMonthlyBudget > 0 && !DuckDbResourcePolicy.hasInteractiveWaiters()) {
                List<ArchiveCatalog.CompactionWindowCandidate> defragCandidates =
                        catalog.findEligibleMonthlyGenerationsForDefrag(
                                now, observationTable, MONTHLY_DEFRAG_OVERLAY_THRESHOLD, remainingMonthlyBudget);
                for (ArchiveCatalog.CompactionWindowCandidate candidate : defragCandidates) {
                    if (defragmented > 0 && DuckDbResourcePolicy.hasInteractiveWaiters())
                        break;
                    if (defragmentMonthlyGeneration(candidate, now))
                        defragmented++;
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
        final UUID generationId = UUID.randomUUID();
        final String targetProfile;
        final LocalDate directoryDate;
        final Path tempParquetFile;
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
                String targetProfile,
                LocalDate directoryDate,
                Path tempParquetFile,
                long rowCount,
                long minId,
                long maxId,
                Instant minPhenomenonTime,
                Instant maxPhenomenonTime,
                Instant minResultTime,
                Instant maxResultTime,
                Map<Long, StreamStatsAccumulator> streamAccumulators,
                Map<Long, ArchiveCatalog.StreamLatestCandidate> latestByStream,
                Map<String, Long> eventLocators,
                Map<Long, PendingRowOwner> referencedRowOwners) {
            this.targetProfile = targetProfile;
            this.directoryDate = directoryDate;
            this.tempParquetFile = tempParquetFile;
            this.rowCount = rowCount;
            this.minId = minId;
            this.maxId = maxId;
            this.minPhenomenonTime = minPhenomenonTime;
            this.maxPhenomenonTime = maxPhenomenonTime;
            this.minResultTime = minResultTime;
            this.maxResultTime = maxResultTime;
            this.streamAccumulators = streamAccumulators;
            this.latestByStream = latestByStream;
            this.eventLocators = eventLocators;
            this.referencedRowOwners = referencedRowOwners;
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
            throw err;
        } catch (Exception checked) {
            if (!swapCommitted && !anyTargetRegisteredOrUncertain(swapAttempted, allBundles, checked))
                deletePathsQuietly(publishedFilesToRollback);
            throw new IllegalStateException("Archive compaction failed for window [" +
                    jobWindowStart + ", " + jobWindowEnd + ")", checked);
        }
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
        long maxBytesPerPart = config.effectiveMaximumUncompressedBytesPerGeneration();
        long maxRowsPerPart = config.maximumRowsPerGeneration;

        String expectedHexDigest;
        long sourceProfileRows = 0L;
        List<PendingPart> pendingParts = new ArrayList<>();
        List<PublishedFileBundle> bundles = new ArrayList<>();

        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(config, "archive-compaction")) {
            Path opDir = duckSession.operationDirectory();
            try (Statement ddl = duckSession.connection().createStatement()) {
                ddl.execute("CREATE TEMP TABLE compaction_part (" +
                        "id_scope INTEGER NOT NULL, id_value BIGINT NOT NULL, " +
                        "datastream_scope INTEGER NOT NULL, datastream_id BIGINT NOT NULL, " +
                        "foi_scope INTEGER, foi_id BIGINT, " +
                        "phenomenon_time TIMESTAMP, result_time TIMESTAMP, result_json VARCHAR)");
                ddl.execute("CREATE TEMP TABLE source_row_hashes (" +
                        "id_value BIGINT NOT NULL, row_sha256 BLOB NOT NULL)");
            }

            PartAccumulator currentPart = new PartAccumulator();
            List<StagedRow> pendingBatch = new ArrayList<>(DUCKDB_BATCH_ROWS);
            List<RowHashEntry> hashBatch = new ArrayList<>(DUCKDB_BATCH_ROWS);

            Path realRoot = root.toRealPath();
            for (int sIdx = 0; sIdx < sources.size(); sIdx++) {
                ArchiveGeneration src = sources.get(sIdx);
                Path srcFile = realRoot.resolve(src.objectPath().normalize()).normalize();
                Path stagedSrc = ArchiveExporter.stageToLocalDirectory(
                        srcFile, opDir, "src-" + sIdx + "-" + src.id() + ".parquet");
                try {
                    StringBuilder readSql = new StringBuilder(
                            "SELECT id_scope, id_value, datastream_scope, datastream_id, " +
                                    "foi_scope, foi_id, phenomenon_time, result_time, result_json " +
                                    "FROM read_parquet('")
                            .append(ArchiveExporter.sqlLiteral(stagedSrc.toString()))
                            .append("', union_by_name=true)");
                    if (spec.allowedStreamIds() != null) {
                        if (spec.allowedStreamIds().isEmpty())
                            continue;
                        String inList = spec.allowedStreamIds().stream()
                                .sorted()
                                .map(String::valueOf)
                                .collect(Collectors.joining(","));
                        readSql.append(" WHERE datastream_id IN (").append(inList).append(')');
                    }
                    readSql.append(" ORDER BY phenomenon_time ASC NULLS FIRST, result_time ASC NULLS FIRST, id_value ASC");

                    try (Statement readStmt = duckSession.connection().createStatement();
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
                                flushPendingStagedRows(duckSession.connection(), pendingBatch);
                                PendingPart written = writePartFromTempTable(
                                        duckSession.connection(), opDir, spec, currentPart);
                                pendingParts.add(written);
                                currentPart = new PartAccumulator();
                            }

                            StagedRow effective = new StagedRow(
                                    rowIdScope, obsId, dsScope, dsId, foiScope, foiId, phenTime, resTime, resultJson);
                            pendingBatch.add(effective);
                            if (pendingBatch.size() >= DUCKDB_INSERT_BATCH_SIZE) {
                                flushPendingStagedRows(duckSession.connection(), pendingBatch);
                            }

                            byte[] rowSha = computeSingleRowSha256(
                                    obsId, dsId, foiId, phenTime, resTime, resultJson);
                            hashBatch.add(new RowHashEntry(obsId, rowSha));
                            if (hashBatch.size() >= DUCKDB_INSERT_BATCH_SIZE) {
                                flushRowHashes(duckSession.connection(), hashBatch);
                            }

                            currentPart.observe(effective, estBytes, trackOwnerIds.contains(obsId));
                            sourceProfileRows++;
                        }
                    }
                } finally {
                    Files.deleteIfExists(stagedSrc);
                }
            }

            flushPendingStagedRows(duckSession.connection(), pendingBatch);
            flushRowHashes(duckSession.connection(), hashBatch);
            if (currentPart.rowCount > 0) {
                PendingPart written = writePartFromTempTable(
                        duckSession.connection(), opDir, spec, currentPart);
                pendingParts.add(written);
            }

            if (sourceProfileRows == 0L) {
                return new ProfileBuildResult(0L, List.of(), List.of(), Map.of());
            }

            expectedHexDigest = computeOrderedAggregateDigest(duckSession.connection(), "source_row_hashes");
            assignPartWindows(pendingParts, jobWindowStart, jobWindowEnd);

            // Stage partial target bundles from session-local temp files before opDir closes.
            for (PendingPart part : pendingParts) {
                PublishedFileBundle bundle = publishPartFiles(
                        part, spec, schemaCatalog, expectedHexDigest, publishedFilesToRollback);
                bundles.add(bundle);
            }
        }

        // Verify canonical row-digest parity and Parquet V2 format in a fresh session
        // AFTER closing archive-compaction so background slot cap=1 never self-blocks.
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

    private record RowHashEntry(long idValue, byte[] rowSha256) {}

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

    private PublishedFileBundle publishPartFiles(
            PendingPart part,
            ProfileTargetSpec spec,
            ArchiveCatalog.CompactionSchemaCatalog schemaCatalog,
            String aggregateDigestHex,
            List<Path> rollbackPaths) throws Exception {
        Path directory = root.resolve("datasets/observations/date=" + part.directoryDate)
                .resolve("profile=" + ArchiveExporter.safePathComponent(part.targetProfile));
        ArchivePathPolicy.prepareDirectory(root, directory);

        UUID genId = part.generationId;
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

        ArchiveExporter.copyBounded(part.tempParquetFile, partialParquet);
        Files.deleteIfExists(part.tempParquetFile);
        ArchiveExporter.forceFile(partialParquet);
        String parquetSha256 = ArchiveExporter.sha256(partialParquet);
        long parquetBytes = Files.size(partialParquet);

        List<ArchiveCatalog.SchemaSnapshot> partSchemas = part.streamAccumulators.keySet().stream()
                .sorted()
                .map(dsId -> {
                    ArchiveCatalog.SchemaSnapshot s = schemaCatalog.schemasByStreamId().get(dsId);
                    if (s == null)
                        throw new IllegalStateException("Missing datastream schema for stream " + dsId);
                    return s;
                })
                .toList();

        Files.deleteIfExists(partialSchemas);
        ArchiveExporter.writeSchemaSnapshot(partialSchemas, partSchemas);
        ArchiveExporter.forceFile(partialSchemas);
        String schemaSha256 = ArchiveExporter.sha256(partialSchemas);
        long schemaBytes = Files.size(partialSchemas);

        ArchiveGeneration generation = new ArchiveGeneration(
                genId,
                part.targetProfile,
                part.assignedWindowStart,
                part.assignedWindowEnd,
                relativeParquet,
                parquetSha256,
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
        Files.deleteIfExists(partialManifest);
        ArchiveExporter.writeManifest(
                partialManifest,
                idScope,
                2,
                manifestCols,
                generation,
                parquetBytes,
                partSchemas,
                relativeSchemas,
                schemaSha256,
                schemaBytes);
        ArchiveExporter.forceFile(partialManifest);
        String manifestSha256 = ArchiveExporter.sha256(partialManifest);
        long manifestBytes = Files.size(partialManifest);

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
                        relativeManifest,
                        parquetBytes,
                        manifestSha256,
                        manifestBytes,
                        relativeSchemas,
                        schemaSha256,
                        schemaBytes,
                        partSchemas,
                        summaries,
                        List.copyOf(part.latestByStream.values()),
                        locators,
                        parityDetail);
        return new PublishedFileBundle(
                activation,
                directory,
                partialParquet,
                finalParquet,
                partialSchemas,
                finalSchemas,
                partialManifest,
                finalManifest,
                created);
    }

    private void verifyPublishedPartsParity(
            List<PublishedFileBundle> bundles,
            ProfileTargetSpec spec,
            long expectedTotalRows,
            String expectedAggregateDigest) throws Exception {
        try (DuckDbResourcePolicy.Session verifySession = DuckDbResourcePolicy.open(config, "compaction-verify")) {
            Path opDir = verifySession.operationDirectory();
            try (Statement ddl = verifySession.connection().createStatement()) {
                ddl.execute("CREATE TEMP TABLE target_row_hashes (" +
                        "id_value BIGINT NOT NULL, row_sha256 BLOB NOT NULL)");
            }
            long actualRows = 0L;
            List<RowHashEntry> hashBatch = new ArrayList<>(DUCKDB_INSERT_BATCH_SIZE);

            for (int i = 0; i < bundles.size(); i++) {
                PublishedFileBundle bundle = bundles.get(i);
                ArchiveGeneration gen = bundle.activation().generation();
                Path diskParquet = bundle.partialParquet();
                String diskSha = ArchiveExporter.sha256(diskParquet);
                if (!gen.sha256().equals(diskSha)) {
                    throw new IOException("Compacted Parquet SHA-256 mismatch on " + diskParquet);
                }
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
                            hashBatch.add(new RowHashEntry(obsId, rowSha));
                            if (hashBatch.size() >= DUCKDB_INSERT_BATCH_SIZE) {
                                flushTargetRowHashes(verifySession.connection(), hashBatch);
                            }
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
            flushTargetRowHashes(verifySession.connection(), hashBatch);
            if (actualRows != expectedTotalRows) {
                throw new IOException("Compacted Parquet total row mismatch: expected " +
                        expectedTotalRows + " actual " + actualRows);
            }
            String actualDigest = computeOrderedAggregateDigest(verifySession.connection(), "target_row_hashes");
            if (!expectedAggregateDigest.equals(actualDigest)) {
                throw new IOException("Compacted Parquet SHA-256 row-digest parity mismatch: expected " +
                        expectedAggregateDigest + " actual " + actualDigest);
            }
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

    private PendingPart writePartFromTempTable(
            java.sql.Connection duckConn,
            Path opDir,
            ProfileTargetSpec spec,
            PartAccumulator acc) throws Exception {
        Path tempOut = opDir.resolve("part-" + UUID.randomUUID() + ".parquet").normalize();
        if (!tempOut.getParent().equals(opDir.normalize()))
            throw new IllegalStateException("Compaction temporary Parquet path escaped operation directory");

        String profileLit = ArchiveExporter.sqlLiteral(spec.targetProfile());
        String copySql;
        if (spec.projectedEventColumns()) {
            copySql = "COPY (" +
                    "SELECT id_scope, id_value, datastream_scope, datastream_id, " +
                    "foi_scope, foi_id, phenomenon_time, result_time, result_json, " +
                    "2 AS archive_version, '" + profileLit + "' AS profile, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.normalizedValue') = 'VARCHAR' " +
                    "     THEN json_extract_string(result_json, '$.normalizedValue') ELSE NULL END AS ocr_norm, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.occupancyObsId') = 'VARCHAR' " +
                    "     THEN json_extract_string(result_json, '$.occupancyObsId') ELSE NULL END AS occ_obs_id, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.eventId') = 'VARCHAR' " +
                    "     THEN json_extract_string(result_json, '$.eventId') ELSE NULL END AS event_id, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.adjudicationCode') IN ('UBIGINT', 'BIGINT', 'DOUBLE') " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.adjudicationCode') AS INTEGER) ELSE NULL END AS adj_code, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.adjudicatedIdsCount') IN ('UBIGINT', 'BIGINT', 'DOUBLE') " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.adjudicatedIdsCount') AS INTEGER) " +
                    "     WHEN json_type(result_json) = 'OBJECT' " +
                    "      AND (json_type(result_json, '$.adjudicatedIdsCount') IS NULL OR json_type(result_json, '$.adjudicatedIdsCount') = 'NULL') " +
                    "      AND json_type(result_json, '$.adjudicatedIds') = 'ARRAY' " +
                    "     THEN CAST(json_array_length(json_extract(result_json, '$.adjudicatedIds')) AS INTEGER) " +
                    "     ELSE NULL END AS adj_count, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.gammaAlarm') = 'BOOLEAN' " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.gammaAlarm') AS BOOLEAN) ELSE NULL END AS gamma_alarm, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.neutronAlarm') = 'BOOLEAN' " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.neutronAlarm') AS BOOLEAN) ELSE NULL END AS neutron_alarm, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.maxGammaCount') IN ('UBIGINT', 'BIGINT', 'DOUBLE') " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.maxGammaCount') AS DOUBLE) ELSE NULL END AS max_gamma, " +
                    "CASE WHEN json_type(result_json) = 'OBJECT' AND json_type(result_json, '$.maxNeutronCount') IN ('UBIGINT', 'BIGINT', 'DOUBLE') " +
                    "     THEN TRY_CAST(json_extract(result_json, '$.maxNeutronCount') AS DOUBLE) ELSE NULL END AS max_neutron " +
                    "FROM compaction_part " +
                    "ORDER BY datastream_id, phenomenon_time, result_time, id_value" +
                    ") TO '" + ArchiveExporter.sqlLiteral(tempOut.toString()) + "' (" +
                    "FORMAT PARQUET, PARQUET_VERSION V2, COMPRESSION ZSTD, " +
                    "COMPRESSION_LEVEL " + config.zstdCompressionLevel + ", " +
                    "ROW_GROUP_SIZE " + spec.rowGroupSize() + ", " +
                    "BLOOM_FILTER_FALSE_POSITIVE_RATIO 0.01)";
        } else {
            copySql = "COPY (" +
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

        try (Statement stmt = duckConn.createStatement()) {
            stmt.execute(copySql);
            stmt.execute("DELETE FROM compaction_part");
        }
        return new PendingPart(
                spec.targetProfile(),
                spec.directoryDate(),
                tempOut,
                acc.rowCount,
                acc.minId,
                acc.maxId,
                acc.minPhenomenonTime,
                acc.maxPhenomenonTime,
                acc.minResultTime,
                acc.maxResultTime,
                acc.streamAccumulators,
                acc.latestByStream,
                acc.eventLocators,
                acc.referencedRowOwners);
    }

    private static void flushPendingStagedRows(
            java.sql.Connection duckConn, List<StagedRow> batch) throws Exception {
        if (batch.isEmpty())
            return;
        StringBuilder sql = new StringBuilder("INSERT INTO compaction_part VALUES ");
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0)
                sql.append(',');
            sql.append("(?,?,?,?,?,?,?,?,?)");
        }
        try (PreparedStatement ps = duckConn.prepareStatement(sql.toString())) {
            int p = 1;
            for (StagedRow row : batch) {
                ps.setInt(p++, row.idScope());
                ps.setLong(p++, row.id());
                ps.setInt(p++, row.dataStreamScope());
                ps.setLong(p++, row.dataStreamId());
                if (row.foiId() == null) {
                    ps.setNull(p++, Types.INTEGER);
                    ps.setNull(p++, Types.BIGINT);
                } else {
                    ps.setInt(p++, row.foiScope() != null ? row.foiScope() : row.idScope());
                    ps.setLong(p++, row.foiId());
                }
                if (row.phenomenonTime() == null)
                    ps.setNull(p++, Types.TIMESTAMP);
                else
                    UtcJdbcTime.setDuckDbInstant(ps, p++, row.phenomenonTime());
                if (row.resultTime() == null)
                    ps.setNull(p++, Types.TIMESTAMP);
                else
                    UtcJdbcTime.setDuckDbInstant(ps, p++, row.resultTime());
                ps.setString(p++, row.resultJson());
            }
            ps.executeUpdate();
        }
        batch.clear();
    }

    private static void flushRowHashes(
            java.sql.Connection duckConn, List<RowHashEntry> batch) throws Exception {
        flushHashesToTable(duckConn, "source_row_hashes", batch);
    }

    private static void flushTargetRowHashes(
            java.sql.Connection duckConn, List<RowHashEntry> batch) throws Exception {
        flushHashesToTable(duckConn, "target_row_hashes", batch);
    }

    private static void flushHashesToTable(
            java.sql.Connection duckConn, String tableName, List<RowHashEntry> batch) throws Exception {
        if (batch.isEmpty())
            return;
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" VALUES ");
        for (int i = 0; i < batch.size(); i++) {
            if (i > 0)
                sql.append(',');
            sql.append("(?,?)");
        }
        try (PreparedStatement ps = duckConn.prepareStatement(sql.toString())) {
            int p = 1;
            for (RowHashEntry entry : batch) {
                ps.setLong(p++, entry.idValue());
                ps.setBytes(p++, entry.rowSha256());
            }
            ps.executeUpdate();
        }
        batch.clear();
    }

    private static String computeOrderedAggregateDigest(
            java.sql.Connection duckConn, String tableName) throws Exception {
        MessageDigest aggregate = MessageDigest.getInstance("SHA-256");
        long prevId = Long.MIN_VALUE;
        boolean first = true;
        ByteBuffer idBuf = ByteBuffer.allocate(8);
        try (Statement stmt = duckConn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT id_value, row_sha256 FROM " + tableName + " ORDER BY id_value ASC")) {
            while (rs.next()) {
                long id = rs.getLong(1);
                if (!first && id == prevId) {
                    throw new IllegalStateException("Duplicate observation ID detected during compaction parity: " + id);
                }
                first = false;
                prevId = id;
                idBuf.clear();
                idBuf.putLong(id);
                aggregate.update(idBuf.array());
                aggregate.update(rs.getBytes(2));
            }
        }
        return HexFormat.of().formatHex(aggregate.digest());
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
        for (ArchiveCatalog.DeletingGenerationRecord rec : candidates) {
            if (archiveReader.activePinCount(rec.generationId()) > 0)
                continue;

            archiveReader.invalidateGenerations(
                    rec.objectRelativePaths(),
                    rec.sha256() != null ? List.of(rec.sha256()) : List.of());

            // Unlink physical files BEFORE deleting DELETING catalog rows so filesystem audit stays 100% clean.
            boolean allUnlinked = true;
            Set<Path> parentDirs = new LinkedHashSet<>();
            for (Path rel : rec.objectRelativePaths()) {
                try {
                    Path target = resolveSafeRegularDeleteTarget(rel);
                    if (target != null) {
                        if (target.getParent() != null)
                            parentDirs.add(target.getParent());
                        Files.deleteIfExists(target);
                    }
                } catch (IOException ioError) {
                    allUnlinked = false;
                }
            }
            if (!allUnlinked)
                continue;

            pruneEmptyProfileAndDateDirectories(parentDirs);
            if (catalog.finalizeDeletedGeneration(rec.generationId(), rec.jobId(), now)) {
                retiredIds.add(rec.generationId());
                finalized++;
            }
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
            try (var stream = Files.newDirectoryStream(dir)) {
                if (stream.iterator().hasNext())
                    return;
            }
            Files.deleteIfExists(dir);
        } catch (DirectoryNotEmptyException ignored) {
        } catch (IOException ignored) {
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
