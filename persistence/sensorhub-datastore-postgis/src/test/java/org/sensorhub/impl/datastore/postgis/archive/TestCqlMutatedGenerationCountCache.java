package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.archive.ParquetArchiveReader.GenerationSummary;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;

import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class TestCqlMutatedGenerationCountCache {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void excludedIdsDigestIsDeterministicOrderIndependentAndCollisionResistant() {
        assertEquals("", ParquetArchiveReader.computeExcludedIdsDigest(null));
        assertEquals("", ParquetArchiveReader.computeExcludedIdsDigest(Set.of()));

        String digestAsc = ParquetArchiveReader.computeExcludedIdsDigest(List.of(12L, 450L, 9001L));
        String digestUnsorted = ParquetArchiveReader.computeExcludedIdsDigest(List.of(9001L, 12L, 450L, 12L));
        assertEquals(64, digestAsc.length());
        assertEquals(digestAsc, digestUnsorted);

        assertNotEquals(digestAsc, ParquetArchiveReader.computeExcludedIdsDigest(List.of(12L, 450L)));
        assertNotEquals(digestAsc, ParquetArchiveReader.computeExcludedIdsDigest(List.of(12L, 451L, 9001L)));
        assertNotEquals(
                ParquetArchiveReader.computeExcludedIdsDigest(List.of(Long.MIN_VALUE, 0L)),
                ParquetArchiveReader.computeExcludedIdsDigest(List.of(Long.MIN_VALUE, 1L)));
    }

    @Test
    public void mutatedGenerationsCachePerGenerationCountsAndInvalidateOnlyChangedGeneration() throws Exception {
        Path root = temporary.newFolder("cql-mutated-cache-root").toPath();
        Path duckTmp = temporary.getRoot().toPath().resolve("cql-mutated-cache-ducktmp");
        TieredArchiveConfig config = singleSessionConfig(root, duckTmp);
        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);

        Instant baseStart = Instant.parse("2026-11-15T00:00:00Z");
        List<ArchiveGeneration> generations = new ArrayList<>();
        Map<UUID, List<FixtureRow>> rowsByGen = new LinkedHashMap<>();
        Set<Long> emlMutatedIds = new HashSet<>();

        // 12 sealed 30-minute generations; 8 of 12 (> 50%) carry EML Code 9999 overlays.
        for (int g = 0; g < 12; g++) {
            Instant winStart = baseStart.plusSeconds(g * 1_800L);
            Instant winEnd = winStart.plusSeconds(1_800L);
            long idBase = (g + 1L) * 100L;
            List<FixtureRow> rows = List.of(
                    new FixtureRow(idBase + 1, 10L, winStart.plusSeconds(60), winStart.plusSeconds(61),
                            occupancyJson(winStart.plusSeconds(55), winStart.plusSeconds(60), true, false, 85.0, 0)),
                    new FixtureRow(idBase + 2, 10L, winStart.plusSeconds(120), winStart.plusSeconds(121),
                            occupancyJson(winStart.plusSeconds(115), winStart.plusSeconds(120), true, false, 120.0, 0)),
                    new FixtureRow(idBase + 3, 10L, winStart.plusSeconds(180), winStart.plusSeconds(181),
                            occupancyJson(winStart.plusSeconds(175), winStart.plusSeconds(180), false, false, 95.0, 0)),
                    new FixtureRow(idBase + 4, 10L, winStart.plusSeconds(240), winStart.plusSeconds(241),
                            occupancyJson(winStart.plusSeconds(235), winStart.plusSeconds(240), false, true, 20.0, 0)));
            ArchiveGeneration gen = writeParquetGeneration(
                    root, "datasets/observations/date=2026-11-15/profile=RAW_GATED/gen-" + g + ".parquet",
                    winStart, winEnd, rows);
            generations.add(gen);
            rowsByGen.put(gen.id(), rows);
            if (g % 3 != 2) {
                // 8 of 12 generations have EML Code 9999 auto-adjudicated idBase + 1.
                emlMutatedIds.add(idBase + 1);
            }
        }

        ObsFilter alarmHighGamma = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(4, 10L))
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(baseStart, baseStart.plusSeconds(12 * 1_800L))
                        .build())
                .withResultTime(new TemporalFilter.Builder()
                        .withRange(baseStart, baseStart.plusSeconds(12 * 1_800L + 60L))
                        .build())
                .withCQLFilter("(gammaAlarm = true OR neutronAlarm = true) AND maxGamma >= 50")
                .build();

        long expectedInitial = bruteForceUnmutatedMatches(rowsByGen, alarmHighGamma, emlMutatedIds);
        // Each generation has 2 matching base rows (idBase+1, idBase+2); 8 have idBase+1 excluded -> 24 - 8 = 16.
        assertEquals(16L, expectedInitial);

        long firstCount = reader.countCql(generations, alarmHighGamma, Set.of(10L), null, emlMutatedIds);
        assertEquals(expectedInitial, firstCount);
        assertEquals(1L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals(0L, reader.cqlCountCacheHitsForTesting());
        assertEquals(12L, reader.cqlCountScannedGenerationsForTesting());

        // Hold the sole DuckDB permit busy; repeat query must hit cqlGenerationCounts for all 12 generations
        // (including the 8 EML-mutated ones) with 0 DuckDB session acquisitions.
        try (DuckDbResourcePolicy.Session occupied = DuckDbResourcePolicy.open(config, "export")) {
            assertNotEquals(null, occupied.connection());
            long cachedCount = reader.countCql(generations, alarmHighGamma, Set.of(10L), null, emlMutatedIds);
            assertEquals(expectedInitial, cachedCount);
        }
        assertEquals("Cached mutated CQL count must open 0 new DuckDB sessions",
                1L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals(12L, reader.cqlCountCacheHitsForTesting());
        assertEquals(12L, reader.cqlCountScannedGenerationsForTesting());

        // Operator adjudicates cold observation 502L inside generation index 4 (idBase = 500L).
        Set<Long> updatedExcludedIds = new HashSet<>(emlMutatedIds);
        updatedExcludedIds.add(502L);
        long expectedAfterAdjudication = bruteForceUnmutatedMatches(rowsByGen, alarmHighGamma, updatedExcludedIds);
        assertEquals(15L, expectedAfterAdjudication);

        long updatedCount = reader.countCql(generations, alarmHighGamma, Set.of(10L), null, updatedExcludedIds);
        assertEquals(expectedAfterAdjudication, updatedCount);
        assertEquals("Only 1 new DuckDB session may open for the single invalidated generation",
                2L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals("11 of 12 generations must hit cache when only generation #4 gained an exclusion",
                23L, reader.cqlCountCacheHitsForTesting());
        assertEquals("Only generation #4 may be rescanned in DuckDB",
                13L, reader.cqlCountScannedGenerationsForTesting());

        // Subsequent poll with updatedExcludedIds hits cache across all 12 generations with 0 DuckDB sessions.
        try (DuckDbResourcePolicy.Session occupied = DuckDbResourcePolicy.open(config, "export")) {
            assertNotEquals(null, occupied.connection());
            assertEquals(15L, reader.countCql(generations, alarmHighGamma, Set.of(10L), null, updatedExcludedIds));
        }
        assertEquals(2L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals(35L, reader.cqlCountCacheHitsForTesting());
        assertEquals(13L, reader.cqlCountScannedGenerationsForTesting());
    }

    @Test
    public void zeroAlarmGenerationSummaryPruningSkipsDuckDbCompletelyAndPreservesConservativeSafety() throws Exception {
        Path root = temporary.newFolder("cql-zero-alarm-prune-root").toPath();
        Path duckTmp = temporary.getRoot().toPath().resolve("cql-zero-alarm-prune-ducktmp");
        TieredArchiveConfig config = singleSessionConfig(root, duckTmp);
        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);

        Instant baseStart = Instant.parse("2026-11-16T00:00:00Z");
        List<ArchiveGeneration> zeroAlarmGens = new ArrayList<>();
        Map<UUID, GenerationSummary> summaries = new HashMap<>();

        // 4 generations with high maxGamma non-alarm background/occupancy rows (0 gamma, 0 neutron alarms).
        for (int g = 0; g < 4; g++) {
            Instant winStart = baseStart.plusSeconds(g * 1_800L);
            Instant winEnd = winStart.plusSeconds(1_800L);
            long idBase = (g + 1L) * 1_000L;
            List<FixtureRow> rows = List.of(
                    new FixtureRow(idBase + 1, 10L, winStart.plusSeconds(30), winStart.plusSeconds(31),
                            occupancyJson(winStart.plusSeconds(20), winStart.plusSeconds(30), false, false, 150.0, 0)),
                    new FixtureRow(idBase + 2, 10L, winStart.plusSeconds(90), winStart.plusSeconds(91),
                            occupancyJson(winStart.plusSeconds(80), winStart.plusSeconds(90), false, false, 30.0, 0)));
            ArchiveGeneration gen = writeParquetGeneration(
                    root, "datasets/observations/date=2026-11-16/profile=RAW_GATED/zero-" + g + ".parquet",
                    winStart, winEnd, rows);
            zeroAlarmGens.add(gen);
            summaries.put(gen.id(), new GenerationSummary(2L, 0L, 0L, 0L));
        }

        // Generation 4 has ONLY a Gamma & Neutron alarm (gammaAlarm=true AND neutronAlarm=true), 0 Gamma-only / Neutron-only.
        Instant gnStart = baseStart.plusSeconds(4 * 1_800L);
        ArchiveGeneration gnOnlyGen = writeParquetGeneration(
                root, "datasets/observations/date=2026-11-16/profile=RAW_GATED/gn-only.parquet",
                gnStart, gnStart.plusSeconds(1_800L), List.of(
                        new FixtureRow(5_001L, 10L, gnStart.plusSeconds(45), gnStart.plusSeconds(46),
                                occupancyJson(gnStart.plusSeconds(40), gnStart.plusSeconds(45), true, true, 200.0, 0))));
        summaries.put(gnOnlyGen.id(), new GenerationSummary(1L, 0L, 0L, 1L));

        // Generation 5 has 1 Gamma-only alarm with maxGamma=80 and 1 Neutron-only alarm with maxGamma=60.
        Instant matchStart = baseStart.plusSeconds(5 * 1_800L);
        ArchiveGeneration matchingGen = writeParquetGeneration(
                root, "datasets/observations/date=2026-11-16/profile=RAW_GATED/matching.parquet",
                matchStart, matchStart.plusSeconds(1_800L), List.of(
                        new FixtureRow(6_001L, 10L, matchStart.plusSeconds(30), matchStart.plusSeconds(31),
                                occupancyJson(matchStart.plusSeconds(25), matchStart.plusSeconds(30), true, false, 80.0, 0)),
                        new FixtureRow(6_002L, 10L, matchStart.plusSeconds(60), matchStart.plusSeconds(61),
                                occupancyJson(matchStart.plusSeconds(55), matchStart.plusSeconds(60), false, true, 60.0, 0))));
        summaries.put(matchingGen.id(), new GenerationSummary(2L, 1L, 1L, 0L));

        // Exact CQL emitted by EventFilter.ts for: Status in [Gamma, Neutron] AND maxGamma >= 50
        String statusInGammaOrNeutronAndMaxGamma =
                "(((gammaAlarm=true AND neutronAlarm=false) OR (gammaAlarm=false AND neutronAlarm=true)) AND (maxGamma>=50))";
        ObsFilter statusAndMaxGammaFilter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(4, 10L))
                .withPhenomenonTime()
                .withRange(baseStart, baseStart.plusSeconds(6 * 1_800L))
                .done()
                .withCQLFilter(statusInGammaOrNeutronAndMaxGamma)
                .build();

        List<ArchiveGeneration> zeroMatchCandidates = new ArrayList<>(zeroAlarmGens);
        zeroMatchCandidates.add(gnOnlyGen);

        // Hold the sole DuckDB permit busy; all 5 zero-match generations (4 zero-alarm + 1 Gamma&Neutron-only)
        // must prune to 0L in memory without acquiring any DuckDB session!
        try (DuckDbResourcePolicy.Session occupied = DuckDbResourcePolicy.open(config, "export")) {
            assertNotEquals(null, occupied.connection());
            long prunedCount = reader.countCql(
                    zeroMatchCandidates, statusAndMaxGammaFilter, Set.of(10L), null, Set.of(), summaries);
            assertEquals(0L, prunedCount);

            // Bounded-limit countCql must also prune before opening DuckDB.
            ObsFilter boundedStatusFilter = ObsFilter.Builder.from(statusAndMaxGammaFilter).withLimit(5L).build();
            assertEquals(0L, reader.countCql(
                    zeroMatchCandidates, boundedStatusFilter, Set.of(10L), null, Set.of(), summaries));
        }
        assertEquals("Zero-match summary pruning must open 0 DuckDB sessions",
                0L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals(10L, reader.cqlCountZeroMatchPrunedGenerationsForTesting());
        assertEquals(0L, reader.cqlCountScannedGenerationsForTesting());

        // Across all 6 generations, only matchingGen (1 file) may be scanned in DuckDB -> count = 2L.
        List<ArchiveGeneration> allSix = new ArrayList<>(zeroMatchCandidates);
        allSix.add(matchingGen);
        assertEquals(2L, reader.countCql(allSix, statusAndMaxGammaFilter, Set.of(10L), null, Set.of(), summaries));
        assertEquals(1L, reader.cqlCountDuckDbSessionOpensForTesting());
        assertEquals(1L, reader.cqlCountScannedGenerationsForTesting());

        // Conservative safety: (gammaAlarm = true) OR (maxGamma >= 50) MUST NOT prune zeroAlarmGens
        // because non-alarm rows with maxGamma = 150 satisfy the second disjunct!
        ObsFilter nonAlarmDisjunct = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(4, 10L))
                .withCQLFilter("(gammaAlarm = true) OR (maxGamma >= 50)")
                .build();
        assertFalse(JsonCqlEvaluator.isGuaranteedZeroMatchBySummary(
                nonAlarmDisjunct.getCQLFilter(), new GenerationSummary(2L, 0L, 0L, 0L)));
        assertEquals(4L, reader.countCql(zeroAlarmGens, nonAlarmDisjunct, Set.of(10L), null, Set.of(), summaries));

        // Conservative safety: NOT (gammaAlarm = true) and unpopulated summaries MUST NOT prune.
        ObsFilter negatedAlarm = new ObsFilter.Builder()
                .withCQLFilter("NOT (gammaAlarm = true)")
                .build();
        assertFalse(JsonCqlEvaluator.isGuaranteedZeroMatchBySummary(
                negatedAlarm.getCQLFilter(), new GenerationSummary(2L, 0L, 0L, 0L)));
        assertFalse(JsonCqlEvaluator.isGuaranteedZeroMatchBySummary(
                statusAndMaxGammaFilter.getCQLFilter(),
                new GenerationSummary(false, 0L, 0L, 0L, 0L, 0L, 0L)));
    }

    @Test
    public void dashboardPostAcknowledgeEndTimeCutoffSummarizesPostCutoffGenerationsWithZeroDuckDbSessions()
            throws Exception {
        Path root = temporary.newFolder("dashboard-endtime-cutoff-root").toPath();
        Path duckTmp = temporary.getRoot().toPath().resolve("dashboard-endtime-cutoff-ducktmp");
        TieredArchiveConfig config = singleSessionConfig(root, duckTmp);
        ParquetArchiveReader reader = new ParquetArchiveReader(root, config);

        // Operator clicked Acknowledge All at cutoff = 2026-11-15T02:12:32.834Z;
        // Dashboard Unread Count queries phenomenonTime = (cutoff - 60s)/.. and CQL:
        // ((gammaAlarm=true OR neutronAlarm=true) AND adjudicatedIdsCount=0) AND (endTime>'2026-11-15T02:12:32.834Z')
        Instant cutoff = Instant.parse("2026-11-15T02:12:32.834Z");
        Instant phenMin = cutoff.minusSeconds(60L);
        Instant resultMax = Instant.parse("2026-11-25T14:13:36.945Z");
        Instant gen0Start = Instant.parse("2026-11-15T02:00:00Z");

        List<ArchiveGeneration> allGenerations = new ArrayList<>();
        Map<UUID, List<FixtureRow>> baseRowsByGen = new LinkedHashMap<>();
        Map<UUID, Long> baseUnadjudicatedAlarmSummaryByGen = new HashMap<>();
        Map<UUID, Set<Long>> mutatedIdsByGen = new HashMap<>();
        Map<UUID, Map<Long, Integer>> baseSummaryMaskByGen = new HashMap<>();
        List<String> overlayJsons = new ArrayList<>();

        for (int g = 0; g < 10; g++) {
            Instant winStart = gen0Start.plusSeconds(g * 1_800L);
            Instant winEnd = winStart.plusSeconds(1_800L);
            long idBase = (g + 1L) * 1_000L;
            List<FixtureRow> rows = new ArrayList<>();
            if (g == 0) {
                // Straddling generation G0 ([02:00Z, 02:30Z)):
                // Row 1001: ended at 02:12:00Z (after phenMin 02:11:32.834Z, BEFORE cutoff 02:12:32.834Z) -> must NOT match!
                rows.add(new FixtureRow(idBase + 1, 10L,
                        Instant.parse("2026-11-15T02:12:00Z"), Instant.parse("2026-11-15T02:12:01Z"),
                        occupancyJson(Instant.parse("2026-11-15T02:11:55Z"), Instant.parse("2026-11-15T02:12:00Z"),
                                true, false, 90.0, 0)));
                // Row 1002: ended at 02:20:00Z (after cutoff), unadjudicated -> MATCHES!
                rows.add(new FixtureRow(idBase + 2, 10L,
                        Instant.parse("2026-11-15T02:20:00Z"), Instant.parse("2026-11-15T02:20:01Z"),
                        occupancyJson(Instant.parse("2026-11-15T02:19:50Z"), Instant.parse("2026-11-15T02:20:00Z"),
                                true, false, 110.0, 0)));
                // Row 1003: ended at 02:25:00Z (after cutoff), EML Code 9999 auto-adjudicated via overlay -> excluded!
                String emlBaseJson = occupancyJson(
                        Instant.parse("2026-11-15T02:24:50Z"), Instant.parse("2026-11-15T02:25:00Z"),
                        true, false, 75.0, 0);
                rows.add(new FixtureRow(idBase + 3, 10L,
                        Instant.parse("2026-11-15T02:25:00Z"), Instant.parse("2026-11-15T02:25:01Z"),
                        emlBaseJson));
                overlayJsons.add(occupancyJson(
                        Instant.parse("2026-11-15T02:24:50Z"), Instant.parse("2026-11-15T02:25:00Z"),
                        true, false, 75.0, 1));
            } else {
                // Post-cutoff generations G1..G9 (windowStart >= 02:30:00Z > cutoff):
                // Each has 2 unadjudicated base alarms + 1 non-alarm occupancy.
                String alarm1 = occupancyJson(winStart.plusSeconds(10), winStart.plusSeconds(15), true, false, 88.0, 0);
                String alarm2 = occupancyJson(winStart.plusSeconds(40), winStart.plusSeconds(45), false, true, 12.0, 0);
                String nonAlarm = occupancyJson(winStart.plusSeconds(70), winStart.plusSeconds(75), false, false, 15.0, 0);
                rows.add(new FixtureRow(idBase + 1, 10L, winStart.plusSeconds(15), winStart.plusSeconds(16), alarm1));
                rows.add(new FixtureRow(idBase + 2, 10L, winStart.plusSeconds(45), winStart.plusSeconds(46), alarm2));
                rows.add(new FixtureRow(idBase + 3, 10L, winStart.plusSeconds(75), winStart.plusSeconds(76), nonAlarm));
            }
            ArchiveGeneration gen = writeParquetGeneration(
                    root, "datasets/observations/date=2026-11-15/profile=RAW_GATED/dash-" + g + ".parquet",
                    winStart, winEnd, rows);
            allGenerations.add(gen);
            baseRowsByGen.put(gen.id(), rows);

            if (g == 0) {
                baseUnadjudicatedAlarmSummaryByGen.put(gen.id(), 3L);
                mutatedIdsByGen.put(gen.id(), Set.of(idBase + 3));
                baseSummaryMaskByGen.put(gen.id(), Map.of(
                        idBase + 3, JsonCqlEvaluator.computeSummaryMask(rows.get(2).resultJson())));
            } else {
                baseUnadjudicatedAlarmSummaryByGen.put(gen.id(), 2L);
                if (g % 2 == 1) {
                    // Odd post-cutoff generations G1, G3, G5, G7, G9 (5 generations) have EML Code 9999 overlay on idBase+1!
                    mutatedIdsByGen.put(gen.id(), Set.of(idBase + 1));
                    baseSummaryMaskByGen.put(gen.id(), Map.of(
                            idBase + 1, JsonCqlEvaluator.computeSummaryMask(rows.get(0).resultJson())));
                    overlayJsons.add(occupancyJson(
                            winStart.plusSeconds(10), winStart.plusSeconds(15), true, false, 88.0, 1));
                }
            }
        }

        String unreadCql = "((gammaAlarm=true OR neutronAlarm=true) AND adjudicatedIdsCount=0) AND (endTime>'" + cutoff + "')";
        ObsFilter unreadFilter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(4, 10L))
                .withPhenomenonTime(new TemporalFilter.Builder().withRange(phenMin, Instant.MAX).build())
                .withResultTime(new TemporalFilter.Builder().withRange(Instant.MIN, resultMax).build())
                .withCQLFilter(unreadCql)
                .build();

        // Verify both conjunct orders resolve minContainedWindowStart immediately after cutoff (no 24h lag).
        JsonCqlEvaluator.TimeContainedSummarySpec tcSpec =
                JsonCqlEvaluator.timeContainedSummarySpec(unreadFilter.getCQLFilter()).orElseThrow();
        assertEquals("COALESCE(s.unadjudicated_alarm_count, 0)", tcSpec.exactExpr());
        assertEquals(cutoff.plusMillis(1L), tcSpec.minContainedWindowStart());

        ObsFilter reversedConjuncts = new ObsFilter.Builder()
                .withCQLFilter("(endTime>'" + cutoff + "') AND ((gammaAlarm=true OR neutronAlarm=true) AND adjudicatedIdsCount=0)")
                .build();
        assertEquals(cutoff.plusMillis(1L),
                JsonCqlEvaluator.timeContainedSummarySpec(reversedConjuncts.getCQLFilter())
                        .orElseThrow().minContainedWindowStart());

        JsonCqlEvaluator.SummaryCounterSpec summarySpec =
                JsonCqlEvaluator.summaryCounterSpec(unreadFilter.getCQLFilter()).orElseThrow();
        ArchiveCatalog mockCatalog = catalogBackedBySummaryMap(baseUnadjudicatedAlarmSummaryByGen, config);

        // 1. Post-cutoff generations G1..G9 (including 5 EML-mutated ones) summarize 100% in PostgreSQL:
        //    0 remaining Parquet candidates and 0 DuckDB sessions opened even while DuckDB is saturated!
        List<ArchiveGeneration> postCutoffGens = allGenerations.subList(1, 10);
        try (DuckDbResourcePolicy.Session occupied = DuckDbResourcePolicy.open(config, "export")) {
            assertNotEquals(null, occupied.connection());
            ArchiveCatalog.ColdSummaryPlan postCutoffPlan = mockCatalog.summarizeColdCandidates(
                    postCutoffGens, unreadFilter.getPhenomenonTime(), unreadFilter.getResultTime(),
                    Set.of(10L), mutatedIdsByGen.keySet(), summarySpec, tcSpec);
            assertTrue("All post-cutoff generations must be summarized without DuckDB fallback",
                    postCutoffPlan.remainingCandidates().isEmpty());
            assertEquals(18L, postCutoffPlan.summarizedCount()); // 9 * 2 base unadjudicated alarms
            assertEquals(5, postCutoffPlan.exactMutatedCandidates().size());

            List<org.geotools.api.filter.Filter> maskConjuncts =
                    JsonCqlEvaluator.extractSummaryMaskConjuncts(unreadFilter.getCQLFilter(), true);
            long mutatedBaseMatches = 0L;
            for (ArchiveGeneration mutatedGen : postCutoffPlan.exactMutatedCandidates()) {
                for (int mask : baseSummaryMaskByGen.get(mutatedGen.id()).values()) {
                    if (JsonCqlEvaluator.tryEvaluateSummaryMaskConjuncts(maskConjuncts, mask).orElseThrow())
                        mutatedBaseMatches++;
                }
            }
            assertEquals(5L, mutatedBaseMatches);
            long postCutoffUnmutatedCount = postCutoffPlan.summarizedCount() - mutatedBaseMatches;
            assertEquals(13L, postCutoffUnmutatedCount);
        }
        assertEquals("Post-cutoff summary + base_summary_mask path must open 0 DuckDB sessions",
                0L, reader.cqlCountDuckDbSessionOpensForTesting());

        // 2. Full G0..G9 pipeline: G1..G9 summarize in PostgreSQL (13L), only straddling G0 (1 file) scans in DuckDB!
        ArchiveCatalog.ColdSummaryPlan fullPlan = mockCatalog.summarizeColdCandidates(
                allGenerations, unreadFilter.getPhenomenonTime(), unreadFilter.getResultTime(),
                Set.of(10L), mutatedIdsByGen.keySet(), summarySpec, tcSpec);
        assertEquals(List.of(allGenerations.get(0)), fullPlan.remainingCandidates());

        List<org.geotools.api.filter.Filter> maskConjuncts =
                JsonCqlEvaluator.extractSummaryMaskConjuncts(unreadFilter.getCQLFilter(), true);
        long mutatedSummaryBaseMatches = 0L;
        for (ArchiveGeneration mutatedGen : fullPlan.exactMutatedCandidates()) {
            for (int mask : baseSummaryMaskByGen.get(mutatedGen.id()).values()) {
                if (JsonCqlEvaluator.tryEvaluateSummaryMaskConjuncts(maskConjuncts, mask).orElseThrow())
                    mutatedSummaryBaseMatches++;
            }
        }
        long summarizedColdCount = fullPlan.summarizedCount() - mutatedSummaryBaseMatches;
        Set<Long> parquetMutatedIds = fullPlan.remainingCandidates().stream()
                .flatMap(g -> mutatedIdsByGen.getOrDefault(g.id(), Set.of()).stream())
                .collect(java.util.stream.Collectors.toSet());
        long scannedBoundaryCount = reader.countCql(
                fullPlan.remainingCandidates(), unreadFilter, Set.of(10L), null,
                parquetMutatedIds, mutatedIdsByGen, null);
        long matchingOverlayCount = overlayJsons.stream()
                .filter(json -> JsonCqlEvaluator.matches(unreadFilter.getCQLFilter(), json))
                .count();
        long actualTotal = summarizedColdCount + scannedBoundaryCount + matchingOverlayCount;

        Set<Long> allMutatedFlat = mutatedIdsByGen.values().stream()
                .flatMap(Set::stream)
                .collect(java.util.stream.Collectors.toSet());
        long expectedTotal = bruteForceUnmutatedMatches(baseRowsByGen, unreadFilter, allMutatedFlat)
                + matchingOverlayCount;

        assertEquals(1L, scannedBoundaryCount); // Only row 1002 in G0 matches (1001 <= cutoff, 1003 EML-adjudicated)
        assertEquals(0L, matchingOverlayCount); // All 6 EML overlays have adjudicatedIdsCount = 1
        assertEquals(14L, expectedTotal);
        assertEquals(expectedTotal, actualTotal);
        assertEquals("Only the single straddling generation G0 may be scanned in DuckDB",
                1L, reader.cqlCountScannedGenerationsForTesting());
    }

    private static TieredArchiveConfig singleSessionConfig(Path root, Path duckTmp) {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = root.toAbsolutePath().normalize().toString();
        config.duckDbTemporaryDirectory = duckTmp.toAbsolutePath().normalize().toString();
        config.duckDbMemoryLimitMegabytes = 64;
        config.duckDbThreads = 1;
        config.duckDbMaximumTemporaryMegabytes = 96;
        config.duckDbMaximumConcurrentSessions = 1;
        config.duckDbSessionAcquireTimeoutSeconds = 1;
        return config;
    }

    private static String occupancyJson(
            Instant startTime,
            Instant endTime,
            boolean gammaAlarm,
            boolean neutronAlarm,
            double maxGamma,
            int adjudicatedCount) {
        String adjArray = adjudicatedCount <= 0 ? "[]" : "[\"code:9999\"]";
        return "{\"samplingTime\":\"" + endTime + "\"," +
                "\"startTime\":\"" + startTime + "\"," +
                "\"endTime\":\"" + endTime + "\"," +
                "\"gammaAlarm\":" + gammaAlarm + "," +
                "\"neutronAlarm\":" + neutronAlarm + "," +
                "\"maxGamma\":" + maxGamma + "," +
                "\"adjudicatedIdsCount\":" + adjudicatedCount + "," +
                "\"adjudicatedIds\":" + adjArray + "}";
    }

    private static long bruteForceUnmutatedMatches(
            Map<UUID, List<FixtureRow>> rowsByGen,
            ObsFilter filter,
            Set<Long> mutatedIds) {
        long count = 0L;
        for (List<FixtureRow> rows : rowsByGen.values()) {
            for (FixtureRow row : rows) {
                if (mutatedIds.contains(row.id()))
                    continue;
                if (filter.getPhenomenonTime() != null &&
                        !filter.getPhenomenonTime().getRange().contains(row.phenomenonTime()))
                    continue;
                if (filter.getResultTime() != null &&
                        !filter.getResultTime().getRange().contains(row.resultTime()))
                    continue;
                if (JsonCqlEvaluator.matches(filter.getCQLFilter(), row.resultJson()))
                    count++;
            }
        }
        return count;
    }

    private static ArchiveCatalog catalogBackedBySummaryMap(
            Map<UUID, Long> unadjudicatedAlarmCountByGen,
            TieredArchiveConfig config) {
        List<Map.Entry<UUID, Long>> entries = new ArrayList<>(unadjudicatedAlarmCountByGen.entrySet());
        Connection mockConnection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "setReadOnly", "close" -> null;
                    case "createArrayOf" -> (Array) Proxy.newProxyInstance(
                            Array.class.getClassLoader(),
                            new Class<?>[]{Array.class},
                            (arrProxy, arrMethod, arrArgs) -> null);
                    case "prepareStatement" -> (PreparedStatement) Proxy.newProxyInstance(
                            PreparedStatement.class.getClassLoader(),
                            new Class<?>[]{PreparedStatement.class},
                            (psProxy, psMethod, psArgs) -> {
                                if ("executeQuery".equals(psMethod.getName())) {
                                    AtomicInteger cursor = new AtomicInteger(-1);
                                    return (ResultSet) Proxy.newProxyInstance(
                                            ResultSet.class.getClassLoader(),
                                            new Class<?>[]{ResultSet.class},
                                            (rsProxy, rsMethod, rsArgs) -> switch (rsMethod.getName()) {
                                                case "next" -> cursor.incrementAndGet() < entries.size();
                                                case "getObject" -> entries.get(cursor.get()).getKey();
                                                case "getBoolean" -> {
                                                    int col = (Integer) rsArgs[0];
                                                    // col 2 = summary_populated (true),
                                                    // col 4 = exactReady (false for extra conjuncts),
                                                    // col 6 = tcReady (true: occupancy schema declares endTime:Time)
                                                    yield col == 2 || col == 6;
                                                }
                                                case "getLong" -> {
                                                    int col = (Integer) rsArgs[0];
                                                    long val = entries.get(cursor.get()).getValue();
                                                    // col 3 = upperBound, col 5 = exact (0), col 7 = tcExact
                                                    yield (col == 3 || col == 7) ? val : 0L;
                                                }
                                                case "close" -> null;
                                                default -> null;
                                            });
                                }
                                return null;
                            });
                    default -> null;
                });
        return new ArchiveCatalog(() -> mockConnection, "obs", 4, config);
    }

    private static ArchiveGeneration writeParquetGeneration(
            Path root,
            String relativePath,
            Instant windowStart,
            Instant windowEnd,
            List<FixtureRow> rows) throws Exception {
        Path object = root.resolve(relativePath);
        Files.createDirectories(object.getParent());
        long minId = Long.MAX_VALUE;
        long maxId = Long.MIN_VALUE;
        Instant minResult = null;
        Instant maxResult = null;

        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement statement = duck.createStatement()) {
            statement.execute("CREATE TABLE rows (" +
                    "id_scope INTEGER, id_value BIGINT, datastream_scope INTEGER, datastream_id BIGINT, " +
                    "foi_scope INTEGER, foi_id BIGINT, phenomenon_time TIMESTAMP, result_time TIMESTAMP, " +
                    "result_json VARCHAR, archive_version INTEGER, profile VARCHAR)");
            try (PreparedStatement insert = duck.prepareStatement(
                    "INSERT INTO rows VALUES (4, ?, 4, ?, NULL, NULL, ?, ?, ?, 1, 'RAW_GATED')")) {
                for (FixtureRow row : rows) {
                    insert.setLong(1, row.id());
                    insert.setLong(2, row.dataStreamId());
                    UtcJdbcTime.setDuckDbInstant(insert, 3, row.phenomenonTime());
                    UtcJdbcTime.setDuckDbInstant(insert, 4, row.resultTime());
                    insert.setString(5, row.resultJson());
                    insert.addBatch();
                    minId = Math.min(minId, row.id());
                    maxId = Math.max(maxId, row.id());
                    minResult = minResult == null || row.resultTime().isBefore(minResult)
                            ? row.resultTime() : minResult;
                    maxResult = maxResult == null || row.resultTime().isAfter(maxResult)
                            ? row.resultTime() : maxResult;
                }
                insert.executeBatch();
            }
            statement.execute("COPY rows TO '" + object.toString().replace("'", "''") +
                    "' (FORMAT PARQUET, COMPRESSION ZSTD)");
        }
        if (rows.isEmpty()) {
            minId = 0L;
            maxId = 0L;
        }
        return new ArchiveGeneration(
                UUID.randomUUID(),
                "RAW_GATED",
                windowStart,
                windowEnd,
                Path.of(relativePath),
                ArchiveExporter.sha256(object),
                rows.size(),
                minId,
                maxId,
                "SEALED",
                true,
                maxResult,
                minResult);
    }

    private record FixtureRow(
            long id,
            long dataStreamId,
            Instant phenomenonTime,
            Instant resultTime,
            String resultJson) {}
}
