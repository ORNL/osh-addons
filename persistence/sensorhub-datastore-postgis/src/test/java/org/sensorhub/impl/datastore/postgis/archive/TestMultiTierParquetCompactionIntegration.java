package org.sensorhub.impl.datastore.postgis.archive;

import net.opengis.swe.v20.DataBlock;
import net.opengis.swe.v20.DataRecord;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.DataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.feature.FeatureId;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.store.obs.OscarTieredObsStore;
import org.vast.data.TextEncodingImpl;
import org.vast.swe.SWEHelper;

import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.sql.Connection;
import java.sql.DriverManager;
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
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * End-to-end PostgreSQL + DuckDB + Parquet V2 integration verification for
 * split-at-export ({@code RAW_EVENTS} + {@code RAW_TELEMETRY}), event-only
 * multi-tier compaction ({@code RAW_EVENTS} -&gt; {@code DAILY_EVENTS} -&gt;
 * {@code MONTHLY_EVENTS}), disjoint rollover partitioning, overlapping source
 * clustering, settled-overlay folding, periodic defragmentation, OCR Bloom
 * catalog pruning, stale {@code .partial} self-healing, retention-hold and FK
 * safety, and online-backup grace windows.
 */
public class TestMultiTierParquetCompactionIntegration {
    private static final String PG_HOST_PORT = "localhost:5432";
    private static final String PG_DB = "gis";
    private static final String PG_USER = "postgres";
    private static final String PG_PASS = "postgres";
    private static final int ID_SCOPE = 4;

    private static final Pattern VALID_ARCHIVE_DIR = Pattern.compile(
            "^(datasets|datasets/observations|datasets/observations/date=\\d{4}-\\d{2}-\\d{2}" +
                    "|datasets/observations/date=\\d{4}-\\d{2}-\\d{2}/profile=[A-Za-z0-9_-]+)$");
    private static final Pattern VALID_ARCHIVE_OBJECT = Pattern.compile(
            "^datasets/observations/date=\\d{4}-\\d{2}-\\d{2}/profile=[A-Za-z0-9_-]+" +
                    "/generation-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}" +
                    "\\.(parquet|schemas\\.json\\.gz|manifest\\.json)$");

    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    private final List<OscarTieredObsStore> openStores = new ArrayList<>();
    private final List<String> createdStoreKeys = new ArrayList<>();

    @After
    public void tearDown() throws Exception {
        for (OscarTieredObsStore store : openStores) {
            try {
                store.close();
            } catch (Throwable ignored) {
            }
        }
        openStores.clear();

        if (!createdStoreKeys.isEmpty()) {
            try (Connection conn = openPg()) {
                for (String key : createdStoreKeys) {
                    cleanupStoreState(conn, key);
                }
            }
            createdStoreKeys.clear();
        }
    }

    @Test
    public void splitAtExportAndDayOneEventCompactionLeaveTelemetryUntouchedAndRollOverDisjointParts()
            throws Exception {
        Path archiveRoot = temporary.newFolder("day1-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("day1-spill");
        String table = uniqueTableName("compact_day1");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("occupancy", "vehicleOcr", "gammaCounts", "dailyFile", "connectionStatus", "occupancyStatus"));
        config.compactionUnlinkGraceMinutes = 15;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 101L, "occupancy");
        BigId ocrStream = addVehicleOcrStream(store, 102L, "vehicleOcr");
        BigId telStream = addTelemetryStream(store, 103L, "gammaCounts");
        BigId dailyFileStream = addTelemetryStream(store, 104L, "dailyFile");
        BigId connStatusStream = addTelemetryStream(store, 105L, "connectionStatus");
        BigId occStatusStream = addTelemetryStream(store, 106L, "occupancyStatus");

        Instant legacyDay = Instant.parse("2026-08-09T00:00:00Z");
        Instant dayD = Instant.parse("2026-08-10T00:00:00Z");
        Instant now = Instant.parse("2026-08-13T12:00:00Z");

        // Plant a legacy pre-upgrade RAW_GATED generation on Day D-1 to prove it is never rewritten
        // and stays 100% readable alongside V2 RAW_EVENTS / RAW_TELEMETRY / DAILY_EVENTS.
        BigId legacyObsId = store.add(eventOccupancyObs(
                occStream, legacyDay.plusSeconds(30L), legacyDay.plusSeconds(30L),
                "evt-legacy-raw-gated", true, false, 222.0, 2.0, -1, 0));
        commitStore(store);
        ArchiveGeneration legacyGen = store.archiveWindow(
                legacyDay, legacyDay.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_GATED)
                .orElseThrow();

        Map<BigId, String> expectedEventIds = new HashMap<>();
        Map<BigId, String> expectedOcrNorms = new HashMap<>();
        Map<BigId, Double> expectedTelemetryGamma = new HashMap<>();
        List<UUID> rawTelemetryIds = new ArrayList<>();

        // Insert INTERLEAVED event + telemetry + high-rate status observations across three 30-min windows in Day D
        // so [min_id, max_id] of RAW_EVENTS/DAILY_EVENTS and RAW_TELEMETRY overlap.
        for (int slot = 0; slot < 3; slot++) {
            Instant winStart = dayD.plus(slot * 30L, ChronoUnit.MINUTES);
            for (int i = 0; i < 10; i++) {
                Instant tEvent = winStart.plusSeconds(30L + i * 120L);
                String evtCode = "evt-d1-" + slot + "-" + i;
                boolean gammaAlarm = (i % 3 == 0);
                BigId occId = store.add(eventOccupancyObs(
                        occStream, tEvent, tEvent, evtCode, gammaAlarm, false,
                        gammaAlarm ? 160.0 + i : 18.0 + i, 2.0,
                        gammaAlarm ? 0 : -1, 0));
                expectedEventIds.put(occId, evtCode);

                BigId gammaId = store.add(telemetryObs(telStream, tEvent.plusSeconds(5L), tEvent.plusSeconds(5L),
                        1_000.0 + slot * 100.0 + i * 10.0, 3.0));
                expectedTelemetryGamma.put(gammaId, 1_000.0 + slot * 100.0 + i * 10.0);

                BigId dailyId = store.add(telemetryObs(dailyFileStream, tEvent.plusSeconds(8L), tEvent.plusSeconds(8L),
                        2_000.0 + slot * 100.0 + i, 1.0));
                expectedTelemetryGamma.put(dailyId, 2_000.0 + slot * 100.0 + i);

                BigId connId = store.add(telemetryObs(connStatusStream, tEvent.plusSeconds(11L), tEvent.plusSeconds(11L),
                        3_000.0 + slot * 100.0 + i, 1.0));
                expectedTelemetryGamma.put(connId, 3_000.0 + slot * 100.0 + i);

                BigId occStatId = store.add(telemetryObs(occStatusStream, tEvent.plusSeconds(14L), tEvent.plusSeconds(14L),
                        4_000.0 + slot * 100.0 + i, 1.0));
                expectedTelemetryGamma.put(occStatId, 4_000.0 + slot * 100.0 + i);

                if (i % 2 == 0) {
                    Instant tOcr = tEvent.plusSeconds(20L);
                    String norm = "MSCU1234" + slot + i + "5";
                    BigId ocrId = store.add(vehicleOcrObs(ocrStream, tOcr, tOcr, "occ-ref-" + slot + "-" + i, norm));
                    expectedOcrNorms.put(ocrId, norm);
                }
            }
            commitStore(store);
            ArchiveGeneration evGen = store.archiveWindow(
                    winStart, winStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS)
                    .orElseThrow();
            ArchiveGeneration telGen = store.archiveWindow(
                    winStart, winStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_TELEMETRY)
                    .orElseThrow();
            assertEquals("Each 30m RAW_EVENTS window must contain 10 occupancy + 5 OCR rows", 15L, evGen.rowCount());
            assertEquals("Each 30m RAW_TELEMETRY window must contain 40 telemetry/status rows", 40L, telGen.rowCount());
            rawTelemetryIds.add(telGen.id());
        }

        // Plant a synthetic historical RAW_EVENTS generation on 2026-08-11 whose activated_at is < 2h old!
        Instant syntheticDay = Instant.parse("2026-08-11T00:00:00Z");
        BigId freshHistoricalId = store.add(eventOccupancyObs(
                occStream, syntheticDay.plusSeconds(120L), syntheticDay.plusSeconds(120L),
                "evt-fresh-historical", false, false, 19.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration freshHistoricalGen = store.archiveWindow(
                syntheticDay, syntheticDay.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS)
                .orElseThrow();

        long expectedTotalColdRows = 1L + expectedEventIds.size() + expectedOcrNorms.size()
                + expectedTelemetryGamma.size() + 1L;

        // Verify parity, retire all 30m stage partitions, and seal every generation.
        store.maintainArchiveLifecycle();
        assertTrue("All PARQUET_PRIMARY 30m staging partitions must retire once both RAW_EVENTS and RAW_TELEMETRY verify",
                store.getImmutableStagePartitions().isEmpty());

        // Verify split-at-export V2 formats, column counts, and selective ocr_bloom BEFORE Day-1 compaction!
        try (Connection conn = openPg()) {
            List<Path> rawEventPaths = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_RAW_EVENTS);
            List<Path> rawTelPaths = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_RAW_TELEMETRY);
            assertEquals(4, rawEventPaths.size());
            assertEquals(3, rawTelPaths.size());
            for (Path p : rawEventPaths) {
                assertParquetV2AndColumnCount(archiveRoot.resolve(p), 20);
            }
            for (Path p : rawTelPaths) {
                assertParquetV2AndColumnCount(archiveRoot.resolve(p), 11);
            }
            // RAW_TELEMETRY must leave ocr_bloom NULL on every stream row!
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM oscar_archive_schema s " +
                            "JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                            "WHERE g.store_key = ? AND g.profile = 'RAW_TELEMETRY' AND s.ocr_bloom IS NOT NULL")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("RAW_TELEMETRY must not allocate ocr_bloom bytes", 0L, rs.getLong(1));
                }
            }
            // Backdate all generations > 2h, except freshHistoricalGen (< 2h) to prove activated_at guard!
            try (PreparedStatement backdateAll = conn.prepareStatement(
                    "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
                UtcJdbcTime.setInstant(backdateAll, 1, now.minus(3, ChronoUnit.HOURS));
                backdateAll.setString(2, table);
                assertEquals(8, backdateAll.executeUpdate());
            }
            try (PreparedStatement recentSynth = conn.prepareStatement(
                    "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
                UtcJdbcTime.setInstant(recentSynth, 1, now.minus(20, ChronoUnit.MINUTES));
                recentSynth.setObject(2, freshHistoricalGen.id());
                assertEquals(1, recentSynth.executeUpdate());
            }
        }

        // Run Day-1 compaction with maxRows=20 so 45 event rows roll over cleanly into 3 strictly disjoint DAILY_EVENTS parts,
        // while ALL 3 RAW_TELEMETRY generations and legacy RAW_GATED remain 100% untouched (zero telemetry rewrites)!
        config.maximumRowsPerGeneration = 20;
        ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
        assertEquals(1, summary.daysCompacted());
        assertEquals(0, summary.monthsConsolidated());
        assertEquals("15-minute grace window must defer physical unlink immediately after swap",
                0, summary.deletingGenerationsDrained());

        try (Connection conn = openPg()) {
            // 1. Freshly activated (< 2h) RAW_EVENTS and legacy RAW_GATED must remain completely untouched!
            assertEquals("SEALED", queryGenerationState(conn, freshHistoricalGen.id()));
            assertEquals(TieredArchiveConfig.PROFILE_RAW_EVENTS, queryGenerationProfile(conn, freshHistoricalGen.id()));
            assertEquals("SEALED", queryGenerationState(conn, legacyGen.id()));
            assertEquals(TieredArchiveConfig.PROFILE_RAW_GATED, queryGenerationProfile(conn, legacyGen.id()));

            // 2. ALL 3 RAW_TELEMETRY generations must remain SEALED with identical UUIDs and ZERO DAILY_TELEMETRY created!
            for (UUID telId : rawTelemetryIds) {
                assertEquals("RAW_TELEMETRY must never be rewritten by compaction", "SEALED", queryGenerationState(conn, telId));
                assertEquals(TieredArchiveConfig.PROFILE_RAW_TELEMETRY, queryGenerationProfile(conn, telId));
            }
            assertEquals(0, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY).size());

            // 3. Day D's 3 RAW_EVENTS generations are DELETING with row_count = 0 and files preserved during grace window.
            int deletingCount = 0;
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id, row_count, object_path FROM oscar_archive_generation " +
                            "WHERE store_key = ? AND state = 'DELETING'")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        deletingCount++;
                        assertEquals("DELETING generation must zero row_count immediately", 0L, rs.getLong(2));
                        assertTrue("DELETING generation Parquet must stay on disk during grace window",
                                Files.isRegularFile(archiveRoot.resolve(rs.getString(3)), LinkOption.NOFOLLOW_LINKS));
                    }
                }
            }
            assertEquals(3, deletingCount);

            // 4. Total catalog row_count across store_key is immediately exact!
            assertEquals(expectedTotalColdRows, querySumRowCount(conn, table));
            assertEquals(expectedTotalColdRows, store.getNumRecords());

            // 5. Verify multi-part DAILY_EVENTS rollover produced 3 strictly disjoint V2 parts covering [dayD, dayD+1d)!
            List<Path> dailyEventFiles = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS);
            assertEquals("45 event rows at maxRows=20 must roll over into 3 bounded DAILY_EVENTS parts",
                    3, dailyEventFiles.size());
            for (Path evRel : dailyEventFiles) {
                assertParquetV2AndColumnCount(archiveRoot.resolve(evRel), 20);
            }
            assertStrictlyDisjointProfileWindows(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS,
                    dayD, dayD.plus(1, ChronoUnit.DAYS), 3);

            // 6. Verify overlapping [min_id, max_id] in oscar_archive_id_block between DAILY_EVENTS and RAW_TELEMETRY!
            assertOverlappingIdBlocksExist(conn, table,
                    TieredArchiveConfig.PROFILE_DAILY_EVENTS, TieredArchiveConfig.PROFILE_RAW_TELEMETRY);

            // 7. Verify oscar_archive_schema 256-byte ocr_bloom on vehicleOcr and 1-byte sentinel on occupancy across DAILY_EVENTS.
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT s.datastream_id, min(octet_length(s.ocr_bloom)), max(octet_length(s.ocr_bloom)), " +
                            "min(s.ocr_min_norm), max(s.ocr_max_norm) " +
                            "FROM oscar_archive_schema s JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                            "WHERE g.store_key = ? AND g.profile = 'DAILY_EVENTS' AND g.state = 'SEALED' " +
                            "GROUP BY s.datastream_id")) {
                ps.setString(1, table);
                Map<Long, Integer> bloomLengths = new HashMap<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long dsId = rs.getLong(1);
                        assertEquals(rs.getInt(2), rs.getInt(3));
                        bloomLengths.put(dsId, rs.getInt(2));
                        if (dsId == ocrStream.getIdAsLong()) {
                            assertEquals("MSCU1234005", rs.getString(4));
                            assertEquals("MSCU1234285", rs.getString(5));
                        }
                    }
                }
                assertEquals(Integer.valueOf(OcrBloomFilter.BLOOM_BYTES), bloomLengths.get(ocrStream.getIdAsLong()));
                assertEquals(Integer.valueOf(1), bloomLengths.get(occStream.getIdAsLong()));
            }

            // 8. Run oscar-storage.sh archive verify SQL + filesystem reconciliation DURING grace window!
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        // 9. Point reads (store.get) across overlapping DAILY_EVENTS + RAW_TELEMETRY + legacy RAW_GATED id_blocks!
        for (Map.Entry<BigId, String> e : expectedEventIds.entrySet()) {
            IObsData obs = store.get(e.getKey());
            assertNotNull("Missing event point read " + e.getKey(), obs);
            assertEquals(e.getValue(), obs.getResult().getStringValue(0));
        }
        for (Map.Entry<BigId, String> e : expectedOcrNorms.entrySet()) {
            IObsData obs = store.get(e.getKey());
            assertNotNull("Missing OCR point read " + e.getKey(), obs);
            assertEquals(e.getValue(), obs.getResult().getStringValue(1));
        }
        for (Map.Entry<BigId, Double> e : expectedTelemetryGamma.entrySet()) {
            IObsData obs = store.get(e.getKey());
            assertNotNull("Missing telemetry point read " + e.getKey(), obs);
            assertEquals(e.getValue(), obs.getResult().getDoubleValue(0), 1e-6);
        }
        assertNotNull("Legacy RAW_GATED observation must remain readable", store.get(legacyObsId));
        assertNotNull("Fresh historical RAW_EVENTS observation must remain readable", store.get(freshHistoricalId));

        // 10. OCR Bloom candidate pruning + store CQL count/select parity across multi-part DAILY_EVENTS!
        ObsFilter absentOcr = new ObsFilter.Builder()
                .withDataStreams(ocrStream)
                .withCQLFilter("normalizedValue = 'ZZZZ9999999'")
                .build();
        List<ArchiveGeneration> ocrCandidates = store.getArchiveCatalog().candidates(
                dayD, dayD.plus(1, ChronoUnit.DAYS), null, null, null, Set.of(ocrStream.getIdAsLong()));
        assertEquals(3, ocrCandidates.size());
        assertTrue("OCR Bloom + zone-map must prune non-matching DAILY_EVENTS parts before DuckDB",
                store.getArchiveCatalog().pruneOcrCandidates(
                        ocrCandidates, Set.of(ocrStream.getIdAsLong()), absentOcr.getCQLFilter()).isEmpty());
        assertEquals(0L, store.countMatchingEntries(absentOcr));

        ObsFilter presentOcr = new ObsFilter.Builder()
                .withDataStreams(ocrStream)
                .withCQLFilter("normalizedValue LIKE 'MSCU12341%'")
                .build();
        assertEquals(5L, store.countMatchingEntries(presentOcr));
        try (Stream<IObsData> s = store.select(presentOcr)) {
            assertEquals(5L, s.count());
        }

        // 11. Advance past 15-minute grace window -> DELETING RAW_EVENTS unlink, prune empty RAW_EVENTS dir, pass verify!
        try (Connection conn = openPg();
             PreparedStatement backdateSealed = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET sealed_at = ? WHERE store_key = ? AND state = 'DELETING'")) {
            UtcJdbcTime.setInstant(backdateSealed, 1, now.minus(16, ChronoUnit.MINUTES));
            backdateSealed.setString(2, table);
            assertEquals(3, backdateSealed.executeUpdate());
        }
        assertEquals(3, store.drainDeletingGenerations(now));
        assertFalse("Empty profile=RAW_EVENTS directory for Day D must be pruned after unlink",
                Files.exists(archiveRoot.resolve("datasets/observations/date=2026-08-10/profile=RAW_EVENTS")));
        assertTrue("RAW_TELEMETRY directory for Day D must remain intact",
                Files.isDirectory(archiveRoot.resolve("datasets/observations/date=2026-08-10/profile=RAW_TELEMETRY")));
        try (Connection conn = openPg()) {
            assertArchiveVerifyClean(conn, table, archiveRoot);
            assertEquals(expectedTotalColdRows, querySumRowCount(conn, table));
        }
    }

    @Test
    public void settledAndConcurrentOverlaysTombstonesHoldsDeletionPlansAndReaderPinsPreserveExactIntegrity()
            throws Exception {
        Path archiveRoot = temporary.newFolder("overlays-holds-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("overlays-holds-spill");
        String table = uniqueTableName("compact_ovl");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("occupancy", "vehicleOcr", "gammaCounts"));
        config.compactionUnlinkGraceMinutes = 0; // Immediate drain once unpinned!

        OscarTieredObsStore store = openStore(table, config);
        BigId occStreamA = addEventOccupancyStream(store, 201L, "occupancy");
        BigId occStreamB = addEventOccupancyStream(store, 202L, "occupancy");
        BigId occStreamC = addEventOccupancyStream(store, 203L, "occupancy");

        Instant day1 = Instant.parse("2026-08-15T00:00:00Z");
        Instant day2Held = Instant.parse("2026-08-16T00:00:00Z");
        Instant now = Instant.parse("2026-08-20T12:00:00Z");

        // Day 1 Window 0: [00:00, 00:30)
        BigId obsSettled = store.add(eventOccupancyObs(
                occStreamA, day1.plusSeconds(60L), day1.plusSeconds(60L),
                "evt-initial-unadj", true, false, 140.0, 1.0, -1, 0));
        BigId obsDeleted = store.add(eventOccupancyObs(
                occStreamA, day1.plusSeconds(120L), day1.plusSeconds(120L),
                "evt-to-delete", true, true, 200.0, 15.0, -1, 0));
        BigId obsKeepA = store.add(eventOccupancyObs(
                occStreamA, day1.plusSeconds(180L), day1.plusSeconds(180L),
                "evt-keep-a", false, false, 12.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration d1Gen0 = store.archiveWindow(
                day1, day1.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();

        // Day 1 Window 1: [00:30, 01:00)
        BigId obsRecent = store.add(eventOccupancyObs(
                occStreamB, day1.plusSeconds(1_860L), day1.plusSeconds(1_860L),
                "evt-recent-base", true, false, 110.0, 2.0, -1, 0));
        BigId obsKeepB = store.add(eventOccupancyObs(
                occStreamB, day1.plusSeconds(1_920L), day1.plusSeconds(1_920L),
                "evt-keep-b", false, false, 15.0, 1.0, -1, 0));
        BigId obsLatestC = store.add(eventOccupancyObs(
                occStreamC, day1.plusSeconds(1_950L), day1.plusSeconds(1_950L),
                "evt-latest-c-base", false, false, 20.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration d1Gen1 = store.archiveWindow(
                day1.plus(30, ChronoUnit.MINUTES), day1.plus(60, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS)
                .orElseThrow();

        // Day 2 (Held): [00:00, 00:30)
        BigId obsHeld = store.add(eventOccupancyObs(
                occStreamA, day2Held.plusSeconds(60L), day2Held.plusSeconds(60L),
                "evt-held-day2", true, false, 300.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration d2HeldGen = store.archiveWindow(
                day2Held, day2Held.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();

        store.maintainArchiveLifecycle();
        assertTrue(store.getImmutableStagePartitions().isEmpty());
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(2, ChronoUnit.DAYS));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }

        // Apply cold mutations on Day 1:
        store.put(obsSettled, eventOccupancyObs(
                occStreamA, day1.plusSeconds(60L), day1.plusSeconds(60L),
                "evt-adjudicated-settled", true, false, 140.0, 1.0, 4, 2));
        Instant advancedTime = day1.plusSeconds(2_100L);
        store.put(obsRecent, eventOccupancyObs(
                occStreamB, advancedTime, advancedTime,
                "evt-recent-updated", true, true, 999.5, 42.0, 6, 1));
        store.put(obsLatestC, eventOccupancyObs(
                occStreamC, day1.plusSeconds(1_950L), day1.plusSeconds(1_950L),
                "evt-latest-c-overlaid", true, false, 777.25, 3.0, 4, 1));
        assertNotNull(store.remove(obsDeleted));

        UUID expiredPlanId = UUID.randomUUID();
        UUID holdId = UUID.randomUUID();
        try (Connection conn = openPg()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE oscar_obs_overlay SET updated_at = ? WHERE store_key = ? AND observation_id = ?")) {
                UtcJdbcTime.setInstant(ps, 1, now.minus(4, ChronoUnit.HOURS));
                ps.setString(2, table);
                ps.setLong(3, obsSettled.getIdAsLong());
                assertEquals(1, ps.executeUpdate());

                UtcJdbcTime.setInstant(ps, 1, now.minus(5, ChronoUnit.MINUTES));
                ps.setString(2, table);
                ps.setLong(3, obsRecent.getIdAsLong());
                assertEquals(1, ps.executeUpdate());

                UtcJdbcTime.setInstant(ps, 1, now.minus(5, ChronoUnit.MINUTES));
                ps.setString(2, table);
                ps.setLong(3, obsLatestC.getIdAsLong());
                assertEquals(1, ps.executeUpdate());
            }

            try (PreparedStatement planIns = conn.prepareStatement(
                    "INSERT INTO oscar_deletion_plan (id, store_key, id_scope, subject_type, subject_id, state, " +
                            "estimated_rows, estimated_bytes, hold_count, confirmation_hash, requested_by, created_at, expires_at) " +
                            "VALUES (?, ?, ?, 'ARCHIVE_BEFORE', '2026-08-16T00:00:00Z', 'PLANNED', 3, 100, 0, ?, 'tester', ?, ?)")) {
                planIns.setObject(1, expiredPlanId);
                planIns.setString(2, table);
                planIns.setInt(3, ID_SCOPE);
                planIns.setString(4, "0".repeat(64));
                UtcJdbcTime.setInstant(planIns, 5, now.minus(2, ChronoUnit.DAYS));
                UtcJdbcTime.setInstant(planIns, 6, now.minus(1, ChronoUnit.HOURS));
                planIns.executeUpdate();
            }
            try (PreparedStatement itemIns = conn.prepareStatement(
                    "INSERT INTO oscar_deletion_plan_item (plan_id, generation_id) VALUES (?, ?)")) {
                itemIns.setObject(1, expiredPlanId);
                itemIns.setObject(2, d1Gen0.id());
                itemIns.executeUpdate();
            }

            try (PreparedStatement holdIns = conn.prepareStatement(
                    "INSERT INTO oscar_retention_hold (id, store_key, id_scope, subject_type, subject_id, reason, created_by, resolution_mode) " +
                            "VALUES (?, ?, ?, 'OBSERVATION', ?, 'legal-hold-probe', 'tester', 'COLD_RANGE_CONSERVATIVE')")) {
                holdIns.setObject(1, holdId);
                holdIns.setString(2, table);
                holdIns.setInt(3, ID_SCOPE);
                holdIns.setString(4, Long.toString(obsHeld.getIdAsLong()));
                holdIns.executeUpdate();
            }
        }

        try (ParquetArchiveReader.PinnedGenerations pin = store.getArchiveReader().pinGenerations(List.of(d1Gen0.id()))) {
            ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
            assertEquals("Day 1 compacts while held Day 2 is skipped", 1, summary.daysCompacted());
            assertEquals("Unpinned d1Gen1 drains immediately at grace=0 while pinned d1Gen0 waits",
                    1, summary.deletingGenerationsDrained());
            assertTrue("Pinned d1Gen0 Parquet file must remain on disk while reader pin is active",
                    Files.isRegularFile(archiveRoot.resolve(d1Gen0.objectPath()), LinkOption.NOFOLLOW_LINKS));
        }

        assertEquals(1, store.drainDeletingGenerations(now));
        assertFalse("Unpinned d1Gen0 Parquet file must be unlinked after pin release",
                Files.exists(archiveRoot.resolve(d1Gen0.objectPath()), LinkOption.NOFOLLOW_LINKS));

        UUID day1CompactedGenId;
        try (Connection conn = openPg()) {
            assertEquals("SEALED", queryGenerationState(conn, d2HeldGen.id()));
            assertEquals(TieredArchiveConfig.PROFILE_RAW_EVENTS, queryGenerationProfile(conn, d2HeldGen.id()));

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM oscar_archive_generation WHERE store_key = ? AND profile = 'DAILY_EVENTS' AND state = 'SEALED'")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    day1CompactedGenId = rs.getObject(1, UUID.class);
                    assertFalse(rs.next());
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM oscar_obs_overlay WHERE store_key = ? AND observation_id = ?")) {
                ps.setString(1, table);
                ps.setLong(2, obsSettled.getIdAsLong());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0L, rs.getLong(1));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT generation_id FROM oscar_obs_overlay WHERE store_key = ? AND observation_id = ?")) {
                ps.setString(1, table);
                ps.setLong(2, obsRecent.getIdAsLong());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(day1CompactedGenId, rs.getObject(1, UUID.class));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT generation_id FROM oscar_tombstone WHERE store_key = ? AND subject_id = ?")) {
                ps.setString(1, table);
                ps.setString(2, Long.toString(obsDeleted.getIdAsLong()));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(day1CompactedGenId, rs.getObject(1, UUID.class));
                }
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT datastream_id, observation_id, profile, generation_id FROM oscar_obs_latest WHERE store_key = ?")) {
                ps.setString(1, table);
                Map<Long, String> latestProfiles = new HashMap<>();
                Map<Long, Long> latestObsIds = new HashMap<>();
                Map<Long, UUID> latestGenIds = new HashMap<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        latestProfiles.put(rs.getLong(1), rs.getString(3));
                        latestObsIds.put(rs.getLong(1), rs.getLong(2));
                        latestGenIds.put(rs.getLong(1), rs.getObject(4, UUID.class));
                    }
                }
                assertEquals("DAILY_EVENTS", latestProfiles.get(occStreamB.getIdAsLong()));
                assertEquals(Long.valueOf(obsKeepB.getIdAsLong()), latestObsIds.get(occStreamB.getIdAsLong()));
                assertEquals(day1CompactedGenId, latestGenIds.get(occStreamB.getIdAsLong()));

                assertEquals("OVERLAY", latestProfiles.get(occStreamC.getIdAsLong()));
                assertEquals(Long.valueOf(obsLatestC.getIdAsLong()), latestObsIds.get(occStreamC.getIdAsLong()));
                assertEquals(day1CompactedGenId, latestGenIds.get(occStreamC.getIdAsLong()));

                assertEquals(Long.valueOf(obsHeld.getIdAsLong()), latestObsIds.get(occStreamA.getIdAsLong()));
            }

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT observation_id, generation_id FROM oscar_event_locator WHERE store_key = ? AND public_id = ?")) {
                ps.setString(1, table);
                ps.setString(2, "evt-adjudicated-settled");
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(obsSettled.getIdAsLong(), rs.getLong(1));
                    assertEquals(day1CompactedGenId, rs.getObject(2, UUID.class));
                }
            }

            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        assertNull("Tombstoned observation must remain deleted after compaction", store.get(obsDeleted));

        IObsData foldedRead = store.get(obsSettled);
        assertNotNull(foldedRead);
        assertEquals("evt-adjudicated-settled", foldedRead.getResult().getStringValue(0));
        assertEquals(4, foldedRead.getResult().getIntValue(5));
        assertEquals(2, foldedRead.getResult().getIntValue(6));

        IObsData recentOverlayRead = store.get(obsRecent);
        assertNotNull(recentOverlayRead);
        assertEquals("evt-recent-updated", recentOverlayRead.getResult().getStringValue(0));
        assertEquals(999.5, recentOverlayRead.getResult().getDoubleValue(3), 1e-6);

        ObsFilter code4Filter = new ObsFilter.Builder()
                .withCQLFilter("adjudicationCode = 4 AND adjudicatedIdsCount = 2")
                .build();
        assertEquals(1L, store.countMatchingEntries(code4Filter));
        try (var entries = store.selectEntries(code4Filter)) {
            var list = entries.toList();
            assertEquals(1, list.size());
            assertEquals(obsSettled, list.get(0).getKey());
        }

        try (var latest = store.selectEntries(new ObsFilter.Builder()
                .withDataStreams(occStreamA, occStreamB, occStreamC)
                .withPhenomenonTime(new TemporalFilter.Builder().withLatestTime().build())
                .build())) {
            Map<Long, Map.Entry<BigId, IObsData>> byStream = new HashMap<>();
            latest.forEach(e -> byStream.put(e.getValue().getDataStreamID().getIdAsLong(), e));
            assertEquals(obsHeld, byStream.get(occStreamA.getIdAsLong()).getKey());
            assertEquals(obsRecent, byStream.get(occStreamB.getIdAsLong()).getKey());
            assertEquals(999.5, byStream.get(occStreamB.getIdAsLong()).getValue().getResult().getDoubleValue(3), 1e-6);
            assertEquals(obsLatestC, byStream.get(occStreamC.getIdAsLong()).getKey());
            assertEquals(777.25, byStream.get(occStreamC.getIdAsLong()).getValue().getResult().getDoubleValue(3), 1e-6);
        }
    }

    @Test
    public void monthlyConsolidationAndSixtyFourOverlayDefragmentationKeepTelemetryUntouched()
            throws Exception {
        Path archiveRoot = temporary.newFolder("monthly-defrag-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("monthly-defrag-spill");
        String table = uniqueTableName("compact_month");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("occupancy", "vehicleOcr", "gammaCounts"));
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 301L, "occupancy");
        BigId ocrStream = addVehicleOcrStream(store, 302L, "vehicleOcr");
        BigId telStream = addTelemetryStream(store, 303L, "gammaCounts");

        Instant julyStart = Instant.parse("2026-07-01T00:00:00Z");
        Instant now = Instant.parse("2026-08-20T12:00:00Z"); // > 15 days after 2026-08-01T00:00:00Z

        List<BigId> julyEventObsIds = new ArrayList<>();
        List<UUID> rawTelemetryGenIds = new ArrayList<>();
        for (int day = 0; day < 3; day++) {
            Instant dayStart = julyStart.plus(day * 5L, ChronoUnit.DAYS);
            for (int i = 0; i < 25; i++) {
                Instant t = dayStart.plusSeconds(30L + i * 30L);
                BigId occId = store.add(eventOccupancyObs(
                        occStream, t, t, "evt-july-" + day + "-" + i,
                        i % 5 == 0, false, 95.0 + i, 1.0, -1, 0));
                julyEventObsIds.add(occId);
            }
            for (int i = 0; i < 5; i++) {
                Instant t = dayStart.plusSeconds(900L + i * 30L);
                store.add(vehicleOcrObs(ocrStream, t, t, "occ-j-" + day + "-" + i, "TGHU777" + day + i + "01"));
            }
            for (int i = 0; i < 6; i++) {
                Instant t = dayStart.plusSeconds(1_200L + i * 10L);
                store.add(telemetryObs(telStream, t, t, 50.0 + i, 1.0));
            }
            commitStore(store);
            store.archiveWindow(dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS)
                    .orElseThrow();
            ArchiveGeneration telGen = store.archiveWindow(
                    dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_TELEMETRY)
                    .orElseThrow();
            rawTelemetryGenIds.add(telGen.id());
        }

        long expectedTotal = 75L + 15L + 18L;

        store.maintainArchiveLifecycle();
        assertTrue(store.getImmutableStagePartitions().isEmpty());
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(ps, 1, Instant.parse("2026-07-20T00:00:00Z"));
            ps.setString(2, table);
            assertEquals(6, ps.executeUpdate());
        }

        // Cycle 1: Compacts all 3 July days from RAW_EVENTS -> DAILY_EVENTS, leaving RAW_TELEMETRY untouched.
        ArchiveCompactionManager.CompactionCycleSummary cycle1 = store.runCompactionCycle(now);
        assertEquals(3, cycle1.daysCompacted());
        assertEquals("Freshly compacted DAILY_EVENTS have activated_at = now (< 7d), waiting before monthly merge",
                0, cycle1.monthsConsolidated());

        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? " +
                             "WHERE store_key = ? AND profile = 'DAILY_EVENTS'")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(8, ChronoUnit.DAYS));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }

        // Cycle 2: Consolidates all 3 July DAILY_EVENTS into 1 MONTHLY_EVENTS under date=2026-07-01/profile=MONTHLY_EVENTS.
        ArchiveCompactionManager.CompactionCycleSummary cycle2 = store.runCompactionCycle(now);
        assertEquals(0, cycle2.daysCompacted());
        assertEquals(1, cycle2.monthsConsolidated());

        UUID initialMonthlyGenId;
        try (Connection conn = openPg()) {
            assertEquals(0, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS).size());
            assertEquals(0, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY).size());
            assertEquals(3, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).size());
            for (UUID telId : rawTelemetryGenIds) {
                assertEquals("SEALED", queryGenerationState(conn, telId));
            }
            List<Path> monthlyPaths = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_MONTHLY_EVENTS);
            assertEquals(1, monthlyPaths.size());
            assertTrue("MONTHLY_EVENTS must reside under date=2026-07-01/profile=MONTHLY_EVENTS: " + monthlyPaths.get(0),
                    monthlyPaths.get(0).toString().startsWith("datasets/observations/date=2026-07-01/profile=MONTHLY_EVENTS/"));
            assertParquetV2AndColumnCount(archiveRoot.resolve(monthlyPaths.get(0)), 20);
            assertEquals(expectedTotal, querySumRowCount(conn, table));
            assertArchiveVerifyClean(conn, table, archiveRoot);

            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM oscar_archive_generation WHERE store_key = ? AND profile = 'MONTHLY_EVENTS' AND state = 'SEALED'")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    initialMonthlyGenId = rs.getObject(1, UUID.class);
                }
            }
        }

        // Apply 65 (>= 64 threshold) overlays onto July MONTHLY_EVENTS observations.
        for (int i = 0; i < 65; i++) {
            BigId targetId = julyEventObsIds.get(i);
            IObsData existing = store.get(targetId);
            assertNotNull(existing);
            store.put(targetId, eventOccupancyObs(
                    occStream, existing.getPhenomenonTime(), existing.getResultTime(),
                    "evt-defrag-" + i, true, false, 500.0 + i, 2.0, 4, 1));
        }

        try (Connection conn = openPg()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
                UtcJdbcTime.setInstant(ps, 1, now.minus(8, ChronoUnit.DAYS));
                ps.setObject(2, initialMonthlyGenId);
                assertEquals(1, ps.executeUpdate());
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "UPDATE oscar_obs_overlay SET updated_at = ? WHERE store_key = ?")) {
                UtcJdbcTime.setInstant(ps, 1, now.minus(8, ChronoUnit.DAYS));
                ps.setString(2, table);
                assertEquals(65, ps.executeUpdate());
            }
        }

        // Cycle 3: Periodic MONTHLY_EVENTS defragmentation folds all 65 settled overlays into a new MONTHLY_EVENTS file.
        ArchiveCompactionManager.CompactionCycleSummary cycle3 = store.runCompactionCycle(now);
        assertEquals(1, cycle3.monthlyGenerationsDefragmented());

        try (Connection conn = openPg()) {
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM oscar_obs_overlay WHERE store_key = ?")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals("All 65 settled overlays must be folded and removed from oscar_obs_overlay",
                            0L, rs.getLong(1));
                }
            }
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM oscar_archive_generation WHERE store_key = ? AND profile = 'MONTHLY_EVENTS' AND state = 'SEALED'")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    UUID newMonthlyGenId = rs.getObject(1, UUID.class);
                    assertFalse("Defragmentation must replace prior MONTHLY_EVENTS generation ID",
                            initialMonthlyGenId.equals(newMonthlyGenId));
                    assertFalse(rs.next());
                }
            }
            assertEquals(expectedTotal, querySumRowCount(conn, table));
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        ObsFilter foldedAdjFilter = new ObsFilter.Builder()
                .withDataStreams(occStream)
                .withCQLFilter("adjudicationCode = 4 AND adjudicatedIdsCount = 1")
                .build();
        assertEquals(65L, store.countMatchingEntries(foldedAdjFilter));
        IObsData sampleFolded = store.get(julyEventObsIds.get(64));
        assertNotNull(sampleFolded);
        assertEquals("evt-defrag-64", sampleFolded.getResult().getStringValue(0));
        assertEquals(564.0, sampleFolded.getResult().getDoubleValue(3), 1e-6);
    }

    @Test
    public void parquetPrimaryModeBlocksUnretiredStagePartitionAndCompactsEventsOnlyAfterStageRetirement()
            throws Exception {
        Path archiveRoot = temporary.newFolder("parquet-primary-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("parquet-primary-spill");
        String table = uniqueTableName("compact_pp");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("occupancy", "gammaCounts", "dailyFile"));
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 401L, "occupancy");
        BigId telStream = addTelemetryStream(store, 402L, "gammaCounts");
        BigId dailyFileStream = addTelemetryStream(store, 403L, "dailyFile");

        Instant dayStart = Instant.parse("2026-08-05T00:00:00Z");
        Instant now = Instant.parse("2026-08-10T12:00:00Z");

        BigId occId = store.add(eventOccupancyObs(
                occStream, dayStart.plusSeconds(30L), dayStart.plusSeconds(30L),
                "evt-pp-1", true, false, 180.0, 2.0, -1, 0));
        BigId telId = store.add(telemetryObs(
                telStream, dayStart.plusSeconds(45L), dayStart.plusSeconds(45L), 65.0, 1.0));
        BigId dailyFileId = store.add(telemetryObs(
                dailyFileStream, dayStart.plusSeconds(50L), dayStart.plusSeconds(50L), 99.0, 0.0));
        commitStore(store);

        // Export ONLY RAW_EVENTS first -> stage child still holds unowned telemetry, so lifecycle cannot retire it yet
        // AND compaction MUST NOT touch rawEvGen!
        ArchiveGeneration rawEvGen = store.archiveWindow(
                dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(1, ChronoUnit.DAYS));
            ps.setObject(2, rawEvGen.id());
            assertEquals(1, ps.executeUpdate());
        }
        assertEquals(0, store.runCompactionCycle(now).daysCompacted());

        // Export complementary RAW_TELEMETRY (captures both gammaCounts and dailyFile) -> lifecycle retires stage child!
        ArchiveGeneration rawTelGen = store.archiveWindow(
                dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        assertEquals(2L, rawTelGen.rowCount());
        store.maintainArchiveLifecycle();
        assertTrue("Stage partitions must retire once both RAW_EVENTS and RAW_TELEMETRY verify",
                store.getImmutableStagePartitions().isEmpty());

        // Compact retired day: RAW_EVENTS -> DAILY_EVENTS, while RAW_TELEMETRY stays untouched!
        ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
        assertEquals(1, summary.daysCompacted());
        assertEquals(1, summary.deletingGenerationsDrained());

        try (Connection conn = openPg()) {
            assertEquals(1, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS).size());
            assertEquals(1, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).size());
            assertEquals(0, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY).size());
            assertEquals("SEALED", queryGenerationState(conn, rawTelGen.id()));
            assertEquals(3L, querySumRowCount(conn, table));
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        assertNotNull(store.get(occId));
        assertNotNull(store.get(telId));
        assertNotNull(store.get(dailyFileId));
        assertEquals(3L, store.getNumRecords());
    }

    @Test
    public void staleUnregisteredPartialsAreSelfHealedWhileFreshAndRegisteredPartialsSurvive()
            throws Exception {
        Path archiveRoot = temporary.newFolder("partial-sweep-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("partial-sweep-spill");
        String table = uniqueTableName("compact_part");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot, List.of("occupancy"));
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 501L, "occupancy");

        Instant dayStart = Instant.parse("2026-08-02T00:00:00Z");
        Instant now = Instant.parse("2026-08-03T12:00:00Z");
        store.add(eventOccupancyObs(occStream, dayStart.plusSeconds(30L), dayStart.plusSeconds(30L),
                "evt-reg", false, false, 10.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration registeredGen = store.archiveWindow(
                dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        store.maintainArchiveLifecycle();

        // 1. Stale (>= 2h) unregistered .partial files in an otherwise empty date/profile folder -> must be removed + dir pruned.
        Path staleDir = archiveRoot.resolve("datasets/observations/date=2026-08-01/profile=RAW_EVENTS");
        Files.createDirectories(staleDir);
        UUID staleUuid = UUID.randomUUID();
        Path staleParquet = staleDir.resolve("generation-" + staleUuid + ".parquet.partial");
        Path staleManifest = staleDir.resolve("generation-" + staleUuid + ".manifest.json.partial");
        Files.writeString(staleParquet, "interrupted-parquet");
        Files.writeString(staleManifest, "interrupted-manifest");
        FileTime threeHoursAgo = FileTime.from(now.minus(3, ChronoUnit.HOURS));
        Files.setLastModifiedTime(staleParquet, threeHoursAgo);
        Files.setLastModifiedTime(staleManifest, threeHoursAgo);

        // 2. Fresh (< 2h) unregistered .partial file -> must be preserved!
        Path freshDir = archiveRoot.resolve("datasets/observations/date=2026-08-03/profile=RAW_EVENTS");
        Files.createDirectories(freshDir);
        Path freshPartial = freshDir.resolve("generation-" + UUID.randomUUID() + ".parquet.partial");
        Files.writeString(freshPartial, "in-flight-parquet");
        Files.setLastModifiedTime(freshPartial, FileTime.from(now.minus(15, ChronoUnit.MINUTES)));

        // 3. Stale (>= 2h) .partial whose UUID IS registered in oscar_archive_generation -> must fail closed & survive!
        Path registeredDir = archiveRoot.resolve("datasets/observations/date=2026-08-02/profile=RAW_EVENTS");
        Path registeredPartial = registeredDir.resolve("generation-" + registeredGen.id() + ".parquet.partial");
        Files.writeString(registeredPartial, "registered-id-partial");
        Files.setLastModifiedTime(registeredPartial, threeHoursAgo);

        store.drainDeletingGenerations(now);

        assertFalse("Stale unregistered .parquet.partial must be swept", Files.exists(staleParquet));
        assertFalse("Stale unregistered .manifest.json.partial must be swept", Files.exists(staleManifest));
        assertFalse("Empty stale date directory must be pruned", Files.exists(staleDir.getParent()));
        assertTrue("Fresh (< 2h) .partial file must not be touched", Files.isRegularFile(freshPartial));
        assertTrue("Registered-UUID .partial file must fail closed and survive", Files.isRegularFile(registeredPartial));
    }

    @Test
    public void overlappingMultiPartSourcesClusterAndRollOverIntoStrictlyDisjointParts()
            throws Exception {
        Path archiveRoot = temporary.newFolder("cluster-rollover-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("cluster-rollover-spill");
        String table = uniqueTableName("compact_clst");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot, List.of("occupancy"));
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 601L, "occupancy");

        Instant dayD = Instant.parse("2026-08-12T00:00:00Z");
        Instant now = Instant.parse("2026-08-15T12:00:00Z");

        // Insert 30 rows in [00:00, 00:30) and export as 2 OVERLAPPING same-window RAW_EVENTS generations (maxRows=15),
        // plus 10 rows in [00:30, 01:00).
        Map<BigId, String> expectedEvents = new HashMap<>();
        for (int i = 0; i < 30; i++) {
            Instant t = dayD.plusSeconds(30L + i * 45L);
            String evt = "evt-clst-0-" + i;
            expectedEvents.put(store.add(eventOccupancyObs(occStream, t, t, evt, i % 4 == 0, false, 80.0 + i, 1.0, -1, 0)), evt);
        }
        for (int i = 0; i < 10; i++) {
            Instant t = dayD.plusSeconds(1_860L + i * 60L);
            String evt = "evt-clst-1-" + i;
            expectedEvents.put(store.add(eventOccupancyObs(occStream, t, t, evt, false, false, 20.0 + i, 1.0, -1, 0)), evt);
        }
        commitStore(store);

        config.maximumRowsPerGeneration = 15;
        ArchiveGeneration partA = store.archiveWindow(
                dayD, dayD.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        ArchiveGeneration partB = store.archiveWindow(
                dayD, dayD.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        ArchiveGeneration win1 = store.archiveWindow(
                dayD.plus(30, ChronoUnit.MINUTES), dayD.plus(60, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS)
                .orElseThrow();
        assertEquals(15L, partA.rowCount());
        assertEquals(15L, partB.rowCount());
        assertEquals(10L, win1.rowCount());
        assertEquals(2, ArchiveCompactionManager.clusterOverlappingSources(List.of(win1, partB, partA)).size());

        store.maintainArchiveLifecycle();
        assertTrue(store.getImmutableStagePartitions().isEmpty());
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(4, ChronoUnit.HOURS));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }

        // Compact with maxRows=12 -> overlapping inputs merge cleanly and roll into 4 strictly disjoint DAILY_EVENTS parts!
        config.maximumRowsPerGeneration = 12;
        ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
        assertEquals(1, summary.daysCompacted());
        assertEquals(3, summary.deletingGenerationsDrained());

        try (Connection conn = openPg()) {
            List<Path> dailyParts = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS);
            assertEquals(4, dailyParts.size());
            assertStrictlyDisjointProfileWindows(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS,
                    dayD, dayD.plus(1, ChronoUnit.DAYS), 4);
            assertEquals(40L, querySumRowCount(conn, table));
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        for (Map.Entry<BigId, String> e : expectedEvents.entrySet()) {
            IObsData obs = store.get(e.getKey());
            assertNotNull(obs);
            assertEquals(e.getValue(), obs.getResult().getStringValue(0));
        }

        // Confirm zero leaked scratch or DuckDB operation directories remain under spillRoot.
        try (Stream<Path> remaining = Files.list(spillRoot)) {
            List<String> names = remaining.map(p -> p.getFileName().toString()).toList();
            assertEquals(List.of(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME), names);
        }
    }

    // --- Helpers ---

    private static Connection openPg() throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://" + PG_HOST_PORT + "/" + PG_DB, PG_USER, PG_PASS);
    }

    private static String uniqueTableName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private static TieredArchiveConfig parquetPrimaryConfig(
            Path archiveRoot, Path spillRoot, List<String> outputNames) {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.enabled = true;
        config.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        config.rootDirectory = archiveRoot.toString();
        config.duckDbTemporaryDirectory = spillRoot.toString();
        config.duckDbMemoryLimitMegabytes = 128;
        config.duckDbThreads = 1;
        config.duckDbMaximumTemporaryMegabytes = 256;
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 10;
        config.outputNames = new ArrayList<>(outputNames);
        config.deleteHotAfterArchive = true;
        config.rollbackGraceDays = 1;
        config.maximumGenerationsPerCycle = 24;
        config.maximumVerificationsPerCycle = 24;
        config.maximumDeletionsPerCycle = 24;
        return config;
    }

    private OscarTieredObsStore openStore(String table, TieredArchiveConfig config) {
        createdStoreKeys.add(table);
        OscarTieredObsStore store = new OscarTieredObsStore(
                PG_HOST_PORT, PG_DB, PG_USER, PG_PASS, table, ID_SCOPE, IdProviderType.SEQUENTIAL, config);
        openStores.add(store);
        return store;
    }

    private static void commitStore(OscarTieredObsStore store) throws Exception {
        store.commit();
        store.getDataStreams().commit();
    }

    private static void cleanupStoreState(Connection conn, String table) throws Exception {
        String safe = ArchiveExporter.safeIdentifier(table);
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_obs_stage_owner WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_obs_stage_partition WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_obs_storage_route WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_deletion_plan WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_retention_hold WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_obs_overlay WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_tombstone WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_obs_latest WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_event_locator WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_archive_job WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (PreparedStatement ps = conn.prepareStatement("DELETE FROM oscar_archive_generation WHERE store_key = ?")) {
            ps.setString(1, safe);
            ps.executeUpdate();
        } catch (Exception ignored) {
        }
        try (Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + safe + " CASCADE");
            st.execute("DROP TABLE IF EXISTS " + safe + "_datastreams CASCADE");
        }
    }

    private static BigId addEventOccupancyStream(OscarTieredObsStore store, long sysNum, String outputName)
            throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("eventId", swe.createText().build())
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .addField("maxGammaCount", swe.createQuantity().build())
                .addField("maxNeutronCount", swe.createQuantity().build())
                .addField("adjudicationCode", swe.createCount().build())
                .addField("adjudicatedIdsCount", swe.createCount().build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData eventOccupancyObs(
            BigId stream, Instant phenTime, Instant resTime, String eventId,
            boolean gammaAlarm, boolean neutronAlarm, double maxGamma, double maxNeutron,
            int adjCode, int adjIdsCount) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("occupancy")
                .addField("eventId", swe.createText().build())
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .addField("maxGammaCount", swe.createQuantity().build())
                .addField("maxNeutronCount", swe.createQuantity().build())
                .addField("adjudicationCode", swe.createCount().build())
                .addField("adjudicatedIdsCount", swe.createCount().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setStringValue(0, eventId);
        block.setBooleanValue(1, gammaAlarm);
        block.setBooleanValue(2, neutronAlarm);
        block.setDoubleValue(3, maxGamma);
        block.setDoubleValue(4, maxNeutron);
        block.setIntValue(5, adjCode);
        block.setIntValue(6, adjIdsCount);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(phenTime)
                .withResultTime(resTime)
                .withResult(block)
                .build();
    }

    private static BigId addVehicleOcrStream(OscarTieredObsStore store, long sysNum, String outputName)
            throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("occupancyObsId", swe.createText().build())
                .addField("normalizedValue", swe.createText().build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData vehicleOcrObs(
            BigId stream, Instant phenTime, Instant resTime, String occObsId, String normValue) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("vehicleOcr")
                .addField("occupancyObsId", swe.createText().build())
                .addField("normalizedValue", swe.createText().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setStringValue(0, occObsId);
        block.setStringValue(1, normValue);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(phenTime)
                .withResultTime(resTime)
                .withResult(block)
                .build();
    }

    private static BigId addTelemetryStream(OscarTieredObsStore store, long sysNum, String outputName)
            throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("gammaGrossCount", swe.createQuantity().build())
                .addField("neutronGrossCount", swe.createQuantity().build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData telemetryObs(
            BigId stream, Instant phenTime, Instant resTime, double gammaGross, double neutronGross) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("gammaCounts")
                .addField("gammaGrossCount", swe.createQuantity().build())
                .addField("neutronGrossCount", swe.createQuantity().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setDoubleValue(0, gammaGross);
        block.setDoubleValue(1, neutronGross);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(phenTime)
                .withResultTime(resTime)
                .withResult(block)
                .build();
    }

    private static String queryGenerationState(Connection conn, UUID genId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT state FROM oscar_archive_generation WHERE id = ?")) {
            ps.setObject(1, genId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private static String queryGenerationProfile(Connection conn, UUID genId) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT profile FROM oscar_archive_generation WHERE id = ?")) {
            ps.setObject(1, genId);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getString(1);
            }
        }
    }

    private static long querySumRowCount(Connection conn, String storeKey) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT COALESCE(sum(row_count), 0) FROM oscar_archive_generation WHERE store_key = ?")) {
            ps.setString(1, storeKey);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                return rs.getLong(1);
            }
        }
    }

    private static List<Path> listActiveProfilePaths(Connection conn, String storeKey, String profile)
            throws Exception {
        List<Path> out = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT object_path FROM oscar_archive_generation " +
                        "WHERE store_key = ? AND profile = ? AND state IN ('ACTIVE', 'SEALED') " +
                        "ORDER BY window_start, window_end, id")) {
            ps.setString(1, storeKey);
            ps.setString(2, profile);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(Path.of(rs.getString(1)));
                }
            }
        }
        return out;
    }

    private static void assertStrictlyDisjointProfileWindows(
            Connection conn, String storeKey, String profile,
            Instant expectedStart, Instant expectedEnd, int expectedParts) throws Exception {
        List<Instant[]> bounds = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT window_start, window_end FROM oscar_archive_generation " +
                        "WHERE store_key = ? AND profile = ? AND state IN ('ACTIVE', 'SEALED') " +
                        "ORDER BY window_start, window_end, id")) {
            ps.setString(1, storeKey);
            ps.setString(2, profile);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    bounds.add(new Instant[]{
                            UtcJdbcTime.getInstant(rs, 1),
                            UtcJdbcTime.getInstant(rs, 2)});
                }
            }
        }
        assertEquals(expectedParts, bounds.size());
        assertEquals(expectedStart, bounds.get(0)[0]);
        assertEquals(expectedEnd, bounds.get(bounds.size() - 1)[1]);
        for (int i = 0; i < bounds.size(); i++) {
            assertTrue("Each part window must have non-empty positive width",
                    bounds.get(i)[0].isBefore(bounds.get(i)[1]));
            if (i > 0) {
                assertEquals("Adjacent rolled-over parts must be contiguous and strictly disjoint",
                        bounds.get(i - 1)[1], bounds.get(i)[0]);
            }
        }
    }

    private static void assertParquetV2AndColumnCount(Path parquetFile, int expectedColumns) throws Exception {
        try (Connection duck = DriverManager.getConnection("jdbc:duckdb:");
             Statement st = duck.createStatement()) {
            String esc = parquetFile.toString().replace("'", "''");
            try (ResultSet rs = st.executeQuery("SELECT format_version FROM parquet_file_metadata('" + esc + "')")) {
                assertTrue(rs.next());
                assertTrue("Expected Parquet V2 format_version on " + parquetFile + ", got " + rs.getString(1),
                        rs.getString(1).startsWith("2"));
            }
            int leafCols = 0;
            try (ResultSet rs = st.executeQuery(
                    "SELECT count(*) FROM parquet_schema('" + esc + "') WHERE num_children IS NULL")) {
                assertTrue(rs.next());
                leafCols = rs.getInt(1);
            }
            assertEquals("Unexpected Parquet leaf column count on " + parquetFile, expectedColumns, leafCols);
        }
    }

    private static void assertOverlappingIdBlocksExist(
            Connection conn, String storeKey, String profileA, String profileB) throws Exception {
        String sql = "SELECT count(*) FROM oscar_archive_id_block a " +
                "JOIN oscar_archive_generation ga ON ga.id = a.generation_id " +
                "JOIN oscar_archive_id_block b ON b.generation_id <> a.generation_id " +
                "JOIN oscar_archive_generation gb ON gb.id = b.generation_id " +
                "WHERE ga.store_key = ? AND gb.store_key = ? " +
                "  AND ga.state IN ('ACTIVE', 'SEALED') AND gb.state IN ('ACTIVE', 'SEALED') " +
                "  AND ga.profile = ? AND gb.profile = ? " +
                "  AND a.min_id <= b.max_id AND b.min_id <= a.max_id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, storeKey);
            ps.setString(2, storeKey);
            ps.setString(3, profileA);
            ps.setString(4, profileB);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue("Expected overlapping [min_id, max_id] blocks across " + profileA + " and " + profileB,
                        rs.getLong(1) > 0L);
            }
        }
    }

    /**
     * Mirrors the exact catalog + filesystem verification logic of
     * {@code dist/release/oscar-storage.sh archive verify} scoped to {@code storeKey}.
     */
    private static void assertArchiveVerifyClean(Connection conn, String storeKey, Path archiveRoot)
            throws Exception {
        String catalogSql =
                "WITH required(kind) AS (VALUES ('OBSERVATIONS_PARQUET'),('GENERATION_MANIFEST'),('DATASTREAM_SCHEMAS')), " +
                "required_rows AS (" +
                "  SELECT 'REQUIRED'::text row_type, g.id::text generation_id, required.kind, " +
                "         count(o.generation_id) object_count, coalesce(min(o.state),'') object_state, " +
                "         coalesce(min(o.relative_path),'') relative_path, coalesce(min(o.sha256),'') object_sha, " +
                "         coalesce(g.object_path,'') generation_object_path, " +
                "         coalesce(g.sha256,'') generation_sha, coalesce(g.manifest_path,'') generation_manifest_path " +
                "  FROM oscar_archive_generation g CROSS JOIN required " +
                "  LEFT JOIN oscar_archive_object o ON o.generation_id = g.id AND o.kind = required.kind " +
                "  WHERE g.store_key = ? AND g.state IN ('ACTIVE','SEALED') " +
                "  GROUP BY g.id, required.kind, g.object_path, g.sha256, g.manifest_path" +
                "), invalid_owner_rows AS (" +
                "  SELECT 'INVALID_OWNER'::text, g.id::text, coalesce(o.kind,''), 1::bigint, o.state, " +
                "         coalesce(o.relative_path,''), coalesce(o.sha256,''), coalesce(g.object_path,''), " +
                "         coalesce(g.sha256,''), coalesce(g.manifest_path,'') " +
                "  FROM oscar_archive_object o JOIN oscar_archive_generation g ON g.id = o.generation_id " +
                "  WHERE g.store_key = ? AND o.state = 'ACTIVE' AND g.state NOT IN ('ACTIVE','SEALED')" +
                "), integrity_rows AS (" +
                "  SELECT 'RELATIONAL'::text, g.id::text, 'ID_BLOCK_AND_SCHEMAS'::text, " +
                "         (SELECT count(*) FROM oscar_archive_id_block block WHERE block.generation_id = g.id)::bigint, " +
                "         CASE WHEN (SELECT count(*) FROM oscar_archive_id_block block WHERE block.generation_id = g.id) = 1 " +
                "                   AND EXISTS (SELECT 1 FROM oscar_archive_id_block block " +
                "                               WHERE block.generation_id = g.id AND block.min_id = g.min_id AND block.max_id = g.max_id) " +
                "              THEN 'MATCH' ELSE 'MISMATCH' END, " +
                "         (SELECT count(*)::text FROM oscar_archive_schema s WHERE s.generation_id = g.id), " +
                "         ''::text, coalesce(g.object_path,''), coalesce(g.sha256,''), coalesce(g.manifest_path,'') " +
                "  FROM oscar_archive_generation g WHERE g.store_key = ? AND g.state IN ('ACTIVE','SEALED')" +
                ") " +
                "SELECT * FROM required_rows UNION ALL SELECT * FROM invalid_owner_rows UNION ALL SELECT * FROM integrity_rows";

        try (PreparedStatement ps = conn.prepareStatement(catalogSql)) {
            ps.setString(1, storeKey);
            ps.setString(2, storeKey);
            ps.setString(3, storeKey);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rowType = rs.getString(1);
                    String genId = rs.getString(2);
                    String kind = rs.getString(3);
                    long count = rs.getLong(4);
                    String state = rs.getString(5);
                    String relative = rs.getString(6);
                    String expectedSha = rs.getString(7);
                    String genObjPath = rs.getString(8);
                    String genSha = rs.getString(9);
                    String genManifestPath = rs.getString(10);

                    if ("REQUIRED".equals(rowType)) {
                        assertEquals("Missing required object " + kind + " on " + genId, 1L, count);
                        assertEquals("ACTIVE", state);
                        assertTrue("Invalid object path grammar: " + relative, VALID_ARCHIVE_OBJECT.matcher(relative).matches());
                        if ("OBSERVATIONS_PARQUET".equals(kind)) {
                            assertEquals(genObjPath, relative);
                            assertEquals(genSha, expectedSha);
                        } else if ("GENERATION_MANIFEST".equals(kind)) {
                            assertEquals(genManifestPath, relative);
                        }
                        Path file = archiveRoot.resolve(relative);
                        assertTrue("Missing archive file on disk: " + file, Files.isRegularFile(file, LinkOption.NOFOLLOW_LINKS));
                        assertEquals("Checksum mismatch on " + relative, expectedSha, ArchiveExporter.sha256(file));
                    } else if ("INVALID_OWNER".equals(rowType)) {
                        throw new AssertionError("Active archive object belongs to non-visible generation " + genId + " kind=" + kind);
                    } else if ("RELATIONAL".equals(rowType)) {
                        assertEquals("Expected 1 id_block on " + genId, 1L, count);
                        assertEquals("ID block bounds mismatch on " + genId, "MATCH", state);
                        assertTrue("Expected >= 1 schema rows on " + genId, Integer.parseInt(relative) >= 1);
                    }
                }
            }
        }

        Set<String> registeredObjects = new HashSet<>();
        try (PreparedStatement ps = conn.prepareStatement(
                "SELECT o.relative_path FROM oscar_archive_object o " +
                        "JOIN oscar_archive_generation g ON g.id = o.generation_id " +
                        "WHERE g.store_key = ? AND o.state IN ('ACTIVE', 'DELETING')")) {
            ps.setString(1, storeKey);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    String rel = rs.getString(1);
                    assertTrue("Invalid catalog relative path: " + rel, VALID_ARCHIVE_OBJECT.matcher(rel).matches());
                    registeredObjects.add(rel);
                }
            }
        }

        Path realRoot = archiveRoot.toRealPath();
        try (Stream<Path> walk = Files.walk(realRoot)) {
            for (Path entry : walk.toList()) {
                if (entry.equals(realRoot))
                    continue;
                assertFalse("Symlink forbidden inside archiveRoot: " + entry, Files.isSymbolicLink(entry));
                String rel = realRoot.relativize(entry).toString().replace('\\', '/');
                if (Files.isDirectory(entry, LinkOption.NOFOLLOW_LINKS)) {
                    assertTrue("Unapproved archive directory layout: " + rel, VALID_ARCHIVE_DIR.matcher(rel).matches());
                } else {
                    assertTrue("Unapproved archive file layout: " + rel, VALID_ARCHIVE_OBJECT.matcher(rel).matches());
                    assertTrue("Unregistered orphan file on disk: " + rel, registeredObjects.contains(rel));
                }
            }
        }
    }

    @Test
    public void extendedFaultAndAlarmCategorySummariesServeFromCatalogAndBackfillPreUpgradeNulls() throws Exception {
        Path archiveRoot = temporary.newFolder("extended-summary-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("extended-summary-spill");
        String table = uniqueTableName("ext_summary");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("alarm", "gammaCounts", "neutronCounts", "occupancyStatus"));
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId gammaStream = addAlarmStateStream(store, 601L, "gammaCounts",
                new String[]{"Background", "Alarm", "Fault - Gamma High", "Fault - Gamma Low"});
        BigId neutronStream = addAlarmStateStream(store, 602L, "neutronCounts",
                new String[]{"Background", "Alarm", "Fault - Neutron High", "Fault - Neutron Low"});
        BigId occStatusStream = addAlarmStateStream(store, 603L, "occupancyStatus",
                new String[]{"Occupied", "Fault - Extended Occupancy"});
        BigId rs350AlarmStream = addAlarmCategoryStream(store, 604L, "alarm");

        Instant day = Instant.parse("2026-08-22T00:00:00Z");
        Instant now = Instant.parse("2026-08-25T12:00:00Z");

        store.add(alarmStateObs(gammaStream, day.plusSeconds(30L), "Background"));
        store.add(alarmStateObs(gammaStream, day.plusSeconds(60L), "Fault - Gamma High"));
        store.add(alarmStateObs(gammaStream, day.plusSeconds(90L), "Fault - Gamma Low"));

        store.add(alarmStateObs(neutronStream, day.plusSeconds(30L), "Background"));
        store.add(alarmStateObs(neutronStream, day.plusSeconds(60L), "Fault - Neutron High"));
        store.add(alarmStateObs(neutronStream, day.plusSeconds(90L), "Fault - Neutron High"));
        store.add(alarmStateObs(neutronStream, day.plusSeconds(120L), "Fault - Neutron Low"));

        store.add(alarmStateObs(occStatusStream, day.plusSeconds(60L), "Fault - Extended Occupancy"));

        store.add(alarmCategoryObs(rs350AlarmStream, day.plusSeconds(45L), "Gamma"));
        store.add(alarmCategoryObs(rs350AlarmStream, day.plusSeconds(75L), "Gamma"));
        store.add(alarmCategoryObs(rs350AlarmStream, day.plusSeconds(105L), "Neutron"));
        commitStore(store);

        // Split-at-export writes RS-350 alarm -> RAW_EVENTS and gammaCounts/neutronCounts/occupancyStatus -> RAW_TELEMETRY.
        ArchiveGeneration rawEvents = store.archiveWindow(
                day, day.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        store.archiveWindow(day, day.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        store.maintainArchiveLifecycle();
        try (Connection conn = openPg();
             PreparedStatement backdate = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
            UtcJdbcTime.setInstant(backdate, 1, now.minus(4, ChronoUnit.HOURS));
            backdate.setObject(2, rawEvents.id());
            backdate.executeUpdate();
        }
        ArchiveCompactionManager.CompactionCycleSummary compactSummary = store.runCompactionCycle(now);
        assertEquals(1, compactSummary.daysCompacted());

        ObsFilter neutronHighFilter = new ObsFilter.Builder()
                .withDataStreams(neutronStream, rs350AlarmStream)
                .withCQLFilter("alarmState = 'Fault - Neutron High'")
                .build();
        ObsFilter allFaultsFilter = new ObsFilter.Builder()
                .withDataStreams(gammaStream, neutronStream, occStatusStream, rs350AlarmStream)
                .withCQLFilter("alarmState = 'Fault - Neutron High' OR alarmState = 'Fault - Gamma Low' OR alarmState = 'Fault - Gamma High' OR alarmState = 'Fault - Extended Occupancy'")
                .build();
        ObsFilter rs350GammaFilter = new ObsFilter.Builder()
                .withDataStreams(rs350AlarmStream)
                .withCQLFilter("alarmCategoryCode = 'Gamma'")
                .build();
        ObsFilter rs350NeutronFilter = new ObsFilter.Builder()
                .withDataStreams(rs350AlarmStream)
                .withCQLFilter("alarmCategoryCode = 'Neutron'")
                .build();

        assertEquals(2L, store.countMatchingEntries(neutronHighFilter));
        assertEquals(5L, store.countMatchingEntries(allFaultsFilter));
        assertEquals(2L, store.countMatchingEntries(rs350GammaFilter));
        assertEquals(1L, store.countMatchingEntries(rs350NeutronFilter));

        // Catalog summary plan must answer all four queries purely from PostgreSQL with 0 remaining DuckDB candidates!
        List<ArchiveGeneration> allCold = store.getArchiveCatalog().candidates(
                day, day.plus(1, ChronoUnit.DAYS), null, null, null, null);
        assertEquals(2, allCold.size()); // 1 DAILY_EVENTS + 1 RAW_TELEMETRY
        var nhPlan = store.getArchiveCatalog().summarizeColdCandidates(
                allCold, null, null, Set.of(neutronStream.getIdAsLong(), rs350AlarmStream.getIdAsLong()),
                Set.of(), org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.summaryCounterSpec(
                        neutronHighFilter.getCQLFilter()).orElseThrow());
        assertEquals(2L, nhPlan.summarizedCount());
        assertTrue("Expected zero DuckDB candidates for neutronFaultCQL", nhPlan.remainingCandidates().isEmpty());

        var faultPlan = store.getArchiveCatalog().summarizeColdCandidates(
                allCold, null, null, Set.of(gammaStream.getIdAsLong(), neutronStream.getIdAsLong(),
                        occStatusStream.getIdAsLong(), rs350AlarmStream.getIdAsLong()),
                Set.of(), org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.summaryCounterSpec(
                        allFaultsFilter.getCQLFilter()).orElseThrow());
        assertEquals(5L, faultPlan.summarizedCount());
        assertTrue("Expected zero DuckDB candidates for faultCQL", faultPlan.remainingCandidates().isEmpty());

        // Simulate pre-upgrade NULL extended columns:
        // 1) Counts MUST stay 100% exact via DuckDB fallback (never falsely pruned to 0).
        // 2) migrate() MUST zero-fill ineligible schemas (gammaCounts, occupancyStatus) while keeping eligible streams NULL.
        // 3) backfillExtendedSummaryCountersBatch MUST populate all remaining eligible rows with exact counts.
        try (Connection conn = openPg(); Statement stmt = conn.createStatement()) {
            stmt.executeUpdate("UPDATE oscar_archive_schema SET " +
                    "summary_version = NULL, neutron_high_fault_count = NULL, alarm_category_gamma_count = NULL, alarm_category_neutron_count = NULL");
        }
        assertEquals(2L, store.countMatchingEntries(neutronHighFilter));
        assertEquals(5L, store.countMatchingEntries(allFaultsFilter));
        assertEquals(2L, store.countMatchingEntries(rs350GammaFilter));
        assertEquals(1L, store.countMatchingEntries(rs350NeutronFilter));

        store.getArchiveCatalog().migrate();
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT datastream_id, neutron_high_fault_count, alarm_category_gamma_count, alarm_category_neutron_count " +
                             "FROM oscar_archive_schema WHERE generation_id IN (SELECT id FROM oscar_archive_generation WHERE store_key = ? AND state IN ('ACTIVE','SEALED'))")) {
            ps.setString(1, table);
            Map<Long, Long[]> afterMigrate = new HashMap<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    afterMigrate.put(rs.getLong(1), new Long[]{
                            rs.getObject(2, Long.class),
                            rs.getObject(3, Long.class),
                            rs.getObject(4, Long.class)
                    });
                }
            }
            // gammaCounts & occupancyStatus have Gamma/ExtendedOccupancy schemas without Neutron High or alarmCategoryCode -> zeroed immediately
            assertEquals(Long.valueOf(0L), afterMigrate.get(gammaStream.getIdAsLong())[0]);
            assertEquals(Long.valueOf(0L), afterMigrate.get(gammaStream.getIdAsLong())[1]);
            assertEquals(Long.valueOf(0L), afterMigrate.get(occStatusStream.getIdAsLong())[0]);
            // neutronCounts has Neutron High allowedValue -> stays NULL until DuckDB backfill; alarmCategoryCode zeroed immediately
            assertEquals(null, afterMigrate.get(neutronStream.getIdAsLong())[0]);
            assertEquals(Long.valueOf(0L), afterMigrate.get(neutronStream.getIdAsLong())[1]);
            // rs350 alarm has alarmCategoryCode -> alarmCategory columns stay NULL until DuckDB backfill; neutron_high zeroed immediately
            assertEquals(Long.valueOf(0L), afterMigrate.get(rs350AlarmStream.getIdAsLong())[0]);
            assertEquals(null, afterMigrate.get(rs350AlarmStream.getIdAsLong())[1]);
        }

        int backfilledGens = store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 32);
        assertEquals(2, backfilledGens);
        assertEquals(0, store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 32));

        var postBackfillPlan = store.getArchiveCatalog().summarizeColdCandidates(
                allCold, null, null, Set.of(gammaStream.getIdAsLong(), neutronStream.getIdAsLong(),
                        occStatusStream.getIdAsLong(), rs350AlarmStream.getIdAsLong()),
                Set.of(), org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.summaryCounterSpec(
                        allFaultsFilter.getCQLFilter()).orElseThrow());
        assertEquals(5L, postBackfillPlan.summarizedCount());
        assertTrue(postBackfillPlan.remainingCandidates().isEmpty());

        List<ArchiveGeneration> rs350Cold = store.getArchiveCatalog().candidates(
                day, day.plus(1, ChronoUnit.DAYS), null, null, null, Set.of(rs350AlarmStream.getIdAsLong()));
        assertEquals(1, rs350Cold.size()); // Only DAILY_EVENTS contains rs350AlarmStream; RAW_TELEMETRY is excluded
        var postRsGammaPlan = store.getArchiveCatalog().summarizeColdCandidates(
                rs350Cold, null, null, Set.of(rs350AlarmStream.getIdAsLong()),
                Set.of(), org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.summaryCounterSpec(
                        rs350GammaFilter.getCQLFilter()).orElseThrow());
        assertEquals(2L, postRsGammaPlan.summarizedCount());
        assertTrue(postRsGammaPlan.remainingCandidates().isEmpty());
    }

    @Test
    public void tenYearScalePushdownsServeStatusFaultOrFromSummariesScopeEventProfilesAndSliceUnfilteredLimit()
            throws Exception {
        String table = uniqueTableName("scale10y");
        Path archiveRoot = temporary.newFolder("archive-scale10y").toPath();
        Path duckTmp = archiveRoot.resolveSibling("ducktmp-scale10y");
        TieredArchiveConfig cfg = parquetPrimaryConfig(archiveRoot, duckTmp,
                List.of("occupancy", "tamper", "alarm", "gammaCounts", "neutronCounts"));
        OscarTieredObsStore store = openStore(table, cfg);

        BigId occStream = addEventOccupancyStream(store, 201L, "occupancy");
        BigId tamperStream = addTamperStream(store, 201L);
        BigId gammaStream = addAlarmStateStream(store, 201L, "gammaCounts",
                new String[]{"Background", "Alarm", "Fault - Gamma High", "Fault - Gamma Low"});
        BigId neutronStream = addAlarmStateStream(store, 201L, "neutronCounts",
                new String[]{"Background", "Alarm", "Fault - Neutron High"});
        BigId combinedStream = addCombinedTamperAndFaultStream(store, 202L, "alarm");

        // Export 3 separate UTC windows producing multiple RAW_EVENTS and RAW_TELEMETRY generations.
        Instant w1 = Instant.parse("2026-08-01T00:00:00Z");
        Instant w2 = Instant.parse("2026-08-01T00:30:00Z");
        Instant w3 = Instant.parse("2026-08-01T01:00:00Z");
        Instant w4 = Instant.parse("2026-08-01T01:30:00Z");
        for (int i = 0; i < 6; i++) {
            Instant t1 = w1.plusSeconds(300L + i * 10L);
            Instant t2 = w2.plusSeconds(300L + i * 10L);
            Instant t3 = w3.plusSeconds(300L + i * 10L);
            store.add(eventOccupancyObs(occStream, t1, t1, "ev-w1-" + i, true, false, 100, 10, 0, 0));
            store.add(eventOccupancyObs(occStream, t2, t2, "ev-w2-" + i, false, true, 10, 100, 0, 0));
            store.add(eventOccupancyObs(occStream, t3, t3, "ev-w3-" + i, false, false, 10, 10, 0, 0));
        }
        store.add(tamperObs(tamperStream, w1.plusSeconds(60), true));
        store.add(tamperObs(tamperStream, w1.plusSeconds(120), false));
        store.add(tamperObs(tamperStream, w2.plusSeconds(60), true));
        store.add(alarmStateObs(gammaStream, w1.plusSeconds(90), "Fault - Gamma High"));
        store.add(alarmStateObs(gammaStream, w1.plusSeconds(150), "Background"));
        store.add(alarmStateObs(neutronStream, w2.plusSeconds(90), "Fault - Neutron High"));
        store.add(alarmStateObs(neutronStream, w3.plusSeconds(90), "Background"));
        // Combined row matching BOTH tamperStatus=true AND Fault - Gamma High in one stream -> must fall back to DuckDB
        // to avoid double-counting.
        store.add(combinedTamperFaultObs(combinedStream, w3.plusSeconds(180), true, "Fault - Gamma High"));
        commitStore(store);

        store.archiveWindow(w1, w2, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        store.archiveWindow(w1, w2, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        store.archiveWindow(w2, w3, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        store.archiveWindow(w2, w3, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        store.archiveWindow(w3, w4, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        store.archiveWindow(w3, w4, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        store.maintainArchiveLifecycle();

        List<ArchiveGeneration> allGens = store.getArchiveCatalog().candidates(
                Instant.parse("2026-08-01T00:00:00Z"), Instant.parse("2026-08-01T02:00:00Z"), null);
        long rawEventsCount = allGens.stream().filter(g -> TieredArchiveConfig.PROFILE_RAW_EVENTS.equals(g.profile())).count();
        long rawTelemetryCount = allGens.stream().filter(g -> TieredArchiveConfig.PROFILE_RAW_TELEMETRY.equals(g.profile())).count();
        assertEquals(3L, rawEventsCount);
        assertEquals(3L, rawTelemetryCount);

        // 1) Profile-scoped catalog pruning: event-only streams restrict to EVENT_QUERY_PROFILES; mixed/telemetry return null.
        assertEquals(TieredArchiveConfig.EVENT_QUERY_PROFILES,
                store.resolveAllowedProfilesForDataStreams(Set.of(occStream.getIdAsLong(), tamperStream.getIdAsLong())));
        assertEquals(null, store.resolveAllowedProfilesForDataStreams(Set.of(gammaStream.getIdAsLong())));
        assertEquals(null, store.resolveAllowedProfilesForDataStreams(
                Set.of(tamperStream.getIdAsLong(), gammaStream.getIdAsLong())));
        List<ArchiveGeneration> eventCandidates = store.getArchiveCatalog().candidates(
                Instant.parse("2026-08-01T00:00:00Z"), Instant.parse("2026-08-01T02:00:00Z"),
                null, null, null, Set.of(occStream.getIdAsLong()), null, null,
                TieredArchiveConfig.EVENT_QUERY_PROFILES);
        assertEquals(3, eventCandidates.size());
        assertTrue(eventCandidates.stream().allMatch(g -> TieredArchiveConfig.PROFILE_RAW_EVENTS.equals(g.profile())));

        // 2) StatusTable tamperStatus=true OR alarmState='Fault - ...' answers 100% from PostgreSQL summaries
        //    when tamper and fault live on separate streams (0 DuckDB candidates remaining).
        String statusCql = "tamperStatus=true OR alarmState='Fault - Neutron High' OR " +
                "alarmState='Fault - Gamma High' OR alarmState='Fault - Gamma Low' OR " +
                "alarmState='Fault - Extended Occupancy'";
        ObsFilter separateStreamsStatusFilter = new ObsFilter.Builder()
                .withDataStreams(tamperStream, gammaStream, neutronStream)
                .withCQLFilter(statusCql)
                .build();
        assertEquals(4L, store.countMatchingEntries(separateStreamsStatusFilter));
        var separateSpec = org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator
                .summaryCounterSpec(separateStreamsStatusFilter.getCQLFilter()).orElseThrow();
        Set<Long> statusStreams = Set.of(
                tamperStream.getIdAsLong(), gammaStream.getIdAsLong(), neutronStream.getIdAsLong());
        List<ArchiveGeneration> statusCandidates = store.getArchiveCatalog().candidates(
                Instant.parse("2026-08-01T00:00:00Z"), Instant.parse("2026-08-01T02:00:00Z"),
                null, null, null, statusStreams);
        var separatePlan = store.getArchiveCatalog().summarizeColdCandidates(
                statusCandidates, null, null, statusStreams, Set.of(), separateSpec);
        assertEquals(4L, separatePlan.summarizedCount());
        assertTrue("Separate tamper & detector fault streams must answer purely from PostgreSQL summaries",
                separatePlan.remainingCandidates().isEmpty());

        // 3) If a single stream contains BOTH tamperStatus=true AND Fault - Gamma High in one row, exactReadyExpr
        //    falls back to DuckDB instead of double-counting (logical count MUST be 1, not 2!).
        ObsFilter combinedStatusFilter = new ObsFilter.Builder()
                .withDataStreams(combinedStream)
                .withCQLFilter(statusCql)
                .build();
        assertEquals(1L, store.countMatchingEntries(combinedStatusFilter));
        List<ArchiveGeneration> combinedCandidates = store.getArchiveCatalog().candidates(
                Instant.parse("2026-08-01T00:00:00Z"), Instant.parse("2026-08-01T02:00:00Z"),
                null, null, null, Set.of(combinedStream.getIdAsLong()));
        var combinedPlan = store.getArchiveCatalog().summarizeColdCandidates(
                combinedCandidates, null, null, Set.of(combinedStream.getIdAsLong()),
                Set.of(), separateSpec);
        assertEquals(0L, combinedPlan.summarizedCount());
        assertEquals(1, combinedPlan.remainingCandidates().size());

        // 4) Plain unfiltered bounded query (GET /observations?limit=5) slices primaryCandidates to ONLY the
        //    first generation (1 Parquet file opened instead of all 3).
        store.getArchiveReader().resetMaxBatchFileCountSeenForTesting();
        ObsFilter boundedPlain = new ObsFilter.Builder()
                .withDataStreams(occStream)
                .withLimit(5L)
                .build();
        long fetchedCount;
        try (var stream = store.selectEntries(boundedPlain)) {
            fetchedCount = stream.count();
        }
        assertEquals(5L, fetchedCount);
        assertEquals("Plain ?limit=5 query must open only 1 Parquet file when first generation holds >= 5 rows",
                1, store.getArchiveReader().maxBatchFileCountSeenForTesting());
    }

    @Test
    public void largeArchiveUpgradeDefersSchemaBackfillFromMigrateAndDrainsIncrementallyNewestFirst() throws Exception {
        String table = uniqueTableName("upgdrain");
        Path archiveRoot = temporary.newFolder("archive-upgdrain").toPath();
        Path duckTmp = archiveRoot.resolveSibling("ducktmp-upgdrain");
        TieredArchiveConfig cfg = parquetPrimaryConfig(archiveRoot, duckTmp,
                List.of("occupancy", "gammaCounts", "neutronCounts"));
        OscarTieredObsStore store = openStore(table, cfg);

        BigId occStream = addEventOccupancyStream(store, 301L, "occupancy");
        BigId gammaStream = addAlarmStateStream(store, 301L, "gammaCounts",
                new String[]{"Background", "Alarm", "Fault - Gamma High", "Fault - Gamma Low"});
        BigId neutronStream = addAlarmStateStream(store, 301L, "neutronCounts",
                new String[]{"Background", "Alarm", "Fault - Neutron High"});

        // Export 6 half-hour windows (12 generations: 6 RAW_EVENTS + 6 RAW_TELEMETRY).
        Instant base = Instant.parse("2026-08-05T00:00:00Z");
        for (int i = 0; i < 6; i++) {
            Instant winStart = base.plusSeconds(i * 1800L);
            Instant winEnd = winStart.plusSeconds(1800L);
            Instant ts = winStart.plusSeconds(60L);
            store.add(eventOccupancyObs(occStream, ts, ts, "occ-upg-" + i, i % 2 == 0, false, 150, 2, 0, 0));
            store.add(alarmStateObs(neutronStream, ts, i == 5 ? "Fault - Neutron High" : "Background"));
            store.add(alarmStateObs(gammaStream, ts, "Background"));
            commitStore(store);
            store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
            store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        }
        store.maintainArchiveLifecycle();

        // Simulate a pre-v3.12.0 upgraded catalog where newly added columns are NULL across all 12 generations.
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE oscar_archive_schema SET " +
                             "summary_version = NULL, " +
                             "unadjudicated_gamma_alarm_count = NULL, " +
                             "unadjudicated_neutron_alarm_count = NULL, " +
                             "unadjudicated_gamma_neutron_alarm_count = NULL, " +
                             "adjudicated_gamma_alarm_count = NULL, " +
                             "adjudicated_neutron_alarm_count = NULL, " +
                             "adjudicated_gamma_neutron_alarm_count = NULL, " +
                             "neutron_high_fault_count = NULL, " +
                             "alarm_category_gamma_count = NULL, " +
                             "alarm_category_neutron_count = NULL " +
                             "WHERE generation_id IN (SELECT id FROM oscar_archive_generation WHERE store_key = ?)")) {
            ps.setString(1, table);
            ps.executeUpdate();
        }

        // With archive exceeding inline threshold (4), migrate() must NOT run a synchronous full-table UPDATE.
        store.getArchiveCatalog().setInlineMigrationMaxGenerationsForTesting(4);
        assertFalse(store.getArchiveCatalog().hasAtMostGenerations(4));
        store.getArchiveCatalog().migrate();
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FROM oscar_archive_schema s " +
                             "JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                             "WHERE g.store_key = ? AND s.alarm_category_gamma_count IS NULL")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals("migrate() must defer full-table schema backfill when archive exceeds inline threshold",
                        18L, rs.getLong(1));
            }
        }

        // Incremental SQL backfill drains in bounded batches without infinite-looping on eligible neutron streams.
        int firstBatch = store.getArchiveCatalog().backfillPendingSchemaUpgradeColumnsBatch(5);
        assertEquals(5, firstBatch);
        int drained = firstBatch;
        int rounds = 0;
        while (rounds++ < 10) {
            int n = store.getArchiveCatalog().backfillPendingSchemaUpgradeColumnsBatch(5);
            if (n == 0)
                break;
            drained += n;
        }
        assertEquals(12, drained);
        assertEquals("SQL upgrade backfill must converge to 0 without looping on DuckDB-eligible streams",
                0, store.getArchiveCatalog().backfillPendingSchemaUpgradeColumnsBatch(5));

        // Ineligible schemas (occupancy, gamma) are zero-filled in SQL; eligible neutron stream remains NULL for DuckDB.
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT count(*) FILTER (WHERE s.alarm_category_gamma_count IS NULL), " +
                             "count(*) FILTER (WHERE s.neutron_high_fault_count IS NULL AND s.datastream_id = ?), " +
                             "count(*) FILTER (WHERE s.neutron_high_fault_count IS NULL AND s.datastream_id = ?) " +
                             "FROM oscar_archive_schema s " +
                             "JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                             "WHERE g.store_key = ?")) {
            ps.setLong(1, gammaStream.getIdAsLong());
            ps.setLong(2, neutronStream.getIdAsLong());
            ps.setString(3, table);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(0L, rs.getLong(1));
                assertEquals(0L, rs.getLong(2));
                assertEquals(6L, rs.getLong(3));
            }
        }

        // Targeted DuckDB backfill fills the remaining eligible neutron streams and preserves exact fault count.
        assertEquals(6, store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 32));
        assertEquals(0, store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 32));
        ObsFilter neutronHighFault = new ObsFilter.Builder()
                .withDataStreams(neutronStream)
                .withCQLFilter("alarmState='Fault - Neutron High'")
                .build();
        assertEquals(1L, store.countMatchingEntries(neutronHighFault));
    }

    @Test
    public void singlePassActivationWritesCompactV2SparseSummariesWithoutDeadTuplesOrBackfillChurn()
            throws Exception {
        String table = uniqueTableName("v2compact");
        Path archiveRoot = temporary.newFolder("archive-v2compact").toPath();
        Path duckTmp = archiveRoot.resolveSibling("ducktmp-v2compact");
        TieredArchiveConfig cfg = parquetPrimaryConfig(archiveRoot, duckTmp,
                List.of("occupancy", "gammaCounts"));
        OscarTieredObsStore store = openStore(table, cfg);
        BigId occStream = addEventOccupancyStream(store, 401L, "occupancy");
        BigId gammaStream = addAlarmStateStream(store, 401L, "gammaCounts",
                new String[]{"Background", "Fault - Gamma High"});

        Instant winStart = Instant.parse("2026-11-10T00:00:00Z");
        Instant winEnd = winStart.plus(30, ChronoUnit.MINUTES);
        Instant ts = winStart.plusSeconds(10);
        store.add(eventOccupancyObs(occStream, ts, ts, "occ-v2-1", true, false, 150, 2, 0, 0));
        store.add(alarmStateObs(gammaStream, winStart.plusSeconds(20), "Background"));
        commitStore(store);

        ArchiveGeneration rawEvGen = store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_EVENTS)
                .orElseThrow();
        ArchiveGeneration rawTelGen = store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_TELEMETRY)
                .orElseThrow();

        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT s.datastream_id, s.summary_populated, s.summary_version, s.row_count, " +
                             "s.gamma_alarm_count, s.neutron_alarm_count, s.neutron_high_fault_count, " +
                             "s.alarm_category_gamma_count " +
                             "FROM oscar_archive_schema s WHERE s.generation_id IN (?, ?)")) {
            ps.setObject(1, rawEvGen.id());
            ps.setObject(2, rawTelGen.id());
            int rows = 0;
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    rows++;
                    long dsId = rs.getLong(1);
                    assertTrue("summary_populated must be true on birth", rs.getBoolean(2));
                    assertEquals("summary_version must be 2 on birth",
                            Short.valueOf((short) 2), rs.getObject(3, Short.class));
                    assertEquals(1L, rs.getLong(4));
                    if (dsId == occStream.getIdAsLong()) {
                        assertEquals("Non-zero gamma_alarm_count stored as exact 1",
                                Long.valueOf(1L), rs.getObject(5, Long.class));
                        assertEquals("Zero neutron_alarm_count stored sparsely as SQL NULL",
                                null, rs.getObject(6, Long.class));
                        assertEquals("Zero neutron_high_fault_count stored sparsely as SQL NULL",
                                null, rs.getObject(7, Long.class));
                        assertEquals("Zero alarm_category_gamma_count stored sparsely as SQL NULL",
                                null, rs.getObject(8, Long.class));
                    } else {
                        assertEquals("Zero gamma_alarm_count on telemetry stream stored sparsely as SQL NULL",
                                null, rs.getObject(5, Long.class));
                        assertEquals("Zero neutron_high_fault_count stored sparsely as SQL NULL",
                                null, rs.getObject(7, Long.class));
                    }
                }
            }
            assertEquals(2, rows);
        }

        // Compact V2 NULL zeros must NEVER trigger any legacy upgrade/summary/extended backfill.
        assertEquals(0, store.getArchiveCatalog().backfillPendingSchemaUpgradeColumnsBatch(16));
        assertEquals(0, store.getArchiveCatalog().backfillGenerationSummariesBatch(archiveRoot, 16));
        assertEquals(0, store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 16));

        // Exact CQL summary queries must evaluate compact V2 NULL zeros as exact 0 with ZERO DuckDB opens.
        ObsFilter neutronHighFilter = new ObsFilter.Builder()
                .withCQLFilter("alarmState = 'Fault - Neutron High'")
                .build();
        ObsFilter gammaUnadjFilter = new ObsFilter.Builder()
                .withCQLFilter("gammaAlarm = true AND neutronAlarm = false AND adjudicatedIdsCount = 0")
                .build();
        ObsFilter neutronUnadjFilter = new ObsFilter.Builder()
                .withCQLFilter("gammaAlarm = false AND neutronAlarm = true AND adjudicatedIdsCount = 0")
                .build();
        assertEquals(0L, store.countMatchingEntries(neutronHighFilter));
        assertEquals(1L, store.countMatchingEntries(gammaUnadjFilter));
        assertEquals(0L, store.countMatchingEntries(neutronUnadjFilter));

        // Bloat-gated overlay maintenance inside reindexMutableTables completes cleanly.
        store.getArchiveCatalog().reindexMutableTables(table, table + "_datastreams");
    }

    @Test
    public void summaryBackfillFlagsMarkCleanLegacyGenerationsAndAvoidRepeatSchemaScans() throws Exception {
        String table = uniqueTableName("bfflags");
        Path archiveRoot = temporary.newFolder("archive-bfflags").toPath();
        Path duckTmp = archiveRoot.resolveSibling("ducktmp-bfflags");
        TieredArchiveConfig cfg = parquetPrimaryConfig(archiveRoot, duckTmp,
                List.of("occupancy", "gammaCounts"));
        OscarTieredObsStore store = openStore(table, cfg);
        BigId occStream = addEventOccupancyStream(store, 411L, "occupancy");
        BigId gammaStream = addAlarmStateStream(store, 411L, "gammaCounts",
                new String[]{"Background", "Fault - Gamma High"});

        Instant winStart = Instant.parse("2026-11-12T00:00:00Z");
        Instant winEnd = winStart.plus(30, ChronoUnit.MINUTES);
        store.add(eventOccupancyObs(occStream, winStart.plusSeconds(10), winStart.plusSeconds(10),
                "occ-flag-1", true, false, 150, 2, 0, 0));
        store.add(alarmStateObs(gammaStream, winStart.plusSeconds(20), "Background"));
        commitStore(store);

        ArchiveGeneration evGen = store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_EVENTS)
                .orElseThrow();
        ArchiveGeneration telGen = store.archiveWindow(winStart, winEnd, TieredArchiveConfig.PROFILE_RAW_TELEMETRY)
                .orElseThrow();

        // Newly activated compact V2 generations are born with all 3 backfill flags set (7).
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT summary_backfill_flags FROM oscar_archive_generation WHERE id IN (?, ?)")) {
            ps.setObject(1, evGen.id());
            ps.setObject(2, telGen.id());
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    assertEquals(7, rs.getShort(1));
                    count++;
                }
                assertEquals(2, count);
            }
        }

        // Simulate a legacy already-clean upgraded site where flags start at 0 on oscar_archive_generation.
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE oscar_archive_generation SET summary_backfill_flags = 0 WHERE store_key = ?")) {
            ps.setString(1, table);
            assertEquals(2, ps.executeUpdate());
        }

        // One bounded check pass confirms 0 updates are needed AND marks all 3 flags (7) so future calls never scan oscar_archive_schema.
        assertEquals(0, store.getArchiveCatalog().backfillPendingSchemaUpgradeColumnsBatch(128));
        assertEquals(0, store.getArchiveCatalog().backfillGenerationSummariesBatch(archiveRoot, 64));
        assertEquals(0, store.getArchiveCatalog().backfillExtendedSummaryCountersBatch(archiveRoot, 32));

        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "SELECT summary_backfill_flags FROM oscar_archive_generation WHERE store_key = ?")) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                int count = 0;
                while (rs.next()) {
                    assertEquals(7, rs.getShort(1));
                    count++;
                }
                assertEquals(2, count);
            }
        }
    }

    @Test
    public void telemetryStoragePressureRetiresOldestUnheldRawTelemetryOnlyAndPreservesEventsHoldsAndLatest()
            throws Exception {
        String table = uniqueTableName("telpress");
        Path archiveRoot = temporary.newFolder("archive-telpress").toPath();
        Path duckTmp = archiveRoot.resolveSibling("ducktmp-telpress");
        TieredArchiveConfig cfg = parquetPrimaryConfig(archiveRoot, duckTmp,
                List.of("occupancy", "gammaCounts"));
        cfg.telemetryPressureEnabled = true;
        cfg.telemetryPressureTriggerUsagePercent = 85;
        cfg.telemetryPressureTargetUsagePercent = 80;
        cfg.telemetryPressureMinimumRetentionDays = 365;
        cfg.telemetryPressureCriticalUsagePercent = 92;
        cfg.telemetryPressureCriticalMinimumRetentionDays = 30;
        cfg.compactionUnlinkGraceMinutes = 15;
        OscarTieredObsStore store = openStore(table, cfg);

        BigId occStream = addEventOccupancyStream(store, 421L, "occupancy");
        BigId gammaStream = addAlarmStateStream(store, 421L, "gammaCounts",
                new String[]{"Background", "Fault - Gamma High"});

        Instant now = Instant.parse("2026-11-28T00:00:00Z");
        Instant oldUnheldStart = now.minus(400, ChronoUnit.DAYS);
        Instant oldHeldStart = now.minus(399, ChronoUnit.DAYS);
        Instant midStart = now.minus(60, ChronoUnit.DAYS);
        Instant recentStart = now.minus(5, ChronoUnit.DAYS);

        ArchiveGeneration[] telGens = new ArchiveGeneration[4];
        ArchiveGeneration[] evGens = new ArchiveGeneration[4];
        Instant[] starts = {oldUnheldStart, oldHeldStart, midStart, recentStart};
        for (int i = 0; i < starts.length; i++) {
            Instant ws = starts[i];
            Instant we = ws.plus(30, ChronoUnit.MINUTES);
            store.add(eventOccupancyObs(occStream, ws.plusSeconds(10), ws.plusSeconds(10),
                    "occ-tp-" + i, true, false, 150, 2, 0, 0));
            store.add(alarmStateObs(gammaStream, ws.plusSeconds(20), "Background"));
            commitStore(store);
            evGens[i] = store.archiveWindow(ws, we, TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
            telGens[i] = store.archiveWindow(ws, we, TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();
        }
        store.maintainArchiveLifecycle();

        // Place an active ARCHIVE_GENERATION retention hold on telGens[1] (399 days old).
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "INSERT INTO oscar_retention_hold(id, store_key, id_scope, subject_type, subject_id, reason, created_by, resolution_mode) " +
                             "VALUES (?, ?, ?, 'ARCHIVE_GENERATION', ?, 'Legal hold test', 'tester', 'GENERATION_EXACT')")) {
            ps.setObject(1, UUID.randomUUID());
            ps.setString(2, table);
            ps.setInt(3, ID_SCOPE);
            ps.setString(4, telGens[1].id().toString());
            ps.executeUpdate();
        }

        // Below 85% trigger -> nothing retired.
        ArchiveCompactionManager.TelemetryPressureResult underThreshold =
                store.getArchiveCompactionManager().enforceTelemetryStoragePressure(now, 100_000L, 20_000L);
        assertEquals(80, underThreshold.usagePercent());
        assertFalse(underThreshold.critical());
        assertEquals(0, underThreshold.generationsRetired());

        // At 86% (standard pressure, 365d floor) -> retires ONLY telGens[0] (400d old, unheld).
        // telGens[1] is held, telGens[2] is 60d (<365d), telGens[3] is 5d (and latest), and ALL evGens are protected.
        ArchiveCompactionManager.TelemetryPressureResult standardPressure =
                store.getArchiveCompactionManager().enforceTelemetryStoragePressure(now, 100_000L, 14_000L);
        assertEquals(86, standardPressure.usagePercent());
        assertFalse(standardPressure.critical());
        assertEquals(1, standardPressure.generationsRetired());
        assertTrue(standardPressure.queuedBytes() > 0L);

        // Pending DELETING bytes suppress duplicate retirements while waiting out unlink grace.
        ArchiveCompactionManager.TelemetryPressureResult suppressedRepeat =
                store.getArchiveCompactionManager().enforceTelemetryStoragePressure(now, 10_000L, 1_400L);
        assertEquals(0, suppressedRepeat.generationsRetired());

        // At 93% (critical pressure, 30d floor) with large deficit -> also retires telGens[2] (60d old, unheld),
        // still skipping held telGens[1], 5d telGens[3], and all 4 RAW_EVENTS generations.
        ArchiveCompactionManager.TelemetryPressureResult criticalPressure =
                store.getArchiveCompactionManager().enforceTelemetryStoragePressure(now, 10_000_000L, 700_000L);
        assertEquals(93, criticalPressure.usagePercent());
        assertTrue(criticalPressure.critical());
        assertEquals(1, criticalPressure.generationsRetired());

        // Advance sealed_at past 15-minute unlink grace and drain DELETING generations.
        try (Connection c = openPg();
             PreparedStatement ps = c.prepareStatement(
                     "UPDATE oscar_archive_generation SET sealed_at = ? WHERE store_key = ? AND state = 'DELETING'")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(20, ChronoUnit.MINUTES));
            ps.setString(2, table);
            assertEquals(2, ps.executeUpdate());
        }
        assertEquals(2, store.drainDeletingGenerations(now));

        // Retired Parquet files are gone; held telemetry, recent telemetry, and ALL 4 RAW_EVENTS remain intact & queryable.
        assertFalse(Files.exists(archiveRoot.resolve(telGens[0].objectPath())));
        assertFalse(Files.exists(archiveRoot.resolve(telGens[2].objectPath())));
        assertTrue(Files.exists(archiveRoot.resolve(telGens[1].objectPath())));
        assertTrue(Files.exists(archiveRoot.resolve(telGens[3].objectPath())));
        for (ArchiveGeneration evGen : evGens) {
            assertTrue("Operational RAW_EVENTS must never be retired by telemetry pressure",
                    Files.exists(archiveRoot.resolve(evGen.objectPath())));
        }
        assertEquals(4L, store.countMatchingEntries(new ObsFilter.Builder().withDataStreams(occStream).build()));
        assertEquals(2L, store.countMatchingEntries(new ObsFilter.Builder().withDataStreams(gammaStream).build()));
    }

    @Test
    public void streamingExportSinglePassCountersSpatialEnvelopesAndBboxPruningSurviveCompactionAndOverlays()
            throws Exception {
        Path archiveRoot = temporary.newFolder("spatial-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("spatial-spill");
        String table = uniqueTableName("spatial_stream");

        TieredArchiveConfig config = parquetPrimaryConfig(archiveRoot, spillRoot,
                List.of("occupancy", "alarm", "location", "gammaCounts"));
        config.compactionUnlinkGraceMinutes = 15;
        OscarTieredObsStore store = openStore(table, config);

        BigId fixedOccStream = addEventOccupancyStream(store, 501L, "occupancy");
        BigId mobileOakRidgeStream = addMobileLocationStream(store, 502L, "alarm");
        BigId mobilePacificStream = addMobileLocationStream(store, 503L, "alarm");
        BigId mobileTrackTelStream = addMobileLocationStream(store, 505L, "location");
        BigId fixedTelStream = addTelemetryStream(store, 504L, "gammaCounts");

        Instant dayD = Instant.parse("2026-09-10T00:00:00Z");
        Instant now = Instant.parse("2026-09-13T12:00:00Z");

        // Hour 0: Oak Ridge mobile detector patrol + fixed portal occupancy + telemetry (1 Hz location + fixed gammaCounts)
        Instant h0 = dayD;
        store.add(eventOccupancyObs(fixedOccStream, h0.plusSeconds(10), h0.plusSeconds(10),
                "fixed-occ-1", true, false, 300, 1, 0, 0));
        store.add(mobileLocationObs(mobileOakRidgeStream, h0.plusSeconds(20),
                35.9200, -84.3300, 260.0, true, false));
        BigId oakRidgeLatestId = store.add(mobileLocationObs(mobileOakRidgeStream, h0.plusSeconds(40),
                35.9450, -84.2950, 272.5, false, false));
        store.add(mobileLocationObs(mobileTrackTelStream, h0.plusSeconds(45),
                48.8566, 2.3522, 35.0, false, false));
        store.add(telemetryObs(fixedTelStream, h0.plusSeconds(50), h0.plusSeconds(50), 115.0, 1.0));
        commitStore(store);

        ArchiveGeneration h0Events = store.archiveWindow(
                h0, h0.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();
        ArchiveGeneration h0Tel = store.archiveWindow(
                h0, h0.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_TELEMETRY).orElseThrow();

        // Hour 1: Antimeridian / Pacific Rim mobile patrol crossing +/-180 deg longitude
        Instant h1 = dayD.plus(1, ChronoUnit.HOURS);
        store.add(mobileLocationObs(mobilePacificStream, h1.plusSeconds(10),
                -18.1416, 178.4419, 14.0, false, true));
        BigId pacificLatestId = store.add(mobileLocationObs(mobilePacificStream, h1.plusSeconds(30),
                -17.9000, -179.2500, 22.0, false, false));
        commitStore(store);

        ArchiveGeneration h1Events = store.archiveWindow(
                h1, h1.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_EVENTS).orElseThrow();

        // 1. Verify exact spatial bounds & compact sparse-zero summary counters on oscar_archive_schema
        try (Connection conn = openPg();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT datastream_id, summary_version, row_count, gamma_alarm_count, neutron_alarm_count, " +
                             "min_lat, max_lat, min_lon, max_lon " +
                             "FROM oscar_archive_schema WHERE generation_id = ?")) {
            stmt.setObject(1, h0Events.id());
            Map<Long, double[]> boundsByDs = new HashMap<>();
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    long dsId = rs.getLong(1);
                    assertEquals(2, rs.getShort(2));
                    if (dsId == fixedOccStream.getIdAsLong()) {
                        assertEquals(1L, rs.getLong(3));
                        assertEquals(1L, rs.getLong(4));
                        assertNull(rs.getObject(5)); // sparse zero -> NULL
                        assertNull(rs.getObject(6)); // fixed portal has NULL spatial bounds
                    } else if (dsId == mobileOakRidgeStream.getIdAsLong()) {
                        assertEquals(2L, rs.getLong(3));
                        assertEquals(1L, rs.getLong(4));
                        boundsByDs.put(dsId, new double[]{
                                rs.getDouble(6), rs.getDouble(7), rs.getDouble(8), rs.getDouble(9)});
                    }
                }
            }
            double[] oakBounds = boundsByDs.get(mobileOakRidgeStream.getIdAsLong());
            assertNotNull(oakBounds);
            assertEquals(35.9200, oakBounds[0], 1e-6);
            assertEquals(35.9450, oakBounds[1], 1e-6);
            assertEquals(-84.3300, oakBounds[2], 1e-6);
            assertEquals(-84.2950, oakBounds[3], 1e-6);
        }

        // Fixed telemetry stream must have NULL spatial bounds while 1 Hz mobile location telemetry carries Paris bounds
        try (Connection conn = openPg();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT datastream_id, min_lat, max_lat, min_lon, max_lon FROM oscar_archive_schema WHERE generation_id = ?")) {
            stmt.setObject(1, h0Tel.id());
            int telRows = 0;
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    telRows++;
                    long dsId = rs.getLong(1);
                    if (dsId == fixedTelStream.getIdAsLong()) {
                        assertNull(rs.getObject(2));
                        assertNull(rs.getObject(3));
                        assertNull(rs.getObject(4));
                        assertNull(rs.getObject(5));
                    } else if (dsId == mobileTrackTelStream.getIdAsLong()) {
                        assertEquals(48.8566, rs.getDouble(2), 1e-6);
                        assertEquals(48.8566, rs.getDouble(3), 1e-6);
                        assertEquals(2.3522, rs.getDouble(4), 1e-6);
                        assertEquals(2.3522, rs.getDouble(5), 1e-6);
                    }
                }
            }
            assertEquals(2, telRows);
        }

        // 2. Verify catalog-only bbox pruning (0 DuckDB opens!)
        ArchiveCatalog catalog = store.getArchiveCatalog();
        List<ArchiveGeneration> oakHits = catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), 35.90, 35.96, -84.35, -84.25);
        assertEquals(List.of(h0Events.id()), oakHits.stream().map(ArchiveGeneration::id).toList());

        // Paris bbox matches RAW_TELEMETRY mobile track generation directly
        List<ArchiveGeneration> parisHits = catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), 48.80, 48.90, 2.30, 2.40);
        assertEquals(List.of(h0Tel.id()), parisHits.stream().map(ArchiveGeneration::id).toList());

        // Non-intersecting Tokyo box returns empty
        assertTrue(catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), 35.60, 35.80, 139.60, 139.85).isEmpty());

        // Fixed non-spatial stream is pruned even inside matching Oak Ridge bbox
        assertTrue(catalog.listGenerationsIntersectingBbox(
                Set.of(fixedOccStream.getIdAsLong(), fixedTelStream.getIdAsLong()),
                dayD, dayD.plus(1, ChronoUnit.DAYS), 35.90, 35.96, -84.35, -84.25).isEmpty());

        // Antimeridian-crossing bbox (175 E to 175 W) matches Pacific generation and prunes Oak Ridge
        List<ArchiveGeneration> pacificHits = catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), -20.0, -15.0, 175.0, -175.0);
        assertEquals(List.of(h1Events.id()), pacificHits.stream().map(ArchiveGeneration::id).toList());

        // 3. Verify oscar_obs_latest spatial positions + overlay update + compaction propagation
        List<ArchiveCatalog.LatestSpatialPosition> allLatest =
                catalog.selectLatestSpatialPositions(null, null, null, null, null);
        assertEquals(3, allLatest.size());
        Map<Long, ArchiveCatalog.LatestSpatialPosition> latestByDs = new HashMap<>();
        for (ArchiveCatalog.LatestSpatialPosition p : allLatest) {
            latestByDs.put(p.dataStreamId(), p);
        }
        ArchiveCatalog.LatestSpatialPosition oakPos = latestByDs.get(mobileOakRidgeStream.getIdAsLong());
        assertNotNull(oakPos);
        assertEquals(oakRidgeLatestId.getIdAsLong(), oakPos.observationId());
        assertEquals(35.9450, oakPos.lat(), 1e-6);
        assertEquals(-84.2950, oakPos.lon(), 1e-6);
        assertEquals(272.5, oakPos.alt(), 1e-6);

        // Antimeridian bbox filter on selectLatestSpatialPositions returns only Pacific unit
        List<ArchiveCatalog.LatestSpatialPosition> pacLatest =
                catalog.selectLatestSpatialPositions(null, -20.0, -15.0, 175.0, -175.0);
        assertEquals(1, pacLatest.size());
        assertEquals(mobilePacificStream.getIdAsLong(), pacLatest.get(0).dataStreamId());
        assertEquals(pacificLatestId.getIdAsLong(), pacLatest.get(0).observationId());

        // Overlay correction on latest archived mobile observation updates latest_lat/lon/alt immediately
        store.put(oakRidgeLatestId, mobileLocationObs(mobileOakRidgeStream, h0.plusSeconds(40),
                35.9511, -84.2888, 281.0, false, false));
        commitStore(store);
        List<ArchiveCatalog.LatestSpatialPosition> afterOverlay = catalog.selectLatestSpatialPositions(
                Set.of(mobileOakRidgeStream.getIdAsLong()), null, null, null, null);
        assertEquals(1, afterOverlay.size());
        assertEquals(35.9511, afterOverlay.get(0).lat(), 1e-6);
        assertEquals(-84.2888, afterOverlay.get(0).lon(), 1e-6);
        assertEquals(281.0, afterOverlay.get(0).alt(), 1e-6);

        // 4. Compact RAW_EVENTS -> DAILY_EVENTS and confirm spatial envelopes & latest positions survive intact
        store.maintainArchiveLifecycle();
        try (Connection conn = openPg();
             PreparedStatement backdateGen = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?");
             PreparedStatement backdateOv = conn.prepareStatement(
                     "UPDATE oscar_obs_overlay SET updated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(backdateGen, 1, now.minus(3, ChronoUnit.HOURS));
            backdateGen.setString(2, table);
            backdateGen.executeUpdate();
            UtcJdbcTime.setInstant(backdateOv, 1, now.minus(3, ChronoUnit.HOURS));
            backdateOv.setString(2, table);
            backdateOv.executeUpdate();
        }
        ArchiveCompactionManager.CompactionCycleSummary compactSummary = store.runCompactionCycle(now);
        assertEquals(1, compactSummary.daysCompacted());
        List<ArchiveGeneration> postCompactOak = catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), 35.90, 35.96, -84.35, -84.25);
        assertEquals(1, postCompactOak.size());
        assertEquals(TieredArchiveConfig.PROFILE_DAILY_EVENTS, postCompactOak.get(0).profile());

        // Also confirm the compacted DAILY_EVENTS bounds reflect the folded overlay coordinate (35.9511, -84.2888)!
        try (Connection conn = openPg();
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT min_lat, max_lat, min_lon, max_lon FROM oscar_archive_schema " +
                             "WHERE generation_id = ? AND datastream_id = ?")) {
            stmt.setObject(1, postCompactOak.get(0).id());
            stmt.setLong(2, mobileOakRidgeStream.getIdAsLong());
            try (ResultSet rs = stmt.executeQuery()) {
                assertTrue(rs.next());
                assertEquals(35.9200, rs.getDouble(1), 1e-6);
                assertEquals(35.9511, rs.getDouble(2), 1e-6);
                assertEquals(-84.3300, rs.getDouble(3), 1e-6);
                assertEquals(-84.2888, rs.getDouble(4), 1e-6);
            }
        }

        List<ArchiveGeneration> postCompactPac = catalog.listGenerationsIntersectingBbox(
                null, dayD, dayD.plus(1, ChronoUnit.DAYS), -20.0, -15.0, 175.0, -175.0);
        assertEquals(1, postCompactPac.size());
        assertEquals(TieredArchiveConfig.PROFILE_DAILY_EVENTS, postCompactPac.get(0).profile());

        // Tombstoning Pacific latest observation removes it from selectLatestSpatialPositions
        store.remove(pacificLatestId);
        commitStore(store);
        assertTrue(catalog.selectLatestSpatialPositions(
                Set.of(mobilePacificStream.getIdAsLong()), null, null, null, null).isEmpty());
    }

    private static BigId addMobileLocationStream(OscarTieredObsStore store, long sysNum, String outputName)
            throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("latitude", swe.createQuantity().build())
                .addField("longitude", swe.createQuantity().build())
                .addField("altitude", swe.createQuantity().build())
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:mobile:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData mobileLocationObs(
            BigId stream, Instant time, double lat, double lon, double alt,
            boolean gammaAlarm, boolean neutronAlarm) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("mobileLocation")
                .addField("latitude", swe.createQuantity().build())
                .addField("longitude", swe.createQuantity().build())
                .addField("altitude", swe.createQuantity().build())
                .addField("gammaAlarm", swe.createBoolean().build())
                .addField("neutronAlarm", swe.createBoolean().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setDoubleValue(0, lat);
        block.setDoubleValue(1, lon);
        block.setDoubleValue(2, alt);
        block.setBooleanValue(3, gammaAlarm);
        block.setBooleanValue(4, neutronAlarm);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(time)
                .withResultTime(time)
                .withResult(block)
                .build();
    }

    private static BigId addTamperStream(OscarTieredObsStore store, long sysNum) throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("tamper")
                .addField("tamperStatus", swe.createBoolean().build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName("tamper")
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData tamperObs(BigId stream, Instant time, boolean tampered) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("tamper")
                .addField("tamperStatus", swe.createBoolean().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setBooleanValue(0, tampered);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(time)
                .withResultTime(time)
                .withResult(block)
                .build();
    }

    private static BigId addCombinedTamperAndFaultStream(
            OscarTieredObsStore store, long sysNum, String outputName) throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("tamperStatus", swe.createBoolean().build())
                .addField("alarmState", swe.createCategory().addAllowedValues("Fault - Gamma High", "Background").build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData combinedTamperFaultObs(
            BigId stream, Instant time, boolean tampered, String alarmState) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("alarm")
                .addField("tamperStatus", swe.createBoolean().build())
                .addField("alarmState", swe.createCategory().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setBooleanValue(0, tampered);
        block.setStringValue(1, alarmState);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(time)
                .withResultTime(time)
                .withResult(block)
                .build();
    }

    private static BigId addAlarmStateStream(
            OscarTieredObsStore store, long sysNum, String outputName, String[] allowedStates) throws Exception {
        SWEHelper swe = new SWEHelper();
        var catBuilder = swe.createCategory().name("alarmState");
        if (allowedStates != null && allowedStates.length > 0)
            catBuilder.addAllowedValues(allowedStates);
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("alarmState", catBuilder.build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData alarmStateObs(BigId stream, Instant time, String state) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("alarmStateRec")
                .addField("alarmState", swe.createCategory().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setStringValue(0, state);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(time)
                .withResultTime(time)
                .withResult(block)
                .build();
    }

    private static BigId addAlarmCategoryStream(OscarTieredObsStore store, long sysNum, String outputName)
            throws Exception {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name(outputName)
                .addField("alarmCategoryCode", swe.createCategory().addAllowedValues("Gamma", "Neutron").build())
                .build();
        DataStreamInfo info = new DataStreamInfo.Builder()
                .withName(outputName)
                .withSystem(new FeatureId(BigId.fromLong(ID_SCOPE, sysNum), "urn:oscar:test:sys:" + sysNum))
                .withRecordDescription(rec)
                .withRecordEncoding(new TextEncodingImpl())
                .build();
        return store.getDataStreams().add(info).getInternalID();
    }

    private static IObsData alarmCategoryObs(BigId stream, Instant time, String category) {
        SWEHelper swe = new SWEHelper();
        DataRecord rec = swe.createRecord().name("alarm")
                .addField("alarmCategoryCode", swe.createCategory().build())
                .build();
        DataBlock block = rec.createDataBlock();
        block.setStringValue(0, category);
        return new ObsData.Builder()
                .withDataStream(stream)
                .withPhenomenonTime(time)
                .withResultTime(time)
                .withResult(block)
                .build();
    }
}
