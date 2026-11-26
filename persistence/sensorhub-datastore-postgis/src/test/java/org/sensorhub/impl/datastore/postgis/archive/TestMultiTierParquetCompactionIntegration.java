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
 * multi-tier LSM compaction, settled-overlay folding, monthly consolidation,
 * periodic defragmentation, OCR Bloom catalog pruning, interleaved ID point
 * lookups, retention-hold and FK safety, and online-backup grace windows.
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
    public void dayOneCompactionSplitsEventAndTelemetryProfilesWithInterleavedIdsAndHonorsActivatedAtGuard()
            throws Exception {
        Path archiveRoot = temporary.newFolder("day1-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("day1-spill");
        String table = uniqueTableName("compact_day1");

        TieredArchiveConfig config = baseConfig(archiveRoot, spillRoot);
        config.compactionUnlinkGraceMinutes = 15;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 101L, "occupancy");
        BigId ocrStream = addVehicleOcrStream(store, 102L, "vehicleOcr");
        BigId telStream = addTelemetryStream(store, 103L, "gammaCounts");

        Instant dayD = Instant.parse("2026-08-10T00:00:00Z");
        Instant now = Instant.parse("2026-08-13T12:00:00Z");

        Map<BigId, String> expectedEventIds = new HashMap<>();
        Map<BigId, String> expectedOcrNorms = new HashMap<>();
        Map<BigId, Double> expectedTelemetryGamma = new HashMap<>();

        // Insert INTERLEAVED event + telemetry observations across 3 hourly windows in Day D
        // so [min_id, max_id] of DAILY_EVENTS and multi-part DAILY_TELEMETRY overlap!
        for (int hour = 0; hour < 3; hour++) {
            Instant winStart = dayD.plus(hour, ChronoUnit.HOURS);
            for (int i = 0; i < 10; i++) {
                Instant tEvent = winStart.plusSeconds(60L + i * 180L);
                String evtCode = "evt-d1-" + hour + "-" + i;
                boolean gammaAlarm = (i % 3 == 0);
                BigId occId = store.add(eventOccupancyObs(
                        occStream, tEvent, tEvent, evtCode, gammaAlarm, false,
                        gammaAlarm ? 160.0 + i : 18.0 + i, 2.0,
                        gammaAlarm ? 0 : -1, 0));
                expectedEventIds.put(occId, evtCode);

                for (int t = 0; t < 4; t++) {
                    Instant tTel = tEvent.plusSeconds(5L + t * 3L);
                    double gross = 1_000.0 + hour * 100.0 + i * 10.0 + t;
                    BigId telId = store.add(telemetryObs(telStream, tTel, tTel, gross, 3.0));
                    expectedTelemetryGamma.put(telId, gross);
                }

                if (i % 2 == 0) {
                    Instant tOcr = tEvent.plusSeconds(20L);
                    String norm = "MSCU1234" + hour + i + "5";
                    BigId ocrId = store.add(vehicleOcrObs(ocrStream, tOcr, tOcr, "occ-ref-" + hour + "-" + i, norm));
                    expectedOcrNorms.put(ocrId, norm);
                }
            }
            commitStore(store);
            store.archiveWindow(winStart, winStart.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED)
                    .orElseThrow();
        }

        // Also create a synthetic historical RAW_GATED generation on 2026-08-11 whose activated_at is < 2h old!
        Instant syntheticDay = Instant.parse("2026-08-11T00:00:00Z");
        BigId freshHistoricalId = store.add(telemetryObs(
                telStream, syntheticDay.plusSeconds(120L), syntheticDay.plusSeconds(120L), 777.0, 1.0));
        commitStore(store);
        ArchiveGeneration freshHistoricalGen = store.archiveWindow(
                syntheticDay, syntheticDay.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED)
                .orElseThrow();

        long expectedTotalColdRows = expectedEventIds.size() + expectedOcrNorms.size() + expectedTelemetryGamma.size() + 1L;

        // Backdate all 4 RAW_GATED generations so maintainArchiveLifecycle verifies parity, retires hot rows, and seals them;
        // then stamp freshHistoricalGen activated_at = now - 20m (< 2h) to prove Red-Team #3 activated_at guard!
        try (Connection conn = openPg();
             PreparedStatement backdateAll = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(backdateAll, 1, now.minus(3, ChronoUnit.HOURS));
            backdateAll.setString(2, table);
            assertEquals(4, backdateAll.executeUpdate());
        }
        store.maintainArchiveLifecycle();
        try (Connection conn = openPg();
             PreparedStatement recentSynth = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
            UtcJdbcTime.setInstant(recentSynth, 1, now.minus(20, ChronoUnit.MINUTES));
            recentSynth.setObject(2, freshHistoricalGen.id());
            assertEquals(1, recentSynth.executeUpdate());
        }

        // Run Day-1 compaction with maxRows=45 so 45 event rows stay 1 DAILY_EVENTS file and 120 telemetry rows split into 3 parts!
        config.maximumRowsPerGeneration = 45;
        ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
        assertEquals(1, summary.daysCompacted());
        assertEquals(0, summary.monthsConsolidated());
        assertEquals("15-minute grace window must defer physical unlink immediately after swap",
                0, summary.deletingGenerationsDrained());

        try (Connection conn = openPg()) {
            // 1. Freshly activated (< 2h) synthetic historical RAW_GATED generation must remain untouched!
            assertEquals("SEALED", queryGenerationState(conn, freshHistoricalGen.id()));
            assertEquals(TieredArchiveConfig.PROFILE_RAW_GATED, queryGenerationProfile(conn, freshHistoricalGen.id()));

            // 2. Day D's 3 RAW_GATED generations are DELETING with row_count = 0 (zero double-counting!)
            //    and physical files still present on disk for online pg_basebackup consistency.
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

            // 3. Total catalog row_count across store_key is immediately exact!
            assertEquals(expectedTotalColdRows, querySumRowCount(conn, table));
            assertEquals(expectedTotalColdRows, store.getNumRecords());

            // 4. Verify DAILY_EVENTS (1 generation, 45 rows) and multi-part DAILY_TELEMETRY (3 parts: 45 + 45 + 30 = 120 rows)
            List<Path> dailyEventFiles = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS);
            List<Path> dailyTelFiles = listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY);
            assertEquals(1, dailyEventFiles.size());
            assertEquals("120 telemetry rows at maxRows=45 must split into 3 bounded parts", 3, dailyTelFiles.size());

            // Verify Parquet V2 format & column counts (20 cols for DAILY_EVENTS, 11 cols for DAILY_TELEMETRY)
            assertParquetV2AndColumnCount(archiveRoot.resolve(dailyEventFiles.get(0)), 20);
            for (Path telRel : dailyTelFiles) {
                assertParquetV2AndColumnCount(archiveRoot.resolve(telRel), 11);
            }

            // Verify overlapping [min_id, max_id] in oscar_archive_id_block between DAILY_EVENTS and DAILY_TELEMETRY!
            assertOverlappingIdBlocksExist(conn, table);

            // Verify oscar_archive_schema 256-byte ocr_bloom + min/max on vehicleOcr and 1-byte sentinel on occupancy
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT s.datastream_id, octet_length(s.ocr_bloom), s.ocr_min_norm, s.ocr_max_norm " +
                            "FROM oscar_archive_schema s JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                            "WHERE g.store_key = ? AND g.profile = 'DAILY_EVENTS' AND g.state = 'SEALED'")) {
                ps.setString(1, table);
                Map<Long, Integer> bloomLengths = new HashMap<>();
                try (ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        long dsId = rs.getLong(1);
                        bloomLengths.put(dsId, rs.getInt(2));
                        if (dsId == ocrStream.getIdAsLong()) {
                            assertEquals("MSCU1234005", rs.getString(3));
                            assertEquals("MSCU1234285", rs.getString(4));
                        }
                    }
                }
                assertEquals(Integer.valueOf(OcrBloomFilter.BLOOM_BYTES), bloomLengths.get(ocrStream.getIdAsLong()));
                assertEquals(Integer.valueOf(1), bloomLengths.get(occStream.getIdAsLong()));
            }

            // 5. Run oscar-storage.sh archive verify SQL + filesystem reconciliation DURING grace window!
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        // 6. Point reads (store.get) across overlapping DAILY_EVENTS + DAILY_TELEMETRY id_blocks!
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
        assertNotNull(store.get(freshHistoricalId));

        // 7. OCR Bloom candidate pruning + store CQL count/select parity
        ObsFilter absentOcr = new ObsFilter.Builder()
                .withDataStreams(ocrStream)
                .withCQLFilter("normalizedValue = 'ZZZZ9999999'")
                .build();
        List<ArchiveGeneration> ocrCandidates = store.getArchiveCatalog().candidates(
                dayD, dayD.plus(1, ChronoUnit.DAYS), null, null, null, Set.of(ocrStream.getIdAsLong()));
        assertEquals(1, ocrCandidates.size());
        assertTrue("OCR Bloom + zone-map must prune non-matching DAILY_EVENTS generation before DuckDB",
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

        // 8. Advance past 15-minute grace window -> DELETING generations unlink, prune empty RAW_GATED dir, and pass verify!
        try (Connection conn = openPg();
             PreparedStatement backdateSealed = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET sealed_at = ? WHERE store_key = ? AND state = 'DELETING'")) {
            UtcJdbcTime.setInstant(backdateSealed, 1, now.minus(16, ChronoUnit.MINUTES));
            backdateSealed.setString(2, table);
            assertEquals(3, backdateSealed.executeUpdate());
        }
        assertEquals(3, store.drainDeletingGenerations(now));
        assertFalse("Empty profile=RAW_GATED directory for Day D must be pruned after unlink",
                Files.exists(archiveRoot.resolve("datasets/observations/date=2026-08-10/profile=RAW_GATED")));
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

        TieredArchiveConfig config = baseConfig(archiveRoot, spillRoot);
        config.compactionUnlinkGraceMinutes = 0; // Immediate drain once unpinned!

        OscarTieredObsStore store = openStore(table, config);
        BigId occStreamA = addEventOccupancyStream(store, 201L, "occupancy");
        BigId occStreamB = addEventOccupancyStream(store, 202L, "occupancy");
        BigId occStreamC = addEventOccupancyStream(store, 203L, "occupancy");

        Instant day1 = Instant.parse("2026-08-15T00:00:00Z");
        Instant day2Held = Instant.parse("2026-08-16T00:00:00Z");
        Instant now = Instant.parse("2026-08-20T12:00:00Z");

        // Day 1 Window 0: [00:00, 01:00)
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
                day1, day1.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED).orElseThrow();

        // Day 1 Window 1: [01:00, 02:00)
        BigId obsRecent = store.add(eventOccupancyObs(
                occStreamB, day1.plusSeconds(3_660L), day1.plusSeconds(3_660L),
                "evt-recent-base", true, false, 110.0, 2.0, -1, 0));
        BigId obsKeepB = store.add(eventOccupancyObs(
                occStreamB, day1.plusSeconds(3_720L), day1.plusSeconds(3_720L),
                "evt-keep-b", false, false, 15.0, 1.0, -1, 0));
        BigId obsLatestC = store.add(eventOccupancyObs(
                occStreamC, day1.plusSeconds(3_750L), day1.plusSeconds(3_750L),
                "evt-latest-c-base", false, false, 20.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration d1Gen1 = store.archiveWindow(
                day1.plus(1, ChronoUnit.HOURS), day1.plus(2, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED)
                .orElseThrow();

        // Day 2 (Held): [00:00, 01:00)
        BigId obsHeld = store.add(eventOccupancyObs(
                occStreamA, day2Held.plusSeconds(60L), day2Held.plusSeconds(60L),
                "evt-held-day2", true, false, 300.0, 1.0, -1, 0));
        commitStore(store);
        ArchiveGeneration d2HeldGen = store.archiveWindow(
                day2Held, day2Held.plus(1, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED).orElseThrow();

        // Backdate activated_at on all 3 generations and run production maintainArchiveLifecycle to verify parity & seal.
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(2, ChronoUnit.DAYS));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }
        store.maintainArchiveLifecycle();

        // Apply cold mutations on Day 1:
        // 1. Settled adjudication overlay on obsSettled (backdated to now - 4h >= 2h cutoff)
        store.put(obsSettled, eventOccupancyObs(
                occStreamA, day1.plusSeconds(60L), day1.plusSeconds(60L),
                "evt-adjudicated-settled", true, false, 140.0, 1.0, 4, 2));
        // 2. Recent/concurrent overlay on obsRecent (updated_at = now - 5m < 2h cutoff, advancing time past obsKeepB!)
        Instant advancedTime = day1.plusSeconds(3_900L);
        store.put(obsRecent, eventOccupancyObs(
                occStreamB, advancedTime, advancedTime,
                "evt-recent-updated", true, true, 999.5, 42.0, 6, 1));
        // 3. Recent/concurrent in-place overlay directly on occStreamC's oscar_obs_latest row!
        store.put(obsLatestC, eventOccupancyObs(
                occStreamC, day1.plusSeconds(3_750L), day1.plusSeconds(3_750L),
                "evt-latest-c-overlaid", true, false, 777.25, 3.0, 4, 1));
        // 4. Cold tombstone on obsDeleted
        assertNotNull(store.remove(obsDeleted));

        UUID expiredPlanId = UUID.randomUUID();
        UUID holdId = UUID.randomUUID();
        try (Connection conn = openPg()) {
            // Backdate obsSettled overlay to now - 4h (settled) and obsRecent + obsLatestC overlays to now - 5m (unsettled)
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

            // Insert an EXPIRED PLANNED oscar_deletion_plan referencing d1Gen0 (must not block compaction & must clean up on drain!)
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

            // Place an active OBSERVATION retention hold on obsHeld (Day 2) -> Day 2 must NOT compact!
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

        // Pin d1Gen0 via ParquetArchiveReader BEFORE running compaction with grace=0:
        // Day 1 must compact, d1Gen1 can drain immediately, while d1Gen0 MUST stay on disk until unpinned!
        try (ParquetArchiveReader.PinnedGenerations pin = store.getArchiveReader().pinGenerations(List.of(d1Gen0.id()))) {
            ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
            assertEquals("Day 1 compacts while held Day 2 is skipped", 1, summary.daysCompacted());
            assertEquals("Unpinned d1Gen1 drains immediately at grace=0 while pinned d1Gen0 waits",
                    1, summary.deletingGenerationsDrained());
            assertTrue("Pinned d1Gen0 Parquet file must remain on disk while reader pin is active",
                    Files.isRegularFile(archiveRoot.resolve(d1Gen0.objectPath()), LinkOption.NOFOLLOW_LINKS));
        }

        // Now pin is closed -> drain remaining DELETING generation d1Gen0!
        assertEquals(1, store.drainDeletingGenerations(now));
        assertFalse("Unpinned d1Gen0 Parquet file must be unlinked after pin release",
                Files.exists(archiveRoot.resolve(d1Gen0.objectPath()), LinkOption.NOFOLLOW_LINKS));

        UUID day1CompactedGenId;
        try (Connection conn = openPg()) {
            // Day 2 held generation remained untouched in RAW_GATED!
            assertEquals("SEALED", queryGenerationState(conn, d2HeldGen.id()));
            assertEquals(TieredArchiveConfig.PROFILE_RAW_GATED, queryGenerationProfile(conn, d2HeldGen.id()));

            // Find Day 1's new DAILY_EVENTS generation
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT id FROM oscar_archive_generation WHERE store_key = ? AND profile = 'DAILY_EVENTS' AND state = 'SEALED'")) {
                ps.setString(1, table);
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    day1CompactedGenId = rs.getObject(1, UUID.class);
                    assertFalse(rs.next());
                }
            }

            // Settled overlay (obsSettled) was FOLDED into Parquet and deleted from oscar_obs_overlay!
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT count(*) FROM oscar_obs_overlay WHERE store_key = ? AND observation_id = ?")) {
                ps.setString(1, table);
                ps.setLong(2, obsSettled.getIdAsLong());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(0L, rs.getLong(1));
                }
            }

            // Recent overlay (obsRecent) was PRESERVED in oscar_obs_overlay and remapped to day1CompactedGenId!
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT generation_id FROM oscar_obs_overlay WHERE store_key = ? AND observation_id = ?")) {
                ps.setString(1, table);
                ps.setLong(2, obsRecent.getIdAsLong());
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(day1CompactedGenId, rs.getObject(1, UUID.class));
                }
            }

            // Tombstone (obsDeleted) was remapped to day1CompactedGenId!
            try (PreparedStatement ps = conn.prepareStatement(
                    "SELECT generation_id FROM oscar_tombstone WHERE store_key = ? AND subject_id = ?")) {
                ps.setString(1, table);
                ps.setString(2, Long.toString(obsDeleted.getIdAsLong()));
                try (ResultSet rs = ps.executeQuery()) {
                    assertTrue(rs.next());
                    assertEquals(day1CompactedGenId, rs.getObject(1, UUID.class));
                }
            }

            // oscar_obs_latest remapped occStreamB (obsKeepB -> DAILY_EVENTS) & preserved in-place OVERLAY on occStreamC!
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

            // oscar_event_locator remapped folded eventId 'evt-adjudicated-settled' to day1CompactedGenId!
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

        // Verify read/get/count semantics across folded overlay, live overlay, tombstone, and held generation!
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

        // Projected V2 CQL query for adjudicationCode = 4 AND adjudicatedIdsCount = 2 matches folded obsSettled!
        ObsFilter code4Filter = new ObsFilter.Builder()
                .withCQLFilter("adjudicationCode = 4 AND adjudicatedIdsCount = 2")
                .build();
        assertEquals(1L, store.countMatchingEntries(code4Filter));
        try (var entries = store.selectEntries(code4Filter)) {
            var list = entries.toList();
            assertEquals(1, list.size());
            assertEquals(obsSettled, list.get(0).getKey());
        }

        // Latest per stream returns obsHeld on occStreamA, overlaid obsRecent (999.5) on occStreamB,
        // and overlaid obsLatestC (777.25) on occStreamC!
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
    public void monthlyConsolidationAndSixtyFourOverlayDefragmentationProduceSingleMonthlyEventsBundle()
            throws Exception {
        Path archiveRoot = temporary.newFolder("monthly-defrag-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("monthly-defrag-spill");
        String table = uniqueTableName("compact_month");

        TieredArchiveConfig config = baseConfig(archiveRoot, spillRoot);
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 301L, "occupancy");
        BigId ocrStream = addVehicleOcrStream(store, 302L, "vehicleOcr");
        BigId telStream = addTelemetryStream(store, 303L, "gammaCounts");

        Instant julyStart = Instant.parse("2026-07-01T00:00:00Z");
        Instant now = Instant.parse("2026-08-20T12:00:00Z"); // > 7 days after 2026-08-01T00:00:00Z!

        List<BigId> julyEventObsIds = new ArrayList<>();
        // Populate 3 days in July 2026 (25 event rows + 5 OCR rows + 6 telemetry rows per day = 75 event, 15 OCR, 18 tel)
        for (int day = 0; day < 3; day++) {
            Instant dayStart = julyStart.plus(day * 5L, ChronoUnit.DAYS);
            for (int i = 0; i < 25; i++) {
                Instant t = dayStart.plusSeconds(60L + i * 60L);
                BigId occId = store.add(eventOccupancyObs(
                        occStream, t, t, "evt-july-" + day + "-" + i,
                        i % 5 == 0, false, 95.0 + i, 1.0, -1, 0));
                julyEventObsIds.add(occId);
            }
            for (int i = 0; i < 5; i++) {
                Instant t = dayStart.plusSeconds(2_000L + i * 30L);
                store.add(vehicleOcrObs(ocrStream, t, t, "occ-j-" + day + "-" + i, "TGHU777" + day + i + "01"));
            }
            for (int i = 0; i < 6; i++) {
                Instant t = dayStart.plusSeconds(3_000L + i * 10L);
                store.add(telemetryObs(telStream, t, t, 50.0 + i, 1.0));
            }
            commitStore(store);
            store.archiveWindow(dayStart, dayStart.plus(2, ChronoUnit.HOURS), TieredArchiveConfig.PROFILE_RAW_GATED)
                    .orElseThrow();
        }

        long expectedTotal = 75L + 15L + 18L;

        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE store_key = ?")) {
            UtcJdbcTime.setInstant(ps, 1, Instant.parse("2026-07-20T00:00:00Z"));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }
        store.maintainArchiveLifecycle();

        // Cycle 1: Compacts all 3 July days from RAW_GATED -> DAILY_EVENTS + DAILY_TELEMETRY
        // AND in the same cycle consolidates July's newly sealed DAILY_EVENTS -> MONTHLY_EVENTS
        // once DAILY_EVENTS activated_at is >= 7 days old. First verify Day-1 produces 3 DAILY_EVENTS + 3 DAILY_TELEMETRY!
        ArchiveCompactionManager.CompactionCycleSummary cycle1 = store.runCompactionCycle(now);
        assertEquals(3, cycle1.daysCompacted());
        assertEquals("Freshly compacted DAILY_EVENTS have activated_at = now (< 7d), waiting before monthly merge",
                0, cycle1.monthsConsolidated());

        // Backdate DAILY_EVENTS activated_at by 8 days to simulate 1 week passing after Day-1 compaction.
        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? " +
                             "WHERE store_key = ? AND profile = 'DAILY_EVENTS'")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(8, ChronoUnit.DAYS));
            ps.setString(2, table);
            assertEquals(3, ps.executeUpdate());
        }

        // Cycle 2: Consolidates all 3 July DAILY_EVENTS into 1 MONTHLY_EVENTS under date=2026-07-01/profile=MONTHLY_EVENTS!
        ArchiveCompactionManager.CompactionCycleSummary cycle2 = store.runCompactionCycle(now);
        assertEquals(0, cycle2.daysCompacted());
        assertEquals(1, cycle2.monthsConsolidated());

        UUID initialMonthlyGenId;
        try (Connection conn = openPg()) {
            assertEquals(0, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS).size());
            assertEquals(3, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY).size());
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

        // Apply 65 (>= 64 threshold) overlays onto July MONTHLY_EVENTS observations!
        for (int i = 0; i < 65; i++) {
            BigId targetId = julyEventObsIds.get(i);
            IObsData existing = store.get(targetId);
            assertNotNull(existing);
            store.put(targetId, eventOccupancyObs(
                    occStream, existing.getPhenomenonTime(), existing.getResultTime(),
                    "evt-defrag-" + i, true, false, 500.0 + i, 2.0, 4, 1));
        }

        // Backdate MONTHLY_EVENTS activated_at AND all 65 overlays' updated_at by 8 days (>= 7d monthly settled cutoff).
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

        // Cycle 3: Periodic MONTHLY_EVENTS defragmentation folds all 65 settled overlays into a new MONTHLY_EVENTS file!
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

        // Verify all 65 folded adjudications match via projected V2 column CQL count and point get!
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
    public void parquetPrimaryModeBlocksUnretiredStagePartitionAndCompactsAfterStageRetirement() throws Exception {
        Path archiveRoot = temporary.newFolder("parquet-primary-archive").toPath();
        Path spillRoot = archiveRoot.resolveSibling("parquet-primary-spill");
        String table = uniqueTableName("compact_pp");

        TieredArchiveConfig config = baseConfig(archiveRoot, spillRoot);
        config.storageMode = TieredArchiveConfig.StorageMode.PARQUET_PRIMARY;
        config.compactionUnlinkGraceMinutes = 0;

        OscarTieredObsStore store = openStore(table, config);
        BigId occStream = addEventOccupancyStream(store, 401L, "occupancy");
        BigId telStream = addTelemetryStream(store, 402L, "gammaCounts");

        Instant dayStart = Instant.parse("2026-08-05T00:00:00Z");
        Instant now = Instant.parse("2026-08-10T12:00:00Z");

        BigId occId = store.add(eventOccupancyObs(
                occStream, dayStart.plusSeconds(30L), dayStart.plusSeconds(30L),
                "evt-pp-1", true, false, 180.0, 2.0, -1, 0));
        BigId telId = store.add(telemetryObs(
                telStream, dayStart.plusSeconds(45L), dayStart.plusSeconds(45L), 65.0, 1.0));
        commitStore(store);

        // Export 30-minute PARQUET_PRIMARY window -> creates oscar_obs_stage_owner rows on ATTACHED stage partition!
        ArchiveGeneration rawGen = store.archiveWindow(
                dayStart, dayStart.plus(30, ChronoUnit.MINUTES), TieredArchiveConfig.PROFILE_RAW_GATED).orElseThrow();

        try (Connection conn = openPg();
             PreparedStatement ps = conn.prepareStatement(
                     "UPDATE oscar_archive_generation SET activated_at = ? WHERE id = ?")) {
            UtcJdbcTime.setInstant(ps, 1, now.minus(1, ChronoUnit.DAYS));
            ps.setObject(2, rawGen.id());
            assertEquals(1, ps.executeUpdate());
        }

        // Before maintainArchiveLifecycle retires the staging partition, compaction MUST NOT touch rawGen!
        assertEquals(0, store.runCompactionCycle(now).daysCompacted());

        // Retire verified PARQUET_PRIMARY stage partition via maintainArchiveLifecycle, then re-backdate activated_at
        store.maintainArchiveLifecycle();
        assertTrue("Stage partitions must be retired after parity verification",
                store.getImmutableStagePartitions().isEmpty());

        // Compact retired PARQUET_PRIMARY day into DAILY_EVENTS + DAILY_TELEMETRY!
        ArchiveCompactionManager.CompactionCycleSummary summary = store.runCompactionCycle(now);
        assertEquals(1, summary.daysCompacted());
        assertEquals(1, summary.deletingGenerationsDrained());

        try (Connection conn = openPg()) {
            assertEquals(1, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_EVENTS).size());
            assertEquals(1, listActiveProfilePaths(conn, table, TieredArchiveConfig.PROFILE_DAILY_TELEMETRY).size());
            assertEquals(2L, querySumRowCount(conn, table));
            assertArchiveVerifyClean(conn, table, archiveRoot);
        }

        assertNotNull(store.get(occId));
        assertNotNull(store.get(telId));
        assertEquals(2L, store.getNumRecords());
    }

    // --- Helpers ---

    private static Connection openPg() throws Exception {
        return DriverManager.getConnection("jdbc:postgresql://" + PG_HOST_PORT + "/" + PG_DB, PG_USER, PG_PASS);
    }

    private static String uniqueTableName(String prefix) {
        return prefix + "_" + UUID.randomUUID().toString().replace("-", "").substring(0, 10);
    }

    private static TieredArchiveConfig baseConfig(Path archiveRoot, Path spillRoot) {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.enabled = true;
        config.rootDirectory = archiveRoot.toString();
        config.duckDbTemporaryDirectory = spillRoot.toString();
        config.duckDbMemoryLimitMegabytes = 128;
        config.duckDbThreads = 1;
        config.duckDbMaximumTemporaryMegabytes = 256;
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 10;
        config.outputNames = List.of("occupancy", "vehicleOcr", "gammaCounts");
        config.deleteHotAfterArchive = true;
        config.rollbackGraceDays = 1;
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
                        "ORDER BY window_start, id")) {
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

    private static void assertOverlappingIdBlocksExist(Connection conn, String storeKey) throws Exception {
        String sql = "SELECT count(*) FROM oscar_archive_id_block a " +
                "JOIN oscar_archive_generation ga ON ga.id = a.generation_id " +
                "JOIN oscar_archive_id_block b ON b.generation_id <> a.generation_id " +
                "JOIN oscar_archive_generation gb ON gb.id = b.generation_id " +
                "WHERE ga.store_key = ? AND gb.store_key = ? " +
                "  AND ga.state IN ('ACTIVE', 'SEALED') AND gb.state IN ('ACTIVE', 'SEALED') " +
                "  AND ga.profile = 'DAILY_EVENTS' AND gb.profile = 'DAILY_TELEMETRY' " +
                "  AND a.min_id <= b.max_id AND b.min_id <= a.max_id";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, storeKey);
            ps.setString(2, storeKey);
            try (ResultSet rs = ps.executeQuery()) {
                assertTrue(rs.next());
                assertTrue("Expected overlapping [min_id, max_id] blocks across DAILY_EVENTS and DAILY_TELEMETRY",
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

        // Reconcile every physical file/dir in archiveRoot against ACTIVE/DELETING catalog rows (audit_archive_filesystem)
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
}
