package org.sensorhub.impl.datastore.postgis.archive;

import com.google.common.collect.BoundType;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Transactional PostgreSQL catalog for immutable archive generations.
 *
 * <p>Object publication is manifest-last and cannot share a transaction with
 * PostgreSQL.  The explicit state machine makes every intermediate state
 * recoverable and prevents a partial file from becoming query-visible.</p>
 */
public final class ArchiveCatalog {
    public static final int SCHEMA_VERSION = 5;
    public static final String SCHEMA_CHECKSUM =
            "sha256:b37753a17c972d57418545c05a2544068affc7804b8622db3a0f0c4b422c63b5";
    private static final int PREVIOUS_SCHEMA_VERSION = 4;
    private static final String PREVIOUS_SCHEMA_CHECKSUM =
            "sha256:7aa2b6dd22cbed4499eb9fb16966323bbf90d14bff4d35e03a1670868fb2f8e8";
    static final long MAX_BACKUP_ARCHIVE_GENERATIONS = 1_000_000L;
    static final long MAX_BACKUP_ARCHIVE_GENERATION_INVENTORY_BYTES = 37_000_000L;
    private static final long MIGRATION_LOCK = 0x4f5343415253544fL;

    private final SqlConnectionProvider connections;
    private final String storeKey;
    private final int idScope;
    private final TieredArchiveConfig resourceConfig;
    private final AtomicInteger openOverlayCursors = new AtomicInteger();
    private final AtomicLong overlayRowsRead = new AtomicLong();
    private final Set<String> persistedSchemaBlobHashes = java.util.concurrent.ConcurrentHashMap.newKeySet();

    /*
     * Lock ordering for every observation mutation is deliberately fixed:
     *
     *  1. shared store mutation lock;
     *  2. observation locks in ascending numeric ID order;
     *  3. the retention-hold table in SHARE mode for a removal;
     *  4. hot observation row, overlay row, then tombstone/generation rows.
     *
     * Archive activation takes the exclusive form of the store lock before it
     * marks hot rows.  It never takes an observation lock.  Consequently it
     * cannot form a store-lock/row-lock cycle with an observation mutation.
     * hashtextextended collisions are conservative (extra serialization), not
     * a correctness failure.
     */
    private String storeMutationLockKey() {
        return "oscar:archive-store:" + storeKey + ':' + idScope;
    }

    private String observationMutationLockPrefix() {
        return "oscar:archive-observation:" + storeKey + ':' + idScope + ':';
    }

    public ArchiveCatalog(SqlConnectionProvider connections, String storeKey, int idScope) {
        this(connections, storeKey, idScope, defaultResourceConfig());
    }

    public ArchiveCatalog(SqlConnectionProvider connections, String storeKey, int idScope,
                          TieredArchiveConfig resourceConfig) {
        this.connections = connections;
        this.storeKey = safeIdentifier(storeKey);
        this.idScope = idScope;
        this.resourceConfig = java.util.Objects.requireNonNull(resourceConfig, "resourceConfig");
    }

    private static TieredArchiveConfig defaultResourceConfig() {
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.duckDbTemporaryDirectory = Path.of(
                System.getProperty("java.io.tmpdir"), "oscar-duckdb-test").toString();
        return config;
    }

    public String storeKey() {
        return storeKey;
    }

    public int idScope() {
        return idScope;
    }

    public void lockForActivation(Connection connection) throws SQLException {
        requireTransaction(connection);
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
            statement.setString(1, storeMutationLockKey());
            statement.execute();
        }
    }

    public void lockForMutation(Connection connection, long observationId) throws SQLException {
        lockForMutation(connection, List.of(observationId));
    }

    public void lockForMutation(Connection connection, Collection<Long> observationIds) throws SQLException {
        requireTransaction(connection);
        try (PreparedStatement storeLock = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock_shared(hashtextextended(?, 0))")) {
            storeLock.setString(1, storeMutationLockKey());
            storeLock.execute();
        }
        List<Long> ordered = observationIds.stream().distinct().sorted().toList();
        if (ordered.isEmpty())
            return;
        java.sql.Array ids = connection.createArrayOf("bigint", ordered.toArray(Long[]::new));
        try (PreparedStatement observationLock = connection.prepareStatement(
                "SELECT count(*) FROM (" +
                        "SELECT pg_advisory_xact_lock(hashtextextended(? || observation_id::text, 0)) " +
                        "FROM unnest(?::bigint[]) WITH ORDINALITY requested(observation_id, ordinal) " +
                        "ORDER BY ordinal) acquired")) {
            // One server round trip acquires every per-ID lock.  The Java-side
            // sort plus WITH ORDINALITY makes the evaluation order explicit;
            // the volatile lock call cannot be elided from the ordered derived
            // table.  This preserves deadlock avoidance without turning a
            // 10,000-row JDBC batch into 10,000 network round trips.
            observationLock.setString(1, observationMutationLockPrefix());
            observationLock.setArray(2, ids);
            try (ResultSet result = observationLock.executeQuery()) {
                if (!result.next() || result.getLong(1) != ordered.size())
                    throw new SQLException("PostgreSQL did not acquire every requested observation lock");
            }
        } finally {
            ids.free();
        }
    }

    /**
     * Serializes a logical removal with retention-hold creation/release and
     * returns the active exact observation and generation holds relevant to a
     * bounded mutation batch. The caller must already own the store/per-ID
     * mutation locks and must keep this transaction open through the mutation.
     */
    public ActiveRemovalHolds lockActiveRemovalHolds(Connection connection,
                                                     Collection<Long> observationIds)
            throws SQLException {
        requireTransaction(connection);
        try (Statement lock = connection.createStatement()) {
            // CLI hold creation/release uses SHARE ROW EXCLUSIVE. SHARE is
            // compatible across removers but conflicts with that writer lock,
            // making either the hold or the removal win completely.
            lock.execute("LOCK TABLE oscar_retention_hold IN SHARE MODE");
        }

        List<Long> requested = observationIds.stream()
                .distinct()
                .sorted()
                .toList();
        java.sql.Array ids = connection.createArrayOf("bigint", requested.toArray(Long[]::new));
        Set<Long> heldObservations = new java.util.HashSet<>();
        Set<UUID> heldGenerations = new java.util.HashSet<>();
        try (PreparedStatement statement = connection.prepareStatement(
                "WITH requested(id) AS (SELECT DISTINCT unnest(?::bigint[])) " +
                        "SELECT DISTINCT hold.subject_type, hold.subject_id " +
                        "FROM oscar_retention_hold hold WHERE hold.store_key=? AND hold.id_scope=? " +
                        "AND hold.released_at IS NULL AND ((hold.subject_type='OBSERVATION' AND EXISTS (" +
                        "SELECT 1 FROM requested WHERE hold.subject_id=requested.id::text)) OR " +
                        "(hold.subject_type='ARCHIVE_GENERATION' AND EXISTS (" +
                        "SELECT 1 FROM oscar_archive_id_block block " +
                        "JOIN oscar_archive_generation generation ON generation.id=block.generation_id " +
                        "JOIN requested ON requested.id BETWEEN block.min_id AND block.max_id " +
                        "WHERE block.generation_id::text=hold.subject_id " +
                        "AND generation.store_key=hold.store_key AND generation.id_scope=hold.id_scope " +
                        "AND generation.state IN ('ACTIVE','SEALED'))))")) {
            statement.setArray(1, ids);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    String subjectType = result.getString(1);
                    String subjectId = result.getString(2);
                    try {
                        if ("OBSERVATION".equals(subjectType))
                            heldObservations.add(Long.parseLong(subjectId));
                        else if ("ARCHIVE_GENERATION".equals(subjectType))
                            heldGenerations.add(UUID.fromString(subjectId));
                    } catch (IllegalArgumentException invalidCatalogValue) {
                        throw new SQLException("Invalid active retention-hold subject", invalidCatalogValue);
                    }
                }
            }
        } finally {
            ids.free();
        }
        return new ActiveRemovalHolds(heldObservations, heldGenerations);
    }

    public record ActiveRemovalHolds(Set<Long> observationIds, Set<UUID> generationIds) {
        public ActiveRemovalHolds {
            observationIds = Set.copyOf(observationIds);
            generationIds = Set.copyOf(generationIds);
        }

        public boolean blocks(long observationId, UUID generationId) {
            return observationIds.contains(observationId) ||
                    (generationId != null && generationIds.contains(generationId));
        }
    }

    public static final class RetentionHoldException extends IllegalStateException {
        public RetentionHoldException(long observationId, UUID generationId) {
            super("Active retention hold blocks removal of observation " + observationId +
                    (generationId == null ? "" : " in archive generation " + generationId));
        }
    }

    public static void requireRemovalAllowed(ActiveRemovalHolds holds,
                                             long observationId, UUID generationId) {
        if (holds == null)
            throw new IllegalArgumentException("A transaction-scoped retention-hold snapshot is required");
        if (holds.blocks(observationId, generationId))
            throw new RetentionHoldException(observationId, generationId);
    }

    private static void requireTransaction(Connection connection) throws SQLException {
        if (connection == null || connection.getAutoCommit())
            throw new IllegalArgumentException("Archive mutation locks require a caller-owned transaction");
    }

    public void migrate() {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            int migratedSchemaRows = 0;
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_xact_lock(" + MIGRATION_LOCK + ")");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_storage_schema_version (" +
                        "version integer PRIMARY KEY, checksum text NOT NULL, applied_at timestamptz NOT NULL DEFAULT now())");
                try (ResultSet result = statement.executeQuery(
                        "SELECT max(version) FROM oscar_storage_schema_version")) {
                    if (result.next()) {
                        int newest = result.getInt(1);
                        if (!result.wasNull() && newest > SCHEMA_VERSION)
                            throw new IllegalStateException(
                                    "Storage schema version " + newest +
                                            " is newer than supported version " + SCHEMA_VERSION);
                    }
                }
                requireRecordedSchemaChecksum(connection, PREVIOUS_SCHEMA_VERSION,
                        PREVIOUS_SCHEMA_CHECKSUM);
                requireRecordedSchemaChecksum(connection, SCHEMA_VERSION, SCHEMA_CHECKSUM);
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_job (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "profile text NOT NULL, window_start timestamptz NOT NULL, " +
                        "window_end timestamptz NOT NULL, revision integer NOT NULL DEFAULT 1, state text NOT NULL, attempt integer NOT NULL DEFAULT 1, " +
                        "error text, created_at timestamptz NOT NULL DEFAULT now(), updated_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_generation (" +
                        "id uuid PRIMARY KEY, job_id uuid NOT NULL REFERENCES oscar_archive_job(id), " +
                        "store_key text NOT NULL, id_scope integer NOT NULL, profile text NOT NULL, " +
                        "window_start timestamptz NOT NULL, window_end timestamptz NOT NULL, state text NOT NULL, " +
                        "object_path text NOT NULL, manifest_path text NOT NULL, sha256 text NOT NULL, row_count bigint NOT NULL, " +
                        "min_id bigint NOT NULL, max_id bigint NOT NULL, parity_verified boolean NOT NULL DEFAULT false, " +
                        "activated_at timestamptz NOT NULL DEFAULT now(), sealed_at timestamptz, " +
                        "max_result_time timestamptz, min_result_time timestamptz)");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS revision integer NOT NULL DEFAULT 1");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_archive_job ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_archive_job ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_job ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS max_result_time timestamptz");
                statement.execute("ALTER TABLE oscar_archive_generation ADD COLUMN IF NOT EXISTS min_result_time timestamptz");
                statement.execute("UPDATE oscar_archive_generation SET min_result_time = LEAST(window_start, max_result_time) " +
                        "WHERE min_result_time IS NULL AND max_result_time IS NOT NULL");
                statement.execute("ALTER TABLE oscar_archive_generation ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_generation ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_archive_job DROP CONSTRAINT IF EXISTS oscar_archive_job_profile_window_start_window_end_key");
                statement.execute("ALTER TABLE oscar_archive_job DROP CONSTRAINT IF EXISTS oscar_archive_job_profile_window_start_window_end_revision_key");
                statement.execute("ALTER TABLE oscar_archive_generation DROP CONSTRAINT IF EXISTS oscar_archive_generation_profile_window_start_window_end_key");
                statement.execute("DROP INDEX IF EXISTS oscar_archive_job_window_revision_idx");
                statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS oscar_archive_job_window_revision_idx ON oscar_archive_job " +
                        "(store_key, id_scope, profile, window_start, window_end, revision)");
                statement.execute("DROP INDEX IF EXISTS oscar_archive_generation_time_idx");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_generation_time_idx ON oscar_archive_generation " +
                        "(store_key, id_scope, profile, window_start, window_end) WHERE state IN ('ACTIVE','SEALED')");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_generation_window_idx ON oscar_archive_generation " +
                        "(store_key, id_scope, window_start, window_end) WHERE state IN ('ACTIVE','SEALED')");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_generation_result_time_idx ON oscar_archive_generation " +
                        "(store_key, id_scope, max_result_time) WHERE state IN ('ACTIVE','SEALED')");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_generation_min_result_time_idx ON oscar_archive_generation " +
                        "(store_key, id_scope, min_result_time) WHERE state IN ('ACTIVE','SEALED')");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_object (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "relative_path text NOT NULL, kind text NOT NULL, sha256 text NOT NULL, bytes bigint NOT NULL, " +
                        "state text NOT NULL DEFAULT 'ACTIVE', PRIMARY KEY(generation_id, relative_path))");
                // An immutable object may belong to exactly one generation.
                // This also prevents a future catalog bug from making one
                // generation's purge delete another generation's object.
                statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS oscar_archive_object_relative_path_idx " +
                        "ON oscar_archive_object(relative_path)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_id_block (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "min_id bigint NOT NULL, max_id bigint NOT NULL, PRIMARY KEY(generation_id, min_id))");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_id_lookup_idx ON oscar_archive_id_block (min_id, max_id)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_schema_blob (" +
                        "schema_sha256 text PRIMARY KEY, schema_json jsonb NOT NULL)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_schema (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "datastream_id bigint NOT NULL, schema_json jsonb, " +
                        "schema_sha256 text REFERENCES oscar_archive_schema_blob(schema_sha256), " +
                        "PRIMARY KEY(generation_id, datastream_id))");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS " +
                        "schema_sha256 text REFERENCES oscar_archive_schema_blob(schema_sha256)");
                statement.execute("ALTER TABLE oscar_archive_schema ALTER COLUMN schema_json DROP NOT NULL");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS summary_populated boolean NOT NULL DEFAULT false");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS row_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS gamma_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS gamma_neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS non_alarm_occupancy_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS gamma_true_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS neutron_true_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS any_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS unadjudicated_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS adjudicated_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS unadjudicated_gamma_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS unadjudicated_neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS unadjudicated_gamma_neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS adjudicated_gamma_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS adjudicated_neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS adjudicated_gamma_neutron_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS gamma_high_fault_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS gamma_low_fault_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS neutron_fault_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS extended_occupancy_fault_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS tamper_true_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS rs350_gamma_alarm_count bigint");
                statement.execute("ALTER TABLE oscar_archive_schema ADD COLUMN IF NOT EXISTS rs350_neutron_alarm_count bigint");
                statement.execute("UPDATE oscar_archive_schema SET " +
                        "unadjudicated_gamma_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(unadjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(adjudicated_alarm_count, 0) = 0 THEN COALESCE(gamma_alarm_count, 0) " +
                        "  WHEN COALESCE(neutron_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN COALESCE(unadjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END, " +
                        "unadjudicated_neutron_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(unadjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(adjudicated_alarm_count, 0) = 0 THEN COALESCE(neutron_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN COALESCE(unadjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(neutron_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END, " +
                        "unadjudicated_gamma_neutron_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(unadjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(adjudicated_alarm_count, 0) = 0 THEN COALESCE(gamma_neutron_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(neutron_alarm_count, 0) = 0 THEN COALESCE(unadjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END, " +
                        "adjudicated_gamma_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(adjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(unadjudicated_alarm_count, 0) = 0 THEN COALESCE(gamma_alarm_count, 0) " +
                        "  WHEN COALESCE(neutron_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN COALESCE(adjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END, " +
                        "adjudicated_neutron_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(adjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(unadjudicated_alarm_count, 0) = 0 THEN COALESCE(neutron_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN COALESCE(adjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(neutron_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END, " +
                        "adjudicated_gamma_neutron_alarm_count = CASE " +
                        "  WHEN COALESCE(any_alarm_count, 0) = 0 OR COALESCE(adjudicated_alarm_count, 0) = 0 THEN 0 " +
                        "  WHEN COALESCE(unadjudicated_alarm_count, 0) = 0 THEN COALESCE(gamma_neutron_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(neutron_alarm_count, 0) = 0 THEN COALESCE(adjudicated_alarm_count, 0) " +
                        "  WHEN COALESCE(gamma_neutron_alarm_count, 0) = 0 THEN 0 " +
                        "  ELSE NULL END " +
                        "WHERE summary_populated AND (" +
                        "  COALESCE(any_alarm_count, 0) = 0 OR " +
                        "  COALESCE(unadjudicated_alarm_count, 0) = 0 OR " +
                        "  COALESCE(adjudicated_alarm_count, 0) = 0 OR " +
                        "  (COALESCE(neutron_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0) OR " +
                        "  (COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(gamma_neutron_alarm_count, 0) = 0) OR " +
                        "  (COALESCE(gamma_alarm_count, 0) = 0 AND COALESCE(neutron_alarm_count, 0) = 0)" +
                        ") AND (" +
                        "  unadjudicated_gamma_alarm_count IS NULL OR " +
                        "  unadjudicated_neutron_alarm_count IS NULL OR " +
                        "  unadjudicated_gamma_neutron_alarm_count IS NULL" +
                        ")");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_archive_schema_datastream_idx " +
                        "ON oscar_archive_schema (datastream_id, generation_id)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_obs_latest (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, datastream_id bigint NOT NULL, " +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "observation_id bigint NOT NULL, foi_id bigint, " +
                        "phenomenon_time timestamptz, result_time timestamptz, " +
                        "result_json jsonb NOT NULL, schema_json jsonb NOT NULL, profile text NOT NULL, " +
                        "updated_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(store_key, id_scope, datastream_id))");
                statement.execute("INSERT INTO oscar_archive_schema_blob(schema_sha256, schema_json) " +
                        "SELECT DISTINCT encode(sha256(convert_to(schema_json::text, 'UTF8')), 'hex'), schema_json " +
                        "FROM oscar_archive_schema WHERE schema_json IS NOT NULL " +
                        "ON CONFLICT (schema_sha256) DO NOTHING");
                migratedSchemaRows = statement.executeUpdate("UPDATE oscar_archive_schema " +
                        "SET schema_sha256=encode(sha256(convert_to(schema_json::text, 'UTF8')), 'hex'), " +
                        "schema_json=NULL WHERE schema_json IS NOT NULL");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_event_locator (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, public_id text NOT NULL, " +
                        "observation_id bigint NOT NULL, generation_id uuid REFERENCES oscar_archive_generation(id), " +
                        "state text NOT NULL DEFAULT 'ACTIVE', updated_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(store_key, id_scope, public_id))");
                statement.execute("ALTER TABLE oscar_event_locator ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_event_locator ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_event_locator ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_event_locator ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_event_locator'::regclass " +
                        "AND conname='oscar_event_locator_pkey' AND pg_get_constraintdef(oid) NOT LIKE '%store_key%') THEN " +
                        "ALTER TABLE oscar_event_locator DROP CONSTRAINT oscar_event_locator_pkey; " +
                        "ALTER TABLE oscar_event_locator ADD CONSTRAINT oscar_event_locator_pkey " +
                        "PRIMARY KEY(store_key, id_scope, public_id); END IF; END $$");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_obs_overlay (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, observation_id bigint NOT NULL, generation_id uuid NOT NULL " +
                        "REFERENCES oscar_archive_generation(id), datastream_id bigint NOT NULL, foi_id bigint, " +
                        "phenomenon_time timestamptz, result_time timestamptz, result_json jsonb NOT NULL, " +
                        "schema_json jsonb NOT NULL, " +
                        "updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(store_key, id_scope, observation_id))");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS generation_id uuid " +
                        "REFERENCES oscar_archive_generation(id)");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS schema_json jsonb");
                statement.execute("ALTER TABLE oscar_obs_overlay ADD COLUMN IF NOT EXISTS schema_sha256 text " +
                        "REFERENCES oscar_archive_schema_blob(schema_sha256)");
                statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN schema_json DROP NOT NULL");
                // Once the validated constraint is installed, invalid schema-less
                // rows cannot be inserted. Avoid a history-sized repair scan on
                // every restart for a site with years of adjudication overlays.
                boolean needsOverlaySchemaRepair;
                try (ResultSet constraint = statement.executeQuery(
                        "SELECT EXISTS (SELECT 1 FROM pg_constraint " +
                        "WHERE conrelid='oscar_obs_overlay'::regclass " +
                        "AND conname='oscar_obs_overlay_schema_present_chk' AND convalidated)")) {
                    constraint.next();
                    needsOverlaySchemaRepair = !constraint.getBoolean(1);
                }
                if (needsOverlaySchemaRepair) {
                    statement.execute("UPDATE oscar_obs_overlay overlay_row " +
                            "SET schema_json=COALESCE(schema_row.schema_json, blob.schema_json) " +
                            "FROM oscar_archive_schema schema_row " +
                            "LEFT JOIN oscar_archive_schema_blob blob ON blob.schema_sha256=schema_row.schema_sha256 " +
                            "WHERE overlay_row.schema_json IS NULL AND overlay_row.schema_sha256 IS NULL " +
                            "AND overlay_row.generation_id=schema_row.generation_id " +
                            "AND overlay_row.datastream_id=schema_row.datastream_id");
                }
                // A pre-v4 overlay has no exact generation owner. Coarse ID
                // ranges are not ownership, so retain it for an operator-led
                // migration without allowing it to become query-visible.
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_obs_overlay_legacy_unowned (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, observation_id bigint NOT NULL, " +
                        "datastream_id bigint NOT NULL, foi_id bigint, phenomenon_time timestamptz, " +
                        "result_time timestamptz, result_json jsonb NOT NULL, schema_json jsonb, updated_at timestamptz, " +
                        "quarantined_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("ALTER TABLE oscar_obs_overlay_legacy_unowned " +
                        "ADD COLUMN IF NOT EXISTS schema_json jsonb");
                if (needsOverlaySchemaRepair) {
                    statement.execute("INSERT INTO oscar_obs_overlay_legacy_unowned(" +
                            "store_key,id_scope,observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json,schema_json,updated_at) " +
                            "SELECT store_key,id_scope,observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json,schema_json,updated_at " +
                            "FROM oscar_obs_overlay WHERE generation_id IS NULL OR (schema_json IS NULL AND schema_sha256 IS NULL)");
                    statement.execute("DELETE FROM oscar_obs_overlay WHERE generation_id IS NULL OR (schema_json IS NULL AND schema_sha256 IS NULL)");
                    statement.execute("ALTER TABLE oscar_obs_overlay ALTER COLUMN generation_id SET NOT NULL");
                    statement.execute("DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint " +
                            "WHERE conrelid='oscar_obs_overlay'::regclass AND conname='oscar_obs_overlay_schema_present_chk') THEN " +
                            "ALTER TABLE oscar_obs_overlay ADD CONSTRAINT oscar_obs_overlay_schema_present_chk " +
                            "CHECK (schema_json IS NOT NULL OR schema_sha256 IS NOT NULL); END IF; END $$");
                }
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_legacy_schema_idx " +
                        "ON oscar_obs_overlay(store_key, id_scope, observation_id) WHERE schema_json IS NOT NULL");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_obs_overlay'::regclass " +
                        "AND conname='oscar_obs_overlay_pkey' AND pg_get_constraintdef(oid)='PRIMARY KEY (observation_id)') THEN " +
                        "ALTER TABLE oscar_obs_overlay DROP CONSTRAINT oscar_obs_overlay_pkey; " +
                        "ALTER TABLE oscar_obs_overlay ADD CONSTRAINT oscar_obs_overlay_pkey " +
                        "PRIMARY KEY(store_key, id_scope, observation_id); END IF; END $$");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_stream_result_idx " +
                        "ON oscar_obs_overlay(store_key, id_scope, datastream_id, result_time DESC)");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_generation_idx " +
                        "ON oscar_obs_overlay(generation_id)");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_overlay_query_idx " +
                        "ON oscar_obs_overlay(generation_id, phenomenon_time, observation_id)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_retention_hold (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "subject_type text NOT NULL, subject_id text NOT NULL, reason text NOT NULL, " +
                        "created_by text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(), " +
                        "released_at timestamptz, released_by text, " +
                        "resolution_mode text NOT NULL)");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS released_by text");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_retention_hold ADD COLUMN IF NOT EXISTS resolution_mode text " +
                        "NOT NULL DEFAULT 'LEGACY_UNKNOWN'");
                statement.execute("UPDATE oscar_retention_hold SET resolution_mode='GENERATION_EXACT' " +
                        "WHERE subject_type='ARCHIVE_GENERATION' AND resolution_mode='LEGACY_UNKNOWN'");
                statement.execute("ALTER TABLE oscar_retention_hold ALTER COLUMN resolution_mode DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_retention_hold DROP CONSTRAINT IF EXISTS " +
                        "oscar_retention_hold_resolution_check");
                statement.execute("ALTER TABLE oscar_retention_hold ADD CONSTRAINT " +
                        "oscar_retention_hold_resolution_check CHECK " +
                        "((subject_type='ARCHIVE_GENERATION' AND resolution_mode='GENERATION_EXACT') OR " +
                        "(subject_type='OBSERVATION' AND resolution_mode IN " +
                        "('HOT_EXACT','COLD_RANGE_CONSERVATIVE','LEGACY_UNKNOWN'))) ");
                statement.execute("ALTER TABLE oscar_retention_hold ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_retention_hold ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DROP INDEX IF EXISTS oscar_retention_hold_active_idx");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_retention_hold_active_idx ON oscar_retention_hold " +
                        "(store_key, id_scope, subject_type, subject_id) WHERE released_at IS NULL");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_tombstone (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, deleted_at timestamptz NOT NULL DEFAULT now(), " +
                        "reason text NOT NULL, policy text NOT NULL, generation_id uuid " +
                        "REFERENCES oscar_archive_generation(id), PRIMARY KEY(subject_type, store_key, id_scope, subject_id))");
                statement.execute("ALTER TABLE oscar_tombstone ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_tombstone ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_tombstone ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_tombstone ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_constraint WHERE conrelid='oscar_tombstone'::regclass " +
                        "AND conname='oscar_tombstone_pkey' AND pg_get_constraintdef(oid)='PRIMARY KEY (subject_type, subject_id)') THEN " +
                        "ALTER TABLE oscar_tombstone DROP CONSTRAINT oscar_tombstone_pkey; " +
                        "ALTER TABLE oscar_tombstone ADD CONSTRAINT oscar_tombstone_pkey " +
                        "PRIMARY KEY(subject_type, store_key, id_scope, subject_id); END IF; END $$");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_tombstone_legacy_unowned (" +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, deleted_at timestamptz NOT NULL, reason text NOT NULL, " +
                        "policy text NOT NULL, quarantined_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("INSERT INTO oscar_tombstone_legacy_unowned(" +
                        "store_key,id_scope,subject_type,subject_id,deleted_at,reason,policy) " +
                        "SELECT store_key,id_scope,subject_type,subject_id,deleted_at,reason,policy " +
                        "FROM oscar_tombstone WHERE subject_type='OBSERVATION' AND generation_id IS NULL");
                statement.execute("DELETE FROM oscar_tombstone " +
                        "WHERE subject_type='OBSERVATION' AND generation_id IS NULL");
                statement.execute("ALTER TABLE oscar_tombstone DROP CONSTRAINT IF EXISTS oscar_tombstone_observation_owner_check");
                statement.execute("ALTER TABLE oscar_tombstone ADD CONSTRAINT oscar_tombstone_observation_owner_check " +
                        "CHECK (subject_type <> 'OBSERVATION' OR generation_id IS NOT NULL)");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_tombstone_generation_idx " +
                        "ON oscar_tombstone(generation_id) WHERE subject_type='OBSERVATION'");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_media_manifest (" +
                        "event_id text NOT NULL, camera_uid text NOT NULL, lane_uid text NOT NULL, manifest_path text NOT NULL, " +
                        "state text NOT NULL, source_sha256 text, policy_version integer NOT NULL, legal_hold boolean NOT NULL DEFAULT false, " +
                        "updated_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(event_id, camera_uid))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_media_asset (" +
                        "event_id text NOT NULL, camera_uid text NOT NULL, ordinal integer NOT NULL, asset_path text NOT NULL, " +
                        "mime_type text NOT NULL, sha256 text NOT NULL, bytes bigint NOT NULL, state text NOT NULL, " +
                        "PRIMARY KEY(event_id, camera_uid, ordinal), FOREIGN KEY(event_id, camera_uid) " +
                        "REFERENCES oscar_media_manifest(event_id, camera_uid) ON DELETE CASCADE)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_backup_epoch (" +
                        "id uuid PRIMARY KEY, state text NOT NULL, postgres_lsn pg_lsn, " +
                        "archive_generation_count bigint NOT NULL, " +
                        "archive_generation_inventory_sha256 text NOT NULL, " +
                        "archive_generation_inventory_bytes bigint NOT NULL, " +
                        "manifest_path text, sha256 text, created_at timestamptz NOT NULL DEFAULT now(), verified_at timestamptz)");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD COLUMN IF NOT EXISTS " +
                        "archive_generation_count bigint");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD COLUMN IF NOT EXISTS " +
                        "archive_generation_inventory_sha256 text");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD COLUMN IF NOT EXISTS " +
                        "archive_generation_inventory_bytes bigint");
                migrateLegacyBackupGenerationInventory(connection);
                statement.execute("ALTER TABLE oscar_backup_epoch DROP COLUMN IF EXISTS archive_generation_ids");
                statement.execute("ALTER TABLE oscar_backup_epoch ALTER COLUMN archive_generation_count SET NOT NULL");
                statement.execute("ALTER TABLE oscar_backup_epoch ALTER COLUMN " +
                        "archive_generation_inventory_sha256 SET NOT NULL");
                statement.execute("ALTER TABLE oscar_backup_epoch ALTER COLUMN " +
                        "archive_generation_inventory_bytes SET NOT NULL");
                statement.execute("ALTER TABLE oscar_backup_epoch DROP CONSTRAINT IF EXISTS " +
                        "oscar_backup_epoch_archive_generation_count_check");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD CONSTRAINT " +
                        "oscar_backup_epoch_archive_generation_count_check CHECK " +
                        "(archive_generation_count BETWEEN 0 AND 1000000)");
                statement.execute("ALTER TABLE oscar_backup_epoch DROP CONSTRAINT IF EXISTS " +
                        "oscar_backup_epoch_archive_generation_inventory_sha256_check");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD CONSTRAINT " +
                        "oscar_backup_epoch_archive_generation_inventory_sha256_check CHECK " +
                        "(archive_generation_inventory_sha256 ~ '^[0-9a-f]{64}$')");
                statement.execute("ALTER TABLE oscar_backup_epoch DROP CONSTRAINT IF EXISTS " +
                        "oscar_backup_epoch_archive_generation_inventory_bytes_check");
                statement.execute("ALTER TABLE oscar_backup_epoch ADD CONSTRAINT " +
                        "oscar_backup_epoch_archive_generation_inventory_bytes_check CHECK " +
                        "(archive_generation_inventory_bytes BETWEEN 0 AND 37000000 " +
                        "AND archive_generation_inventory_bytes = archive_generation_count * 37)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_archive_verification (" +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) ON DELETE CASCADE, " +
                        "verification_type text NOT NULL, status text NOT NULL, expected_rows bigint, actual_rows bigint, " +
                        "detail text, verified_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(generation_id, verification_type, verified_at))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_plan (" +
                        "id uuid PRIMARY KEY, store_key text NOT NULL, id_scope integer NOT NULL, " +
                        "subject_type text NOT NULL, subject_id text NOT NULL, state text NOT NULL, " +
                        "estimated_rows bigint NOT NULL, estimated_bytes bigint NOT NULL, hold_count bigint NOT NULL, " +
                        "confirmation_hash text NOT NULL, requested_by text NOT NULL, confirmed_by text, " +
                        "created_at timestamptz NOT NULL DEFAULT now(), expires_at timestamptz NOT NULL, confirmed_at timestamptz)");
                statement.execute("ALTER TABLE oscar_deletion_plan ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_deletion_plan ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_deletion_plan ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_deletion_plan ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_deletion_plan_scope_idx ON oscar_deletion_plan " +
                        "(store_key, id_scope, state, created_at)");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_plan_item (" +
                        "plan_id uuid NOT NULL REFERENCES oscar_deletion_plan(id) ON DELETE CASCADE, " +
                        "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id), " +
                        "state text NOT NULL DEFAULT 'PLANNED', detail text, updated_at timestamptz NOT NULL DEFAULT now(), " +
                        "PRIMARY KEY(plan_id, generation_id))");
                statement.execute("CREATE TABLE IF NOT EXISTS oscar_deletion_ledger (" +
                        "id bigserial PRIMARY KEY, plan_id uuid NOT NULL REFERENCES oscar_deletion_plan(id), " +
                        "store_key text NOT NULL, id_scope integer NOT NULL, subject_type text NOT NULL, " +
                        "subject_id text NOT NULL, object_path text, outcome text NOT NULL, " +
                        "detail text, recorded_at timestamptz NOT NULL DEFAULT now())");
                statement.execute("ALTER TABLE oscar_deletion_ledger ADD COLUMN IF NOT EXISTS store_key text NOT NULL DEFAULT 'legacy'");
                statement.execute("ALTER TABLE oscar_deletion_ledger ADD COLUMN IF NOT EXISTS id_scope integer NOT NULL DEFAULT -1");
                statement.execute("ALTER TABLE oscar_deletion_ledger ALTER COLUMN store_key DROP DEFAULT");
                statement.execute("ALTER TABLE oscar_deletion_ledger ALTER COLUMN id_scope DROP DEFAULT");
                statement.execute("CREATE INDEX IF NOT EXISTS oscar_deletion_ledger_scope_idx ON oscar_deletion_ledger " +
                        "(store_key, id_scope, recorded_at)");

                // Pre-v4 catalogs supported only one observation store. The
                // first tiered store opened after upgrade adopts that legacy
                // namespace; subsequent stores start clean and isolated.
                String namespace = "store_key='" + storeKey + "', id_scope=" + idScope;
                statement.execute("UPDATE oscar_archive_job SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_archive_generation generation SET " + namespace +
                        " FROM oscar_archive_job job WHERE generation.job_id=job.id " +
                        "AND generation.store_key='legacy' AND generation.id_scope=-1 " +
                        "AND job.store_key='" + storeKey + "' AND job.id_scope=" + idScope);
                statement.execute("UPDATE oscar_obs_overlay overlay_row SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE overlay_row.generation_id=generation.id " +
                        "AND overlay_row.store_key='legacy' AND overlay_row.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_tombstone tombstone SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE tombstone.generation_id=generation.id " +
                        "AND tombstone.store_key='legacy' AND tombstone.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_event_locator locator SET " + namespace +
                        " FROM oscar_archive_generation generation WHERE locator.generation_id=generation.id " +
                        "AND locator.store_key='legacy' AND locator.id_scope=-1 " +
                        "AND generation.store_key='" + storeKey + "' AND generation.id_scope=" + idScope);
                statement.execute("UPDATE oscar_retention_hold SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_deletion_plan SET " + namespace +
                        " WHERE store_key='legacy' AND id_scope=-1");
                statement.execute("UPDATE oscar_deletion_ledger ledger SET " + namespace +
                        " FROM oscar_deletion_plan plan WHERE ledger.plan_id=plan.id " +
                        "AND ledger.store_key='legacy' AND ledger.id_scope=-1 " +
                        "AND plan.store_key='" + storeKey + "' AND plan.id_scope=" + idScope);

                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO oscar_storage_schema_version(version, checksum) VALUES (?, ?) ON CONFLICT (version) DO NOTHING")) {
                    insert.setInt(1, SCHEMA_VERSION);
                    insert.setString(2, SCHEMA_CHECKSUM);
                    insert.executeUpdate();
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
            if (migratedSchemaRows > 0) {
                boolean wasAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(true);
                try (Statement vacuum = connection.createStatement()) {
                    vacuum.execute("VACUUM FULL oscar_archive_schema");
                } finally {
                    connection.setAutoCommit(wasAutoCommit);
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot migrate OSCAR archive catalog", e);
        }
    }

    /** A missing row denotes a fresh/direct upgrade; a recorded migration must be authentic. */
    private static void requireRecordedSchemaChecksum(Connection connection, int version,
                                                       String expectedChecksum)
            throws SQLException {
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT checksum FROM oscar_storage_schema_version WHERE version=?")) {
            select.setInt(1, version);
            try (ResultSet result = select.executeQuery()) {
                if (result.next() && !expectedChecksum.equals(result.getString(1)))
                    throw new IllegalStateException(
                            "Storage schema migration checksum mismatch for version " + version);
            }
        }
    }

    /**
     * Converts the bounded v4 UUID-array receipt into the canonical inventory
     * summary used by v5. The canonical bytes are sorted lowercase UUIDs, each
     * terminated by a single LF, exactly matching archive-generation-ids.txt.
     */
    private static void migrateLegacyBackupGenerationInventory(Connection connection)
            throws SQLException {
        if (!columnExists(connection, "oscar_backup_epoch", "archive_generation_ids"))
            return;
        try (PreparedStatement select = connection.prepareStatement(
                "SELECT id, archive_generation_ids FROM oscar_backup_epoch");
             PreparedStatement update = connection.prepareStatement(
                     "UPDATE oscar_backup_epoch SET archive_generation_count=?, " +
                             "archive_generation_inventory_sha256=?, " +
                             "archive_generation_inventory_bytes=? WHERE id=?");
             ResultSet rows = select.executeQuery()) {
            while (rows.next()) {
                java.sql.Array legacyIds = rows.getArray(2);
                if (legacyIds == null)
                    throw new SQLException("Legacy backup epoch has no archive generation inventory");
                BackupGenerationInventory inventory;
                try {
                    inventory = summarizeBackupGenerationInventory(legacyIds);
                } finally {
                    legacyIds.free();
                }
                update.setLong(1, inventory.count());
                update.setString(2, inventory.sha256());
                update.setLong(3, inventory.bytes());
                update.setObject(4, rows.getObject(1, UUID.class));
                if (update.executeUpdate() != 1)
                    throw new SQLException("Legacy backup epoch disappeared during schema migration");
            }
        }
    }

    private static BackupGenerationInventory summarizeBackupGenerationInventory(
            java.sql.Array legacyIds) throws SQLException {
        Object rawArray = legacyIds.getArray();
        if (!(rawArray instanceof Object[] values))
            throw new SQLException("Unsupported legacy archive generation inventory representation");
        requireBackupGenerationInventoryBounds(values.length, Math.multiplyExact(37L, values.length));
        List<String> canonicalIds = new ArrayList<>(values.length);
        for (Object value : values) {
            if (value == null)
                throw new SQLException("Legacy archive generation inventory contains null");
            try {
                UUID id = UUID.fromString(value.toString());
                if (id.version() < 1 || id.version() > 5 || id.variant() != 2)
                    throw new IllegalArgumentException("noncanonical UUID version or variant");
                canonicalIds.add(id.toString());
            } catch (IllegalArgumentException invalidUuid) {
                throw new SQLException(
                        "Legacy archive generation inventory contains a noncanonical UUID", invalidUuid);
            }
        }
        canonicalIds.sort(String::compareTo);
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            long bytes = 0L;
            String previous = null;
            for (String id : canonicalIds) {
                if (id.equals(previous))
                    throw new SQLException("Legacy archive generation inventory contains a duplicate UUID");
                previous = id;
                byte[] line = (id + '\n').getBytes(StandardCharsets.UTF_8);
                digest.update(line);
                bytes = Math.addExact(bytes, line.length);
            }
            requireBackupGenerationInventoryBounds(canonicalIds.size(), bytes);
            return new BackupGenerationInventory(
                    canonicalIds.size(), HexFormat.of().formatHex(digest.digest()), bytes);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        } catch (ArithmeticException overflow) {
            throw new SQLException("Legacy archive generation inventory is too large", overflow);
        }
    }

    static void requireBackupGenerationInventoryBounds(long count, long bytes) throws SQLException {
        if (count < 0 || count > MAX_BACKUP_ARCHIVE_GENERATIONS)
            throw new SQLException(
                    "Legacy archive generation inventory exceeds the 1,000,000-entry migration limit");
        if (bytes < 0 || bytes > MAX_BACKUP_ARCHIVE_GENERATION_INVENTORY_BYTES)
            throw new SQLException(
                    "Legacy archive generation inventory exceeds the 37,000,000-byte migration limit");
        final long canonicalBytes;
        try {
            canonicalBytes = Math.multiplyExact(count, 37L);
        } catch (ArithmeticException overflow) {
            throw new SQLException("Legacy archive generation inventory is too large", overflow);
        }
        if (bytes != canonicalBytes)
            throw new SQLException(
                    "Legacy archive generation inventory is not in canonical UUID-line form");
    }

    private static boolean columnExists(Connection connection, String table, String column)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM pg_attribute " +
                        "WHERE attrelid=?::regclass AND attname=? AND NOT attisdropped)")) {
            statement.setString(1, table);
            statement.setString(2, column);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    throw new SQLException("PostgreSQL did not return a column-presence result");
                return result.getBoolean(1);
            }
        }
    }

    private record BackupGenerationInventory(long count, String sha256, long bytes) {
    }

    /** Exact legacy/current job window that can safely be reclaimed. */
    public record RetryableJobWindow(Instant start, Instant end) {
        public RetryableJobWindow {
            if (start == null || end == null || !start.isBefore(end))
                throw new IllegalArgumentException("Retryable archive job window must be non-empty");
        }
    }

    private int staleExportingTimeoutMinutes() {
        return resourceConfig.isParquetPrimary()
                ? Math.max(20, resourceConfig.maximumCycleMinutes * 2)
                : 120;
    }

    /**
     * Marks in-flight {@code EXPORTING} jobs for this store as {@code FAILED}
     * during single-owner module startup so a container restart or upgrade does
     * not leave an interrupted window waiting for the stale-export timeout
     * (which would otherwise breach the 1-hour Parquet-primary staging SLO).
     */
    public int reclaimInterruptedExportingJobsOnStartup() {
        String sql = "UPDATE oscar_archive_job job " +
                "SET state='FAILED', error='Interrupted by process restart before activation', updated_at=now() " +
                "WHERE job.store_key=? AND job.id_scope=? AND job.state='EXPORTING' " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_archive_generation gen " +
                "WHERE gen.job_id=job.id AND gen.store_key=job.store_key AND gen.id_scope=job.id_scope)";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            return statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot reclaim interrupted archive jobs on startup", e);
        }
    }

    /**
     * Returns the oldest latest-revision FAILED job, or EXPORTING job stale
     * beyond {@link #staleExportingTimeoutMinutes()}, that is wholly before the
     * current retention cutoff.
     *
     * <p>This exact-window lookup preserves upgrade recovery for sub-hour jobs
     * created by the former time-bisection scheduler.  Reclaiming those jobs
     * before normal full-hour work clears their durable failure state instead
     * of leaving an obsolete FAILED row after the same observations are
     * archived by a differently shaped window.</p>
     */
    public Optional<RetryableJobWindow> nextRetryableJobWindow(String profile, Instant cutoff) {
        if (profile == null || profile.isBlank())
            throw new IllegalArgumentException("Archive profile is required");
        if (cutoff == null)
            throw new IllegalArgumentException("Archive cutoff is required");
        String sql = "SELECT job.window_start, job.window_end FROM oscar_archive_job job " +
                "WHERE job.store_key=? AND job.id_scope=? AND job.profile=? AND job.window_end<=? " +
                "AND (job.state='FAILED' OR (job.state='EXPORTING' " +
                "AND job.updated_at < now() - (? * interval '1 minute'))) " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_archive_job newer " +
                "WHERE newer.store_key=job.store_key AND newer.id_scope=job.id_scope " +
                "AND newer.profile=job.profile AND newer.window_start=job.window_start " +
                "AND newer.window_end=job.window_end AND newer.revision>job.revision) " +
                "ORDER BY job.window_start, job.window_end LIMIT 1";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, profile);
            UtcJdbcTime.setInstant(statement, 4, cutoff);
            statement.setInt(5, staleExportingTimeoutMinutes());
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return Optional.empty();
                return Optional.of(new RetryableJobWindow(
                        UtcJdbcTime.getInstant(result, 1),
                        UtcJdbcTime.getInstant(result, 2)));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect retryable archive jobs", e);
        }
    }

    public Optional<UUID> claimJob(String profile, Instant start, Instant end) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                String lockKey = storeKey + '|' + idScope + '|' + profile + '|' + start + '|' + end;
                try (PreparedStatement lock = connection.prepareStatement(
                        "SELECT pg_advisory_xact_lock(hashtextextended(?, 0))")) {
                    lock.setString(1, lockKey);
                    lock.execute();
                }

                UUID previousId = null;
                String previousState = null;
                int revision = 0;
                boolean stale = false;
                try (PreparedStatement latest = connection.prepareStatement(
                        "SELECT id, revision, state, updated_at < now() - (? * interval '1 minute') stale " +
                                "FROM oscar_archive_job WHERE store_key=? AND id_scope=? " +
                                "AND profile=? AND window_start=? AND window_end=? " +
                                "ORDER BY revision DESC LIMIT 1")) {
                    latest.setInt(1, staleExportingTimeoutMinutes());
                    latest.setString(2, storeKey);
                    latest.setInt(3, idScope);
                    latest.setString(4, profile);
                    UtcJdbcTime.setInstant(latest, 5, start);
                    UtcJdbcTime.setInstant(latest, 6, end);
                    try (ResultSet result = latest.executeQuery()) {
                        if (result.next()) {
                            previousId = result.getObject(1, UUID.class);
                            revision = result.getInt(2);
                            previousState = result.getString(3);
                            stale = result.getBoolean(4);
                        }
                    }
                }

                if ("EXPORTING".equals(previousState) && !stale) {
                    connection.rollback();
                    return Optional.empty();
                }
                if (shouldRetryJob(previousState, stale)) {
                    try (PreparedStatement retry = connection.prepareStatement(
                            "UPDATE oscar_archive_job SET state='EXPORTING', error=NULL, attempt=attempt+1, updated_at=now() " +
                                    "WHERE id=? AND store_key=? AND id_scope=?")) {
                        retry.setObject(1, previousId);
                        retry.setString(2, storeKey);
                        retry.setInt(3, idScope);
                        retry.executeUpdate();
                    }
                    connection.commit();
                    return Optional.of(previousId);
                }

                UUID id = UUID.randomUUID();
                try (PreparedStatement insert = connection.prepareStatement(
                        "INSERT INTO oscar_archive_job(id, store_key, id_scope, profile, window_start, window_end, revision, state) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, 'EXPORTING')")) {
                    insert.setObject(1, id);
                    insert.setString(2, storeKey);
                    insert.setInt(3, idScope);
                    insert.setString(4, profile);
                    UtcJdbcTime.setInstant(insert, 5, start);
                    UtcJdbcTime.setInstant(insert, 6, end);
                    insert.setInt(7, nextRevision(revision));
                    insert.executeUpdate();
                }
                connection.commit();
                return Optional.of(id);
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot claim archive window", e);
        }
    }

    public void activate(UUID jobId, ArchiveGeneration generation, Path parquetPath,
                         Path manifestPath, long bytes,
                         String manifestSha256, long manifestBytes, Path schemaPath, long schemaBytes,
                         String schemaSha256, List<SchemaSnapshot> schemas,
                         String observationTable, String dataStreamTable, List<String> outputNames) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                String observations = safeIdentifier(observationTable);
                String dataStreams = safeIdentifier(dataStreamTable);
                ResultTimeBounds resultTimeBounds = stageExportedRows(connection, parquetPath, generation);
                Instant maxResultTime = resultTimeBounds.maxResultTime();
                Instant minResultTime = resultTimeBounds.minResultTime();
                // Staging does not touch mutable PostgreSQL rows.  Acquire the
                // exclusive store lock only for the activation transaction's
                // critical section so ordinary ingestion is not blocked by
                // Parquet decoding.  Exact row predicates below still reject
                // every mutation that committed before this lock was granted.
                lockForActivation(connection);
                int marked;
                if (resourceConfig.isParquetPrimary()) {
                    marked = markParquetPrimaryStageOwners(connection, observations, generation);
                } else {
                    String markSql = "UPDATE " + observations + " o SET archivegenerationid=? " +
                            "FROM oscar_archive_activation_row exported, " + dataStreams + " d " +
                            "WHERE o.id=exported.observation_id AND d.id=o.datastreamid " +
                            "AND o.archivegenerationid IS NULL " +
                            "AND o.datastreamid=exported.datastream_id " +
                            "AND o.foiid IS NOT DISTINCT FROM exported.foi_id " +
                            "AND o.phenomenontime IS NOT DISTINCT FROM exported.phenomenon_time " +
                            "AND o.resulttime IS NOT DISTINCT FROM exported.result_time " +
                            "AND o.result IS NOT DISTINCT FROM exported.result_json " +
                            "AND o.phenomenontime>=? AND o.phenomenontime<? " +
                            "AND d.data->>'outputName'=ANY (?)";
                    try (PreparedStatement mark = connection.prepareStatement(markSql)) {
                        mark.setObject(1, generation.id());
                        UtcJdbcTime.setInstant(mark, 2, generation.start());
                        UtcJdbcTime.setInstant(mark, 3, generation.end());
                        mark.setArray(4, connection.createArrayOf("text", outputNames.toArray()));
                        marked = mark.executeUpdate();
                    }
                }
                if (marked != generation.rowCount())
                    throw new IllegalStateException("Archive snapshot changed before activation: expected " +
                            generation.rowCount() + " exact rows but marked " + marked);
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_generation(id, job_id, store_key, id_scope, profile, window_start, window_end, state, " +
                                "object_path, manifest_path, sha256, row_count, min_id, max_id, max_result_time, min_result_time) " +
                                "VALUES (?, ?, ?, ?, ?, ?, ?, 'ACTIVE', ?, ?, ?, ?, ?, ?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setObject(2, jobId);
                    statement.setString(3, storeKey);
                    statement.setInt(4, idScope);
                    statement.setString(5, generation.profile());
                    UtcJdbcTime.setInstant(statement, 6, generation.start());
                    UtcJdbcTime.setInstant(statement, 7, generation.end());
                    statement.setString(8, generation.objectPath().toString());
                    statement.setString(9, manifestPath.toString());
                    statement.setString(10, generation.sha256());
                    statement.setLong(11, generation.rowCount());
                    statement.setLong(12, generation.minimumId());
                    statement.setLong(13, generation.maximumId());
                    if (maxResultTime == null)
                        statement.setNull(14, Types.TIMESTAMP_WITH_TIMEZONE);
                    else
                        UtcJdbcTime.setInstant(statement, 14, maxResultTime);
                    if (minResultTime == null)
                        statement.setNull(15, Types.TIMESTAMP_WITH_TIMEZONE);
                    else
                        UtcJdbcTime.setInstant(statement, 15, minResultTime);
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_object(generation_id, relative_path, kind, sha256, bytes) VALUES (?, ?, ?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setString(2, generation.objectPath().toString());
                    statement.setString(3, "OBSERVATIONS_PARQUET");
                    statement.setString(4, generation.sha256());
                    statement.setLong(5, bytes);
                    statement.executeUpdate();
                    statement.setObject(1, generation.id());
                    statement.setString(2, manifestPath.toString());
                    statement.setString(3, "GENERATION_MANIFEST");
                    statement.setString(4, manifestSha256);
                    statement.setLong(5, manifestBytes);
                    statement.executeUpdate();
                    statement.setObject(1, generation.id());
                    statement.setString(2, schemaPath.toString());
                    statement.setString(3, "DATASTREAM_SCHEMAS");
                    statement.setString(4, schemaSha256);
                    statement.setLong(5, schemaBytes);
                    statement.executeUpdate();
                }
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_id_block(generation_id, min_id, max_id) VALUES (?, ?, ?)")) {
                    statement.setObject(1, generation.id());
                    statement.setLong(2, generation.minimumId());
                    statement.setLong(3, generation.maximumId());
                    statement.executeUpdate();
                }
                try (PreparedStatement blobStatement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_schema_blob(schema_sha256, schema_json) " +
                                "VALUES (?, ?::jsonb) ON CONFLICT (schema_sha256) DO NOTHING");
                     PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_archive_schema(generation_id, datastream_id, schema_sha256) VALUES (?, ?, ?)")) {
                    Set<String> insertedBlobs = new HashSet<>();
                    for (SchemaSnapshot schema : schemas) {
                        String schemaHash = sha256Hex(schema.json());
                        if (insertedBlobs.add(schemaHash)) {
                            blobStatement.setString(1, schemaHash);
                            blobStatement.setString(2, schema.json());
                            blobStatement.addBatch();
                        }
                        statement.setObject(1, generation.id());
                        statement.setLong(2, schema.dataStreamId());
                        statement.setString(3, schemaHash);
                        statement.addBatch();
                    }
                    blobStatement.executeBatch();
                    statement.executeBatch();
                }
                populateGenerationSummariesAndLatest(connection, generation);
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE oscar_archive_job SET state='ACTIVE', updated_at=now() " +
                                "WHERE id=? AND store_key=? AND id_scope=?")) {
                    statement.setObject(1, jobId);
                    statement.setString(2, storeKey);
                    statement.setInt(3, idScope);
                    statement.executeUpdate();
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
            if (resourceConfig.isParquetPrimary()) {
                try (Statement analyze = connection.createStatement()) {
                    analyze.execute("ANALYZE oscar_obs_stage_owner");
                }
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot activate archive generation", e);
        }
    }

    /**
     * Marks staged rows in each attached child partition directly rather than
     * scanning the parent inheritance tree or joining {@code oscar_obs_stage_owner}
     * against itself during {@code INSERT}. The primary-key {@code ON CONFLICT DO NOTHING}
     * clause enforces exact single-owner assignment in O(n log n) via B-tree index
     * lookup without triggering a quadratic sequential scan when {@code relpages=0}.
     */
    private int markParquetPrimaryStageOwners(Connection connection, String observations,
                                              ArchiveGeneration generation) throws SQLException {
        List<String> attachedChildren = attachedStageChildTables(connection, observations, idScope);
        int marked = 0;
        for (String child : attachedChildren) {
            String ownSql = "INSERT INTO oscar_obs_stage_owner(" +
                    "store_key,id_scope,child_table,observation_id,generation_id) " +
                    "SELECT ?,?,?,o.id,? FROM ONLY " + child + " o " +
                    "JOIN oscar_archive_activation_row exported ON o.id=exported.observation_id " +
                    "WHERE o.datastreamid=exported.datastream_id " +
                    "AND o.foiid IS NOT DISTINCT FROM exported.foi_id " +
                    "AND o.phenomenontime IS NOT DISTINCT FROM exported.phenomenon_time " +
                    "AND o.resulttime IS NOT DISTINCT FROM exported.result_time " +
                    "AND o.result IS NOT DISTINCT FROM exported.result_json " +
                    "AND o.phenomenontime>=? AND o.phenomenontime<? " +
                    "ON CONFLICT(store_key,id_scope,observation_id) DO NOTHING";
            try (PreparedStatement own = connection.prepareStatement(ownSql)) {
                own.setString(1, observations);
                own.setInt(2, idScope);
                own.setString(3, child);
                own.setObject(4, generation.id());
                UtcJdbcTime.setInstant(own, 5, generation.start());
                UtcJdbcTime.setInstant(own, 6, generation.end());
                marked = Math.addExact(marked, own.executeUpdate());
            }
        }
        return marked;
    }

    /**
     * Lists attached immutable staging partitions so stage-only queries can scan
     * {@code ONLY child} relations directly instead of scanning the mutable parent
     * observation table.
     */
    public static List<String> attachedStageChildTables(Connection connection, String observations,
                                                        int idScope) throws SQLException {
        String store = safeIdentifier(observations);
        String sql = "SELECT child_table FROM oscar_obs_stage_partition " +
                "WHERE store_key=? AND id_scope=? AND state='ATTACHED' " +
                "AND to_regclass(child_table) IS NOT NULL " +
                "ORDER BY window_start, child_table";
        List<String> children = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, store);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    children.add(safeIdentifier(result.getString(1)));
            }
        }
        return children;
    }

    /**
     * Builds a {@code FROM}-clause relation over attached stage child partitions
     * aliased as {@code o}, avoiding a parent inheritance scan across mutable
     * base-table rows.
     */
    public static String stageChildrenFromClause(List<String> attachedChildren, String columns) {
        if (attachedChildren.isEmpty())
            throw new IllegalArgumentException("At least one attached stage child is required");
        if (attachedChildren.size() == 1)
            return "ONLY " + safeIdentifier(attachedChildren.get(0)) + " o";
        StringBuilder union = new StringBuilder("(");
        for (int i = 0; i < attachedChildren.size(); i++) {
            if (i > 0)
                union.append(" UNION ALL ");
            union.append("SELECT ").append(columns)
                    .append(" FROM ONLY ").append(safeIdentifier(attachedChildren.get(i)));
        }
        return union.append(") o").toString();
    }

    private record ResultTimeBounds(Instant minResultTime, Instant maxResultTime) {}

    /**
     * Streams the immutable export back into a transaction-local relation.
     * Activation can then compare every mutable source value instead of
     * treating a coarse ID/time range and a matching count as row ownership.
     * Keeping the ledger in PostgreSQL also lets the final UPDATE lock and
     * recheck each live tuple atomically under normal READ COMMITTED rules.
     */
    private ResultTimeBounds stageExportedRows(Connection postgres, Path parquetPath,
                                               ArchiveGeneration generation) throws Exception {
        try (Statement create = postgres.createStatement()) {
            create.execute("CREATE TEMP TABLE oscar_archive_activation_row (" +
                    "observation_id bigint PRIMARY KEY, datastream_id bigint NOT NULL, foi_id bigint, " +
                    "phenomenon_time timestamp, result_time timestamp, result_json jsonb) ON COMMIT DROP");
        }

        String insert = "INSERT INTO oscar_archive_activation_row(" +
                "observation_id,datastream_id,foi_id,phenomenon_time,result_time,result_json) " +
                "VALUES (?,?,?,?::timestamp,?::timestamp,?::jsonb)";
        long count = 0;
        long minimumId = Long.MAX_VALUE;
        long maximumId = Long.MIN_VALUE;
        Instant maxResultTime = generation.maxResultTime();
        Instant minResultTime = generation.minResultTime();
        try (DuckDbResourcePolicy.Session duckSession = DuckDbResourcePolicy.open(
                resourceConfig, "activation-stage")) {
            Path localParquet = ArchiveExporter.stageToLocalDirectory(
                    parquetPath, duckSession.operationDirectory(), "activation-stage.parquet");
            String parquet = localParquet.toAbsolutePath().normalize().toString().replace("'", "''");
            String select = "SELECT id_scope, id_value, datastream_scope, datastream_id, foi_scope, foi_id, " +
                    "phenomenon_time, result_time, result_json, archive_version, profile " +
                    "FROM read_parquet('" + parquet + "')";
            try (Statement read = duckSession.connection().createStatement();
                 ResultSet rows = read.executeQuery(select);
                 PreparedStatement stage = postgres.prepareStatement(insert)) {
                while (rows.next()) {
                    int exportedIdScope = rows.getInt(1);
                    long observationId = rows.getLong(2);
                    int exportedDataStreamScope = rows.getInt(3);
                    long dataStreamId = rows.getLong(4);
                    Integer foiScope = rows.getObject(5, Integer.class);
                    Long foiId = rows.getObject(6, Long.class);
                    int archiveVersion = rows.getInt(10);
                    String profile = rows.getString(11);
                    if (exportedIdScope != idScope || exportedDataStreamScope != idScope ||
                            (foiId == null ? foiScope != null : foiScope == null || foiScope != idScope) ||
                            archiveVersion != 1 || !generation.profile().equals(profile))
                        throw new IllegalStateException("Archive activation ledger metadata does not match its generation");

                    stage.setLong(1, observationId);
                    stage.setLong(2, dataStreamId);
                    if (foiId == null)
                        stage.setNull(3, Types.BIGINT);
                    else
                        stage.setLong(3, foiId);
                    var phenomenonTime = UtcJdbcTime.getDuckDbInstant(rows, 7);
                    if (phenomenonTime == null)
                        stage.setNull(4, Types.TIMESTAMP);
                    else
                        UtcJdbcTime.setInstant(stage, 4, phenomenonTime);
                    var resultTime = UtcJdbcTime.getDuckDbInstant(rows, 8);
                    if (resultTime == null)
                        stage.setNull(5, Types.TIMESTAMP);
                    else
                        UtcJdbcTime.setInstant(stage, 5, resultTime);
                    Instant candidateResultTime = resultTime != null ? resultTime : phenomenonTime;
                    if (candidateResultTime != null) {
                        if (maxResultTime == null || candidateResultTime.isAfter(maxResultTime))
                            maxResultTime = candidateResultTime;
                        if (minResultTime == null || candidateResultTime.isBefore(minResultTime))
                            minResultTime = candidateResultTime;
                    }
                    String resultJson = rows.getString(9);
                    if (resultJson == null)
                        stage.setNull(6, Types.VARCHAR);
                    else
                        stage.setString(6, resultJson);
                    stage.addBatch();

                    count++;
                    minimumId = Math.min(minimumId, observationId);
                    maximumId = Math.max(maximumId, observationId);
                    if (count % 5_000 == 0)
                        stage.executeBatch();
                }
                stage.executeBatch();
            }
        }
        if (count != generation.rowCount() || minimumId != generation.minimumId() ||
                maximumId != generation.maximumId())
            throw new IllegalStateException("Archive activation ledger count/range does not match its generation");
        try (Statement analyze = postgres.createStatement()) {
            analyze.execute("ANALYZE oscar_archive_activation_row");
        }
        return new ResultTimeBounds(minResultTime, maxResultTime);
    }

    private void populateGenerationSummariesAndLatest(
            Connection connection, ArchiveGeneration generation) throws SQLException {
        String isUnadjudicatedJson = "((jsonb_typeof(result_json->'adjudicatedIds') = 'array' AND jsonb_array_length(result_json->'adjudicatedIds') = 0) " +
                "OR (result_json->'adjudicatedIds' IS NULL AND COALESCE(result_json->>'adjudicatedIdsCount', '0') = '0'))";
        String isAdjudicatedJson = "((jsonb_typeof(result_json->'adjudicatedIds') = 'array' AND jsonb_array_length(result_json->'adjudicatedIds') > 0) " +
                "OR (result_json->'adjudicatedIds' IS NULL AND COALESCE(result_json->>'adjudicatedIdsCount', '0') <> '0'))";
        String updateSummarySql = "UPDATE oscar_archive_schema s SET " +
                "summary_populated = true, " +
                "row_count = agg.row_count, " +
                "gamma_alarm_count = agg.gamma_alarm_count, " +
                "neutron_alarm_count = agg.neutron_alarm_count, " +
                "gamma_neutron_alarm_count = agg.gamma_neutron_alarm_count, " +
                "non_alarm_occupancy_count = agg.non_alarm_occupancy_count, " +
                "gamma_true_count = agg.gamma_true_count, " +
                "neutron_true_count = agg.neutron_true_count, " +
                "any_alarm_count = agg.any_alarm_count, " +
                "unadjudicated_alarm_count = agg.unadjudicated_alarm_count, " +
                "adjudicated_alarm_count = agg.adjudicated_alarm_count, " +
                "unadjudicated_gamma_alarm_count = agg.unadjudicated_gamma_alarm_count, " +
                "unadjudicated_neutron_alarm_count = agg.unadjudicated_neutron_alarm_count, " +
                "unadjudicated_gamma_neutron_alarm_count = agg.unadjudicated_gamma_neutron_alarm_count, " +
                "adjudicated_gamma_alarm_count = agg.adjudicated_gamma_alarm_count, " +
                "adjudicated_neutron_alarm_count = agg.adjudicated_neutron_alarm_count, " +
                "adjudicated_gamma_neutron_alarm_count = agg.adjudicated_gamma_neutron_alarm_count, " +
                "gamma_high_fault_count = agg.gamma_high_fault_count, " +
                "gamma_low_fault_count = agg.gamma_low_fault_count, " +
                "neutron_fault_count = agg.neutron_fault_count, " +
                "extended_occupancy_fault_count = agg.extended_occupancy_fault_count, " +
                "tamper_true_count = agg.tamper_true_count, " +
                "rs350_gamma_alarm_count = agg.rs350_gamma_alarm_count, " +
                "rs350_neutron_alarm_count = agg.rs350_neutron_alarm_count " +
                "FROM (" +
                "SELECT datastream_id, " +
                "count(*)::bigint AS row_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'false'::jsonb)::bigint AS gamma_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'false'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb)::bigint AS neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb)::bigint AS gamma_neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'false'::jsonb AND result_json->'neutronAlarm' = 'false'::jsonb)::bigint AS non_alarm_occupancy_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb)::bigint AS gamma_true_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'neutronAlarm' = 'true'::jsonb)::bigint AS neutron_true_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND (result_json->'gammaAlarm' = 'true'::jsonb OR result_json->'neutronAlarm' = 'true'::jsonb))::bigint AS any_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND (result_json->'gammaAlarm' = 'true'::jsonb OR result_json->'neutronAlarm' = 'true'::jsonb) AND " + isUnadjudicatedJson + ")::bigint AS unadjudicated_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND (result_json->'gammaAlarm' = 'true'::jsonb OR result_json->'neutronAlarm' = 'true'::jsonb) AND " + isAdjudicatedJson + ")::bigint AS adjudicated_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'false'::jsonb AND " + isUnadjudicatedJson + ")::bigint AS unadjudicated_gamma_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'false'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb AND " + isUnadjudicatedJson + ")::bigint AS unadjudicated_neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb AND " + isUnadjudicatedJson + ")::bigint AS unadjudicated_gamma_neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'false'::jsonb AND " + isAdjudicatedJson + ")::bigint AS adjudicated_gamma_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'false'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb AND " + isAdjudicatedJson + ")::bigint AS adjudicated_neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'gammaAlarm' = 'true'::jsonb AND result_json->'neutronAlarm' = 'true'::jsonb AND " + isAdjudicatedJson + ")::bigint AS adjudicated_gamma_neutron_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'alarmState' = '\"Fault - Gamma High\"'::jsonb)::bigint AS gamma_high_fault_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'alarmState' = '\"Fault - Gamma Low\"'::jsonb)::bigint AS gamma_low_fault_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'alarmState' = '\"Fault - Neutron\"'::jsonb)::bigint AS neutron_fault_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'alarmState' = '\"Fault - Extended Occupancy\"'::jsonb)::bigint AS extended_occupancy_fault_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND result_json->'tamperStatus' = 'true'::jsonb)::bigint AS tamper_true_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND jsonb_typeof(result_json->'derivedData'->'alarmCategory') = 'string' AND result_json->'derivedData'->>'alarmCategory' LIKE '%Gamma%')::bigint AS rs350_gamma_alarm_count, " +
                "count(*) FILTER (WHERE jsonb_typeof(result_json) = 'object' AND jsonb_typeof(result_json->'derivedData'->'alarmCategory') = 'string' AND result_json->'derivedData'->>'alarmCategory' LIKE '%Neutron%')::bigint AS rs350_neutron_alarm_count " +
                "FROM oscar_archive_activation_row GROUP BY datastream_id" +
                ") agg WHERE s.generation_id = ? AND s.datastream_id = agg.datastream_id";
        try (PreparedStatement statement = connection.prepareStatement(updateSummarySql)) {
            statement.setObject(1, generation.id());
            statement.executeUpdate();
        }

        String insertLocatorSql = "INSERT INTO oscar_event_locator(" +
                "store_key, id_scope, public_id, observation_id, generation_id, state, updated_at) " +
                "SELECT DISTINCT ON (exported.result_json->>'eventId') " +
                "?, ?, exported.result_json->>'eventId', exported.observation_id, ?, 'ACTIVE', now() " +
                "FROM oscar_archive_activation_row exported " +
                "WHERE jsonb_typeof(exported.result_json) = 'object' " +
                "AND jsonb_typeof(exported.result_json->'eventId') = 'string' " +
                "AND length(exported.result_json->>'eventId') BETWEEN 1 AND 256 " +
                "ORDER BY exported.result_json->>'eventId', exported.observation_id DESC " +
                "ON CONFLICT (store_key, id_scope, public_id) DO UPDATE SET " +
                "observation_id = EXCLUDED.observation_id, " +
                "generation_id = EXCLUDED.generation_id, " +
                "state = 'ACTIVE', " +
                "updated_at = now()";
        try (PreparedStatement statement = connection.prepareStatement(insertLocatorSql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setObject(3, generation.id());
            statement.executeUpdate();
        }

        String upsertLatestSql = "INSERT INTO oscar_obs_latest(" +
                "store_key, id_scope, datastream_id, generation_id, observation_id, " +
                "foi_id, phenomenon_time, result_time, result_json, schema_json, profile, updated_at) " +
                "SELECT DISTINCT ON (exported.datastream_id) " +
                "?, ?, exported.datastream_id, ?, exported.observation_id, " +
                "exported.foi_id, " +
                "exported.phenomenon_time AT TIME ZONE 'UTC', " +
                "exported.result_time AT TIME ZONE 'UTC', " +
                "exported.result_json, " +
                "COALESCE(s.schema_json, blob.schema_json), " +
                "?, now() " +
                "FROM oscar_archive_activation_row exported " +
                "JOIN oscar_archive_schema s ON s.generation_id = ? AND s.datastream_id = exported.datastream_id " +
                "LEFT JOIN oscar_archive_schema_blob blob ON blob.schema_sha256 = s.schema_sha256 " +
                "WHERE COALESCE(s.schema_json, blob.schema_json) IS NOT NULL " +
                "ORDER BY exported.datastream_id, " +
                "exported.phenomenon_time DESC NULLS LAST, " +
                "exported.result_time DESC NULLS LAST, " +
                "exported.observation_id DESC " +
                "ON CONFLICT (store_key, id_scope, datastream_id) DO UPDATE SET " +
                "generation_id = EXCLUDED.generation_id, " +
                "observation_id = EXCLUDED.observation_id, " +
                "foi_id = EXCLUDED.foi_id, " +
                "phenomenon_time = EXCLUDED.phenomenon_time, " +
                "result_time = EXCLUDED.result_time, " +
                "result_json = EXCLUDED.result_json, " +
                "schema_json = EXCLUDED.schema_json, " +
                "profile = EXCLUDED.profile, " +
                "updated_at = now() " +
                "WHERE oscar_obs_latest.phenomenon_time IS NULL " +
                "OR (EXCLUDED.phenomenon_time IS NOT NULL AND (" +
                "EXCLUDED.phenomenon_time > oscar_obs_latest.phenomenon_time " +
                "OR (EXCLUDED.phenomenon_time = oscar_obs_latest.phenomenon_time " +
                "AND EXCLUDED.observation_id >= oscar_obs_latest.observation_id)))";
        try (PreparedStatement statement = connection.prepareStatement(upsertLatestSql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setObject(3, generation.id());
            statement.setString(4, generation.profile());
            statement.setObject(5, generation.id());
            statement.executeUpdate();
        }
    }

    public void failJob(UUID jobId, Throwable error) {
        String message = error == null ? "unknown failure" : String.valueOf(error.getMessage());
        if (message.length() > 4000)
            message = message.substring(0, 4000);
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "UPDATE oscar_archive_job SET state='FAILED', error=?, updated_at=now() " +
                        "WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setString(1, message);
            statement.setObject(2, jobId);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot record archive failure", e);
        }
    }

    /**
     * Completes a reclaimed job that no longer owns any eligible source rows.
     * This is an audited no-op, not an export failure: concurrent removal or
     * an overlapping recovery may legitimately make an old retry window empty.
     */
    public void supersedeEmptyJob(UUID jobId, String detail) {
        String message = detail == null ? "No eligible source rows remain" : detail;
        if (message.length() > 4000)
            message = message.substring(0, 4000);
        String sql = "UPDATE oscar_archive_job job SET state='SUPERSEDED', error=?, updated_at=now() " +
                "WHERE job.id=? AND job.store_key=? AND job.id_scope=? AND job.state='EXPORTING' " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_archive_generation generation WHERE generation.job_id=job.id)";
        try (Connection connection = connections.open(); PreparedStatement statement =
                connection.prepareStatement(sql)) {
            statement.setString(1, message);
            statement.setObject(2, jobId);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            if (statement.executeUpdate() != 1)
                throw new IllegalStateException("Empty archive job was not eligible for supersession");
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot supersede empty archive job", e);
        }
    }

    /** True only after activation has committed the generation catalog row. */
    public boolean isGenerationRegistered(UUID generationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_archive_generation WHERE id=? AND store_key=? AND id_scope=?")) {
            statement.setObject(1, generationId);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive generation registration", e);
        }
    }

    static boolean shouldRetryJob(String state, boolean stale) {
        return "FAILED".equals(state) || ("EXPORTING".equals(state) && stale);
    }

    static int nextRevision(int previousRevision) {
        return Math.incrementExact(previousRevision);
    }

    public List<ArchiveGeneration> candidates(Instant start, Instant end, Long observationId) {
        return candidates(start, end, observationId, null, null, null);
    }

    public List<ArchiveGeneration> candidates(Instant start, Instant end, Long observationId,
                                              Long minObservationId, Long maxObservationId,
                                              Set<Long> dataStreams) {
        return candidates(start, end, observationId, minObservationId, maxObservationId, dataStreams, null, null);
    }

    public List<ArchiveGeneration> candidates(Instant start, Instant end, Long observationId,
                                              Long minObservationId, Long maxObservationId,
                                              Set<Long> dataStreams, Instant minResultTime) {
        return candidates(start, end, observationId, minObservationId, maxObservationId, dataStreams, minResultTime, null);
    }

    public List<ArchiveGeneration> candidates(Instant start, Instant end, Long observationId,
                                              Long minObservationId, Long maxObservationId,
                                              Set<Long> dataStreams, Instant minResultTime,
                                              Instant maxResultTime) {
        if (dataStreams != null && dataStreams.isEmpty())
            return List.of();
        try (Connection connection = connections.open()) {
            return candidates(connection, start, end, observationId,
                    minObservationId, maxObservationId, dataStreams, minResultTime, maxResultTime);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot plan archive query", e);
        }
    }

    /**
     * Connection-scoped candidate lookup for callers already holding archive
     * mutation locks.  Reusing that transaction avoids nested pool borrows and
     * the resulting pool-exhaustion deadlock under concurrent cold mutations.
     */
    public List<ArchiveGeneration> candidates(Connection connection, Instant start,
                                               Instant end, Long observationId)
            throws SQLException {
        return candidates(connection, start, end, observationId, null, null, null, null, null);
    }

    public List<ArchiveGeneration> candidates(Connection connection, Instant start,
                                               Instant end, Long observationId,
                                               Long minObservationId, Long maxObservationId,
                                               Set<Long> dataStreams)
            throws SQLException {
        return candidates(connection, start, end, observationId,
                minObservationId, maxObservationId, dataStreams, null, null);
    }

    public List<ArchiveGeneration> candidates(Connection connection, Instant start,
                                               Instant end, Long observationId,
                                               Long minObservationId, Long maxObservationId,
                                               Set<Long> dataStreams, Instant minResultTime)
            throws SQLException {
        return candidates(connection, start, end, observationId,
                minObservationId, maxObservationId, dataStreams, minResultTime, null);
    }

    public List<ArchiveGeneration> candidates(Connection connection, Instant start,
                                               Instant end, Long observationId,
                                               Long minObservationId, Long maxObservationId,
                                               Set<Long> dataStreams, Instant minResultTime,
                                               Instant maxResultTime)
            throws SQLException {
        if (dataStreams != null && dataStreams.isEmpty())
            return List.of();
        boolean filterIdRange = observationId == null &&
                minObservationId != null && maxObservationId != null;
        boolean filterDataStreams = dataStreams != null;
        String sql = (observationId != null ? "SELECT DISTINCT " : "SELECT ") +
                "g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified, g.max_result_time, g.min_result_time " +
                "FROM oscar_archive_generation g ";
        if (observationId != null)
            sql += "JOIN oscar_archive_id_block b ON b.generation_id=g.id ";
        sql += "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND g.window_start < ? AND g.window_end > ? ";
        if (minResultTime != null)
            sql += "AND (g.max_result_time IS NULL OR g.max_result_time >= ?) ";
        if (maxResultTime != null)
            sql += "AND (g.min_result_time IS NULL OR g.min_result_time <= ?) ";
        if (observationId != null)
            sql += "AND ? BETWEEN b.min_id AND b.max_id ";
        if (filterIdRange)
            sql += "AND g.max_id >= ? AND g.min_id <= ? ";
        if (filterDataStreams)
            sql += "AND EXISTS (SELECT 1 FROM oscar_archive_schema s " +
                    "WHERE s.generation_id=g.id AND s.datastream_id=ANY (?::bigint[])) ";
        sql += "ORDER BY g.window_start, g.id";

        List<ArchiveGeneration> generations = new ArrayList<>();
        java.sql.Array streamArray = filterDataStreams
                ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                : null;
        try {
            if (streamArray != null && minResultTime == null && maxResultTime == null) {
                try (PreparedStatement hasAny = connection.prepareStatement(
                        "SELECT 1 FROM oscar_archive_schema WHERE datastream_id=ANY (?::bigint[]) LIMIT 1")) {
                    hasAny.setArray(1, streamArray);
                    try (ResultSet rs = hasAny.executeQuery()) {
                        if (!rs.next())
                            return generations;
                    }
                }
            }
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int parameter = 1;
                statement.setString(parameter++, storeKey);
                statement.setInt(parameter++, idScope);
                UtcJdbcTime.setInstant(statement, parameter++, end);
                UtcJdbcTime.setInstant(statement, parameter++, start);
                if (minResultTime != null)
                    UtcJdbcTime.setInstant(statement, parameter++, minResultTime);
                if (maxResultTime != null)
                    UtcJdbcTime.setInstant(statement, parameter++, maxResultTime);
                if (observationId != null)
                    statement.setLong(parameter++, observationId);
                if (filterIdRange) {
                    statement.setLong(parameter++, minObservationId);
                    statement.setLong(parameter++, maxObservationId);
                }
                if (streamArray != null)
                    statement.setArray(parameter, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        generations.add(readGeneration(result));
                    }
                }
            }
        } finally {
            if (streamArray != null)
                streamArray.free();
        }
        return generations;
    }

    /**
     * Resolves every active/sealed archive generation whose ID block covers at
     * least one requested observation ID in a single PostgreSQL round trip.
     */
    public List<ArchiveGeneration> candidatesForObservationIds(Collection<Long> observationIds) {
        if (observationIds == null || observationIds.isEmpty())
            return List.of();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            return candidatesForObservationIds(connection, observationIds);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot plan archive point-ID query", e);
        }
    }

    public List<ArchiveGeneration> candidatesForObservationIds(
            Connection connection, Collection<Long> observationIds) throws SQLException {
        if (observationIds == null || observationIds.isEmpty())
            return List.of();
        String sql = "WITH requested(id) AS (SELECT DISTINCT unnest(?::bigint[])) " +
                "SELECT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified, g.max_result_time, g.min_result_time " +
                "FROM oscar_archive_generation g " +
                "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND EXISTS (SELECT 1 FROM requested r JOIN oscar_archive_id_block b " +
                "ON r.id BETWEEN b.min_id AND b.max_id WHERE b.generation_id=g.id) " +
                "ORDER BY g.window_start, g.id";
        List<ArchiveGeneration> generations = new ArrayList<>();
        java.sql.Array ids = connection.createArrayOf("bigint", observationIds.toArray(Long[]::new));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, ids);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    generations.add(readGeneration(result));
            }
        } finally {
            ids.free();
        }
        return generations;
    }

    public boolean isGenerationQueryVisible(UUID generationId) {
        if (generationId == null)
            return false;
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT 1 FROM oscar_archive_generation " +
                             "WHERE id=? AND store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED')")) {
            connection.setReadOnly(true);
            statement.setObject(1, generationId);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive generation visibility", e);
        }
    }

    public Set<UUID> activeOrSealedGenerationIds(Connection connection, Collection<UUID> generationIds)
            throws SQLException {
        if (generationIds == null || generationIds.isEmpty())
            return Set.of();
        Set<UUID> visible = new HashSet<>();
        java.sql.Array genArray = connection.createArrayOf("uuid", generationIds.toArray(UUID[]::new));
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT id FROM oscar_archive_generation " +
                        "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED') " +
                        "AND id=ANY (?::uuid[])")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setArray(3, genArray);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    visible.add(result.getObject(1, UUID.class));
            }
        } finally {
            genArray.free();
        }
        return visible;
    }

    /**
     * Pushes {@code resultTime} lower-bound / {@code resultTime=latest} high-water
     * filtering into PostgreSQL so multi-year archives do not materialize tens of
     * thousands of historical generations whose {@code max_result_time} is older
     * than the requested lower bound or hot tier.
     */
    public List<ArchiveGeneration> candidatesPotentiallyNewerThanResultTime(
            Instant start, Instant end, Set<Long> dataStreams, Instant minResultTime) {
        return candidates(start, end, null, null, null, dataStreams, minResultTime, null);
    }

    /**
     * Backfills {@code max_result_time} on a pre-upgrade generation row once its
     * Parquet metadata has been inspected so future queries filter it in SQL.
     */
    public void backfillMaxResultTime(UUID generationId, Instant maxResultTime) {
        if (generationId == null || maxResultTime == null)
            return;
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(
                     "UPDATE oscar_archive_generation SET max_result_time=?, " +
                             "min_result_time=COALESCE(min_result_time, LEAST(window_start, ?)) " +
                             "WHERE id=? AND store_key=? AND id_scope=? AND max_result_time IS NULL")) {
            UtcJdbcTime.setInstant(statement, 1, maxResultTime);
            UtcJdbcTime.setInstant(statement, 2, maxResultTime);
            statement.setObject(3, generationId);
            statement.setString(4, storeKey);
            statement.setInt(5, idScope);
            statement.executeUpdate();
        } catch (SQLException ignored) {
            // Best-effort metadata cache persistence; in-memory SHA-256 cache remains authoritative.
        }
    }

    public record InitialCandidateBatch(List<ArchiveGeneration> generations, boolean complete) {
    }

    /**
     * Fetches a bounded initial window-cluster prefix for {@code LIMIT}-bounded
     * queries so time-unbounded queries over multi-year archives do not load
     * all {@code 87,600+} catalog rows when the first window cluster already
     * satisfies the caller's limit.
     */
    public InitialCandidateBatch candidatesInitialClusters(
            Instant start, Instant end, Set<Long> dataStreams,
            boolean descending, int batchLimit) {
        return candidatesInitialClusters(start, end, dataStreams, null, null, descending, batchLimit);
    }

    public InitialCandidateBatch candidatesInitialClusters(
            Instant start, Instant end, Set<Long> dataStreams, Instant minResultTime,
            boolean descending, int batchLimit) {
        return candidatesInitialClusters(start, end, dataStreams, minResultTime, null, descending, batchLimit);
    }

    public InitialCandidateBatch candidatesInitialClusters(
            Instant start, Instant end, Set<Long> dataStreams, Instant minResultTime,
            Instant maxResultTime, boolean descending, int batchLimit) {
        if (batchLimit < 2)
            throw new IllegalArgumentException("Candidate batch limit must be at least 2");
        if (dataStreams != null && dataStreams.isEmpty())
            return new InitialCandidateBatch(List.of(), true);
        boolean filterDataStreams = dataStreams != null;
        String orderBy = descending
                ? "ORDER BY g.window_end DESC, g.window_start DESC, g.id ASC LIMIT ?"
                : "ORDER BY g.window_start ASC, g.window_end ASC, g.id ASC LIMIT ?";
        String sql = "SELECT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified, g.max_result_time, g.min_result_time " +
                "FROM oscar_archive_generation g " +
                "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND g.window_start < ? AND g.window_end > ? ";
        if (minResultTime != null)
            sql += "AND (g.max_result_time IS NULL OR g.max_result_time >= ?) ";
        if (maxResultTime != null)
            sql += "AND (g.min_result_time IS NULL OR g.min_result_time <= ?) ";
        if (filterDataStreams)
            sql += "AND EXISTS (SELECT 1 FROM oscar_archive_schema s " +
                    "WHERE s.generation_id=g.id AND s.datastream_id=ANY (?::bigint[])) ";
        sql += orderBy;

        List<ArchiveGeneration> fetched = new ArrayList<>();
        try (Connection connection = connections.open()) {
            java.sql.Array streamArray = filterDataStreams
                    ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                    : null;
            try {
                if (streamArray != null) {
                    try (PreparedStatement hasAny = connection.prepareStatement(
                            "SELECT 1 FROM oscar_archive_schema WHERE datastream_id=ANY (?::bigint[]) LIMIT 1")) {
                        hasAny.setArray(1, streamArray);
                        try (ResultSet rs = hasAny.executeQuery()) {
                            if (!rs.next())
                                return new InitialCandidateBatch(List.of(), true);
                        }
                    }
                }
                try (PreparedStatement statement = connection.prepareStatement(sql)) {
                    int parameter = 1;
                    statement.setString(parameter++, storeKey);
                    statement.setInt(parameter++, idScope);
                    UtcJdbcTime.setInstant(statement, parameter++, end);
                    UtcJdbcTime.setInstant(statement, parameter++, start);
                    if (minResultTime != null)
                        UtcJdbcTime.setInstant(statement, parameter++, minResultTime);
                    if (maxResultTime != null)
                        UtcJdbcTime.setInstant(statement, parameter++, maxResultTime);
                    if (streamArray != null)
                        statement.setArray(parameter++, streamArray);
                    statement.setInt(parameter, Math.addExact(batchLimit, 1));
                    try (ResultSet result = statement.executeQuery()) {
                        while (result.next())
                            fetched.add(readGeneration(result));
                    }
                }
            } finally {
                if (streamArray != null)
                    streamArray.free();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot plan bounded archive query", e);
        }

        if (fetched.size() <= batchLimit) {
            fetched.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                    .thenComparing(ArchiveGeneration::id));
            return new InitialCandidateBatch(fetched, true);
        }

        List<ArchiveGeneration> sample = new ArrayList<>(fetched.subList(0, batchLimit));
        sample.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                .thenComparing(ArchiveGeneration::end)
                .thenComparing(ArchiveGeneration::id));
        List<List<ArchiveGeneration>> clusters = new ArrayList<>();
        Instant clusterEnd = null;
        for (ArchiveGeneration generation : sample) {
            if (clusters.isEmpty() || !generation.start().isBefore(clusterEnd)) {
                clusters.add(new ArrayList<>());
                clusterEnd = generation.end();
            } else if (generation.end().isAfter(clusterEnd)) {
                clusterEnd = generation.end();
            }
            clusters.get(clusters.size() - 1).add(generation);
        }
        if (clusters.size() < 2) {
            List<ArchiveGeneration> all = candidates(
                    start, end, null, null, null, dataStreams, minResultTime, maxResultTime);
            return new InitialCandidateBatch(all, true);
        }
        List<List<ArchiveGeneration>> completeClusters = descending
                ? clusters.subList(1, clusters.size())
                : clusters.subList(0, clusters.size() - 1);
        List<ArchiveGeneration> completePrefix = new ArrayList<>();
        for (List<ArchiveGeneration> cluster : completeClusters)
            completePrefix.addAll(cluster);
        completePrefix.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                .thenComparing(ArchiveGeneration::id));
        return new InitialCandidateBatch(completePrefix, false);
    }

    /**
     * Rebuilds B-tree indexes online on the mutable observation parent table
     * and its companion datastream metadata table so multi-year DELETE/UPDATE
     * churn cannot accumulate unbounded dead index pages.
     */
    public void reindexMutableTables(String observationTable, String dataStreamTable) {
        String observations = safeIdentifier(observationTable);
        String dataStreams = safeIdentifier(dataStreamTable);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            if (!oldAutoCommit)
                connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                if (resourceConfig.isParquetPrimary()) {
                    deduplicateSingletonParentRows(statement, observations, dataStreams);
                    statement.execute("REINDEX TABLE CONCURRENTLY " + observations);
                    statement.execute("REINDEX TABLE CONCURRENTLY oscar_obs_stage_owner");
                }
                statement.execute("REINDEX TABLE CONCURRENTLY oscar_archive_schema");
                statement.execute("REINDEX TABLE CONCURRENTLY " + dataStreams);
            } finally {
                if (!oldAutoCommit)
                    connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot reindex mutable observation tables", e);
        }
    }

    /**
     * Reclaims dead heap and index pages from the parent observation table once
     * legacy rows of newly-allowlisted streams have been drained into stage
     * partitions.
     */
    public void compactDrainedParentTable(String observationTable, String dataStreamTable) {
        String observations = safeIdentifier(observationTable);
        String dataStreams = safeIdentifier(dataStreamTable);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            if (!oldAutoCommit)
                connection.setAutoCommit(true);
            try (Statement statement = connection.createStatement()) {
                if (resourceConfig.isParquetPrimary()) {
                    deduplicateSingletonParentRows(statement, observations, dataStreams);
                    statement.execute("VACUUM (FULL, ANALYZE) oscar_obs_stage_owner");
                }
                statement.execute("VACUUM (FULL, ANALYZE) " + observations);
                statement.execute("VACUUM (FULL, ANALYZE) oscar_archive_schema");
                statement.execute("REINDEX TABLE CONCURRENTLY " + dataStreams);
            } finally {
                if (!oldAutoCommit)
                    connection.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot compact drained observation parent table", e);
        }
    }

    private void deduplicateSingletonParentRows(Statement statement,
                                                String observations,
                                                String dataStreams) throws SQLException {
        statement.execute(
                "DELETE FROM ONLY " + observations + " o USING (" +
                "  SELECT o2.id FROM ONLY " + observations + " o2 " +
                "  JOIN " + dataStreams + " d ON d.id = o2.datastreamid " +
                "  WHERE COALESCE(d.data->>'outputName', d.data->'recordSchema'->>'name', '') " +
                "        IN ('sensorLocation', 'adjudicationCodes', 'siteInfo') " +
                "    AND EXISTS (" +
                "      SELECT 1 FROM ONLY " + observations + " newer " +
                "      WHERE newer.datastreamid = o2.datastreamid " +
                "        AND (newer.phenomenontime > o2.phenomenontime " +
                "             OR (newer.phenomenontime = o2.phenomenontime AND newer.id > o2.id))" +
                "    )" +
                ") dup WHERE o.id = dup.id");
    }

    /**
     * Incrementally populates per-datastream summary counters, event locators,
     * and {@code oscar_obs_latest} for pre-upgrade archive generations.
     */
    public int backfillGenerationSummariesBatch(Path archiveRoot, int maxGenerations) {
        if (archiveRoot == null || maxGenerations < 1)
            return 0;
        try (Connection connection = connections.open();
             PreparedStatement fastPath = connection.prepareStatement(
                     "UPDATE oscar_archive_schema s SET " +
                     "summary_populated = TRUE, " +
                     "unadjudicated_gamma_alarm_count = COALESCE(s.unadjudicated_gamma_alarm_count, 0), " +
                     "unadjudicated_neutron_alarm_count = COALESCE(s.unadjudicated_neutron_alarm_count, 0), " +
                     "unadjudicated_gamma_neutron_alarm_count = COALESCE(s.unadjudicated_gamma_neutron_alarm_count, 0), " +
                     "adjudicated_gamma_alarm_count = COALESCE(s.adjudicated_gamma_alarm_count, 0), " +
                     "adjudicated_neutron_alarm_count = COALESCE(s.adjudicated_neutron_alarm_count, 0), " +
                     "adjudicated_gamma_neutron_alarm_count = COALESCE(s.adjudicated_gamma_neutron_alarm_count, 0) " +
                     "FROM oscar_archive_generation g, " + safeIdentifier(storeKey + "_datastreams") + " d, oscar_obs_latest l " +
                     "WHERE g.id = s.generation_id AND g.store_key=? AND g.id_scope=? " +
                     "AND (NOT s.summary_populated OR s.unadjudicated_gamma_alarm_count IS NULL) " +
                     "AND d.id = s.datastream_id " +
                     "AND l.store_key = g.store_key AND l.id_scope = g.id_scope AND l.datastream_id = s.datastream_id " +
                     "AND l.phenomenon_time >= g.window_end " +
                     "AND COALESCE(d.data->>'outputName', d.data->'recordSchema'->>'name', '') " +
                     "    NOT IN ('occupancy', 'tamper', 'connectionStatus', 'occupancyStatus', 'adjudication', 'report')")) {
            fastPath.setString(1, storeKey);
            fastPath.setInt(2, idScope);
            fastPath.executeUpdate();
        } catch (Exception ignored) {
            // Optional fast-path when datastream table is accessible.
        }
        List<ArchiveGeneration> pending = new ArrayList<>();
        String sql = "SELECT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified, g.max_result_time, g.min_result_time " +
                "FROM oscar_archive_generation g " +
                "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND EXISTS (SELECT 1 FROM oscar_archive_schema s " +
                "WHERE s.generation_id=g.id AND (NOT s.summary_populated OR s.unadjudicated_gamma_alarm_count IS NULL)) " +
                "ORDER BY g.window_end DESC, g.id LIMIT ?";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setInt(3, maxGenerations);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    pending.add(readGeneration(result));
            }
        } catch (SQLException e) {
            return 0;
        }
        int backfilled = 0;
        for (ArchiveGeneration generation : pending) {
            try (Connection connection = connections.open()) {
                boolean oldAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    Path normalizedRoot = archiveRoot.toAbsolutePath().normalize();
                    Path parquetPath = normalizedRoot.resolve(generation.objectPath()).normalize();
                    if (!parquetPath.startsWith(normalizedRoot))
                        throw new IllegalStateException("Archive object path escapes root");
                    ResultTimeBounds bounds = stageExportedRows(connection, parquetPath, generation);
                    if (bounds.minResultTime() != null || bounds.maxResultTime() != null) {
                        try (PreparedStatement updateBounds = connection.prepareStatement(
                                "UPDATE oscar_archive_generation " +
                                        "SET min_result_time = COALESCE(min_result_time, ?), " +
                                        "max_result_time = COALESCE(max_result_time, ?) " +
                                        "WHERE id=? AND store_key=? AND id_scope=?")) {
                            UtcJdbcTime.setInstant(updateBounds, 1, bounds.minResultTime());
                            UtcJdbcTime.setInstant(updateBounds, 2, bounds.maxResultTime());
                            updateBounds.setObject(3, generation.id());
                            updateBounds.setString(4, storeKey);
                            updateBounds.setInt(5, idScope);
                            updateBounds.executeUpdate();
                        }
                    }
                    populateGenerationSummariesAndLatest(connection, generation);
                    connection.commit();
                    backfilled++;
                } catch (Exception ignored) {
                    connection.rollback();
                } finally {
                    connection.setAutoCommit(oldAutoCommit);
                }
            } catch (Exception ignored) {
                // Best-effort background backfill; skips missing/corrupt test fixtures.
            }
        }
        return backfilled;
    }

    /**
     * Returns the subset of {@code dataStreams} that appear in at least one
     * active or sealed archive generation overlapping {@code [start, end)}.
     * Uses {@code oscar_archive_schema_datastream_idx} so datastreams with no
     * archived observations are excluded without scanning Parquet files.
     */
    public Set<Long> filterPresentDataStreams(Instant start, Instant end, Collection<Long> dataStreams) {
        if (dataStreams == null || dataStreams.isEmpty() || !start.isBefore(end))
            return Set.of();
        String sql = "SELECT u.ds_id " +
                "FROM unnest(?::bigint[]) AS u(ds_id) " +
                "WHERE EXISTS (" +
                "SELECT 1 FROM oscar_archive_schema s " +
                "JOIN oscar_archive_generation g ON g.id = s.generation_id " +
                "WHERE s.datastream_id = u.ds_id " +
                "AND g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                "AND g.window_start < ? AND g.window_end > ?)";
        Set<Long> present = new HashSet<>();
        try (Connection connection = connections.open()) {
            java.sql.Array streamArray = connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, streamArray);
                statement.setString(2, storeKey);
                statement.setInt(3, idScope);
                UtcJdbcTime.setInstant(statement, 4, end);
                UtcJdbcTime.setInstant(statement, 5, start);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next())
                        present.add(result.getLong(1));
                }
            } finally {
                streamArray.free();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archived datastreams", e);
        }
        return present;
    }

    private static ArchiveGeneration readGeneration(ResultSet result) throws SQLException {
        return new ArchiveGeneration(
                result.getObject(1, UUID.class),
                result.getString(2),
                UtcJdbcTime.getInstant(result, 3),
                UtcJdbcTime.getInstant(result, 4),
                Path.of(result.getString(5)),
                result.getString(6),
                result.getLong(7),
                result.getLong(8),
                result.getLong(9),
                result.getString(10),
                result.getBoolean(11),
                UtcJdbcTime.getInstant(result, 12),
                UtcJdbcTime.getInstant(result, 13));
    }

    public Optional<Instant> latestWindowEnd(long dataStreamId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(g.window_end) FROM oscar_archive_generation g " +
                        "JOIN oscar_archive_schema s ON s.generation_id=g.id " +
                        "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED') " +
                        "AND s.datastream_id=?")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setLong(3, dataStreamId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? Optional.empty() : Optional.of(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect datastream archive high-water mark", e);
        }
    }

    public Optional<Instant> latestWindowEnd(String profile) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(window_end) FROM oscar_archive_generation WHERE store_key=? AND id_scope=? " +
                        "AND profile=? AND state IN ('ACTIVE','SEALED')")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, profile);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? Optional.empty() : Optional.of(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive high-water mark", e);
        }
    }

    /**
     * Conservative observation high-water mark across every query-visible
     * archive profile in this store namespace. A window end can be later than
     * its last observation, which is intentional: it only causes a bounded
     * cold identity probe for an out-of-order add and never skips one.
     */
    public Optional<Instant> latestWindowEnd() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(window_end) FROM oscar_archive_generation " +
                        "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED')")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next()) return Optional.empty();
                Instant value = UtcJdbcTime.getInstant(result, 1);
                return value == null ? Optional.empty() : Optional.of(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive high-water mark", e);
        }
    }

    /**
     * Durable observation ID high-water for this exact store namespace.  All
     * catalog states are included so IDs are never reused after a generation
     * is sealed or purged and an external reference may still exist.
     */
    public Optional<Long> maximumObservationId() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT max(max_id) FROM oscar_archive_generation WHERE store_key=? AND id_scope=?")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return Optional.empty();
                Long value = result.getObject(1, Long.class);
                return Optional.ofNullable(value);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archived observation ID high-water", e);
        }
    }

    public void recordVerification(UUID generationId, String type, boolean success,
                                   long expectedRows, long actualRows, String detail) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try (PreparedStatement receipt = connection.prepareStatement(
                    "INSERT INTO oscar_archive_verification(generation_id, verification_type, status, expected_rows, actual_rows, detail) " +
                            "SELECT generation.id, ?, ?, ?, ?, ? FROM oscar_archive_generation generation " +
                            "WHERE generation.id=? AND generation.store_key=? AND generation.id_scope=?");
                 PreparedStatement generation = connection.prepareStatement(
                         "UPDATE oscar_archive_generation SET parity_verified=? " +
                                 "WHERE id=? AND store_key=? AND id_scope=?")) {
                receipt.setString(1, type);
                receipt.setString(2, success ? "PASSED" : "FAILED");
                receipt.setLong(3, expectedRows);
                receipt.setLong(4, actualRows);
                receipt.setString(5, detail);
                receipt.setObject(6, generationId);
                receipt.setString(7, storeKey);
                receipt.setInt(8, idScope);
                if (receipt.executeUpdate() != 1)
                    throw new IllegalArgumentException("Archive generation does not belong to this observation store: " + generationId);
                if ("HOT_COLD_PARITY".equals(type)) {
                    generation.setBoolean(1, success);
                    generation.setObject(2, generationId);
                    generation.setString(3, storeKey);
                    generation.setInt(4, idScope);
                    if (generation.executeUpdate() != 1)
                        throw new IllegalStateException("Archive generation disappeared while recording parity: " + generationId);
                }
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot record archive verification", e);
        }
    }

    public List<ArchiveGeneration> readyForHotDeletion(Instant activatedBefore) {
        return readyForHotDeletion(activatedBefore, 10);
    }

    public List<ArchiveGeneration> readyForHotDeletion(Instant activatedBefore, int limit) {
        if (limit < 1)
            throw new IllegalArgumentException("Hot-deletion generation limit must be positive");
        List<ArchiveGeneration> generations = new ArrayList<>();
        String sql = "SELECT g.id, g.profile, g.window_start, g.window_end, g.object_path, g.sha256, " +
                "g.row_count, g.min_id, g.max_id, g.state, g.parity_verified, g.max_result_time, g.min_result_time " +
                "FROM oscar_archive_generation g WHERE g.store_key=? AND g.id_scope=? " +
                "AND g.state='ACTIVE' AND g.parity_verified=true " +
                "AND g.activated_at <= ? AND NOT EXISTS (SELECT 1 FROM oscar_retention_hold h " +
                "WHERE h.store_key=g.store_key AND h.id_scope=g.id_scope AND h.released_at IS NULL " +
                "AND h.subject_type='ARCHIVE_GENERATION' AND h.subject_id=g.id::text) " +
                "ORDER BY g.activated_at LIMIT ?";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            UtcJdbcTime.setInstant(statement, 3, activatedBefore);
            statement.setInt(4, limit);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    generations.add(readGeneration(result));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot list generations ready for hot deletion", e);
        }
        return generations;
    }

    public List<ArchiveGeneration> unverifiedGenerations(int limit) {
        List<ArchiveGeneration> generations = new ArrayList<>();
        String sql = "SELECT id, profile, window_start, window_end, object_path, sha256, row_count, min_id, max_id, state, parity_verified, max_result_time, min_result_time " +
                "FROM oscar_archive_generation WHERE store_key=? AND id_scope=? " +
                        "AND state='ACTIVE' AND parity_verified=false ORDER BY activated_at LIMIT ?";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setInt(3, limit);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    generations.add(readGeneration(result));
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot list unverified archive generations", e);
        }
        return generations;
    }

    public void markSealed(UUID generationId) {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                // Sealing is a store-wide lifecycle boundary.  Serialize it
                // with all logical mutations just like activation, after all
                // per-ID hot-deletion transactions have completed.
                lockForActivation(connection);
                try (PreparedStatement statement = connection.prepareStatement(
                        "UPDATE oscar_archive_generation SET state='SEALED', sealed_at=now() " +
                                "WHERE id=? AND store_key=? AND id_scope=? AND state='ACTIVE' AND parity_verified=true")) {
                    statement.setObject(1, generationId);
                    statement.setString(2, storeKey);
                    statement.setInt(3, idScope);
                    if (statement.executeUpdate() != 1)
                        throw new IllegalStateException("Generation is not eligible to seal: " + generationId);
                }
                connection.commit();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot seal archive generation", e);
        }
    }

    public boolean hasObservationHold(long observationId) {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_retention_hold WHERE store_key=? AND id_scope=? AND released_at IS NULL " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect observation hold", e);
        }
    }

    public Optional<String> schemaJson(UUID generationId, long dataStreamId) {
        try (Connection connection = connections.open()) {
            return schemaJson(connection, generationId, dataStreamId);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot load archived datastream schema", e);
        }
    }

    public Optional<String> schemaJson(Connection connection, UUID generationId, long dataStreamId)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT COALESCE(schema.schema_json, blob.schema_json)::text FROM oscar_archive_schema schema " +
                        "LEFT JOIN oscar_archive_schema_blob blob ON blob.schema_sha256=schema.schema_sha256 " +
                        "JOIN oscar_archive_generation generation ON generation.id=schema.generation_id " +
                        "WHERE schema.generation_id=? AND schema.datastream_id=? " +
                        "AND generation.store_key=? AND generation.id_scope=?")) {
            statement.setObject(1, generationId);
            statement.setLong(2, dataStreamId);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? Optional.ofNullable(result.getString(1)) : Optional.empty();
            }
        }
    }

    public static String computeSchemaSha256(String schemaJson) {
        return sha256Hex(schemaJson);
    }

    public void recordPersistedSchemaBlobHashes(Collection<String> hashes) {
        if (hashes == null || hashes.isEmpty() || persistedSchemaBlobHashes.size() >= 4096)
            return;
        for (String hash : hashes) {
            if (hash != null && persistedSchemaBlobHashes.size() < 4096)
                persistedSchemaBlobHashes.add(hash);
        }
    }

    public record PointLookupStatus(boolean tombstoned, ArchivedOverlay overlay) {}

    /**
     * Checks both active/sealed observation tombstone status and overlay state
     * in a single PostgreSQL round trip, preserving tombstone precedence.
     */
    public PointLookupStatus lookupOverlayOrTombstone(long observationId, int scope, String profile) {
        String sql = "SELECT (" +
                "SELECT EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_type='OBSERVATION' AND tombstone.subject_id=? " +
                "AND tombstone.generation_id IS NOT NULL " +
                "AND EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED')))" +
                ") AS is_tombstoned, " +
                "overlay_info.generation_id, overlay_info.datastream_id, overlay_info.foi_id, " +
                "overlay_info.phenomenon_time, overlay_info.result_time, " +
                "overlay_info.result_json_text, overlay_info.schema_json_text " +
                "FROM (SELECT 1) AS probe " +
                "LEFT JOIN LATERAL (" +
                "SELECT overlay_row.generation_id, overlay_row.datastream_id, overlay_row.foi_id, " +
                "overlay_row.phenomenon_time, overlay_row.result_time, " +
                "overlay_row.result_json::text AS result_json_text, " +
                "COALESCE(overlay_row.schema_json, overlay_schema_blob.schema_json)::text AS schema_json_text " +
                "FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? AND overlay_row.observation_id=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')" +
                ") overlay_info ON true";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            connection.setReadOnly(true);
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            statement.setString(4, storeKey);
            statement.setInt(5, idScope);
            statement.setLong(6, observationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return new PointLookupStatus(false, null);
                if (result.getBoolean(1))
                    return new PointLookupStatus(true, null);
                UUID generationId = result.getObject(2, UUID.class);
                if (generationId == null)
                    return new PointLookupStatus(false, null);
                Long foi = result.getObject(4, Long.class);
                Instant phenomenon = UtcJdbcTime.getInstant(result, 5);
                Instant resultTime = UtcJdbcTime.getInstant(result, 6);
                ArchivedObservation observation = new ArchivedObservation(
                        scope, observationId, scope, result.getLong(3),
                        foi == null ? null : scope, foi, phenomenon,
                        resultTime, result.getString(7), profile);
                return new PointLookupStatus(false,
                        new ArchivedOverlay(generationId, result.getString(8), observation));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive overlay/tombstone state", e);
        }
    }

    private static String sha256Hex(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    public Optional<ArchivedObservation> overlay(long observationId, int scope, String profile) {
        return overlayWithGeneration(observationId, scope, profile).map(ArchivedOverlay::observation);
    }

    public Optional<ArchivedOverlay> overlayWithGeneration(long observationId, int scope, String profile) {
        try (Connection connection = connections.open()) {
            return overlayWithGeneration(connection, observationId, scope, profile, false);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read archive overlay", e);
        }
    }

    public Optional<ArchivedOverlay> overlayWithGeneration(Connection connection, long observationId,
                                                            int scope, String profile,
                                                            boolean forUpdate) throws SQLException {
        String sql =
                "SELECT overlay_row.generation_id, overlay_row.datastream_id, overlay_row.foi_id, " +
                        "overlay_row.phenomenon_time, overlay_row.result_time, overlay_row.result_json::text, " +
                        "COALESCE(overlay_row.schema_json, overlay_schema_blob.schema_json)::text " +
                        "FROM oscar_obs_overlay overlay_row JOIN oscar_archive_generation generation " +
                        "ON generation.id=overlay_row.generation_id " +
                        "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                        "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " +
                        "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? AND overlay_row.observation_id=? " +
                        "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                        "AND generation.state IN ('ACTIVE','SEALED')" +
                        (forUpdate ? " FOR UPDATE OF overlay_row" : "");
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setLong(3, observationId);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return Optional.empty();
                UUID generationId = result.getObject(1, UUID.class);
                Long foi = result.getObject(3, Long.class);
                Instant phenomenon = UtcJdbcTime.getInstant(result, 4);
                Instant resultTime = UtcJdbcTime.getInstant(result, 5);
                ArchivedObservation observation = new ArchivedObservation(
                        scope, observationId, scope, result.getLong(2),
                        foi == null ? null : scope, foi, phenomenon,
                        resultTime == null ? null : resultTime, result.getString(6), profile);
                return Optional.of(new ArchivedOverlay(generationId, result.getString(7), observation));
            }
        }
    }

    /**
     * Convert a bounded page of legacy inline schemas without rewriting archive
     * files or blocking startup on an unbounded table migration. Each page is
     * independently atomic and safe to retry after a crash.
     */
    public int migrateLegacyOverlaySchemas(int limit) {
        if (limit <= 0 || limit > 250)
            throw new IllegalArgumentException("Overlay schema migration limit must be in 1..250");
        record LegacySchema(long observationId, String json, String hash) {}
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                List<LegacySchema> rows = new ArrayList<>();
                try (PreparedStatement select = connection.prepareStatement(
                        "SELECT observation_id, schema_json::text FROM oscar_obs_overlay " +
                        "WHERE store_key=? AND id_scope=? AND schema_json IS NOT NULL " +
                        "ORDER BY observation_id LIMIT ?")) {
                    select.setString(1, storeKey);
                    select.setInt(2, idScope);
                    select.setInt(3, limit);
                    try (ResultSet result = select.executeQuery()) {
                        while (result.next()) {
                            String json = result.getString(2);
                            rows.add(new LegacySchema(result.getLong(1), json, sha256Hex(json)));
                        }
                    }
                }
                if (!rows.isEmpty()) {
                    try (PreparedStatement blob = connection.prepareStatement(
                            "INSERT INTO oscar_archive_schema_blob(schema_sha256,schema_json) " +
                            "VALUES (?, ?::jsonb) ON CONFLICT (schema_sha256) DO NOTHING");
                         PreparedStatement update = connection.prepareStatement(
                            "UPDATE oscar_obs_overlay SET schema_sha256=?, schema_json=NULL " +
                            "WHERE store_key=? AND id_scope=? AND observation_id=? AND schema_json=?::jsonb")) {
                        // Insert blobs before locking overlay rows, matching upsert's
                        // lock order. The conditional update preserves a concurrent
                        // adjudication's newer schema. Duplicate workers may overlap.
                        // Different stores can migrate concurrently. Acquire
                        // unique blob keys in the same order to avoid cycles.
                        for (LegacySchema row : rows.stream()
                                .sorted(Comparator.comparing(LegacySchema::hash)).toList()) {
                            blob.setString(1, row.hash());
                            blob.setString(2, row.json());
                            blob.addBatch();
                        }
                        blob.executeBatch();
                        for (LegacySchema row : rows) {
                            update.setString(1, row.hash());
                            update.setString(2, storeKey);
                            update.setInt(3, idScope);
                            update.setLong(4, row.observationId());
                            update.setString(5, row.json());
                            update.addBatch();
                        }
                        int changed = 0;
                        for (int count : update.executeBatch())
                            if (count > 0 || count == Statement.SUCCESS_NO_INFO)
                                changed++;
                        connection.commit();
                        return changed;
                    }
                }
                connection.commit();
                return 0;
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot migrate legacy overlay schemas", error);
        }
    }

    public record OverlayBatchItem(UUID generationId, ArchivedObservation observation,
                                   String schemaJson, String schemaSha256) {}

    /**
     * Inserts any uncached schema blobs in ascending {@code schemaSha256} order
     * before the caller locks {@code oscar_obs_overlay} rows, preserving the
     * global lock order required by concurrent legacy-schema migration.
     */
    public void ensureSchemaBlobsPersisted(Connection connection,
                                           Map<String, String> schemaJsonByHash) throws SQLException {
        if (schemaJsonByHash == null || schemaJsonByHash.isEmpty())
            return;
        List<Map.Entry<String, String>> pending = schemaJsonByHash.entrySet().stream()
                .filter(entry -> entry.getKey() != null && entry.getValue() != null &&
                        !persistedSchemaBlobHashes.contains(entry.getKey()))
                .sorted(Map.Entry.comparingByKey())
                .toList();
        if (pending.isEmpty())
            return;
        try (PreparedStatement blob = connection.prepareStatement(
                "INSERT INTO oscar_archive_schema_blob(schema_sha256, schema_json) VALUES (?, ?::jsonb) " +
                        "ON CONFLICT (schema_sha256) DO NOTHING")) {
            for (Map.Entry<String, String> entry : pending) {
                blob.setString(1, entry.getKey());
                blob.setString(2, entry.getValue());
                blob.addBatch();
            }
            blob.executeBatch();
        }
    }

    /**
     * Batch-upserts generation-owned overlays and updates {@code oscar_obs_latest}
     * within a caller-owned transaction after {@link #ensureSchemaBlobsPersisted}
     * and {@link #lockForMutation(Connection, Collection)} have completed.
     */
    public void upsertOverlaysBatch(Connection connection, List<OverlayBatchItem> items) throws SQLException {
        if (items == null || items.isEmpty())
            return;
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_obs_overlay(store_key, id_scope, observation_id, generation_id, datastream_id, foi_id, phenomenon_time, result_time, result_json, schema_sha256) " +
                        "SELECT ?, ?, ?, generation.id, ?, ?, ?, ?, ?::jsonb, ? " +
                        "FROM oscar_archive_generation generation WHERE generation.id=? " +
                        "AND generation.store_key=? AND generation.id_scope=? " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "ON CONFLICT (store_key, id_scope, observation_id) DO UPDATE SET " +
                        "generation_id=EXCLUDED.generation_id, datastream_id=EXCLUDED.datastream_id, " +
                        "foi_id=EXCLUDED.foi_id, phenomenon_time=EXCLUDED.phenomenon_time, " +
                        "result_time=EXCLUDED.result_time, result_json=EXCLUDED.result_json, " +
                        "schema_json=NULL, schema_sha256=EXCLUDED.schema_sha256, updated_at=now()")) {
            for (OverlayBatchItem item : items) {
                ArchivedObservation observation = item.observation();
                statement.setString(1, storeKey);
                statement.setInt(2, idScope);
                statement.setLong(3, observation.id());
                statement.setLong(4, observation.dataStreamId());
                if (observation.foiId() == null)
                    statement.setNull(5, java.sql.Types.BIGINT);
                else
                    statement.setLong(5, observation.foiId());
                if (observation.phenomenonTime() == null)
                    statement.setNull(6, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                else
                    UtcJdbcTime.setInstant(statement, 6, observation.phenomenonTime());
                if (observation.resultTime() == null)
                    statement.setNull(7, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                else
                    UtcJdbcTime.setInstant(statement, 7, observation.resultTime());
                statement.setString(8, observation.resultJson());
                statement.setString(9, item.schemaSha256());
                statement.setObject(10, item.generationId());
                statement.setString(11, storeKey);
                statement.setInt(12, idScope);
                statement.addBatch();
            }
            int[] counts = statement.executeBatch();
            for (int i = 0; i < counts.length; i++) {
                if (counts[i] != 1 && counts[i] != Statement.SUCCESS_NO_INFO) {
                    throw new IllegalArgumentException(
                            "Archive generation does not belong to this observation store: " +
                                    items.get(i).generationId());
                }
            }
        }
        try (PreparedStatement latestUpdate = connection.prepareStatement(
                "UPDATE oscar_obs_latest SET generation_id=?, foi_id=?, phenomenon_time=?, " +
                        "result_time=?, result_json=?::jsonb, schema_json=?::jsonb, updated_at=now() " +
                        "WHERE store_key=? AND id_scope=? AND datastream_id=? AND observation_id=?")) {
            for (OverlayBatchItem item : items) {
                ArchivedObservation observation = item.observation();
                latestUpdate.setObject(1, item.generationId());
                if (observation.foiId() == null)
                    latestUpdate.setNull(2, java.sql.Types.BIGINT);
                else
                    latestUpdate.setLong(2, observation.foiId());
                if (observation.phenomenonTime() == null)
                    latestUpdate.setNull(3, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                else
                    UtcJdbcTime.setInstant(latestUpdate, 3, observation.phenomenonTime());
                if (observation.resultTime() == null)
                    latestUpdate.setNull(4, java.sql.Types.TIMESTAMP_WITH_TIMEZONE);
                else
                    UtcJdbcTime.setInstant(latestUpdate, 4, observation.resultTime());
                latestUpdate.setString(5, observation.resultJson());
                latestUpdate.setString(6, item.schemaJson());
                latestUpdate.setString(7, storeKey);
                latestUpdate.setInt(8, idScope);
                latestUpdate.setLong(9, observation.dataStreamId());
                latestUpdate.setLong(10, observation.id());
                latestUpdate.addBatch();
            }
            latestUpdate.executeBatch();
        }
    }

    public void upsertOverlay(UUID generationId, ArchivedObservation observation, String schemaJson) {
        if (schemaJson == null || schemaJson.isBlank())
            throw new IllegalArgumentException("Archived observation overlays require an immutable schema snapshot");
        String schemaHash = sha256Hex(schemaJson);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                lockForMutation(connection, observation.id());
                upsertOverlay(connection, generationId, observation, schemaJson, schemaHash);
                connection.commit();
                recordPersistedSchemaBlobHashes(List.of(schemaHash));
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot update archived observation overlay", e);
        }
    }

    /**
     * Transaction-scoped overlay upsert used when ingestion discovers that a
     * natural-key correction targets an immutable generation-owned hot row.
     * The caller owns commit/rollback and connection lifecycle and must have
     * acquired {@link #lockForMutation(Connection, Collection)} for this ID.
     */
    public void upsertOverlay(Connection connection, UUID generationId,
                              ArchivedObservation observation, String schemaJson) throws SQLException {
        if (schemaJson == null || schemaJson.isBlank())
            throw new IllegalArgumentException("Archived observation overlays require an immutable schema snapshot");
        upsertOverlay(connection, generationId, observation, schemaJson, sha256Hex(schemaJson));
    }

    public void upsertOverlay(Connection connection, UUID generationId,
                              ArchivedObservation observation, String schemaJson,
                              String schemaHash) throws SQLException {
        if (schemaJson == null || schemaJson.isBlank())
            throw new IllegalArgumentException("Archived observation overlays require an immutable schema snapshot");
        if (schemaHash == null || schemaHash.isBlank())
            schemaHash = sha256Hex(schemaJson);
        if (!persistedSchemaBlobHashes.contains(schemaHash)) {
            try (PreparedStatement blob = connection.prepareStatement(
                    "INSERT INTO oscar_archive_schema_blob(schema_sha256, schema_json) VALUES (?, ?::jsonb) " +
                            "ON CONFLICT (schema_sha256) DO NOTHING")) {
                blob.setString(1, schemaHash);
                blob.setString(2, schemaJson);
                blob.executeUpdate();
            }
        }
        upsertOverlaysBatch(connection, List.of(
                new OverlayBatchItem(generationId, observation, schemaJson, schemaHash)));
    }

    public Set<Long> activeTombstonedObservationIds(Connection connection,
                                                    Collection<Long> observationIds) throws SQLException {
        if (observationIds == null || observationIds.isEmpty())
            return Set.of();
        String sql = "WITH requested(id) AS (SELECT DISTINCT unnest(?::bigint[])) " +
                "SELECT r.id FROM requested r WHERE EXISTS (" +
                "SELECT 1 FROM oscar_tombstone tombstone " +
                "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_type='OBSERVATION' AND tombstone.subject_id=r.id::text " +
                "AND tombstone.generation_id IS NOT NULL " +
                "AND EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED')))";
        Set<Long> tombstoned = new HashSet<>();
        java.sql.Array ids = connection.createArrayOf("bigint", observationIds.toArray(Long[]::new));
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, ids);
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    tombstoned.add(result.getLong(1));
            }
        } finally {
            ids.free();
        }
        return tombstoned;
    }

    public boolean isTombstoned(long observationId) {
        try (Connection connection = connections.open()) {
            return isTombstoned(connection, observationId);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read archive tombstone", e);
        }
    }

    public boolean isTombstoned(Connection connection, long observationId) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM oscar_tombstone tombstone " +
                        "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=? AND generation_id IS NOT NULL " +
                        "AND EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                        "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                        "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED'))")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    public boolean hasObservationMutations() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                     "SELECT EXISTS (SELECT 1 FROM oscar_obs_overlay overlay_row " +
                             "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                             "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                             "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                             "AND generation.state IN ('ACTIVE','SEALED')) OR " +
                             "EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                             "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                             "AND subject_type='OBSERVATION' AND generation_id IS NOT NULL AND EXISTS (" +
                             "SELECT 1 FROM oscar_archive_generation generation " +
                             "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                             "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED')))")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect archive mutations", e);
        }
    }

    public record MutationCounts(long overlayCount, long tombstoneCount) {
        public long total() {
            if (overlayCount >= Long.MAX_VALUE - tombstoneCount)
                return Long.MAX_VALUE;
            return overlayCount + tombstoneCount;
        }
    }

    /**
     * Returns the number of active/sealed overlays and observation tombstones
     * affecting the requested {@code dataStreams} (or all streams when {@code null}).
     */
    public long countObservationMutations(Set<Long> dataStreams) {
        return countObservationMutationBreakdown(dataStreams).total();
    }

    /**
     * Returns separate active/sealed overlay and observation tombstone counts
     * affecting the requested {@code dataStreams} (or all streams when {@code null}).
     */
    public MutationCounts countObservationMutationBreakdown(Set<Long> dataStreams) {
        if (dataStreams != null && dataStreams.isEmpty())
            return new MutationCounts(0L, 0L);
        boolean filterStreams = dataStreams != null;
        String sql = "SELECT (" +
                "SELECT count(*)::bigint FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')" +
                (filterStreams ? " AND overlay_row.datastream_id=ANY (?::bigint[])" : "") +
                "), (" +
                "SELECT count(*)::bigint FROM oscar_tombstone tombstone " +
                "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_type='OBSERVATION' AND tombstone.generation_id IS NOT NULL " +
                "AND EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                "WHERE generation.id=tombstone.generation_id AND generation.store_key=tombstone.store_key " +
                "AND generation.id_scope=tombstone.id_scope AND generation.state IN ('ACTIVE','SEALED'))" +
                (filterStreams
                        ? " AND EXISTS (SELECT 1 FROM oscar_archive_schema s " +
                        "WHERE s.generation_id=tombstone.generation_id AND s.datastream_id=ANY (?::bigint[]))"
                        : "") +
                ")";
        try (Connection connection = connections.open()) {
            java.sql.Array streamArray = filterStreams
                    ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                    : null;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int p = 1;
                statement.setString(p++, storeKey);
                statement.setInt(p++, idScope);
                if (streamArray != null)
                    statement.setArray(p++, streamArray);
                statement.setString(p++, storeKey);
                statement.setInt(p++, idScope);
                if (streamArray != null)
                    statement.setArray(p, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    return result.next()
                            ? new MutationCounts(result.getLong(1), result.getLong(2))
                            : new MutationCounts(0L, 0L);
                }
            } finally {
                if (streamArray != null)
                    streamArray.free();
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot count archive mutations", e);
        }
    }

    /**
     * Loads verified latest cold observations from {@code oscar_obs_latest} for
     * streams that have no hot row in the current staging window and whose
     * latest entry belongs to the newest active/sealed generation for that stream.
     */
    public Map<Long, ArchivedOverlay> loadVerifiedColdLatestByDataStream(int scope, Set<Long> dataStreams) {
        if (dataStreams == null || dataStreams.isEmpty())
            return Map.of();
        String sql = "SELECT latest.datastream_id, " +
                "COALESCE(overlay_row.generation_id, latest.generation_id), " +
                "latest.observation_id, " +
                "COALESCE(overlay_row.foi_id, latest.foi_id), " +
                "COALESCE(overlay_row.phenomenon_time, latest.phenomenon_time), " +
                "COALESCE(overlay_row.result_time, latest.result_time), " +
                "COALESCE(overlay_row.result_json, latest.result_json)::text, " +
                "COALESCE(overlay_row.schema_json, overlay_schema_blob.schema_json, latest.schema_json)::text, " +
                "CASE WHEN overlay_row.observation_id IS NOT NULL THEN 'OVERLAY' ELSE latest.profile END " +
                "FROM oscar_obs_latest latest " +
                "JOIN oscar_archive_generation g ON g.id=latest.generation_id " +
                "LEFT JOIN oscar_obs_overlay overlay_row " +
                "ON overlay_row.store_key=latest.store_key AND overlay_row.id_scope=latest.id_scope " +
                "AND overlay_row.observation_id=latest.observation_id " +
                "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " +
                "WHERE latest.store_key=? AND latest.id_scope=? " +
                "AND latest.datastream_id=ANY (?::bigint[]) " +
                "AND g.store_key=latest.store_key AND g.id_scope=latest.id_scope " +
                "AND g.state IN ('ACTIVE','SEALED') " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                "WHERE tombstone.store_key=latest.store_key AND tombstone.id_scope=latest.id_scope " +
                "AND tombstone.subject_type='OBSERVATION' AND tombstone.subject_id=latest.observation_id::text " +
                "AND tombstone.generation_id IS NOT NULL) " +
                "AND NOT EXISTS (SELECT 1 FROM oscar_archive_schema s2 " +
                "JOIN oscar_archive_generation g2 ON g2.id=s2.generation_id " +
                "WHERE g2.store_key=latest.store_key AND g2.id_scope=latest.id_scope " +
                "AND g2.state IN ('ACTIVE','SEALED') AND s2.datastream_id=latest.datastream_id " +
                "AND g2.window_end > g.window_end)";
        Map<Long, ArchivedOverlay> byStream = new HashMap<>();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            java.sql.Array streamArray = connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new));
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, storeKey);
                statement.setInt(2, idScope);
                statement.setArray(3, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        long dsId = result.getLong(1);
                        UUID genId = result.getObject(2, UUID.class);
                        long obsId = result.getLong(3);
                        Long foiId = result.getObject(4, Long.class);
                        Instant phenomenonTime = UtcJdbcTime.getInstant(result, 5);
                        Instant resultTime = UtcJdbcTime.getInstant(result, 6);
                        String resultJson = result.getString(7);
                        String schemaJson = result.getString(8);
                        String profile = result.getString(9);
                        ArchivedObservation obs = new ArchivedObservation(
                                scope, obsId, scope, dsId,
                                foiId == null ? null : scope, foiId,
                                phenomenonTime, resultTime, resultJson, profile);
                        byStream.put(dsId, new ArchivedOverlay(genId, schemaJson, obs));
                    }
                }
            } finally {
                streamArray.free();
            }
            return byStream;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot read latest cold observation index", e);
        }
    }

    /**
     * Prunes candidate generations whose per-datastream summary counters prove
     * zero matching rows (or filters to the exact generation recorded in
     * {@code oscar_event_locator} when {@code exactEventId} is present).
     */
    public List<ArchiveGeneration> pruneZeroMatchSummaryCandidates(
            List<ArchiveGeneration> candidates, Set<Long> dataStreams,
            String summaryExpr, String exactEventId) {
        if (candidates == null || candidates.isEmpty())
            return List.of();
        List<ArchiveGeneration> working = candidates;
        if (exactEventId != null && !exactEventId.isEmpty()) {
            String locatorSql = "SELECT generation_id FROM oscar_event_locator " +
                    "WHERE store_key=? AND id_scope=? AND public_id=? AND state='ACTIVE' " +
                    "AND generation_id IS NOT NULL";
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(locatorSql)) {
                statement.setString(1, storeKey);
                statement.setInt(2, idScope);
                statement.setString(3, exactEventId);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        UUID targetGen = result.getObject(1, UUID.class);
                        if (targetGen != null) {
                            working = working.stream()
                                    .filter(g -> targetGen.equals(g.id()))
                                    .toList();
                            if (working.isEmpty())
                                return List.of();
                        }
                    }
                }
            } catch (SQLException ignored) {
                // Fall back to full candidate list if locator lookup fails.
            }
        }
        if (summaryExpr == null)
            return working;

        boolean filterStreams = dataStreams != null;
        boolean omitGenFilter = filterStreams && working.size() > 64;
        String sql = "SELECT s.generation_id FROM oscar_archive_schema s WHERE " +
                (omitGenFilter
                        ? "s.datastream_id=ANY (?::bigint[])"
                        : "s.generation_id=ANY (?::uuid[])" +
                          (filterStreams ? " AND s.datastream_id=ANY (?::bigint[])" : "")) +
                " GROUP BY s.generation_id " +
                "HAVING BOOL_AND(s.summary_populated) AND COALESCE(SUM(" + summaryExpr + "), 0) = 0";
        Set<UUID> prune = new HashSet<>();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            java.sql.Array genArray = omitGenFilter ? null : connection.createArrayOf("uuid",
                    working.stream().map(ArchiveGeneration::id).toArray(UUID[]::new));
            java.sql.Array streamArray = filterStreams
                    ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                    : null;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int param = 1;
                if (genArray != null)
                    statement.setArray(param++, genArray);
                if (streamArray != null)
                    statement.setArray(param, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next())
                        prune.add(result.getObject(1, UUID.class));
                }
            } finally {
                if (genArray != null)
                    genArray.free();
                if (streamArray != null)
                    streamArray.free();
            }
        } catch (SQLException e) {
            return working;
        }
        return prune.isEmpty() ? working : working.stream().filter(g -> !prune.contains(g.id())).toList();
    }

    public record CandidateSelectionPlan(
            List<ArchiveGeneration> primaryCandidates,
            List<ArchiveGeneration> overflowCandidates) {}

    /**
     * Prunes zero-match archive generations using {@code oscar_archive_schema}
     * summary counters (or {@code oscar_event_locator} when {@code exactEventId}
     * is present), and slices surviving clusters into a bounded {@code primaryCandidates}
     * prefix that satisfies {@code readerLimit} plus a lazy {@code overflowCandidates}
     * remainder.
     */
    public CandidateSelectionPlan selectSummaryMatchedCandidates(
            List<ArchiveGeneration> candidates,
            TemporalFilter phenomenonTimeFilter,
            TemporalFilter resultTimeFilter,
            Set<Long> dataStreams,
            boolean descending,
            long readerLimit,
            boolean hasMutations,
            JsonCqlEvaluator.SummaryCounterSpec summarySpec,
            String exactEventId) {
        return selectSummaryMatchedCandidates(
                candidates, phenomenonTimeFilter, resultTimeFilter,
                dataStreams, descending, readerLimit, readerLimit,
                hasMutations, summarySpec, exactEventId);
    }

    public CandidateSelectionPlan selectSummaryMatchedCandidates(
            List<ArchiveGeneration> candidates,
            TemporalFilter phenomenonTimeFilter,
            TemporalFilter resultTimeFilter,
            Set<Long> dataStreams,
            boolean descending,
            long callerLimit,
            long readerLimit,
            boolean hasMutations,
            JsonCqlEvaluator.SummaryCounterSpec summarySpec,
            String exactEventId) {
        return selectSummaryMatchedCandidates(
                candidates, phenomenonTimeFilter, resultTimeFilter,
                dataStreams, descending, callerLimit, readerLimit,
                hasMutations, summarySpec, exactEventId, null);
    }

    public CandidateSelectionPlan selectSummaryMatchedCandidates(
            List<ArchiveGeneration> candidates,
            TemporalFilter phenomenonTimeFilter,
            TemporalFilter resultTimeFilter,
            Set<Long> dataStreams,
            boolean descending,
            long callerLimit,
            long readerLimit,
            boolean hasMutations,
            JsonCqlEvaluator.SummaryCounterSpec summarySpec,
            String exactEventId,
            java.util.function.BiFunction<List<ArchiveGeneration>, Map<UUID, Set<Long>>, Map<UUID, Long>> exactMutatedMatchResolver) {
        if (candidates == null || candidates.isEmpty())
            return new CandidateSelectionPlan(List.of(), List.of());

        List<ArchiveGeneration> working = candidates;
        if (exactEventId != null && !exactEventId.isEmpty()) {
            String locatorSql = "SELECT generation_id FROM oscar_event_locator " +
                    "WHERE store_key=? AND id_scope=? AND public_id=? AND state='ACTIVE' " +
                    "AND generation_id IS NOT NULL";
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(locatorSql)) {
                statement.setString(1, storeKey);
                statement.setInt(2, idScope);
                statement.setString(3, exactEventId);
                try (ResultSet result = statement.executeQuery()) {
                    if (result.next()) {
                        UUID targetGen = result.getObject(1, UUID.class);
                        if (targetGen != null) {
                            working = working.stream()
                                    .filter(g -> targetGen.equals(g.id()))
                                    .toList();
                            if (working.isEmpty())
                                return new CandidateSelectionPlan(List.of(), List.of());
                        }
                    }
                }
            } catch (SQLException ignored) {
                // Fall back to full candidate list if locator lookup fails.
            }
        }

        if (summarySpec == null)
            return new CandidateSelectionPlan(working, List.of());

        boolean filterStreams = dataStreams != null;
        boolean omitGenFilter = filterStreams && working.size() > 64;
        String sql = "SELECT s.generation_id, " +
                "BOOL_AND(s.summary_populated), " +
                "COALESCE(SUM(" + summarySpec.upperBoundExpr() + "), 0)::bigint, " +
                "BOOL_AND(" + summarySpec.exactReadyExpr() + "), " +
                "COALESCE(SUM(" + summarySpec.exactExpr() + "), 0)::bigint " +
                "FROM oscar_archive_schema s WHERE " +
                (omitGenFilter
                        ? "s.datastream_id=ANY (?::bigint[])"
                        : "s.generation_id=ANY (?::uuid[])" +
                          (filterStreams ? " AND s.datastream_id=ANY (?::bigint[])" : "")) +
                " GROUP BY s.generation_id";

        Map<UUID, Boolean> populatedByGen = new HashMap<>();
        Map<UUID, Long> upperByGen = new HashMap<>();
        Map<UUID, Boolean> exactReadyByGen = new HashMap<>();
        Map<UUID, Long> exactByGen = new HashMap<>();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            java.sql.Array genArray = omitGenFilter ? null : connection.createArrayOf("uuid",
                    working.stream().map(ArchiveGeneration::id).toArray(UUID[]::new));
            java.sql.Array streamArray = filterStreams
                    ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                    : null;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int param = 1;
                if (genArray != null)
                    statement.setArray(param++, genArray);
                if (streamArray != null)
                    statement.setArray(param, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        UUID genId = result.getObject(1, UUID.class);
                        populatedByGen.put(genId, result.getBoolean(2));
                        upperByGen.put(genId, result.getLong(3));
                        exactReadyByGen.put(genId, result.getBoolean(4));
                        exactByGen.put(genId, result.getLong(5));
                    }
                }
            } finally {
                if (genArray != null)
                    genArray.free();
                if (streamArray != null)
                    streamArray.free();
            }
        } catch (SQLException e) {
            return new CandidateSelectionPlan(working, List.of());
        }

        List<ArchiveGeneration> surviving = new ArrayList<>();
        for (ArchiveGeneration generation : working) {
            UUID genId = generation.id();
            boolean populated = Boolean.TRUE.equals(populatedByGen.get(genId));
            long upper = upperByGen.getOrDefault(genId, 0L);
            if (populated && upper == 0L)
                continue;
            surviving.add(generation);
        }
        if (surviving.isEmpty())
            return new CandidateSelectionPlan(List.of(), List.of());

        boolean boundedTarget = callerLimit != Long.MAX_VALUE && readerLimit != Long.MAX_VALUE;
        if (!hasMutations && (!boundedTarget || surviving.size() <= 1))
            return new CandidateSelectionPlan(surviving, List.of());

        List<List<ArchiveGeneration>> clusters = groupDisjointClusters(surviving);
        if (descending)
            java.util.Collections.reverse(clusters);
        if (!hasMutations && clusters.size() <= 1)
            return new CandidateSelectionPlan(surviving, List.of());

        Map<UUID, Set<Long>> mutatedByGen = hasMutations
                ? mutatedObservationIdsByGeneration(
                        surviving.stream().map(ArchiveGeneration::id).toList())
                : Map.of();
        if (mutatedByGen.isEmpty() && (!boundedTarget || clusters.size() <= 1))
            return new CandidateSelectionPlan(surviving, List.of());

        long targetMatches = boundedTarget ? Math.max(1L, Math.min(callerLimit, readerLimit)) : Long.MAX_VALUE;
        Map<UUID, Long> exactMutatedMatches = new HashMap<>();
        if (exactMutatedMatchResolver != null && !mutatedByGen.isEmpty()) {
            int resolveLimit = clusters.size();
            if (boundedTarget) {
                long fastGuaranteed = 0L;
                for (int i = 0; i < clusters.size(); i++) {
                    List<ArchiveGeneration> cluster = clusters.get(i);
                    boolean clusterExact = true;
                    long clusterExactSum = 0L;
                    for (ArchiveGeneration gen : cluster) {
                        UUID genId = gen.id();
                        long mutatedCount = mutatedByGen.getOrDefault(genId, Set.of()).size();
                        if (Boolean.TRUE.equals(populatedByGen.get(genId)) &&
                                Boolean.TRUE.equals(exactReadyByGen.get(genId)) &&
                                isGenerationWindowFullyContained(gen, phenomenonTimeFilter) &&
                                isGenerationResultTimeFullyContained(gen, resultTimeFilter)) {
                            long exactUnmutated = Math.max(0L, exactByGen.getOrDefault(genId, 0L) - mutatedCount);
                            clusterExactSum = saturateAddLong(clusterExactSum, exactUnmutated);
                        } else {
                            clusterExact = false;
                            break;
                        }
                    }
                    if (clusterExact) {
                        fastGuaranteed = saturateAddLong(fastGuaranteed, clusterExactSum);
                        if (fastGuaranteed >= targetMatches) {
                            resolveLimit = i + 1;
                            break;
                        }
                    }
                }
            }

            List<ArchiveGeneration> eligibleMutated = new ArrayList<>();
            Map<UUID, Integer> lastGenClusterIndex = new HashMap<>();
            for (int i = 0; i < resolveLimit; i++) {
                List<ArchiveGeneration> cluster = clusters.get(i);
                for (ArchiveGeneration gen : cluster) {
                    UUID genId = gen.id();
                    if (!mutatedByGen.getOrDefault(genId, Set.of()).isEmpty() &&
                            Boolean.TRUE.equals(populatedByGen.get(genId)) &&
                            Boolean.TRUE.equals(exactReadyByGen.get(genId)) &&
                            isGenerationWindowFullyContained(gen, phenomenonTimeFilter) &&
                            isGenerationResultTimeFullyContained(gen, resultTimeFilter) &&
                            exactByGen.getOrDefault(genId, 0L) > 0L) {
                        eligibleMutated.add(gen);
                        lastGenClusterIndex.put(genId, i);
                    }
                }
            }

            int nextClusterToCheck = 0;
            long resolvedGuaranteed = 0L;
            for (int start = 0; start < eligibleMutated.size(); start += 256) {
                List<ArchiveGeneration> chunk = eligibleMutated.subList(
                        start, Math.min(eligibleMutated.size(), start + 256));
                exactMutatedMatches.putAll(exactMutatedMatchResolver.apply(chunk, mutatedByGen));
                if (boundedTarget) {
                    boolean moreChunksRemain = start + 256 < eligibleMutated.size();
                    int safeClusterExclusive = moreChunksRemain
                            ? lastGenClusterIndex.getOrDefault(eligibleMutated.get(start + 256).id(), resolveLimit)
                            : resolveLimit;
                    while (nextClusterToCheck < safeClusterExclusive) {
                        List<ArchiveGeneration> cluster = clusters.get(nextClusterToCheck);
                        boolean clusterExact = true;
                        long clusterExactSum = 0L;
                        for (ArchiveGeneration gen : cluster) {
                            UUID genId = gen.id();
                            long mutatedCount = mutatedByGen.getOrDefault(genId, Set.of()).size();
                            if (Boolean.TRUE.equals(populatedByGen.get(genId)) &&
                                    Boolean.TRUE.equals(exactReadyByGen.get(genId)) &&
                                    isGenerationWindowFullyContained(gen, phenomenonTimeFilter) &&
                                    isGenerationResultTimeFullyContained(gen, resultTimeFilter)) {
                                long exactVal = exactByGen.getOrDefault(genId, 0L);
                                long exactUnmutated = exactMutatedMatches.containsKey(genId)
                                        ? Math.max(0L, exactVal - exactMutatedMatches.get(genId))
                                        : Math.max(0L, exactVal - mutatedCount);
                                clusterExactSum = saturateAddLong(clusterExactSum, exactUnmutated);
                            } else {
                                clusterExact = false;
                            }
                        }
                        nextClusterToCheck++;
                        if (clusterExact)
                            resolvedGuaranteed = saturateAddLong(resolvedGuaranteed, clusterExactSum);
                    }
                    if (resolvedGuaranteed >= targetMatches)
                        break;
                }
            }
        }

        List<ArchiveGeneration> primary = new ArrayList<>();
        List<ArchiveGeneration> overflow = new ArrayList<>();
        long guaranteedMatches = 0L;
        long upperBoundMatches = 0L;
        int primaryClusterCount = 0;
        boolean fillOverflow = false;

        for (List<ArchiveGeneration> rawCluster : clusters) {
            List<ArchiveGeneration> cluster = new ArrayList<>(rawCluster.size());
            for (ArchiveGeneration gen : rawCluster) {
                UUID genId = gen.id();
                if (exactMutatedMatches.containsKey(genId)) {
                    long exactRemaining = Math.max(0L,
                            exactByGen.getOrDefault(genId, 0L) - exactMutatedMatches.get(genId));
                    if (exactRemaining == 0L)
                        continue;
                }
                cluster.add(gen);
            }
            if (cluster.isEmpty())
                continue;
            if (!boundedTarget) {
                primary.addAll(cluster);
                continue;
            }
            if (fillOverflow) {
                overflow.addAll(cluster);
                continue;
            }
            primary.addAll(cluster);
            primaryClusterCount++;
            boolean clusterExact = true;
            long clusterExactSum = 0L;
            long clusterUpperSum = 0L;
            for (ArchiveGeneration gen : cluster) {
                UUID genId = gen.id();
                boolean populated = Boolean.TRUE.equals(populatedByGen.get(genId));
                long mutatedCount = mutatedByGen.getOrDefault(genId, Set.of()).size();
                if (exactMutatedMatches.containsKey(genId)) {
                    long exactRemaining = Math.max(0L,
                            exactByGen.getOrDefault(genId, 0L) - exactMutatedMatches.get(genId));
                    clusterUpperSum = saturateAddLong(clusterUpperSum, exactRemaining);
                    clusterExactSum = saturateAddLong(clusterExactSum, exactRemaining);
                    continue;
                }
                long upper = populated ? upperByGen.getOrDefault(genId, 0L) : targetMatches;
                clusterUpperSum = saturateAddLong(clusterUpperSum, Math.max(0L, upper - mutatedCount));
                if (clusterExact &&
                        Boolean.TRUE.equals(exactReadyByGen.get(genId)) &&
                        isGenerationWindowFullyContained(gen, phenomenonTimeFilter) &&
                        isGenerationResultTimeFullyContained(gen, resultTimeFilter)) {
                    long exactUnmutated = Math.max(0L, exactByGen.getOrDefault(genId, 0L) - mutatedCount);
                    clusterExactSum = saturateAddLong(clusterExactSum, exactUnmutated);
                } else {
                    clusterExact = false;
                }
            }
            upperBoundMatches = saturateAddLong(upperBoundMatches, clusterUpperSum);
            if (clusterExact)
                guaranteedMatches = saturateAddLong(guaranteedMatches, clusterExactSum);

            if (guaranteedMatches >= targetMatches) {
                break;
            }
            if (upperBoundMatches >= targetMatches && primaryClusterCount >= 4) {
                fillOverflow = true;
            }
        }

        primary.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                .thenComparing(ArchiveGeneration::end)
                .thenComparing(ArchiveGeneration::id));
        overflow.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                .thenComparing(ArchiveGeneration::end)
                .thenComparing(ArchiveGeneration::id));
        return new CandidateSelectionPlan(primary, overflow);
    }

    private static List<List<ArchiveGeneration>> groupDisjointClusters(List<ArchiveGeneration> generations) {
        List<ArchiveGeneration> ordered = new ArrayList<>(generations);
        ordered.sort(java.util.Comparator.comparing(ArchiveGeneration::start)
                .thenComparing(ArchiveGeneration::end)
                .thenComparing(ArchiveGeneration::id));
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

    private static long saturateAddLong(long a, long b) {
        if (a == Long.MAX_VALUE || b <= 0L)
            return a;
        long r = a + b;
        return r < 0L ? Long.MAX_VALUE : r;
    }

    public record ColdSummaryPlan(
            long summarizedCount,
            List<ArchiveGeneration> remainingCandidates,
            List<ArchiveGeneration> exactMutatedCandidates) {
        public ColdSummaryPlan(long summarizedCount, List<ArchiveGeneration> remainingCandidates) {
            this(summarizedCount, remainingCandidates, List.of());
        }
    }

    /**
     * Sums fully-contained generations directly from {@code oscar_archive_schema}
     * summary counters in PostgreSQL, records mutated interior generations whose
     * base matches must be subtracted by ID, and prunes zero-match generations.
     */
    public ColdSummaryPlan summarizeColdCandidates(
            List<ArchiveGeneration> candidates, TemporalFilter phenomenonTimeFilter,
            Set<Long> dataStreams, Set<UUID> mutatedGenerationIds, String summaryExpr) {
        if (summaryExpr == null)
            return new ColdSummaryPlan(0L, candidates == null ? List.of() : candidates, List.of());
        return summarizeColdCandidates(candidates, phenomenonTimeFilter, null,
                dataStreams, mutatedGenerationIds,
                new JsonCqlEvaluator.SummaryCounterSpec(summaryExpr, summaryExpr, "s.summary_populated"));
    }

    public ColdSummaryPlan summarizeColdCandidates(
            List<ArchiveGeneration> candidates, TemporalFilter phenomenonTimeFilter,
            TemporalFilter resultTimeFilter,
            Set<Long> dataStreams, Set<UUID> mutatedGenerationIds,
            JsonCqlEvaluator.SummaryCounterSpec summarySpec) {
        return summarizeColdCandidates(
                candidates, phenomenonTimeFilter, resultTimeFilter,
                dataStreams, mutatedGenerationIds, summarySpec, null);
    }

    public ColdSummaryPlan summarizeColdCandidates(
            List<ArchiveGeneration> candidates, TemporalFilter phenomenonTimeFilter,
            TemporalFilter resultTimeFilter,
            Set<Long> dataStreams, Set<UUID> mutatedGenerationIds,
            JsonCqlEvaluator.SummaryCounterSpec summarySpec,
            JsonCqlEvaluator.TimeContainedSummarySpec timeContainedSpec) {
        if (candidates == null || candidates.isEmpty() || (summarySpec == null && timeContainedSpec == null))
            return new ColdSummaryPlan(0L, candidates == null ? List.of() : candidates, List.of());
        String upperExpr = summarySpec != null ? summarySpec.upperBoundExpr() : timeContainedSpec.upperBoundExpr();
        String exactReadyExpr = summarySpec != null ? summarySpec.exactReadyExpr() : "false";
        String exactExpr = summarySpec != null ? summarySpec.exactExpr() : "0::bigint";
        boolean checkTimeContained = timeContainedSpec != null && !timeContainedSpec.requiredTimeFields().isEmpty();
        String tcReadyExpr = checkTimeContained
                ? "((" + timeContainedSpec.exactReadyExpr() + ") AND ((" +
                  timeContainedSpec.upperBoundExpr() + ") = 0 OR (" +
                  buildRequiredTimeFieldsSchemaSql(timeContainedSpec.requiredTimeFields()) + ")))"
                : "false";
        String tcExactExpr = checkTimeContained ? timeContainedSpec.exactExpr() : "0::bigint";

        boolean filterStreams = dataStreams != null;
        boolean omitGenFilter = filterStreams && candidates.size() > 64;
        String sql = "SELECT s.generation_id, BOOL_AND(s.summary_populated), " +
                "COALESCE(SUM(" + upperExpr + "), 0)::bigint, " +
                "BOOL_AND(" + exactReadyExpr + "), " +
                "COALESCE(SUM(" + exactExpr + "), 0)::bigint, " +
                "BOOL_AND(" + tcReadyExpr + "), " +
                "COALESCE(SUM(" + tcExactExpr + "), 0)::bigint " +
                "FROM oscar_archive_schema s " +
                (checkTimeContained ? "LEFT JOIN oscar_archive_schema_blob b ON b.schema_sha256 = s.schema_sha256 " : "") +
                "WHERE " +
                (omitGenFilter
                        ? "s.datastream_id=ANY (?::bigint[])"
                        : "s.generation_id=ANY (?::uuid[])" +
                          (filterStreams ? " AND s.datastream_id=ANY (?::bigint[])" : "")) +
                " GROUP BY s.generation_id";
        Map<UUID, Boolean> populatedByGen = new HashMap<>();
        Map<UUID, Long> upperByGen = new HashMap<>();
        Map<UUID, Boolean> exactReadyByGen = new HashMap<>();
        Map<UUID, Long> exactByGen = new HashMap<>();
        Map<UUID, Boolean> tcReadyByGen = checkTimeContained ? new HashMap<>() : Map.of();
        Map<UUID, Long> tcExactByGen = checkTimeContained ? new HashMap<>() : Map.of();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            java.sql.Array genArray = omitGenFilter ? null : connection.createArrayOf("uuid",
                    candidates.stream().map(ArchiveGeneration::id).toArray(UUID[]::new));
            java.sql.Array streamArray = filterStreams
                    ? connection.createArrayOf("bigint", dataStreams.toArray(Long[]::new))
                    : null;
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int param = 1;
                if (genArray != null)
                    statement.setArray(param++, genArray);
                if (streamArray != null)
                    statement.setArray(param, streamArray);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        UUID genId = result.getObject(1, UUID.class);
                        populatedByGen.put(genId, result.getBoolean(2));
                        upperByGen.put(genId, result.getLong(3));
                        exactReadyByGen.put(genId, result.getBoolean(4));
                        exactByGen.put(genId, result.getLong(5));
                        if (checkTimeContained) {
                            tcReadyByGen.put(genId, result.getBoolean(6));
                            tcExactByGen.put(genId, result.getLong(7));
                        }
                    }
                }
            } finally {
                if (genArray != null)
                    genArray.free();
                if (streamArray != null)
                    streamArray.free();
            }
        } catch (SQLException e) {
            return new ColdSummaryPlan(0L, candidates, List.of());
        }

        long summarized = 0L;
        List<ArchiveGeneration> remaining = new ArrayList<>();
        List<ArchiveGeneration> exactMutated = new ArrayList<>();
        for (ArchiveGeneration generation : candidates) {
            UUID genId = generation.id();
            boolean populated = Boolean.TRUE.equals(populatedByGen.get(genId));
            long upper = upperByGen.getOrDefault(genId, 0L);
            boolean exactReady = Boolean.TRUE.equals(exactReadyByGen.get(genId));
            long exact = exactByGen.getOrDefault(genId, 0L);
            boolean mutated = mutatedGenerationIds != null && mutatedGenerationIds.contains(genId);
            if (populated && upper == 0L) {
                continue; // Zero matching base rows in this generation even at window boundaries.
            }
            if (populated && exactReady &&
                    isGenerationWindowFullyContained(generation, phenomenonTimeFilter) &&
                    isGenerationResultTimeFullyContained(generation, resultTimeFilter)) {
                summarized = Math.addExact(summarized, exact);
                if (mutated && exact > 0L) {
                    exactMutated.add(generation);
                }
                continue;
            }
            if (checkTimeContained && populated &&
                    Boolean.TRUE.equals(tcReadyByGen.get(genId)) &&
                    isGenerationWindowFullyContained(generation, phenomenonTimeFilter) &&
                    isGenerationResultTimeFullyContained(generation, resultTimeFilter) &&
                    isGenerationInsideTimeContainedBounds(generation, timeContainedSpec)) {
                long tcExact = tcExactByGen.getOrDefault(genId, 0L);
                summarized = Math.addExact(summarized, tcExact);
                if (mutated && tcExact > 0L) {
                    exactMutated.add(generation);
                }
                continue;
            }
            remaining.add(generation);
        }
        return new ColdSummaryPlan(summarized, remaining, exactMutated);
    }

    private static boolean isGenerationInsideTimeContainedBounds(
            ArchiveGeneration generation, JsonCqlEvaluator.TimeContainedSummarySpec spec) {
        if (generation.start() == null || generation.end() == null)
            return false;
        Instant minStart = spec.minContainedWindowStart();
        if (minStart != null && generation.start().isBefore(minStart))
            return false;
        Instant maxEnd = spec.maxContainedWindowEnd();
        if (maxEnd != null && generation.end().isAfter(maxEnd))
            return false;
        return true;
    }

    private static String buildRequiredTimeFieldsSchemaSql(Set<String> requiredTimeFields) {
        List<String> checks = new ArrayList<>(requiredTimeFields.size());
        for (String field : requiredTimeFields) {
            String literal = switch (field) {
                case "startTime" -> "'[{\"name\":\"startTime\",\"type\":\"Time\"}]'::jsonb";
                case "endTime" -> "'[{\"name\":\"endTime\",\"type\":\"Time\"}]'::jsonb";
                case "samplingTime" -> "'[{\"name\":\"samplingTime\",\"type\":\"Time\"}]'::jsonb";
                default -> throw new IllegalArgumentException("Unsupported time summary field: " + field);
            };
            checks.add("COALESCE(s.schema_json, b.schema_json)->'recordSchema'->'fields' @> " + literal);
        }
        return checks.isEmpty() ? "false" : String.join(" AND ", checks);
    }

    private static boolean isGenerationWindowFullyContained(
            ArchiveGeneration generation, TemporalFilter filter) {
        if (filter == null)
            return true;
        if (filter.isLatestTime() || filter.isCurrentTime())
            return false;
        Instant min = filter.getMin();
        if (min != null && !Instant.MIN.equals(min)) {
            boolean lowerClosed = filter.getRange().lowerBoundType() == BoundType.CLOSED;
            if (lowerClosed ? generation.start().isBefore(min) : !generation.start().isAfter(min))
                return false;
        }
        Instant max = filter.getMax();
        if (max != null && !Instant.MAX.equals(max)) {
            // Every row in generation satisfies phenomenon_time < generation.end().
            if (generation.end().isAfter(max))
                return false;
        }
        return true;
    }

    private static boolean isGenerationResultTimeFullyContained(
            ArchiveGeneration generation, TemporalFilter filter) {
        if (filter == null)
            return true;
        if (filter.isLatestTime() || filter.isCurrentTime())
            return false;
        Instant min = filter.getMin();
        if (min != null && !Instant.MIN.equals(min)) {
            Instant minResult = generation.minResultTime();
            if (minResult == null)
                return false;
            boolean lowerClosed = filter.getRange().lowerBoundType() == BoundType.CLOSED;
            if (lowerClosed ? minResult.isBefore(min) : !minResult.isAfter(min))
                return false;
        }
        Instant max = filter.getMax();
        if (max != null && !Instant.MAX.equals(max)) {
            Instant maxResult = generation.maxResultTime();
            if (maxResult == null)
                return false;
            boolean upperClosed = filter.getRange().upperBoundType() == BoundType.CLOSED;
            if (upperClosed ? maxResult.isAfter(max) : !maxResult.isBefore(max))
                return false;
        }
        return true;
    }

    /** Returns whether this store has any generation visible to cold queries. */
    public boolean hasQueryVisibleGenerations() {
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM oscar_archive_generation " +
                        "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED'))")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect query-visible archive generations", e);
        }
    }

    /**
     * Resolves mutation membership for one bounded page of observation IDs.
     * This avoids retaining every historical tombstone and overlay in heap for
     * each query while preserving exact suppression of cold/hot base rows.
     */
    public Set<Long> mutatedObservationIds(Collection<Long> observationIds) {
        if (observationIds == null || observationIds.isEmpty())
            return Set.of();
        String sql = "WITH requested(id) AS (SELECT unnest(?::bigint[])) SELECT requested.id " +
                "FROM requested WHERE EXISTS (SELECT 1 FROM oscar_obs_overlay overlay_row " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "WHERE overlay_row.observation_id=requested.id " +
                "AND overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')) OR EXISTS (" +
                "SELECT 1 FROM oscar_tombstone tombstone WHERE tombstone.subject_type='OBSERVATION' " +
                "AND tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_id=requested.id::text AND tombstone.generation_id IS NOT NULL AND EXISTS (" +
                "SELECT 1 FROM oscar_archive_generation generation WHERE generation.id=tombstone.generation_id " +
                "AND generation.store_key=tombstone.store_key AND generation.id_scope=tombstone.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')))";
        Set<Long> mutated = new HashSet<>();
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, connection.createArrayOf("bigint", observationIds.toArray()));
            statement.setString(2, storeKey);
            statement.setInt(3, idScope);
            statement.setString(4, storeKey);
            statement.setInt(5, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next())
                    mutated.add(result.getLong(1));
            }
            return mutated;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot resolve observation mutations", e);
        }
    }

    /** Returns exact logical-deletion membership for one bounded hot-row page. */
    public Set<Long> tombstonedObservationIds(Collection<Long> observationIds) {
        if (observationIds == null || observationIds.isEmpty())
            return Set.of();
        String sql = "WITH requested(id) AS (SELECT unnest(?::bigint[])) SELECT requested.id " +
                "FROM requested WHERE EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                "WHERE tombstone.subject_type='OBSERVATION' AND tombstone.store_key=? " +
                "AND tombstone.id_scope=? AND tombstone.subject_id=requested.id::text)";
        Set<Long> tombstoned = new HashSet<>();
        try (Connection connection = connections.open()) {
            java.sql.Array ids = connection.createArrayOf("bigint", observationIds.toArray());
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, ids);
                statement.setString(2, storeKey);
                statement.setInt(3, idScope);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next())
                        tombstoned.add(result.getLong(1));
                }
            } finally {
                ids.free();
            }
            return tombstoned;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot resolve observation tombstones", e);
        }
    }

    /**
     * Streams only query-relevant overlays in the same global order used by
     * hot and Parquet observations. Structured predicates are pushed into
     * PostgreSQL; payload predicates remain in the tiered store after decode.
     * Tombstoned observations are excluded even if they were overlaid prior to
     * deletion.
     */
    public Stream<ArchivedOverlay> streamOverlays(int scope, ObsFilter filter,
                                                   Set<Long> dataStreams, Set<Long> fois) {
        return streamOverlays(scope, filter, dataStreams, fois, 1_000);
    }

    /**
     * A small first page avoids materializing hundreds of unused overlays for
     * short, unfiltered reads. Later pages grow to the usual bounded size.
     */
    public Stream<ArchivedOverlay> streamOverlays(int scope, ObsFilter filter,
                                                   Set<Long> dataStreams, Set<Long> fois,
                                                   int initialPageSize) {
        if (initialPageSize < 1 || initialPageSize > 1_000)
            throw new IllegalArgumentException("Overlay initial page size must be in 1..1000");
        if (dataStreams != null && dataStreams.isEmpty())
            return Stream.empty();
        if (fois != null && fois.isEmpty())
            return Stream.empty();

        List<QueryParameter> parameters = new ArrayList<>();
        String baseSql = buildOverlayQuerySql(
                "overlay_row.generation_id, overlay_row.observation_id, overlay_row.datastream_id, " +
                        "overlay_row.foi_id, overlay_row.phenomenon_time, overlay_row.result_time, " +
                        "overlay_row.result_json, overlay_row.schema_json, overlay_row.schema_sha256",
                filter, dataStreams, fois, parameters);
        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();
        // Do not retain a PostgreSQL connection while the tier merge invokes
        // the hot iterator or a base-row mutation lookup: those callbacks use
        // the same bounded pool and a retained cursor can deadlock at pool size
        // one (or when every pool connection is serving a concurrent merge).
        // Keyset pages keep memory bounded without OFFSET's quadratic rescans.
        OverlayCursor cursor = new OverlayCursor(scope, baseSql,
                List.copyOf(parameters), descending, initialPageSize);
        openOverlayCursors.incrementAndGet();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(cursor,
                        Spliterator.ORDERED | Spliterator.NONNULL), false)
                .onClose(cursor::close);
    }

    /**
     * Counts non-tombstoned active/sealed overlays matching {@code filter}
     * without SWE {@code DataBlock} decoding.
     */
    public long countMatchingOverlays(ObsFilter filter, Set<Long> dataStreams, Set<Long> fois) {
        if (dataStreams != null && dataStreams.isEmpty())
            return 0L;
        if (fois != null && fois.isEmpty())
            return 0L;

        List<QueryParameter> parameters = new ArrayList<>();
        boolean hasCql = filter.getCQLFilter() != null;
        String sql = buildOverlayQuerySql(
                hasCql ? "overlay_row.result_json::text" : "count(*)::bigint",
                filter, dataStreams, fois, parameters);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setReadOnly(true);
            if (hasCql)
                connection.setAutoCommit(false);
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                int parameter = 1;
                for (QueryParameter qp : parameters)
                    qp.bind(connection, statement, parameter++);
                if (hasCql)
                    statement.setFetchSize(1_000);
                try (ResultSet result = statement.executeQuery()) {
                    if (!hasCql)
                        return result.next() ? result.getLong(1) : 0L;
                    java.util.function.Predicate<String> cqlMatcher =
                            JsonCqlEvaluator.compileMatcher(filter.getCQLFilter());
                    long matched = 0L;
                    while (result.next()) {
                        if (cqlMatcher.test(result.getString(1)))
                            matched = Math.addExact(matched, 1L);
                    }
                    return matched;
                }
            } finally {
                if (hasCql) {
                    connection.rollback();
                    connection.setAutoCommit(oldAutoCommit);
                }
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot count observation overlays", e);
        }
    }

    /**
     * Returns mutated (overlaid or tombstoned) observation IDs grouped by
     * owning generation for the given candidate generation set.
     */
    public Map<UUID, Set<Long>> mutatedObservationIdsByGeneration(Collection<UUID> generationIds) {
        if (generationIds == null || generationIds.isEmpty())
            return Map.of();
        String sql = "WITH target_gen(id) AS (SELECT unnest(?::uuid[])) " +
                "SELECT overlay_row.generation_id, overlay_row.observation_id " +
                "FROM oscar_obs_overlay overlay_row " +
                "JOIN target_gen tg ON tg.id=overlay_row.generation_id " +
                "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED') " +
                "UNION " +
                "SELECT tombstone.generation_id, tombstone.subject_id::bigint " +
                "FROM oscar_tombstone tombstone " +
                "JOIN target_gen tg ON tg.id=tombstone.generation_id " +
                "JOIN oscar_archive_generation generation ON generation.id=tombstone.generation_id " +
                "WHERE tombstone.store_key=? AND tombstone.id_scope=? " +
                "AND tombstone.subject_type='OBSERVATION' AND tombstone.subject_id ~ '^[0-9]+$' " +
                "AND generation.store_key=tombstone.store_key AND generation.id_scope=tombstone.id_scope " +
                "AND generation.state IN ('ACTIVE','SEALED')";
        Map<UUID, Set<Long>> byGeneration = new HashMap<>();
        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            java.sql.Array genArray = connection.createArrayOf("uuid", generationIds.toArray());
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setArray(1, genArray);
                statement.setString(2, storeKey);
                statement.setInt(3, idScope);
                statement.setString(4, storeKey);
                statement.setInt(5, idScope);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        UUID genId = result.getObject(1, UUID.class);
                        long obsId = result.getLong(2);
                        byGeneration.computeIfAbsent(genId, ignored -> new HashSet<>()).add(obsId);
                    }
                }
            } finally {
                genArray.free();
            }
            return byGeneration;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot resolve mutated observation IDs by generation", e);
        }
    }

    private String buildOverlayQuerySql(String projection, ObsFilter filter,
                                        Set<Long> dataStreams, Set<Long> fois,
                                        List<QueryParameter> parameters) {
        StringBuilder sql = new StringBuilder("SELECT ").append(projection)
                .append(" FROM oscar_obs_overlay overlay_row " +
                        "JOIN oscar_archive_generation generation ON generation.id=overlay_row.generation_id " +
                        (projection.contains("overlay_schema_blob") ?
                                "LEFT JOIN oscar_archive_schema_blob overlay_schema_blob " +
                                "ON overlay_schema_blob.schema_sha256=overlay_row.schema_sha256 " : "") +
                        "WHERE overlay_row.store_key=? AND overlay_row.id_scope=? " +
                        "AND generation.store_key=overlay_row.store_key AND generation.id_scope=overlay_row.id_scope " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "AND NOT EXISTS (SELECT 1 FROM oscar_tombstone tombstone " +
                        "WHERE tombstone.store_key=overlay_row.store_key " +
                        "AND tombstone.id_scope=overlay_row.id_scope " +
                        "AND tombstone.subject_type='OBSERVATION' " +
                        "AND tombstone.subject_id=overlay_row.observation_id::text " +
                        "AND tombstone.generation_id IS NOT NULL " +
                        "AND EXISTS (SELECT 1 FROM oscar_archive_generation tg " +
                        "WHERE tg.id=tombstone.generation_id AND tg.store_key=tombstone.store_key " +
                        "AND tg.id_scope=tombstone.id_scope AND tg.state IN ('ACTIVE','SEALED')))");
        parameters.add(new StringParameter(storeKey));
        parameters.add(new IntegerParameter(idScope));

        if (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty()) {
            List<Long> ids = filter.getInternalIDs().stream().map(BigId::getIdAsLong).toList();
            appendLongArray(sql, "overlay_row.observation_id", ids, parameters);
        }
        if (dataStreams != null)
            appendLongArray(sql, "overlay_row.datastream_id", dataStreams, parameters);
        if (fois != null) {
            Set<Long> explicit = new HashSet<>(fois);
            boolean includeNull = explicit.remove(0L);
            if (includeNull) {
                sql.append(" AND (overlay_row.foi_id IS NULL");
                if (!explicit.isEmpty()) {
                    sql.append(" OR overlay_row.foi_id=ANY (?::bigint[])");
                    parameters.add(new LongArrayParameter(explicit.toArray()));
                }
                sql.append(')');
            } else {
                appendLongArray(sql, "overlay_row.foi_id", explicit, parameters);
            }
        }
        appendTime(sql, "overlay_row.phenomenon_time", filter.getPhenomenonTime(), parameters);
        appendTime(sql, "overlay_row.result_time", filter.getResultTime(), parameters);
        // Apply only a conservative predicate: mutation membership remains
        // independent of this query and exact CQL evaluation follows on survivors.
        JsonCqlEvaluator.appendPostgresOverlayPrefilter(
                filter.getCQLFilter(), "overlay_row.result_json", sql);
        return sql.toString();
    }

    public int getOpenOverlayCursorCount() {
        return openOverlayCursors.get();
    }

    public long getOverlayRowsRead() {
        return overlayRowsRead.get();
    }

    private static void appendLongArray(StringBuilder sql, String column, Collection<Long> values,
                                        List<QueryParameter> parameters) {
        if (values.isEmpty()) {
            sql.append(" AND false");
            return;
        }
        sql.append(" AND ").append(column).append("=ANY (?::bigint[])");
        parameters.add(new LongArrayParameter(values.toArray()));
    }

    private static void appendTime(StringBuilder sql, String column, TemporalFilter filter,
                                   List<QueryParameter> parameters) {
        if (filter == null || filter.isLatestTime())
            return;
        Instant minimum = filter.getMin();
        Instant maximum = filter.getMax();
        if (minimum != null && !Instant.MIN.equals(minimum)) {
            String operator = filter.getRange().lowerBoundType() == BoundType.CLOSED ? ">=" : ">";
            sql.append(" AND ").append(column).append(operator).append('?');
            parameters.add(new InstantParameter(minimum));
        }
        if (maximum != null && !Instant.MAX.equals(maximum)) {
            String operator = filter.getRange().upperBoundType() == BoundType.CLOSED ? "<=" : "<";
            sql.append(" AND ").append(column).append(operator).append('?');
            parameters.add(new InstantParameter(maximum));
        }
    }

    private interface QueryParameter {
        void bind(Connection connection, PreparedStatement statement, int index) throws SQLException;
    }

    private record LongArrayParameter(Object[] values) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setArray(index, connection.createArrayOf("bigint", values));
        }
    }

    private record InstantParameter(Instant value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            UtcJdbcTime.setInstant(statement, index, value);
        }
    }

    private record StringParameter(String value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setString(index, value);
        }
    }

    private record IntegerParameter(int value) implements QueryParameter {
        @Override
        public void bind(Connection connection, PreparedStatement statement, int index) throws SQLException {
            statement.setInt(index, value);
        }
    }

    private final class OverlayCursor implements Iterator<ArchivedOverlay>, AutoCloseable {
        private static final int PAGE_SIZE = 1_000;

        private final int scope;
        private final String baseSql;
        private final List<QueryParameter> baseParameters;
        private final boolean descending;
        private final ArrayDeque<ArchivedOverlay> ready = new ArrayDeque<>();
        private final AtomicBoolean closed = new AtomicBoolean();
        private int nextPageSize;
        private Instant lastPhenomenonTime;
        private long lastObservationId;
        private boolean hasContinuation;
        private boolean exhausted;

        OverlayCursor(int scope, String baseSql, List<QueryParameter> baseParameters,
                      boolean descending, int initialPageSize) {
            this.scope = scope;
            this.baseSql = baseSql;
            this.baseParameters = baseParameters;
            this.descending = descending;
            this.nextPageSize = initialPageSize;
        }

        @Override
        public boolean hasNext() {
            if (!ready.isEmpty())
                return true;
            if (closed.get())
                return false;
            try {
                if (!exhausted)
                    fetchPage();
                if (ready.isEmpty()) {
                    close();
                    return false;
                }
                return true;
            } catch (Exception error) {
                close();
                throw new IllegalStateException("Cannot decode observation overlay", error);
            }
        }

        @Override
        public ArchivedOverlay next() {
            if (!hasNext())
                throw new NoSuchElementException();
            return ready.removeFirst();
        }

        private void fetchPage() throws SQLException {
            // Limit the ordered candidate rows before converting payloads and
            // resolving shared schema JSON. The outer order preserves the cursor
            // contract even if the join chooses a different physical order.
            StringBuilder sql = new StringBuilder(
                    "SELECT page.generation_id, page.observation_id, page.datastream_id, " +
                    "page.foi_id, page.phenomenon_time, page.result_time, " +
                    "page.result_json::text, COALESCE(page.schema_json, overlay_schema_blob.schema_json)::text " +
                    "FROM (").append(baseSql);
            if (hasContinuation) {
                if (lastPhenomenonTime == null) {
                    sql.append(" AND ((overlay_row.phenomenon_time IS NULL " +
                            "AND overlay_row.observation_id>?) " +
                            "OR overlay_row.phenomenon_time IS NOT NULL)");
                } else {
                    sql.append(" AND overlay_row.phenomenon_time IS NOT NULL AND (")
                            .append("overlay_row.phenomenon_time")
                            .append(descending ? "<?" : ">?")
                            .append(" OR (overlay_row.phenomenon_time=? " +
                                    "AND overlay_row.observation_id>?))");
                }
            }
            sql.append(" ORDER BY overlay_row.phenomenon_time ")
                    .append(descending ? "DESC" : "ASC")
                    .append(" NULLS FIRST, overlay_row.observation_id ASC LIMIT ?) page ")
                    .append("LEFT JOIN oscar_archive_schema_blob overlay_schema_blob ")
                    .append("ON overlay_schema_blob.schema_sha256=page.schema_sha256 ")
                    .append("ORDER BY page.phenomenon_time ")
                    .append(descending ? "DESC" : "ASC")
                    .append(" NULLS FIRST, page.observation_id ASC");

            int fetched = 0;
            int pageSize = nextPageSize;
            try (Connection connection = connections.open();
                 PreparedStatement statement = connection.prepareStatement(sql.toString())) {
                connection.setReadOnly(true);
                int parameter = 1;
                for (QueryParameter baseParameter : baseParameters)
                    baseParameter.bind(connection, statement, parameter++);
                if (hasContinuation) {
                    if (lastPhenomenonTime == null) {
                        statement.setLong(parameter++, lastObservationId);
                    } else {
                        UtcJdbcTime.setInstant(statement, parameter++, lastPhenomenonTime);
                        UtcJdbcTime.setInstant(statement, parameter++, lastPhenomenonTime);
                        statement.setLong(parameter++, lastObservationId);
                    }
                }
                statement.setInt(parameter, pageSize);
                try (ResultSet result = statement.executeQuery()) {
                    while (result.next()) {
                        UUID generationId = result.getObject(1, UUID.class);
                        long id = result.getLong(2);
                        Long foi = result.getObject(4, Long.class);
                        Instant phenomenonTime = UtcJdbcTime.getInstant(result, 5);
                        ready.addLast(new ArchivedOverlay(generationId, result.getString(8),
                                new ArchivedObservation(scope, id, scope, result.getLong(3),
                                        foi == null ? null : scope, foi, phenomenonTime,
                                        UtcJdbcTime.getInstant(result, 6), result.getString(7), "OVERLAY")));
                        lastPhenomenonTime = phenomenonTime;
                        lastObservationId = id;
                        fetched++;
                    }
                }
            }
            overlayRowsRead.addAndGet(fetched);
            hasContinuation = fetched > 0;
            exhausted = fetched < pageSize;
            if (!exhausted)
                nextPageSize = Math.min(PAGE_SIZE, pageSize * 2);
        }

        @Override
        public void close() {
            if (!closed.compareAndSet(false, true))
                return;
            openOverlayCursors.decrementAndGet();
            ready.clear();
        }
    }

    /**
     * Returns the exact logical observation cardinality without opening or
     * decoding Parquet payloads. Generation row IDs are disjoint by contract;
     * hot rows carrying an active generation marker are the deliberate grace
     * overlap and are therefore subtracted once. Observation tombstones are
     * subtracted only while their generation is query-visible.
     */
    public long countTieredRecords(String observationTable) {
        String observations = safeIdentifier(observationTable);
        String overlap = resourceConfig.isParquetPrimary()
                ? "overlap AS (SELECT count(*)::bigint count FROM " + observations + " o " +
                "JOIN oscar_obs_stage_owner owner ON owner.store_key=? AND owner.id_scope=? " +
                "AND owner.observation_id=o.id " +
                "JOIN oscar_archive_generation g ON g.id=owner.generation_id " +
                "WHERE g.store_key=owner.store_key AND g.id_scope=owner.id_scope " +
                "AND g.state IN ('ACTIVE','SEALED')), "
                : "overlap AS (SELECT count(*)::bigint count FROM " + observations + " o " +
                "JOIN oscar_archive_generation g ON g.id=o.archivegenerationid " +
                "WHERE g.store_key=? AND g.id_scope=? AND g.state IN ('ACTIVE','SEALED')), ";
        String sql = "WITH cold AS (" +
                "SELECT COALESCE(sum(row_count), 0)::bigint count FROM oscar_archive_generation " +
                "WHERE store_key=? AND id_scope=? AND state IN ('ACTIVE','SEALED')), " +
                overlap +
                "deleted AS (SELECT count(*)::bigint count FROM oscar_tombstone t " +
                "WHERE t.store_key=? AND t.id_scope=? AND t.subject_type='OBSERVATION' " +
                "AND t.subject_id ~ '^[0-9]+$' " +
                "AND t.generation_id IS NOT NULL AND EXISTS (SELECT 1 FROM oscar_archive_generation exact_generation " +
                "WHERE exact_generation.id=t.generation_id AND exact_generation.store_key=t.store_key " +
                "AND exact_generation.id_scope=t.id_scope AND exact_generation.state IN ('ACTIVE','SEALED'))) " +
                "SELECT (SELECT count(*) FROM " + observations + ") + cold.count - overlap.count - deleted.count " +
                "FROM cold, overlap, deleted";
        try (Connection connection = connections.open(); PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, storeKey);
            statement.setInt(4, idScope);
            statement.setString(5, storeKey);
            statement.setInt(6, idScope);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next())
                    return 0;
                long count = result.getLong(1);
                if (count < 0)
                    throw new IllegalStateException("Tiered observation count invariant was violated");
                return count;
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot count tiered observations", e);
        }
    }

    public void tombstone(long observationId, String reason, String policy) {
        throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
    }

    public void tombstone(long observationId, UUID generationId, String reason, String policy) {
        if (generationId == null)
            throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                lockForMutation(connection, observationId);
                ActiveRemovalHolds holds = lockActiveRemovalHolds(
                        connection, List.of(observationId));
                requireRemovalAllowed(holds, observationId, generationId);
                try (PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO oscar_tombstone(store_key, id_scope, subject_type, subject_id, generation_id, reason, policy) " +
                                "SELECT ?, ?, 'OBSERVATION', ?, generation.id, ?, ? " +
                                "FROM oscar_archive_generation generation WHERE generation.id=? " +
                                "AND generation.store_key=? AND generation.id_scope=? " +
                                "AND generation.state IN ('ACTIVE','SEALED') " +
                                "ON CONFLICT (subject_type, store_key, id_scope, subject_id) DO UPDATE SET " +
                                "generation_id=EXCLUDED.generation_id, reason=EXCLUDED.reason, policy=EXCLUDED.policy, " +
                                "deleted_at=now()")) {
                    statement.setString(1, storeKey);
                    statement.setInt(2, idScope);
                    statement.setString(3, Long.toString(observationId));
                    statement.setString(4, reason);
                    statement.setString(5, policy);
                    statement.setObject(6, generationId);
                    statement.setString(7, storeKey);
                    statement.setInt(8, idScope);
                    if (statement.executeUpdate() != 1)
                        throw new IllegalArgumentException("Archive generation does not belong to this observation store: " + generationId);
                }
                deleteFromLatestIfPresent(connection, observationId);
                connection.commit();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException("Cannot tombstone archived observation", e);
        }
    }

    /**
     * Creates an observation tombstone on a caller-owned transaction and
     * reports whether this call performed the logical visible-to-deleted
     * transition.  A concurrent or repeated deletion returns {@code false}
     * instead of being double-counted. The caller must have acquired
     * {@link #lockForMutation(Connection, Collection)} for this ID.
     */
    public boolean tombstoneIfAbsent(Connection connection, long observationId,
                                     UUID generationId, String reason, String policy)
            throws SQLException {
        ActiveRemovalHolds holds = lockActiveRemovalHolds(connection, List.of(observationId));
        return tombstoneIfAbsent(connection, observationId, generationId, reason, policy, holds);
    }

    /**
     * Batch form using the caller's transaction-scoped hold snapshot. The
     * snapshot must have been acquired after the same transaction took all
     * relevant mutation locks.
     */
    public boolean tombstoneIfAbsent(Connection connection, long observationId,
                                     UUID generationId, String reason, String policy,
                                     ActiveRemovalHolds holds)
            throws SQLException {
        if (connection == null)
            throw new IllegalArgumentException("A transaction connection is required");
        if (generationId == null)
            throw new IllegalArgumentException("Archived observation tombstones require an exact generation ID");
        requireTransaction(connection);
        requireRemovalAllowed(holds, observationId, generationId);
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO oscar_tombstone(store_key, id_scope, subject_type, subject_id, generation_id, reason, policy) " +
                        "SELECT ?, ?, 'OBSERVATION', ?, generation.id, ?, ? " +
                        "FROM oscar_archive_generation generation WHERE generation.id=? " +
                        "AND generation.store_key=? AND generation.id_scope=? " +
                        "AND generation.state IN ('ACTIVE','SEALED') " +
                        "ON CONFLICT (subject_type, store_key, id_scope, subject_id) DO NOTHING")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, Long.toString(observationId));
            statement.setString(4, reason);
            statement.setString(5, policy);
            statement.setObject(6, generationId);
            statement.setString(7, storeKey);
            statement.setInt(8, idScope);
            int inserted = statement.executeUpdate();
            if (inserted == 1) {
                deleteFromLatestIfPresent(connection, observationId);
                return true;
            }
        }

        // Zero rows is valid only for an already deleted observation.  Do not
        // silently convert a stale/cross-store generation reference into a
        // successful removal.
        try (PreparedStatement existing = connection.prepareStatement(
                "SELECT generation_id FROM oscar_tombstone WHERE store_key=? AND id_scope=? " +
                        "AND subject_type='OBSERVATION' AND subject_id=?")) {
            existing.setString(1, storeKey);
            existing.setInt(2, idScope);
            existing.setString(3, Long.toString(observationId));
            try (ResultSet result = existing.executeQuery()) {
                if (result.next())
                    return false;
            }
        }
        throw new IllegalArgumentException(
                "Archive generation is not query-visible for this observation store: " + generationId);
    }

    private void deleteFromLatestIfPresent(Connection connection, long observationId) throws SQLException {
        try (PreparedStatement delete = connection.prepareStatement(
                "DELETE FROM oscar_obs_latest WHERE store_key=? AND id_scope=? AND observation_id=?")) {
            delete.setString(1, storeKey);
            delete.setInt(2, idScope);
            delete.setLong(3, observationId);
            delete.executeUpdate();
        }
        try (PreparedStatement deleteLocator = connection.prepareStatement(
                "DELETE FROM oscar_event_locator WHERE store_key=? AND id_scope=? AND observation_id=?")) {
            deleteLocator.setString(1, storeKey);
            deleteLocator.setInt(2, idScope);
            deleteLocator.setLong(3, observationId);
            deleteLocator.executeUpdate();
        }
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*"))
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        return identifier;
    }

    public record SchemaSnapshot(long dataStreamId, String json) {}
    public record ArchivedOverlay(UUID generationId, String schemaJson, ArchivedObservation observation) {}
}
