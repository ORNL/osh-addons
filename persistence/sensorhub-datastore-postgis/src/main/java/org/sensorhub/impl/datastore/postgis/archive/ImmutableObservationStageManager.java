package org.sensorhub.impl.datastore.postgis.archive;

import com.google.common.collect.BoundType;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

/**
 * Owns the short-lived PostgreSQL landing partitions used by
 * {@link TieredArchiveConfig.StorageMode#PARQUET_PRIMARY}.
 *
 * <p>The normal observation table stores only mutable/workflow observations.
 * Raw immutable observations are routed by a parent-table trigger into small
 * LOGGED inheritance children. PostgreSQL inheritance is intentional here:
 * existing OSH reads, updates and deletes against the observation table see
 * attached children without adding a second query implementation, while each
 * child has its own deliberately small index set. Once every row in a child
 * has a verified Parquet owner, the complete child relation is detached and
 * dropped instead of deleting rows and vacuuming their indexes.</p>
 *
 * <p>This is a fresh-schema feature. It is not an in-place converter for a
 * legacy table that already contains allowlisted raw observations.</p>
 */
public final class ImmutableObservationStageManager {
    private static final String ROUTE_TABLE = "oscar_obs_storage_route";
    private static final String PARTITION_TABLE = "oscar_obs_stage_partition";
    private static final String OWNER_TABLE = "oscar_obs_stage_owner";
    private static final String MUTABLE_COUNTS_TABLE = "oscar_mutable_stream_counts";
    private static final String MUTABLE_COUNTS_DELTA_TABLE = "oscar_mutable_stream_counts_delta";
    private static final String MUTABLE_COUNTS_STATE_TABLE = "oscar_mutable_stream_counts_state";
    private static final int MUTABLE_COUNTS_DELTA_COMPACT_BATCH = 50_000;
    private static final int MUTABLE_COUNTS_DELTA_MAX_BATCHES_PER_CYCLE = 4;
    private static final DateTimeFormatter SQL_TIMESTAMP = DateTimeFormatter.ISO_LOCAL_DATE_TIME;

    private final SqlConnectionProvider connections;
    private final ArchiveCatalog archiveCatalog;
    private final TieredArchiveConfig config;
    private final String observationTable;
    private final String dataStreamTable;
    private final String storeKey;
    private final int idScope;
    private final String identifierHash;
    private final String triggerName;
    private final String triggerFunction;
    private final String mutableCountInsertTrigger;
    private final String mutableCountInsertFunction;
    private final String mutableCountUpdateTrigger;
    private final String mutableCountUpdateFunction;
    private final String mutableCountDeleteTrigger;
    private final String mutableCountDeleteFunction;
    private final String mutableCountTruncateTrigger;
    private final String mutableCountTruncateFunction;
    private final Path archiveRoot;

    public ImmutableObservationStageManager(SqlConnectionProvider connections,
                                            ArchiveCatalog archiveCatalog,
                                            TieredArchiveConfig config,
                                            String observationTable,
                                            String dataStreamTable,
                                            int idScope) {
        this.connections = connections;
        this.archiveCatalog = archiveCatalog;
        this.config = config;
        this.observationTable = safeIdentifier(observationTable);
        this.dataStreamTable = safeIdentifier(dataStreamTable);
        this.storeKey = observationTable;
        this.idScope = idScope;
        this.identifierHash = identifierDigest(storeKey + ":" + idScope);
        this.triggerName = safeIdentifier("oscar_stage_route_" + identifierHash);
        this.triggerFunction = safeIdentifier(triggerName + "_fn");
        this.mutableCountInsertTrigger = safeIdentifier("oscar_mc_ins_" + identifierHash);
        this.mutableCountInsertFunction = safeIdentifier(mutableCountInsertTrigger + "_fn");
        this.mutableCountUpdateTrigger = safeIdentifier("oscar_mc_upd_" + identifierHash);
        this.mutableCountUpdateFunction = safeIdentifier(mutableCountUpdateTrigger + "_fn");
        this.mutableCountDeleteTrigger = safeIdentifier("oscar_mc_del_" + identifierHash);
        this.mutableCountDeleteFunction = safeIdentifier(mutableCountDeleteTrigger + "_fn");
        this.mutableCountTruncateTrigger = safeIdentifier("oscar_mc_trn_" + identifierHash);
        this.mutableCountTruncateFunction = safeIdentifier(mutableCountTruncateTrigger + "_fn");
        this.archiveRoot = Path.of(config.rootDirectory).toAbsolutePath().normalize();
    }

    /** A batch-local immutable routing decision captured from live metadata. */
    public record Route(long dataStreamId, Instant phenomenonTime, boolean immutable) {
    }

    /** Operator-facing physical staging state. */
    public record PartitionState(String childTable, Instant windowStart,
                                 Instant windowEnd, String state) {
    }

    /** Creates/reconciles the durable routing schema and parent trigger. */
    public void initialize() {
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("CREATE TABLE IF NOT EXISTS " + ROUTE_TABLE + " (" +
                            "store_key text NOT NULL, id_scope integer NOT NULL, " +
                            "datastream_id bigint NOT NULL, immutable boolean NOT NULL, " +
                            "updated_at timestamptz NOT NULL DEFAULT now(), " +
                            "PRIMARY KEY(store_key, id_scope, datastream_id))");
                    statement.execute("CREATE TABLE IF NOT EXISTS " + PARTITION_TABLE + " (" +
                            "store_key text NOT NULL, id_scope integer NOT NULL, " +
                            "parent_table text NOT NULL, child_table text NOT NULL, " +
                            "window_start timestamp NOT NULL, window_end timestamp NOT NULL, " +
                            "state text NOT NULL CHECK (state IN ('ATTACHED','DETACHED')), " +
                            "created_at timestamptz NOT NULL DEFAULT now(), detached_at timestamptz, " +
                            "PRIMARY KEY(store_key, id_scope, child_table), " +
                            "CHECK (window_start < window_end))");
                    statement.execute("CREATE UNIQUE INDEX IF NOT EXISTS oscar_obs_stage_one_attached_window " +
                            "ON " + PARTITION_TABLE + "(store_key,id_scope,window_start,window_end) " +
                            "WHERE state='ATTACHED'");
                    statement.execute("CREATE TABLE IF NOT EXISTS " + OWNER_TABLE + " (" +
                            "store_key text NOT NULL, id_scope integer NOT NULL, " +
                            "child_table text NOT NULL, observation_id bigint NOT NULL, " +
                            "generation_id uuid NOT NULL REFERENCES oscar_archive_generation(id) " +
                            "DEFERRABLE INITIALLY DEFERRED, " +
                            "PRIMARY KEY(store_key,id_scope,observation_id))");
                    statement.execute("CREATE INDEX IF NOT EXISTS oscar_obs_stage_owner_child_idx ON " +
                            OWNER_TABLE + "(store_key,id_scope,child_table)");
                    statement.execute("DROP INDEX IF EXISTS oscar_obs_stage_owner_generation_idx");
                    statement.execute(createRoutingFunctionSql());
                    statement.execute("DROP TRIGGER IF EXISTS " + triggerName + " ON " + observationTable);
                    statement.execute("CREATE TRIGGER " + triggerName + " BEFORE INSERT ON " +
                            observationTable + " FOR EACH ROW EXECUTE FUNCTION " + triggerFunction + "()");
                }
                boolean existingParquetPrimary = upgradeAllowlistedImmutableRoutes(connection);
                initializeMutableStreamCounts(connection);
                reconcileCatalog(connection);
                rejectLegacyRawRows(connection, existingParquetPrimary);
                connection.commit();
            } catch (Exception error) {
                connection.rollback();
                throw error;
            } finally {
                connection.setAutoCommit(oldAutoCommit);
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot initialize immutable observation staging", error);
        }
    }

    /**
     * Registers stable datastream classifications and creates every event-time
     * child needed by this transaction before the parent trigger executes.
     * A database-clock skew guard keeps future detector timestamps from
     * defeating the bounded staging residence objective.
     */
    public void prepare(Connection connection, Collection<Route> routes) throws SQLException {
        requireTransaction(connection);
        Map<Long, Boolean> classifications = new HashMap<>();
        Set<Instant> windows = new LinkedHashSet<>();
        Instant receivedAt = databaseTransactionTime(connection);
        Instant maximumAcceptedPhenomenonTime = receivedAt.plus(
                config.maximumFuturePhenomenonSkewMinutes, ChronoUnit.MINUTES);
        for (Route route : routes) {
            Boolean prior = classifications.putIfAbsent(route.dataStreamId(), route.immutable());
            if (prior != null && prior != route.immutable())
                throw new SQLException("A datastream received conflicting immutable routing decisions");
            if (route.immutable()) {
                if (route.phenomenonTime() == null)
                    throw new SQLException("Immutable detector observations require a phenomenon time");
                if (route.phenomenonTime().isAfter(maximumAcceptedPhenomenonTime))
                    throw new SQLException("Immutable detector phenomenon time " +
                            route.phenomenonTime() + " exceeds the PostgreSQL receipt clock " +
                            receivedAt + " by more than " +
                            config.maximumFuturePhenomenonSkewMinutes + " minute(s)");
                windows.add(TieredArchiveConfig.utcWindowStart(
                        route.phenomenonTime(), config.immutableStageWindowMinutes));
            }
        }
        for (Map.Entry<Long, Boolean> route : classifications.entrySet())
            registerRoute(connection, route.getKey(), route.getValue());
        for (Instant start : windows)
            ensurePartition(connection, start,
                    start.plusSeconds(Math.multiplyExact(
                            (long) config.immutableStageWindowMinutes, 60L)));
    }

    /** Verifies and reclaims a bounded number of complete physical children. */
    public int retireVerifiedPartitions() {
        compactMutableStreamCountDeltas();
        List<PartitionState> candidates = listRetirementCandidates(
                Math.max(16, config.maximumStagePartitionsPerCycle));
        int retired = 0;
        for (PartitionState candidate : candidates) {
            if ("ATTACHED".equals(candidate.state())) {
                if (!detachVerified(candidate))
                    continue;
            }
            if (dropDetached(candidate.childTable()))
                retired++;
        }
        return retired;
    }

    public List<PartitionState> listPartitions() {
        String sql = "SELECT child_table,window_start,window_end,state FROM " + PARTITION_TABLE +
                " WHERE store_key=? AND id_scope=? ORDER BY window_start,child_table";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                List<PartitionState> states = new ArrayList<>();
                while (result.next())
                    states.add(new PartitionState(result.getString(1),
                            UtcJdbcTime.getInstant(result, 2),
                            UtcJdbcTime.getInstant(result, 3), result.getString(4)));
                return states;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot list immutable staging partitions", error);
        }
    }

    /**
     * Returns the transaction-time creation of the oldest attached child.
     * Child creation and the first routed write are one transaction, so this
     * is a conservative first-receipt clock without scanning every raw tuple
     * once per worker cycle. Per-row {@code received_at} remains available to
     * detailed operator and regression probes.
     */
    public Optional<Instant> oldestReceivedAt() {
        String partitions = "SELECT min(created_at) FROM " + PARTITION_TABLE +
                " WHERE store_key=? AND id_scope=? AND state='ATTACHED'";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(partitions)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                return result.next()
                        ? Optional.ofNullable(UtcJdbcTime.getInstant(result, 1))
                        : Optional.empty();
            }
        } catch (SQLException error) {
            throw new IllegalStateException(
                    "Cannot determine oldest immutable staging receipt", error);
        }
    }

    private void registerRoute(Connection connection, long dataStreamId,
                               boolean immutable) throws SQLException {
        String sql = "INSERT INTO " + ROUTE_TABLE +
                "(store_key,id_scope,datastream_id,immutable) VALUES (?,?,?,?) " +
                "ON CONFLICT(store_key,id_scope,datastream_id) DO NOTHING";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setLong(3, dataStreamId);
            statement.setBoolean(4, immutable);
            if (statement.executeUpdate() == 1)
                return;
        }
        try (PreparedStatement existing = connection.prepareStatement(
                "SELECT immutable FROM " + ROUTE_TABLE +
                        " WHERE store_key=? AND id_scope=? AND datastream_id=?")) {
            existing.setString(1, storeKey);
            existing.setInt(2, idScope);
            existing.setLong(3, dataStreamId);
            try (ResultSet result = existing.executeQuery()) {
                if (result.next() && result.getBoolean(1) == immutable)
                    return;
            }
        }
        // Changing an established storage class would make a datastream span
        // incompatible authority rules. A new datastream version/ID is the
        // explicit and auditable way to change that classification.
        throw new SQLException("Datastream " + dataStreamId +
                " cannot change between mutable and immutable storage classes");
    }

    private void ensurePartition(Connection connection, Instant start,
                                 Instant end) throws SQLException {
        try (PreparedStatement lock = connection.prepareStatement(
                "SELECT pg_advisory_xact_lock(hashtextextended(?,0))")) {
            lock.setString(1, "oscar-stage-window:" + storeKey + ":" + idScope + ":" + start);
            lock.execute();
        }
        if (attachedPartition(connection, start, end) != null)
            return;

        String child = safeIdentifier("osg_" + identifierHash + "_" +
                Math.floorDiv(start.getEpochSecond(), 60L) + "_" +
                UUID.randomUUID().toString().replace("-", "").substring(0, 8));
        String constraint = safeIdentifier(child + "_window_ck");
        String startSql = sqlTimestamp(start);
        String endSql = sqlTimestamp(end);
        try (Statement ddl = connection.createStatement()) {
            ddl.execute("CREATE TABLE " + child + " (" +
                    "received_at timestamptz NOT NULL DEFAULT transaction_timestamp(), " +
                    "CONSTRAINT " + constraint + " CHECK (phenomenontime IS NOT NULL AND " +
                    "phenomenontime >= TIMESTAMP '" + startSql + "' AND " +
                    "phenomenontime < TIMESTAMP '" + endSql + "')) INHERITS (" +
                    observationTable + ")");
            ddl.execute("CREATE UNIQUE INDEX " + safeIdentifier(child + "_id_uidx") +
                    " ON " + child + "(id)");
            ddl.execute("CREATE UNIQUE INDEX " + safeIdentifier(child + "_natural_uidx") +
                    " ON " + child + "(datastreamid,COALESCE(foiid,-1::bigint)," +
                    "phenomenontime,COALESCE(resulttime,'-infinity'::timestamp))");
            ddl.execute("CREATE INDEX " + safeIdentifier(child + "_stream_rtime_idx") +
                    " ON " + child + "(datastreamid,resulttime,id)");
            ddl.execute("CREATE INDEX " + safeIdentifier(child + "_time_id_asc_idx") +
                    " ON " + child + "(phenomenontime ASC NULLS FIRST,id ASC)");
            ddl.execute("CREATE INDEX " + safeIdentifier(child + "_time_id_desc_idx") +
                    " ON " + child + "(phenomenontime DESC NULLS FIRST,id ASC)");
            ddl.execute("CREATE INDEX " + safeIdentifier(child + "_st_asc_idx") +
                    " ON " + child + "(datastreamid ASC,phenomenontime ASC NULLS FIRST,id ASC)");
            ddl.execute("CREATE INDEX " + safeIdentifier(child + "_st_desc_idx") +
                    " ON " + child + "(datastreamid ASC,phenomenontime DESC NULLS FIRST,id ASC)");
        }
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO " + PARTITION_TABLE +
                        "(store_key,id_scope,parent_table,child_table,window_start,window_end,state) " +
                        "VALUES (?,?,?,?,?,?,'ATTACHED')")) {
            insert.setString(1, storeKey);
            insert.setInt(2, idScope);
            insert.setString(3, observationTable);
            insert.setString(4, child);
            UtcJdbcTime.setInstant(insert, 5, start);
            UtcJdbcTime.setInstant(insert, 6, end);
            insert.executeUpdate();
        }
    }

    private String attachedPartition(Connection connection, Instant start,
                                     Instant end) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT child_table FROM " + PARTITION_TABLE +
                        " WHERE store_key=? AND id_scope=? AND window_start=? AND window_end=? " +
                        "AND state='ATTACHED' FOR SHARE")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            UtcJdbcTime.setInstant(statement, 3, start);
            UtcJdbcTime.setInstant(statement, 4, end);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? result.getString(1) : null;
            }
        }
    }

    private List<PartitionState> listRetirementCandidates(int limit) {
        Instant cutoff = config.archiveCutoff(Instant.now());
        String sql = "SELECT child_table,window_start,window_end,state FROM " + PARTITION_TABLE +
                " WHERE store_key=? AND id_scope=? AND (state='DETACHED' OR window_end<=?) " +
                "ORDER BY CASE state WHEN 'DETACHED' THEN 0 ELSE 1 END,window_start LIMIT ?";
        try (Connection connection = connections.open();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            UtcJdbcTime.setInstant(statement, 3, cutoff);
            statement.setInt(4, limit);
            try (ResultSet result = statement.executeQuery()) {
                List<PartitionState> candidates = new ArrayList<>();
                while (result.next())
                    candidates.add(new PartitionState(result.getString(1),
                            UtcJdbcTime.getInstant(result, 2),
                            UtcJdbcTime.getInstant(result, 3), result.getString(4)));
                return candidates;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot select immutable staging retirement candidates", error);
        }
    }

    private boolean detachVerified(PartitionState candidate) {
        String child = safeIdentifier(candidate.childTable());
        Set<UUID> preflightOwners = verifiedOwnersSnapshot(child);
        if (preflightOwners == null)
            return false;
        verifyArchiveObjects(preflightOwners);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForActivation(connection);
                if (!lockPartitionCatalogRow(connection, child, "ATTACHED")) {
                    connection.rollback();
                    connection.setAutoCommit(oldAutoCommit);
                    return false;
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute("SET LOCAL lock_timeout='250ms'");
                    statement.execute("LOCK TABLE " + child + " IN ACCESS EXCLUSIVE MODE NOWAIT");
                }
                Set<UUID> generations = verifiedOwners(connection, child);
                if (generations == null || !generations.equals(preflightOwners)) {
                    connection.rollback();
                    return false;
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute("ALTER TABLE " + child + " NO INHERIT " + observationTable);
                }
                try (PreparedStatement update = connection.prepareStatement(
                        "UPDATE " + PARTITION_TABLE +
                                " SET state='DETACHED',detached_at=now() WHERE store_key=? AND id_scope=? " +
                                "AND child_table=? AND state='ATTACHED'")) {
                    update.setString(1, storeKey);
                    update.setInt(2, idScope);
                    update.setString(3, child);
                    if (update.executeUpdate() != 1)
                        throw new SQLException("Staging partition catalog changed during detach");
                }
                connection.commit();
                connection.setAutoCommit(oldAutoCommit);
                return true;
            } catch (SQLException busy) {
                connection.rollback();
                connection.setAutoCommit(oldAutoCommit);
                if ("55P03".equals(busy.getSQLState()))
                    return false;
                throw busy;
            } catch (Exception error) {
                connection.rollback();
                connection.setAutoCommit(oldAutoCommit);
                throw error;
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot detach verified staging partition " + child, error);
        }
    }

    private boolean dropDetached(String childTable) {
        String child = safeIdentifier(childTable);
        Set<UUID> preflightOwners = verifiedOwnersSnapshot(child);
        if (preflightOwners == null)
            throw new IllegalStateException(
                    "Detached staging relation lost verified ownership: " + child);
        verifyArchiveObjects(preflightOwners);
        try (Connection connection = connections.open()) {
            boolean oldAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                archiveCatalog.lockForActivation(connection);
                if (!lockPartitionCatalogRow(connection, child, "DETACHED")) {
                    connection.rollback();
                    connection.setAutoCommit(oldAutoCommit);
                    return false;
                }
                if (isInherited(connection, child))
                    throw new SQLException("Detached staging relation is still inherited: " + child);
                Set<UUID> generations = verifiedOwners(connection, child);
                if (generations == null || !generations.equals(preflightOwners))
                    throw new SQLException("Detached staging relation lost verified ownership: " + child);
                try (PreparedStatement deleteOwners = connection.prepareStatement(
                        "DELETE FROM " + OWNER_TABLE +
                                " WHERE store_key=? AND id_scope=? AND child_table=?")) {
                    deleteOwners.setString(1, storeKey);
                    deleteOwners.setInt(2, idScope);
                    deleteOwners.setString(3, child);
                    deleteOwners.executeUpdate();
                }
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP TABLE " + child);
                }
                for (UUID generation : generations) {
                    try (PreparedStatement seal = connection.prepareStatement(
                            "UPDATE oscar_archive_generation generation SET state='SEALED',sealed_at=now() " +
                                    "WHERE generation.id=? AND generation.store_key=? AND generation.id_scope=? " +
                                    "AND generation.state='ACTIVE' AND generation.parity_verified=true " +
                                    "AND NOT EXISTS (SELECT 1 FROM " + OWNER_TABLE +
                                    " remaining WHERE remaining.generation_id=generation.id)")) {
                        seal.setObject(1, generation);
                        seal.setString(2, storeKey);
                        seal.setInt(3, idScope);
                        seal.executeUpdate();
                    }
                }
                try (PreparedStatement delete = connection.prepareStatement(
                        "DELETE FROM " + PARTITION_TABLE +
                                " WHERE store_key=? AND id_scope=? AND child_table=? AND state='DETACHED'")) {
                    delete.setString(1, storeKey);
                    delete.setInt(2, idScope);
                    delete.setString(3, child);
                    if (delete.executeUpdate() != 1)
                        throw new SQLException("Detached staging catalog row disappeared before drop");
                }
                connection.commit();
                connection.setAutoCommit(oldAutoCommit);
                try (Statement analyze = connection.createStatement()) {
                    analyze.execute("ANALYZE " + OWNER_TABLE);
                }
                return true;
            } catch (Exception error) {
                connection.rollback();
                connection.setAutoCommit(oldAutoCommit);
                throw error;
            }
        } catch (Exception error) {
            throw new IllegalStateException("Cannot drop detached staging partition " + child, error);
        }
    }

    /**
     * Returns null until every row has a live parity-verified archive owner;
     * otherwise returns the exact owner set (empty is valid for an empty leaf).
     */
    private Set<UUID> verifiedOwners(Connection connection, String child) throws SQLException {
        Set<UUID> generations = new LinkedHashSet<>();
        long ownedStagedRows = 0;
        String sql = "SELECT owner.generation_id,generation.id," +
                "generation.state,generation.parity_verified " +
                "FROM " + child + " staged LEFT JOIN " + OWNER_TABLE + " owner " +
                "ON owner.store_key=? AND owner.id_scope=? AND owner.child_table=? " +
                "AND owner.observation_id=staged.id " +
                "LEFT JOIN oscar_archive_generation generation ON generation.id=owner.generation_id " +
                "AND generation.store_key=? AND generation.id_scope=?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, child);
            statement.setString(4, storeKey);
            statement.setInt(5, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    UUID stagedOwner = result.getObject(1, UUID.class);
                    UUID catalogOwner = result.getObject(2, UUID.class);
                    String state = result.getString(3);
                    if (stagedOwner == null || catalogOwner == null ||
                            !("ACTIVE".equals(state) || "SEALED".equals(state)) ||
                            !result.getBoolean(4))
                        return null;
                    ownedStagedRows++;
                    generations.add(catalogOwner);
                }
            }
        }
        // An ownership row that no longer names a physical row, or that names
        // a generation from another namespace, is corruption rather than
        // permission to drop the relation. Keep the child fail-closed.
        String integrity = "SELECT count(*),count(staged.id),count(generation.id) FROM " +
                OWNER_TABLE + " owner LEFT JOIN " + child +
                " staged ON staged.id=owner.observation_id " +
                "LEFT JOIN oscar_archive_generation generation ON generation.id=owner.generation_id " +
                "AND generation.store_key=owner.store_key AND generation.id_scope=owner.id_scope " +
                "WHERE owner.store_key=? AND owner.id_scope=? AND owner.child_table=?";
        try (PreparedStatement statement = connection.prepareStatement(integrity)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, child);
            try (ResultSet result = statement.executeQuery()) {
                if (!result.next() || result.getLong(1) != ownedStagedRows ||
                        result.getLong(2) != ownedStagedRows ||
                        result.getLong(3) != ownedStagedRows)
                    return null;
            }
        }
        return generations;
    }

    private Set<UUID> verifiedOwnersSnapshot(String child) {
        try (Connection connection = connections.open()) {
            return verifiedOwners(connection, child);
        } catch (SQLException error) {
            throw new IllegalStateException(
                    "Cannot inspect immutable staging ownership for " + child, error);
        }
    }

    private void verifyArchiveObjects(Set<UUID> generations) {
        if (generations.isEmpty())
            return;
        try (Connection connection = connections.open();
             PreparedStatement generationState = connection.prepareStatement(
                     "SELECT 1 FROM oscar_archive_generation WHERE id=? AND store_key=? " +
                             "AND id_scope=? AND parity_verified=true AND state IN ('ACTIVE','SEALED')");
             PreparedStatement objects = connection.prepareStatement(
                     "SELECT relative_path,sha256,kind FROM oscar_archive_object " +
                             "WHERE generation_id=? AND state='ACTIVE' ORDER BY kind")) {
            for (UUID generation : generations) {
                generationState.setObject(1, generation);
                generationState.setString(2, storeKey);
                generationState.setInt(3, idScope);
                try (ResultSet result = generationState.executeQuery()) {
                    if (!result.next())
                        throw new SQLException("Verified archive generation disappeared: " + generation);
                }
                Set<String> requiredKinds = new LinkedHashSet<>(Set.of(
                        "OBSERVATIONS_PARQUET", "GENERATION_MANIFEST", "DATASTREAM_SCHEMAS"));
                objects.setObject(1, generation);
                try (ResultSet result = objects.executeQuery()) {
                    while (result.next()) {
                        Path object = ArchiveLifecycleManager.resolveArchiveObject(
                                archiveRoot, Path.of(result.getString(1)));
                        if (!result.getString(2).equals(ArchiveExporter.sha256(object)))
                            throw new SQLException(
                                    "Archive checksum changed before staging retirement: " + generation);
                        requiredKinds.remove(result.getString(3));
                    }
                }
                if (!requiredKinds.isEmpty())
                    throw new SQLException("Archive generation " + generation +
                            " is missing required object(s): " + requiredKinds);
            }
        } catch (Exception error) {
            throw new IllegalStateException(
                    "Cannot verify archive objects before staging retirement", error);
        }
    }

    private void reconcileCatalog(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT child_table,state FROM " + PARTITION_TABLE +
                        " WHERE store_key=? AND id_scope=?")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    String child = safeIdentifier(result.getString(1));
                    String state = result.getString(2);
                    boolean exists = relationExists(connection, child);
                    boolean inherited = exists && isInherited(connection, child);
                    if ("ATTACHED".equals(state) && (!exists || !inherited))
                        throw new SQLException("Cataloged attached staging partition is missing/detached: " + child);
                    if ("DETACHED".equals(state) && (!exists || inherited))
                        throw new SQLException("Cataloged detached staging partition has inconsistent DDL state: " + child);
                    if ("ATTACHED".equals(state)) {
                        try (Statement indexDdl = connection.createStatement()) {
                            indexDdl.execute("DROP INDEX IF EXISTS " +
                                    safeIdentifier(child + "_stream_time_idx"));
                            indexDdl.execute("CREATE INDEX IF NOT EXISTS " +
                                    safeIdentifier(child + "_stream_rtime_idx") +
                                    " ON " + child + "(datastreamid,resulttime,id)");
                            indexDdl.execute("CREATE INDEX IF NOT EXISTS " +
                                    safeIdentifier(child + "_time_id_asc_idx") +
                                    " ON " + child + "(phenomenontime ASC NULLS FIRST,id ASC)");
                            indexDdl.execute("CREATE INDEX IF NOT EXISTS " +
                                    safeIdentifier(child + "_time_id_desc_idx") +
                                    " ON " + child + "(phenomenontime DESC NULLS FIRST,id ASC)");
                            indexDdl.execute("CREATE INDEX IF NOT EXISTS " +
                                    safeIdentifier(child + "_st_asc_idx") +
                                    " ON " + child + "(datastreamid ASC,phenomenontime ASC NULLS FIRST,id ASC)");
                            indexDdl.execute("CREATE INDEX IF NOT EXISTS " +
                                    safeIdentifier(child + "_st_desc_idx") +
                                    " ON " + child + "(datastreamid ASC,phenomenontime DESC NULLS FIRST,id ASC)");
                        }
                    }
                }
            }
        }
        try (PreparedStatement orphan = connection.prepareStatement(
                "SELECT owner.child_table,owner.observation_id FROM " + OWNER_TABLE + " owner " +
                        "LEFT JOIN " + PARTITION_TABLE + " partition ON " +
                        "partition.store_key=owner.store_key AND partition.id_scope=owner.id_scope " +
                        "AND partition.child_table=owner.child_table " +
                        "WHERE owner.store_key=? AND owner.id_scope=? AND partition.child_table IS NULL LIMIT 1")) {
            orphan.setString(1, storeKey);
            orphan.setInt(2, idScope);
            try (ResultSet result = orphan.executeQuery()) {
                if (result.next())
                    throw new SQLException("Orphan immutable staging ownership for relation " +
                            result.getString(1) + " and observation " + result.getLong(2));
            }
        }
    }

    private boolean upgradeAllowlistedImmutableRoutes(Connection connection) throws SQLException {
        boolean existingParquetPrimary;
        try (PreparedStatement check = connection.prepareStatement(
                "SELECT EXISTS (SELECT 1 FROM " + ROUTE_TABLE +
                        " WHERE store_key=? AND id_scope=? AND immutable=true)")) {
            check.setString(1, storeKey);
            check.setInt(2, idScope);
            try (ResultSet rs = check.executeQuery()) {
                existingParquetPrimary = rs.next() && rs.getBoolean(1);
            }
        }
        if (!existingParquetPrimary || config.outputNames == null || config.outputNames.isEmpty())
            return false;

        Array allowed = connection.createArrayOf("text", config.outputNames.toArray());
        try (PreparedStatement upgrade = connection.prepareStatement(
                "UPDATE " + ROUTE_TABLE + " route SET immutable=true, updated_at=now() " +
                        "FROM " + dataStreamTable + " ds " +
                        "WHERE route.store_key=? AND route.id_scope=? " +
                        "AND route.datastream_id=ds.id AND route.immutable=false " +
                        "AND ds.data->>'outputName'=ANY (?)")) {
            upgrade.setString(1, storeKey);
            upgrade.setInt(2, idScope);
            upgrade.setArray(3, allowed);
            upgrade.executeUpdate();
        } finally {
            allowed.free();
        }
        return true;
    }

    private void rejectLegacyRawRows(Connection connection, boolean existingParquetPrimary) throws SQLException {
        if (existingParquetPrimary)
            return;
        String sql = "SELECT 1 FROM ONLY " + observationTable + " observation JOIN " +
                dataStreamTable + " datastream ON datastream.id=observation.datastreamid " +
                "WHERE datastream.data->>'outputName'=ANY (?) LIMIT 1";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setArray(1, connection.createArrayOf("text", config.outputNames.toArray()));
            try (ResultSet result = statement.executeQuery()) {
                if (result.next())
                    throw new SQLException("PARQUET_PRIMARY partitioned staging requires a fresh database; " +
                            "allowlisted immutable rows already exist in the mutable observation table");
            }
        }
    }

    /**
     * Drains pre-upgrade rows of newly-allowlisted immutable datastreams from
     * {@code ONLY <observationTable>} into their corresponding UTC-aligned
     * {@code osg_*} child partitions in bounded batches so they are exported to
     * ZSTD Parquet, verified for parity, and dropped via {@code DROP TABLE}.
     */
    public boolean stageLegacyAllowlistedRowsBatch() {
        final int batchLimit = 100_000;
        final int maxWindowsPerCycle = Math.max(
                16,
                Math.max(config.maximumGenerationsPerCycle, config.maximumStagePartitionsPerCycle));
        boolean movedAny = false;
        for (int windowPass = 0; windowPass < maxWindowsPerCycle; windowPass++) {
            try (Connection connection = connections.open()) {
                boolean oldAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    int attachedCount;
                    try (PreparedStatement countStmt = connection.prepareStatement(
                            "SELECT count(*)::integer FROM " + PARTITION_TABLE +
                                    " WHERE store_key=? AND id_scope=? AND state='ATTACHED'")) {
                        countStmt.setString(1, storeKey);
                        countStmt.setInt(2, idScope);
                        try (ResultSet rs = countStmt.executeQuery()) {
                            attachedCount = rs.next() ? rs.getInt(1) : 0;
                        }
                    }

                    List<Long> immutableStreamIds = new ArrayList<>();
                    try (PreparedStatement streams = connection.prepareStatement(
                            "SELECT datastream_id FROM " + ROUTE_TABLE +
                                    " WHERE store_key=? AND id_scope=? AND immutable=true")) {
                        streams.setString(1, storeKey);
                        streams.setInt(2, idScope);
                        try (ResultSet rs = streams.executeQuery()) {
                            while (rs.next())
                                immutableStreamIds.add(rs.getLong(1));
                        }
                    }
                    if (immutableStreamIds.isEmpty()) {
                        connection.rollback();
                        return movedAny;
                    }

                    Instant oldestPhenomenonTime = null;
                    Array streamArray = connection.createArrayOf("bigint", immutableStreamIds.toArray());
                    try (PreparedStatement findOldest = connection.prepareStatement(
                            "SELECT min(sub.min_pt) FROM unnest(?::bigint[]) AS ds_id " +
                                    "CROSS JOIN LATERAL (" +
                                    "  SELECT o.phenomenontime AS min_pt FROM ONLY " + observationTable + " o " +
                                    "  WHERE o.datastreamid=ds_id AND o.phenomenontime IS NOT NULL " +
                                    "  ORDER BY o.phenomenontime ASC, o.id ASC LIMIT 1" +
                                    ") sub")) {
                        findOldest.setArray(1, streamArray);
                        try (ResultSet rs = findOldest.executeQuery()) {
                            if (rs.next())
                                oldestPhenomenonTime = UtcJdbcTime.getInstant(rs, 1);
                        }
                    }
                    if (oldestPhenomenonTime == null) {
                        streamArray.free();
                        connection.rollback();
                        return movedAny;
                    }

                    Instant windowStart = TieredArchiveConfig.utcWindowStart(
                            oldestPhenomenonTime, config.immutableStageWindowMinutes);
                    Instant windowEnd = windowStart.plusSeconds(
                            Math.multiplyExact((long) config.immutableStageWindowMinutes, 60L));
                    boolean alreadyAttached = attachedPartition(connection, windowStart, windowEnd) != null;
                    if (!alreadyAttached && attachedCount >= Math.max(16, config.maximumStagePartitionsPerCycle)) {
                        streamArray.free();
                        connection.rollback();
                        return movedAny;
                    }
                    ensurePartition(connection, windowStart, windowEnd);
                    String child = attachedPartition(connection, windowStart, windowEnd);
                    if (child == null) {
                        streamArray.free();
                        connection.rollback();
                        return movedAny;
                    }
                    String safeChild = safeIdentifier(child);
                    String startSql = sqlTimestamp(windowStart);
                    String endSql = sqlTimestamp(windowEnd);
                    String moveSql = "WITH moved AS (" +
                            "  DELETE FROM ONLY " + observationTable + " o " +
                            "  WHERE o.ctid IN (" +
                            "    SELECT o2.ctid FROM ONLY " + observationTable + " o2 " +
                            "    WHERE o2.datastreamid=ANY (?::bigint[]) " +
                            "      AND o2.phenomenontime >= TIMESTAMP '" + startSql + "' " +
                            "      AND o2.phenomenontime < TIMESTAMP '" + endSql + "' " +
                            "    LIMIT ?" +
                            "  ) " +
                            "  RETURNING o.id,o.datastreamid,o.foiid,o.phenomenontime,o.resulttime,o.result,o.archivegenerationid" +
                            ") INSERT INTO " + safeChild +
                            " (id,datastreamid,foiid,phenomenontime,resulttime,result,archivegenerationid) " +
                            "SELECT id,datastreamid,foiid,phenomenontime,resulttime,result,archivegenerationid FROM moved " +
                            "ON CONFLICT (datastreamid,COALESCE(foiid,-1::bigint),phenomenontime,COALESCE(resulttime,'-infinity'::timestamp)) DO NOTHING";
                    int moved;
                    try (PreparedStatement moveStmt = connection.prepareStatement(moveSql)) {
                        moveStmt.setArray(1, streamArray);
                        moveStmt.setInt(2, batchLimit);
                        moved = moveStmt.executeUpdate();
                    } finally {
                        streamArray.free();
                    }
                    connection.commit();
                    if (moved == 0)
                        return movedAny;
                    movedAny = true;
                } catch (SQLException error) {
                    connection.rollback();
                    throw error;
                } finally {
                    connection.setAutoCommit(oldAutoCommit);
                }
            } catch (SQLException ignored) {
                return movedAny;
            }
        }
        return movedAny;
    }

    private boolean relationExists(Connection connection, String relation) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT to_regclass(?) IS NOT NULL")) {
            statement.setString(1, relation);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() && result.getBoolean(1);
            }
        }
    }

    private boolean isInherited(Connection connection, String child) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM pg_inherits WHERE inhrelid=to_regclass(?) " +
                        "AND inhparent=to_regclass(?)")) {
            statement.setString(1, child);
            statement.setString(2, observationTable);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    /**
     * Serializes routing with whole-child DDL. Ingestion holds a SHARE row lock
     * from partition preparation through commit; retirement takes this UPDATE
     * lock before the relation lock. A writer therefore either finishes in the
     * verified attached child or observes DETACHED and prepares a new child.
     */
    private boolean lockPartitionCatalogRow(Connection connection, String child,
                                            String expectedState) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "SELECT 1 FROM " + PARTITION_TABLE +
                        " WHERE store_key=? AND id_scope=? AND child_table=? AND state=? FOR UPDATE")) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setString(3, child);
            statement.setString(4, expectedState);
            try (ResultSet result = statement.executeQuery()) {
                return result.next();
            }
        }
    }

    private String createRoutingFunctionSql() {
        String store = sqlLiteral(storeKey);
        return "CREATE OR REPLACE FUNCTION " + triggerFunction + "() RETURNS trigger AS $$\n" +
                "DECLARE target_table text;\n" +
                "BEGIN\n" +
                "  IF NOT EXISTS (SELECT 1 FROM " + ROUTE_TABLE +
                " route WHERE route.store_key='" + store + "' AND route.id_scope=" + idScope +
                " AND route.datastream_id=NEW.datastreamid AND route.immutable) THEN\n" +
                "    RETURN NEW;\n" +
                "  END IF;\n" +
                "  IF NEW.phenomenontime IS NULL THEN\n" +
                "    RAISE EXCEPTION 'Immutable detector observations require phenomenon time';\n" +
                "  END IF;\n" +
                "  SELECT partition.child_table INTO target_table FROM " + PARTITION_TABLE +
                " partition WHERE partition.store_key='" + store + "' AND partition.id_scope=" + idScope +
                " AND partition.state='ATTACHED' AND NEW.phenomenontime>=partition.window_start " +
                "AND NEW.phenomenontime<partition.window_end ORDER BY partition.created_at DESC LIMIT 1 FOR SHARE;\n" +
                "  IF target_table IS NULL THEN\n" +
                "    RAISE EXCEPTION 'No prepared immutable staging partition for observation time %', NEW.phenomenontime;\n" +
                "  END IF;\n" +
                "  EXECUTE format($route$INSERT INTO %I (id,datastreamid,foiid,phenomenontime,resulttime,result,archivegenerationid) " +
                "VALUES ($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (datastreamid,COALESCE(foiid,-1::bigint)," +
                "phenomenontime,COALESCE(resulttime,'-infinity'::timestamp)) DO UPDATE SET result=EXCLUDED.result " +
                "WHERE %I.archivegenerationid IS NULL AND %I.result IS DISTINCT FROM EXCLUDED.result " +
                "AND NOT EXISTS (SELECT 1 FROM " + OWNER_TABLE + " owner WHERE owner.store_key='" + store +
                "' AND owner.id_scope=" + idScope + " " +
                "AND owner.observation_id=%I.id)$route$, " +
                "target_table,target_table,target_table,target_table) USING NEW.id,NEW.datastreamid,NEW.foiid,NEW.phenomenontime," +
                "NEW.resulttime,NEW.result,NEW.archivegenerationid;\n" +
                "  RETURN NULL;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql";
    }

    /**
     * Installs statement-level transition triggers maintaining exact per-stream
     * row counts for unarchived mutable rows in {@code ONLY <observationTable>}.
     *
     * <p>Triggers append lock-free delta rows into {@link #MUTABLE_COUNTS_DELTA_TABLE}
     * rather than updating {@link #MUTABLE_COUNTS_TABLE} in place, so multi-statement
     * observation batches and concurrent {@code DatabasePurger} deletions never
     * contend or deadlock on shared per-stream counter rows. Committed deltas are
     * folded into {@link #MUTABLE_COUNTS_TABLE} via {@code FOR UPDATE SKIP LOCKED}
     * during {@link #retireVerifiedPartitions()} and {@link #compactMutableStreamCountDeltas()}.</p>
     */
    private void initializeMutableStreamCounts(Connection connection) throws SQLException {
        String store = sqlLiteral(storeKey);
        String mutableGuard = "%s.archivegenerationid IS NULL AND NOT EXISTS (" +
                "SELECT 1 FROM " + ROUTE_TABLE + " route WHERE route.store_key='" + store +
                "' AND route.id_scope=" + idScope +
                " AND route.datastream_id=%s.datastreamid AND route.immutable)";
        try (Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + MUTABLE_COUNTS_TABLE + " (" +
                    "store_key text NOT NULL, id_scope integer NOT NULL, " +
                    "datastream_id bigint NOT NULL, total_rows bigint NOT NULL DEFAULT 0, " +
                    "null_result_time_rows bigint NOT NULL DEFAULT 0, " +
                    "PRIMARY KEY(store_key, id_scope, datastream_id))");
            statement.execute("CREATE TABLE IF NOT EXISTS " + MUTABLE_COUNTS_DELTA_TABLE + " (" +
                    "store_key text NOT NULL, id_scope integer NOT NULL, " +
                    "datastream_id bigint NOT NULL, total_rows bigint NOT NULL, " +
                    "null_result_time_rows bigint NOT NULL) WITH (" +
                    "autovacuum_vacuum_scale_factor = 0.01, " +
                    "autovacuum_vacuum_threshold = 500, " +
                    "autovacuum_analyze_scale_factor = 0.02, " +
                    "autovacuum_analyze_threshold = 500)");
            statement.execute("CREATE INDEX IF NOT EXISTS oscar_mutable_stream_counts_delta_lookup_idx ON " +
                    MUTABLE_COUNTS_DELTA_TABLE + " (store_key, id_scope, datastream_id)");
            statement.execute("CREATE TABLE IF NOT EXISTS " + MUTABLE_COUNTS_STATE_TABLE + " (" +
                    "store_key text NOT NULL, id_scope integer NOT NULL, " +
                    "table_oid oid NOT NULL DEFAULT 0::oid, " +
                    "initialized_at timestamptz NOT NULL DEFAULT now(), " +
                    "PRIMARY KEY(store_key, id_scope))");
            statement.execute("ALTER TABLE " + MUTABLE_COUNTS_STATE_TABLE +
                    " ADD COLUMN IF NOT EXISTS table_oid oid NOT NULL DEFAULT 0::oid");

            statement.execute("CREATE OR REPLACE FUNCTION " + mutableCountInsertFunction + "() RETURNS trigger AS $$\n" +
                    "BEGIN\n" +
                    "  INSERT INTO " + MUTABLE_COUNTS_DELTA_TABLE +
                    " (store_key, id_scope, datastream_id, total_rows, null_result_time_rows)\n" +
                    "  SELECT '" + store + "', " + idScope + ", ins.datastreamid,\n" +
                    "         count(*)::bigint,\n" +
                    "         count(*) FILTER (WHERE ins.resulttime IS NULL)::bigint\n" +
                    "  FROM ins\n" +
                    "  WHERE " + String.format(mutableGuard, "ins", "ins") + "\n" +
                    "  GROUP BY ins.datastreamid\n" +
                    "  HAVING count(*) <> 0;\n" +
                    "  RETURN NULL;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql");

            statement.execute("CREATE OR REPLACE FUNCTION " + mutableCountUpdateFunction + "() RETURNS trigger AS $$\n" +
                    "BEGIN\n" +
                    "  INSERT INTO " + MUTABLE_COUNTS_DELTA_TABLE +
                    " (store_key, id_scope, datastream_id, total_rows, null_result_time_rows)\n" +
                    "  SELECT '" + store + "', " + idScope + ", delta.datastreamid,\n" +
                    "         sum(delta.d_total)::bigint,\n" +
                    "         sum(delta.d_null)::bigint\n" +
                    "  FROM (\n" +
                    "    SELECT del.datastreamid,\n" +
                    "           -1::bigint AS d_total,\n" +
                    "           CASE WHEN del.resulttime IS NULL THEN -1::bigint ELSE 0::bigint END AS d_null\n" +
                    "    FROM del WHERE " + String.format(mutableGuard, "del", "del") + "\n" +
                    "    UNION ALL\n" +
                    "    SELECT ins.datastreamid,\n" +
                    "           1::bigint AS d_total,\n" +
                    "           CASE WHEN ins.resulttime IS NULL THEN 1::bigint ELSE 0::bigint END AS d_null\n" +
                    "    FROM ins WHERE " + String.format(mutableGuard, "ins", "ins") + "\n" +
                    "  ) delta\n" +
                    "  GROUP BY delta.datastreamid\n" +
                    "  HAVING sum(delta.d_total) <> 0 OR sum(delta.d_null) <> 0;\n" +
                    "  RETURN NULL;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql");

            statement.execute("CREATE OR REPLACE FUNCTION " + mutableCountDeleteFunction + "() RETURNS trigger AS $$\n" +
                    "BEGIN\n" +
                    "  INSERT INTO " + MUTABLE_COUNTS_DELTA_TABLE +
                    " (store_key, id_scope, datastream_id, total_rows, null_result_time_rows)\n" +
                    "  SELECT '" + store + "', " + idScope + ", del.datastreamid,\n" +
                    "         -count(*)::bigint,\n" +
                    "         -count(*) FILTER (WHERE del.resulttime IS NULL)::bigint\n" +
                    "  FROM del\n" +
                    "  WHERE " + String.format(mutableGuard, "del", "del") + "\n" +
                    "  GROUP BY del.datastreamid\n" +
                    "  HAVING count(*) <> 0;\n" +
                    "  RETURN NULL;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql");

            statement.execute("CREATE OR REPLACE FUNCTION " + mutableCountTruncateFunction + "() RETURNS trigger AS $$\n" +
                    "BEGIN\n" +
                    "  DELETE FROM " + MUTABLE_COUNTS_DELTA_TABLE +
                    " WHERE store_key='" + store + "' AND id_scope=" + idScope + ";\n" +
                    "  DELETE FROM " + MUTABLE_COUNTS_TABLE +
                    " WHERE store_key='" + store + "' AND id_scope=" + idScope + ";\n" +
                    "  RETURN NULL;\n" +
                    "END;\n" +
                    "$$ LANGUAGE plpgsql");

            statement.execute("DROP TRIGGER IF EXISTS " + mutableCountInsertTrigger + " ON " + observationTable);
            statement.execute("CREATE TRIGGER " + mutableCountInsertTrigger + " AFTER INSERT ON " +
                    observationTable + " REFERENCING NEW TABLE AS ins FOR EACH STATEMENT EXECUTE FUNCTION " +
                    mutableCountInsertFunction + "()");

            statement.execute("DROP TRIGGER IF EXISTS " + mutableCountUpdateTrigger + " ON " + observationTable);
            statement.execute("CREATE TRIGGER " + mutableCountUpdateTrigger + " AFTER UPDATE ON " +
                    observationTable + " REFERENCING OLD TABLE AS del NEW TABLE AS ins FOR EACH STATEMENT EXECUTE FUNCTION " +
                    mutableCountUpdateFunction + "()");

            statement.execute("DROP TRIGGER IF EXISTS " + mutableCountDeleteTrigger + " ON " + observationTable);
            statement.execute("CREATE TRIGGER " + mutableCountDeleteTrigger + " AFTER DELETE ON " +
                    observationTable + " REFERENCING OLD TABLE AS del FOR EACH STATEMENT EXECUTE FUNCTION " +
                    mutableCountDeleteFunction + "()");

            statement.execute("DROP TRIGGER IF EXISTS " + mutableCountTruncateTrigger + " ON " + observationTable);
            statement.execute("CREATE TRIGGER " + mutableCountTruncateTrigger + " AFTER TRUNCATE ON " +
                    observationTable + " FOR EACH STATEMENT EXECUTE FUNCTION " +
                    mutableCountTruncateFunction + "()");
        }

        long currentTableOid;
        try (PreparedStatement oidQuery = connection.prepareStatement(
                "SELECT to_regclass(?)::oid::bigint")) {
            oidQuery.setString(1, observationTable);
            try (ResultSet rs = oidQuery.executeQuery()) {
                if (!rs.next() || rs.getObject(1) == null)
                    throw new SQLException("Observation table does not exist: " + observationTable);
                currentTableOid = rs.getLong(1);
            }
        }

        boolean initialized;
        try (PreparedStatement check = connection.prepareStatement(
                "SELECT table_oid::bigint FROM " + MUTABLE_COUNTS_STATE_TABLE +
                        " WHERE store_key=? AND id_scope=? FOR UPDATE")) {
            check.setString(1, storeKey);
            check.setInt(2, idScope);
            try (ResultSet rs = check.executeQuery()) {
                initialized = rs.next() && rs.getLong(1) == currentTableOid && currentTableOid != 0L;
            }
        }
        if (!initialized) {
            try (PreparedStatement clearDelta = connection.prepareStatement(
                    "DELETE FROM " + MUTABLE_COUNTS_DELTA_TABLE + " WHERE store_key=? AND id_scope=?")) {
                clearDelta.setString(1, storeKey);
                clearDelta.setInt(2, idScope);
                clearDelta.executeUpdate();
            }
            try (PreparedStatement clear = connection.prepareStatement(
                    "DELETE FROM " + MUTABLE_COUNTS_TABLE + " WHERE store_key=? AND id_scope=?")) {
                clear.setString(1, storeKey);
                clear.setInt(2, idScope);
                clear.executeUpdate();
            }
            try (PreparedStatement populate = connection.prepareStatement(
                    "INSERT INTO " + MUTABLE_COUNTS_TABLE +
                            " (store_key, id_scope, datastream_id, total_rows, null_result_time_rows) " +
                            "SELECT ?, ?, o.datastreamid, count(*)::bigint, " +
                            "count(*) FILTER (WHERE o.resulttime IS NULL)::bigint " +
                            "FROM ONLY " + observationTable + " o " +
                            "WHERE " + String.format(mutableGuard, "o", "o") +
                            " GROUP BY o.datastreamid")) {
                populate.setString(1, storeKey);
                populate.setInt(2, idScope);
                populate.executeUpdate();
            }
            try (PreparedStatement mark = connection.prepareStatement(
                    "INSERT INTO " + MUTABLE_COUNTS_STATE_TABLE +
                            " (store_key, id_scope, table_oid) VALUES (?, ?, ?::bigint::oid) " +
                            "ON CONFLICT (store_key, id_scope) DO UPDATE SET " +
                            "table_oid = EXCLUDED.table_oid, initialized_at = now()")) {
                mark.setString(1, storeKey);
                mark.setInt(2, idScope);
                mark.setLong(3, currentTableOid);
                mark.executeUpdate();
            }
        } else {
            for (String countTable : List.of(MUTABLE_COUNTS_DELTA_TABLE, MUTABLE_COUNTS_TABLE)) {
                try (PreparedStatement purgeImmutable = connection.prepareStatement(
                        "DELETE FROM " + countTable + " c USING " + ROUTE_TABLE + " r " +
                                "WHERE c.store_key=? AND c.id_scope=? " +
                                "AND r.store_key=c.store_key AND r.id_scope=c.id_scope " +
                                "AND r.datastream_id=c.datastream_id AND r.immutable=true")) {
                    purgeImmutable.setString(1, storeKey);
                    purgeImmutable.setInt(2, idScope);
                    purgeImmutable.executeUpdate();
                }
            }
            compactMutableStreamCountDeltasInTx(connection, MUTABLE_COUNTS_DELTA_COMPACT_BATCH);
        }
    }

    /**
     * Folds committed lock-free delta rows from {@link #MUTABLE_COUNTS_DELTA_TABLE}
     * into {@link #MUTABLE_COUNTS_TABLE} without blocking concurrent writers.
     */
    public int compactMutableStreamCountDeltas() {
        int totalDrained = 0;
        for (int batch = 0; batch < MUTABLE_COUNTS_DELTA_MAX_BATCHES_PER_CYCLE; batch++) {
            int drained;
            try (Connection connection = connections.open()) {
                boolean oldAutoCommit = connection.getAutoCommit();
                connection.setAutoCommit(false);
                try {
                    drained = compactMutableStreamCountDeltasInTx(
                            connection, MUTABLE_COUNTS_DELTA_COMPACT_BATCH);
                    connection.commit();
                } catch (SQLException error) {
                    connection.rollback();
                    throw error;
                } finally {
                    connection.setAutoCommit(oldAutoCommit);
                }
            } catch (SQLException error) {
                throw new IllegalStateException("Cannot compact mutable stream count deltas", error);
            }
            totalDrained += drained;
            if (drained < MUTABLE_COUNTS_DELTA_COMPACT_BATCH)
                break;
        }
        return totalDrained;
    }

    private int compactMutableStreamCountDeltasInTx(Connection connection, int limit) throws SQLException {
        requireTransaction(connection);
        try (PreparedStatement lock = connection.prepareStatement(
                "SELECT pg_try_advisory_xact_lock(hashtextextended(?, 0))")) {
            lock.setString(1, "oscar-mc-compact:" + storeKey + ":" + idScope);
            try (ResultSet rs = lock.executeQuery()) {
                if (!rs.next() || !rs.getBoolean(1))
                    return 0;
            }
        }
        String sql = "WITH claimed AS (" +
                "  SELECT ctid FROM " + MUTABLE_COUNTS_DELTA_TABLE +
                "  WHERE store_key=? AND id_scope=? LIMIT ? FOR UPDATE SKIP LOCKED" +
                "), drained AS (" +
                "  DELETE FROM " + MUTABLE_COUNTS_DELTA_TABLE + " d USING claimed" +
                "  WHERE d.ctid = claimed.ctid" +
                "  RETURNING d.store_key, d.id_scope, d.datastream_id, d.total_rows, d.null_result_time_rows" +
                "), folded AS (" +
                "  INSERT INTO " + MUTABLE_COUNTS_TABLE +
                "  (store_key, id_scope, datastream_id, total_rows, null_result_time_rows)" +
                "  SELECT store_key, id_scope, datastream_id," +
                "         sum(total_rows)::bigint," +
                "         sum(null_result_time_rows)::bigint" +
                "  FROM drained" +
                "  GROUP BY store_key, id_scope, datastream_id" +
                "  ORDER BY store_key, id_scope, datastream_id" +
                "  ON CONFLICT (store_key, id_scope, datastream_id) DO UPDATE SET" +
                "    total_rows = " + MUTABLE_COUNTS_TABLE + ".total_rows + EXCLUDED.total_rows," +
                "    null_result_time_rows = " + MUTABLE_COUNTS_TABLE +
                ".null_result_time_rows + EXCLUDED.null_result_time_rows" +
                "  RETURNING 1" +
                ") SELECT (SELECT count(*) FROM drained)::integer, " +
                "(SELECT count(*) FROM folded)::integer";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, storeKey);
            statement.setInt(2, idScope);
            statement.setInt(3, limit);
            try (ResultSet rs = statement.executeQuery()) {
                return rs.next() ? rs.getInt(1) : 0;
            }
        }
    }

    /**
     * Returns whether an unarchived mutable-only count query can be answered
     * in {@code O(streams + post-cutoff tail)} via {@link #MUTABLE_COUNTS_TABLE}
     * rather than scanning the complete mutable B-tree history.
     */
    public boolean supportsFastMutableStreamCount(ObsFilter filter,
                                                  Set<Long> dataStreams,
                                                  Set<Long> fois) {
        if (filter == null || dataStreams == null || dataStreams.isEmpty() || fois != null)
            return false;
        if (filter.getFoiFilter() != null ||
                (filter.getInternalIDs() != null && !filter.getInternalIDs().isEmpty()) ||
                filter.getCQLFilter() != null ||
                filter.getValuePredicate() != null ||
                filter.getPhenomenonLocation() != null) {
            return false;
        }
        TemporalFilter pt = filter.getPhenomenonTime();
        if (pt != null) {
            if (pt.isLatestTime() || pt.isCurrentTime())
                return false;
            if (isValidBound(PostgisUtils.checkAndGetValidInstant(pt.getMin())) ||
                    isValidBound(PostgisUtils.checkAndGetValidInstant(pt.getMax()))) {
                return false;
            }
        }
        TemporalFilter rt = filter.getResultTime();
        if (rt != null) {
            if (rt.isLatestTime() || rt.isCurrentTime())
                return false;
            if (isValidBound(PostgisUtils.checkAndGetValidInstant(rt.getMin())))
                return false;
        }
        return true;
    }

    /**
     * Computes the exact MVCC-consistent count of unarchived mutable observations
     * matching {@code filter} across {@code dataStreams} in a single SQL snapshot.
     */
    public long countMutableStreamObservations(ObsFilter filter, Set<Long> dataStreams) {
        if (dataStreams == null || dataStreams.isEmpty())
            return 0L;
        TemporalFilter rt = filter.getResultTime();
        String maxBound = rt == null ? null : PostgisUtils.checkAndGetValidInstant(rt.getMax());
        boolean hasMaxBound = isValidBound(maxBound);

        String sql;
        String complementOp = null;
        if (!hasMaxBound) {
            sql = "SELECT (" +
                    "COALESCE((SELECT sum(total_rows) FROM " + MUTABLE_COUNTS_TABLE +
                    " WHERE store_key=? AND id_scope=? AND datastream_id=ANY (?::bigint[])), 0) + " +
                    "COALESCE((SELECT sum(total_rows) FROM " + MUTABLE_COUNTS_DELTA_TABLE +
                    " WHERE store_key=? AND id_scope=? AND datastream_id=ANY (?::bigint[])), 0)" +
                    ")::bigint";
        } else {
            complementOp = rt.getRange().upperBoundType() == BoundType.CLOSED ? ">" : ">=";
            sql = "SELECT (" +
                    "COALESCE((SELECT sum(total_rows - null_result_time_rows) FROM " +
                    MUTABLE_COUNTS_TABLE +
                    " WHERE store_key=? AND id_scope=? AND datastream_id=ANY (?::bigint[])), 0) + " +
                    "COALESCE((SELECT sum(total_rows - null_result_time_rows) FROM " +
                    MUTABLE_COUNTS_DELTA_TABLE +
                    " WHERE store_key=? AND id_scope=? AND datastream_id=ANY (?::bigint[])), 0) - " +
                    "(SELECT count(*) FROM ONLY " + observationTable + " o " +
                    "WHERE o.datastreamid=ANY (?::bigint[]) AND o.archivegenerationid IS NULL " +
                    "AND o.resulttime " + complementOp + " ?::timestamp))::bigint";
        }

        try (Connection connection = connections.open()) {
            connection.setReadOnly(true);
            Array streamArray = connection.createArrayOf("bigint", dataStreams.toArray());
            try (PreparedStatement statement = connection.prepareStatement(sql)) {
                statement.setString(1, storeKey);
                statement.setInt(2, idScope);
                statement.setArray(3, streamArray);
                statement.setString(4, storeKey);
                statement.setInt(5, idScope);
                statement.setArray(6, streamArray);
                if (hasMaxBound) {
                    statement.setArray(7, streamArray);
                    statement.setString(8, maxBound);
                }
                try (ResultSet rs = statement.executeQuery()) {
                    if (!rs.next())
                        return 0L;
                    long count = rs.getLong(1);
                    if (count < 0)
                        throw new IllegalStateException("Negative mutable stream count: " + count);
                    return count;
                }
            } finally {
                streamArray.free();
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot count mutable stream observations", error);
        }
    }

    private static boolean isValidBound(String bound) {
        if (bound == null || bound.isEmpty())
            return false;
        String lower = bound.toLowerCase(java.util.Locale.ROOT).trim();
        return !"-infinity".equals(lower) && !"infinity".equals(lower);
    }

    private static String sqlTimestamp(Instant instant) {
        return SQL_TIMESTAMP.format(LocalDateTime.ofInstant(instant, ZoneOffset.UTC));
    }

    private static Instant databaseTransactionTime(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT transaction_timestamp()")) {
            if (!result.next())
                throw new SQLException("PostgreSQL did not return its transaction clock");
            return result.getTimestamp(1).toInstant();
        }
    }

    private static String identifierDigest(String value) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest, 0, 8);
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable", impossible);
        }
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !identifier.matches("[A-Za-z_][A-Za-z0-9_]*") ||
                identifier.length() > 63)
            throw new IllegalArgumentException("Unsafe SQL identifier: " + identifier);
        return identifier;
    }

    private static void requireTransaction(Connection connection) throws SQLException {
        if (connection.getAutoCommit())
            throw new SQLException("Immutable staging preparation requires an explicit transaction");
    }
}
