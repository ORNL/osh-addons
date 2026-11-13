package org.sensorhub.impl.datastore.postgis.store.obs;

import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.datastore.DataStoreException;
import org.sensorhub.api.datastore.obs.DataStreamKey;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

public class PostgisBatchObsStoreImpl extends PostgisObsStoreImpl {
    private static final Logger logger = LoggerFactory.getLogger(PostgisBatchObsStoreImpl.class);
    public static final int BATCH_SIZE = 10000;
    private final ReentrantLock pendingLock = new ReentrantLock();
    // PostgreSQL rejects one INSERT ... ON CONFLICT statement that affects the
    // same target row twice. Coalesce a burst at identical OSH observation
    // identity to its newest value before executing the batch. LinkedHashMap
    // keeps deterministic insertion order and the first allocated row ID.
    private final Map<ObservationNaturalKey, PreparedObservation> pending =
            new LinkedHashMap<>(BATCH_SIZE);
    private final Set<Long> pendingIds = new HashSet<>();
    private final Map<Long, Instant> phenomenonHighWaterByDataStream = new HashMap<>();
    private final Set<Long> initializedHighWaterDataStreams = new HashSet<>();

    public PostgisBatchObsStoreImpl(String url, String dbName, String login, String password, int idScope, IdProviderType dsIdProviderType) {
        this(url,dbName,login,password,DEFAULT_TABLE_NAME,idScope,dsIdProviderType);
    }

    public PostgisBatchObsStoreImpl(String url, String dbName, String login, String password, String dataStoreName,
                                                                        int idScope, IdProviderType dsIdProviderType) {
        super(url, dbName, login, password, dataStoreName,idScope, dsIdProviderType);
    }


    public PostgisBatchObsStoreImpl(String url, String dbName, String login, String password, String dataStoreName,
                                    int idScope, IdProviderType dsIdProviderType, QueryBuilderObsStore queryBuilderObsStore) {
        super(url, dbName, login, password, dataStoreName,idScope, dsIdProviderType,queryBuilderObsStore);
    }


    @Override
    protected void init(String url, String dbName, String login, String password, String[] initScripts) {
        super.init(url, dbName, login, password, initScripts);
        this.connectionManager.enableBatch(BATCH_SIZE);
    }

    @Override
    public BigId add(IObsData obs) {
        DataStreamKey dataStreamKey = new DataStreamKey(obs.getDataStreamID());
        if (!dataStreamStore.containsKey(dataStreamKey))
            throw new IllegalStateException("Unknown datastream with ID: " + obs.getDataStreamID().getIdAsLong());

        BigId requestedId = BigId.fromLong(idScope, idProvider.newInternalID(obs));
        BigId acceptedId = requestedId;
        try {
            // Serialize now. Drivers frequently reuse mutable DataBlock
            // instances, so retaining the IObsData object until the next timed
            // flush could otherwise persist a later sample under this ID.
            PreparedObservation snapshot = prepareObservation(
                    requestedId, dataStreamKey.getInternalID().getIdAsLong(), obs);

            pendingLock.lock();
            try {
                // A failed flush must apply backpressure instead of allowing
                // an unbounded in-memory retry queue to grow while PostgreSQL
                // is unavailable.
                if (pending.size() >= BATCH_SIZE)
                    flushPendingLocked();
                ObservationNaturalKey naturalKey = ObservationNaturalKey.of(snapshot);
                PreparedObservation previous = pending.get(naturalKey);
                if (previous != null) {
                    // Preserve the first ID returned for this pending logical
                    // observation while applying reference-store last-write-
                    // wins semantics to its result.
                    snapshot = new PreparedObservation(previous.id, snapshot.dataStreamId,
                            snapshot.foiId, snapshot.phenomenonTime, snapshot.resultTime,
                            snapshot.resultJson);
                    acceptedId = BigId.fromLong(idScope, previous.id);
                } else if (mayAlreadyBeStored(snapshot)) {
                    // Batch add returns before the timed flush, so resolve an
                    // older/same-time identity now. This preserves IObsStore's
                    // contract that the returned ID addresses the logical row,
                    // including after a process restart. Strictly increasing
                    // live samples stay on the zero-read fast path.
                    Long storedId = findStoredObservationId(snapshot);
                    if (storedId != null) {
                        snapshot = new PreparedObservation(storedId, snapshot.dataStreamId,
                                snapshot.foiId, snapshot.phenomenonTime, snapshot.resultTime,
                                snapshot.resultJson);
                        acceptedId = BigId.fromLong(idScope, storedId);
                    }
                }
                pending.put(naturalKey, snapshot);
                pendingIds.add(snapshot.id);
                advanceHighWater(snapshot);
                if (pending.size() >= BATCH_SIZE)
                    flushPendingLocked();
            } finally {
                pendingLock.unlock();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot queue observation " + requestedId, e);
        }
        return acceptedId;
    }

    private boolean mayAlreadyBeStored(PreparedObservation observation) {
        long dataStreamId = observation.dataStreamId;
        if (initializedHighWaterDataStreams.add(dataStreamId)) {
            Instant storedHighWater = getStoredPhenomenonTimeHighWater(dataStreamId);
            if (storedHighWater != null)
                phenomenonHighWaterByDataStream.put(dataStreamId, storedHighWater);
        }
        Instant highWater = phenomenonHighWaterByDataStream.get(dataStreamId);
        return observation.phenomenonTime == null ||
                (highWater != null && !observation.phenomenonTime.isAfter(highWater));
    }

    private void advanceHighWater(PreparedObservation observation) {
        if (observation.phenomenonTime == null)
            return;
        phenomenonHighWaterByDataStream.merge(observation.dataStreamId,
                observation.phenomenonTime,
                (current, candidate) -> current.isAfter(candidate) ? current : candidate);
    }

    @Override
    public IObsData remove(Object o) {
        try {
            // Preserve map semantics: a just-added observation must be visible
            // before get/remove attempts to return its previous value.
            if (o instanceof BigId id)
                flushPendingForIds(List.of(id.getIdAsLong()));
            else
                commit();
        } catch (DataStoreException e) {
            throw new IllegalStateException("Cannot flush observations before remove", e);
        }
        return super.remove(o);
    }

    protected void flushPendingForIds(java.util.Collection<Long> observationIds) throws DataStoreException {
        if (observationIds == null || observationIds.isEmpty())
            return;
        pendingLock.lock();
        try {
            if (pendingIds.isEmpty())
                return;
            boolean intersects = false;
            for (Long id : observationIds) {
                if (id != null && pendingIds.contains(id)) {
                    intersects = true;
                    break;
                }
            }
            if (!intersects)
                return;
            flushPendingLocked();
        } finally {
            pendingLock.unlock();
        }
        super.commit();
    }

    @Override
    public void commit() throws DataStoreException {
        pendingLock.lock();
        try {
            flushPendingLocked();
        } finally {
            pendingLock.unlock();
        }
        super.commit();
    }

    private void flushPendingLocked() {
        if (pending.isEmpty())
            return;

        try (Connection connection = this.connectionManager.getConnection()) {
            boolean previousAutoCommit = connection.getAutoCommit();
            connection.setAutoCommit(false);
            try {
                beforeObservationWrite(connection, pending.values());
                try (PreparedStatement statement = connection.prepareStatement(preparedObservationUpsertQuery())) {
                    for (PreparedObservation observation : pending.values()) {
                        bindPreparedObservation(statement, observation);
                        statement.addBatch();
                    }
                    statement.executeBatch();
                }
                reconcileProtectedObservations(connection, pending.values());
                connection.commit();
                pending.clear();
                pendingIds.clear();
                logger.debug("Committed observation batch");
            } catch (Exception e) {
                connection.rollback();
                // Isolate an unexpected deterministic integrity failure so it
                // cannot poison unrelated observations, while retaining every
                // row when the failure is transient.
                if (isObservationConflict(e))
                    flushIndividuallyAfterConflict(connection, e);
                else
                    throw e;
            } finally {
                connection.setAutoCommit(previousAutoCommit);
            }
        } catch (Exception e) {
            // Keep the snapshots queued so a later timed commit can retry.
            throw new IllegalStateException("Cannot commit observation batch of " + pending.size() + " rows", e);
        }
    }

    /**
     * Transaction hook invoked before any observation row is inserted or
     * updated. Tiered stores use it to acquire their archive mutation locks in
     * the documented global order; ordinary PostgreSQL stores do nothing.
     */
    protected void beforeObservationWrite(Connection connection,
                                          java.util.Collection<PreparedObservation> observations)
            throws Exception {
        // no-op
    }

    /**
     * Hook for storage tiers that protect a committed base row from in-place
     * mutation. It runs inside the batch transaction after the guarded upsert
     * and before the pending queue is cleared.
     */
    protected Set<Long> reconcileProtectedObservations(Connection connection,
                                                        java.util.Collection<PreparedObservation> observations)
            throws Exception {
        // Ordinary PostgreSQL stores have no immutable tier.
        return Set.of();
    }

    private void flushIndividuallyAfterConflict(Connection connection, Exception batchFailure) throws Exception {
        List<PreparedObservation> snapshot = new ArrayList<>(pending.values());
        ObservationConflictException firstConflict = null;
        try {
            for (PreparedObservation observation : snapshot) {
                try {
                    beforeObservationWrite(connection, List.of(observation));
                    try (PreparedStatement statement = connection.prepareStatement(preparedObservationUpsertQuery())) {
                        bindPreparedObservation(statement, observation);
                        statement.executeUpdate();
                    }
                    Set<Long> protectedIds = reconcileProtectedObservations(
                            connection, List.of(observation));
                    if (!protectedIds.contains(observation.id))
                        validateInsertOutcomes(connection, List.of(observation));
                    connection.commit();
                    pending.remove(ObservationNaturalKey.of(observation));
                } catch (ObservationConflictException conflict) {
                    connection.rollback();
                    pending.remove(ObservationNaturalKey.of(observation));
                    if (firstConflict == null)
                        firstConflict = conflict;
                    else
                        firstConflict.addSuppressed(conflict);
                } catch (Exception transientFailure) {
                    connection.rollback();
                    transientFailure.addSuppressed(batchFailure);
                    if (firstConflict != null)
                        transientFailure.addSuppressed(firstConflict);
                    throw transientFailure;
                }
            }
        } finally {
            pendingIds.clear();
            for (PreparedObservation remaining : pending.values())
                pendingIds.add(remaining.id);
        }
        if (firstConflict != null) {
            firstConflict.addSuppressed(batchFailure);
            throw firstConflict;
        }
    }

    private static boolean isObservationConflict(Throwable error) {
        for (Throwable current = error; current != null; current = current.getCause()) {
            if (current instanceof ObservationConflictException)
                return true;
            if (current instanceof SQLException sql && "23505".equals(sql.getSQLState()))
                return true;
        }
        return false;
    }
}
