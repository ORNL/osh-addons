/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.obs;

import com.google.gson.JsonParser;
import com.google.common.collect.Range;
import net.opengis.swe.v20.DataBlock;
import org.apache.commons.text.StringSubstitutor;
import org.postgresql.util.PGobject;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.data.IDataStreamInfo;
import org.sensorhub.api.data.IObsData;
import org.sensorhub.api.data.ObsData;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.*;
import org.sensorhub.api.datastore.system.ISystemDescStore;
import org.sensorhub.impl.datastore.postgis.IdProviderType;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.store.PostgisStore;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;
import org.sensorhub.impl.datastore.postgis.utils.SerializerUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vast.data.DataBlockByte;
import org.vast.util.TimeExtent;

import java.sql.*;
import java.time.*;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.sensorhub.api.datastore.obs.IObsStore.ObsField.*;

public class PostgisObsStoreImpl extends PostgisStore<QueryBuilderObsStore> implements IObsStore {
    private static final Logger logger = LoggerFactory.getLogger(PostgisObsStoreImpl.class);
    public static final int STREAM_FETCH_SIZE = 20_000;
    public static final String DEFAULT_TABLE_NAME = QueryBuilderObsStore.OBS_STORE_TABLE_NAME;

    protected PostgisDataStreamStoreImpl dataStreamStore;
    protected ISystemDescStore systemDescStore;
    protected IFoiStore foiStore;

    /** Immutable insert payload used to verify every upsert before commit. */
    protected static final class PreparedObservation {
        final long id;
        final long dataStreamId;
        final Long foiId;
        final Instant phenomenonTime;
        final Instant resultTime;
        final String resultJson;

        protected PreparedObservation(long id, long dataStreamId, Long foiId, Instant phenomenonTime,
                                      Instant resultTime, String resultJson) {
            this.id = id;
            this.dataStreamId = dataStreamId;
            this.foiId = foiId;
            this.phenomenonTime = UtcJdbcTime.toDatabasePrecision(phenomenonTime);
            this.resultTime = UtcJdbcTime.toDatabasePrecision(resultTime);
            this.resultJson = resultJson;
        }
    }

    /** A deterministic integrity conflict, as distinct from a transient JDBC failure. */
    protected static class ObservationConflictException extends SQLException {
        private static final long serialVersionUID = 1L;

        ObservationConflictException(String message) {
            super(message, "23505");
        }
    }

    public PostgisObsStoreImpl(String url, String dbName, String login, String password, int idScope, IdProviderType dsIdProviderType) {
        this(url,dbName,login,password,DEFAULT_TABLE_NAME,idScope,dsIdProviderType);
    }

    public PostgisObsStoreImpl(String url, String dbName, String login, String password, String dataStoreName,
                               int idScope, IdProviderType dsIdProviderType) {
        this(url, dbName,login,password,dataStoreName,idScope,dsIdProviderType,new QueryBuilderObsStore(dataStoreName));
    }

    public PostgisObsStoreImpl(String url, String dbName, String login, String password, String dataStoreName,
                               int idScope, IdProviderType dsIdProviderType,QueryBuilderObsStore queryBuilderObsStore) {
        super(idScope, dsIdProviderType,
                queryBuilderObsStore,
                false);
        this.init(url, dbName, login, password, queryBuilder.createInitializationQueries());
    }

    @Override
    protected void init(String url, String dbName, String login, String password, String[] initScripts) {
        super.init(url, dbName, login, password, initScripts);
        ensureNaturalKeyConstraint();
        ensureEventFilterIndexes();
        this.dataStreamStore = new PostgisDataStreamStoreImpl(this, url, dbName, login, password, idScope, idProviderType, false);
        queryBuilder.linkTo(dataStreamStore);
    }

    @Override
    public void close() {
        // A failed observation flush must leave both pools usable for the
        // documented retry path, so close the linked metadata store only
        // after the observation store has committed and closed successfully.
        super.close();
        if (dataStreamStore != null)
            dataStreamStore.close();
    }

    /**
     * Base store initialization runs table DDL only for a new table. Apply the
     * corrected NULL-safe natural key to upgraded tables as a separate online
     * migration so ordinary observation retries cannot create duplicates.
     */
    private void ensureNaturalKeyConstraint() {
        String indexName = queryBuilder.uniqueConstraintName();
        try (Connection connection = connectionManager.getConnection()) {
            boolean invalid = false;
            try (PreparedStatement statement = connection.prepareStatement(
                    "SELECT NOT index_state.indisvalid FROM pg_index index_state " +
                            "WHERE index_state.indexrelid=to_regclass(?)")) {
                statement.setString(1, indexName);
                try (ResultSet result = statement.executeQuery()) {
                    invalid = result.next() && result.getBoolean(1);
                }
            }
            if (invalid) {
                try (Statement statement = connection.createStatement()) {
                    statement.execute("DROP INDEX CONCURRENTLY IF EXISTS " + indexName);
                }
            }
            try (Statement statement = connection.createStatement()) {
                statement.execute(queryBuilder.createUniqueConstraintConcurrently());
            }
        } catch (SQLException error) {
            String detail = "Cannot install the NULL-safe observation natural-key index " + indexName +
                    ". Existing duplicate observations must be reviewed and resolved before startup.";
            throw new IllegalStateException(detail, error);
        }
    }

    private void ensureEventFilterIndexes() {
        try (Connection connection = this.connectionManager.getConnection()) {
            PostgisUtils.executeQueries(connection, queryBuilder.createEventFilterIndexQueries());
            PostgisUtils.executeQueries(connection, new String[]{queryBuilder.configureAutovacuumQuery()});
        } catch (SQLException e) {
            throw new RuntimeException("Cannot create observation event filter indexes", e);
        }
    }

    @Override
    public Stream<Entry<BigId, IObsData>> selectEntries(ObsFilter filter, Set<ObsField> fields) {
        return selectEntries(filter, fields, false, STREAM_FETCH_SIZE);
    }

    protected Stream<Entry<BigId, IObsData>> selectEntries(
            ObsFilter filter, Set<ObsField> fields, boolean parentTableOnly, int initialPageSize) {
        Set<ObsField> requestedFields = fields == null || fields.isEmpty()
                ? null : Set.copyOf(fields);
        boolean projectAfterPredicate = filter.getValuePredicate() != null && requestedFields != null;
        Set<ObsField> queryFields;
        Set<ObsField> parserFields;
        if (projectAfterPredicate || requestedFields == null) {
            // Java predicates must see the complete decoded observation, not
            // a caller-requested projection with an empty result block.
            queryFields = null;
            parserFields = null;
        } else {
            queryFields = new HashSet<>(requestedFields);
            queryFields.add(PHENOMENON_TIME);
            parserFields = requestedFields;
        }

        // Keep the caller-visible limit outside the base query. The keyset
        // iterator pushes an exact finite limit into each SQL page when there
        // is no Java-only value predicate, and otherwise continues bounded
        // pages until enough predicate matches have been accepted.
        ObsFilter queryFilter = filter.getLimit() == Long.MAX_VALUE ? filter :
                ObsFilter.Builder.from(filter).withLimit(Long.MAX_VALUE).build();
        boolean latestSelection = (queryFilter.getPhenomenonTime() != null &&
                queryFilter.getPhenomenonTime().isLatestTime()) ||
                (queryFilter.getResultTime() != null && queryFilter.getResultTime().isLatestTime());
        ParameterizedQuery query;
        if (queryBuilder.supportsLatestResultPerDataStreamQuery(queryFilter)) {
            query = queryBuilder.createParameterizedLatestResultPerDataStreamQuery(
                    queryFilter, queryFields, parentTableOnly);
        } else if (latestSelection) {
            // DISTINCT ON relies on its inner ORDER BY. The outer keyset still
            // supplies the final stable page order.
            query = queryBuilder.createParameterizedSelectEntriesQuery(
                    queryFilter, queryFields, parentTableOnly);
        } else {
            query = queryBuilder.createParameterizedUnorderedSelectEntriesQuery(
                    queryFilter, queryFields, parentTableOnly);
        }
        logger.debug(query.sql());

        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();
        ObservationKeysetIterator<Entry<BigId, IObsData>> iterator =
                new ObservationKeysetIterator<>(
                        query,
                        connectionManager,
                        initialPageSize,
                        STREAM_FETCH_SIZE,
                        filter.getLimit(),
                        descending,
                        (resultSet) -> resultSetToEntry(resultSet, parserFields),
                        (entry) -> filter.getValuePredicate() == null ||
                                filter.getValuePredicate().test(entry.getValue()),
                        filter.getValuePredicate() != null);
        Stream<Entry<BigId, IObsData>> entries = StreamSupport.stream(
                        Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED), false)
                .onClose(iterator::close);
        return projectAfterPredicate
                ? entries.map(entry -> Map.entry(entry.getKey(), project(entry.getValue(), requestedFields)))
                : entries;
    }

    private static IObsData project(IObsData observation, Set<ObsField> fields) {
        ObsData.Builder builder = new ObsData.Builder();
        if (fields.contains(DATASTREAM_ID))
            builder.withDataStream(observation.getDataStreamID());
        if (fields.contains(FOI_ID) && observation.hasFoi())
            builder.withFoi(observation.getFoiID());
        if (observation.getPhenomenonTime() != null)
            builder.withPhenomenonTime(observation.getPhenomenonTime());
        if (fields.contains(RESULT_TIME) && observation.getResultTime() != null)
            builder.withResultTime(observation.getResultTime());
        builder.withResult(fields.contains(RESULT)
                ? observation.getResult() : new DataBlockByte());
        return builder.build();
    }

    private Entry<BigId, IObsData> resultSetToEntry(ResultSet resultSet, Set<ObsField> fields) {
        try {
            BigId id = BigId.fromLong(idScope, resultSet.getLong("id"));
            ObsData.Builder obsDataBuilder = new ObsData.Builder();
            ObsData obsData = null;

            boolean noFields = fields != null && !fields.isEmpty();
            BigId dataStreamId = null;
            if (!noFields || fields.contains(DATASTREAM_ID)) {
                long dataStreamIdAsLong = resultSet.getLong(String.valueOf(DATASTREAM_ID));
                if (!resultSet.wasNull()) {
                    dataStreamId = BigId.fromLong(idScope, dataStreamIdAsLong);
                    obsDataBuilder = obsDataBuilder.withDataStream(dataStreamId);
                }
            }
            if (!noFields || fields.contains(FOI_ID)) {
                long foiId = resultSet.getLong(String.valueOf(FOI_ID));
                if (!resultSet.wasNull() && foiId > 0) {
                    obsDataBuilder = obsDataBuilder.withFoi(BigId.fromLong(idScope, foiId));
                }
            }
            // required
            Instant phenomenonTime = UtcJdbcTime.getInstant(resultSet, String.valueOf(PHENOMENON_TIME));
            if (phenomenonTime != null)
                obsDataBuilder = obsDataBuilder.withPhenomenonTime(phenomenonTime);

            if (!noFields || fields.contains(RESULT_TIME)) {
                Instant resultTime = UtcJdbcTime.getInstant(resultSet, String.valueOf(RESULT_TIME));
                if (resultTime != null)
                    obsDataBuilder = obsDataBuilder.withResultTime(resultTime);
            }

            // get datastream schema from datastreamId
            if (dataStreamId != null) {
                IDataStreamInfo dataStreamInfo = getDataStreamInfo(dataStreamId);
                DataBlock dataBlock = SerializerUtils.readDataBlockFromJson(dataStreamInfo.getRecordStructure(), resultSet.getString(String.valueOf(RESULT)));
                obsData = obsDataBuilder.withResult(dataBlock).build();
            } else {
                // fake result
                obsData = obsDataBuilder.withResult(new DataBlockByte()).build();
            }

            return Map.entry(id, obsData);
        } catch (Exception ex) {
            throw new RuntimeException("Cannot parse resultSet to IObs", ex);
        }
    }

    @Override
    public IDataStreamStore getDataStreams() {
        return this.dataStreamStore;
    }

    /** Central schema lookup so a coordinated metadata mutation can pin it. */
    protected IDataStreamInfo getDataStreamInfo(BigId dataStreamId) {
        return dataStreamStore.get(new DataStreamKey(dataStreamId));
    }

    protected String fillAddStatement(BigId id,long dataStreamKey, IObsData obs) throws SQLException {
        Map<String, Object> values = new HashMap<>();
        values.put("1","'"+id.getIdAsLong()+"'::int8");

        // datastreamid
        values.put("2","'"+dataStreamKey+"'::int8");
        values.put("7","'"+dataStreamKey+"'::int8");

        // insert foiId if any
        if (obs.hasFoi()) {
            values.put("3", "'"+obs.getFoiID().getIdAsLong()+"'::int8");
            values.put("8", "'"+obs.getFoiID().getIdAsLong()+"'::int8");
        } else {
            values.put("3", "NULL");
            values.put("8", "NULL");
        }

        // insert timestamp
        if (obs.getPhenomenonTime() != null) {
            String d = PostgisUtils.getPgDate(obs.getPhenomenonTime());
            values.put("4", "'"+d+"'");
            values.put("9", "'"+d+"'");
        } else {
            values.put("4", "NULL");
            values.put("9", "NULL");
        }

        if (obs.getResultTime() != null) {
            String d = PostgisUtils.getPgDate(obs.getResultTime());
            values.put("5", "'"+d+"'");
            values.put("10", "'"+d+"'");
        } else {
            values.put("5", "NULL");
            values.put("10", "NULL");
        }
        // insert DataBlock
        IDataStreamInfo dataStreamInfo = getDataStreamInfo(obs.getDataStreamID());
        String serializedBlock = SerializerUtils.writeDataBlockToJson(dataStreamInfo.getRecordStructure(),
                dataStreamInfo.getRecordEncoding(), obs.getResult());

        // Escape single quotes for safe SQL interpolation (used by batch path)
        String escapedBlock = serializedBlock.replace("'", "''");
        values.put("6", "'"+escapedBlock+"'");
        values.put("11", "'"+escapedBlock+"'");

        StringSubstitutor sub = new StringSubstitutor(values);
        return sub.replace(queryBuilder.insertObsQuery());
    }

    @Override
    public BigId add(IObsData obs) {
        DataStreamKey dataStreamKey = new DataStreamKey(obs.getDataStreamID());
        if (!dataStreamStore.containsKey(dataStreamKey))
            throw new IllegalStateException("Unknown datastream with ID: " + obs.getDataStreamID().getIdAsLong());
        BigId requestedId = BigId.fromLong(idScope, idProvider.newInternalID(obs));
        long storedId;
        try (Connection connection1 = this.connectionManager.getConnection()) {
            boolean previousAutoCommit = connection1.getAutoCommit();
            connection1.setAutoCommit(false);
            try (PreparedStatement ps = connection1.prepareStatement(preparedObservationUpsertQuery())) {
                PreparedObservation prepared = prepareObservation(requestedId, dataStreamKey.getInternalID().getIdAsLong(), obs);
                bindPreparedObservation(ps, prepared);
                ps.executeUpdate();
                storedId = validateInsertOutcome(connection1, prepared);
                connection1.commit();
            } catch (Exception e) {
                connection1.rollback();
                throw new IllegalStateException("Cannot insert obs", e);
            } finally {
                connection1.setAutoCommit(previousAutoCommit);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Cannot insert obs", e);
        }
        // A replay/correction of an existing logical observation retains its
        // stable row ID, just as the reference MVStore retains its time-series
        // key when replacing the value at the same timestamp.
        return BigId.fromLong(idScope, storedId);
    }

    protected void fillPreparedAddStatement(PreparedStatement ps, BigId id, long dataStreamKey, IObsData obs) throws SQLException {
        bindPreparedObservation(ps, prepareObservation(id, dataStreamKey, obs));
    }

    protected PreparedObservation prepareObservation(BigId id, long dataStreamKey, IObsData obs) {
        IDataStreamInfo dataStreamInfo = getDataStreamInfo(obs.getDataStreamID());
        String serializedBlock = SerializerUtils.writeDataBlockToJson(dataStreamInfo.getRecordStructure(),
                dataStreamInfo.getRecordEncoding(), obs.getResult());

        Long foiId = obs.hasFoi() ? obs.getFoiID().getIdAsLong() : null;

        return new PreparedObservation(id.getIdAsLong(), dataStreamKey, foiId,
                obs.getPhenomenonTime(), obs.getResultTime(), serializedBlock);
    }

    /** Creates an already-serialized insert payload for focused store probes. */
    protected static PreparedObservation preparedObservation(long id, long dataStreamId, Long foiId,
                                                              Instant phenomenonTime, Instant resultTime,
                                                              String resultJson) {
        return new PreparedObservation(id, dataStreamId, foiId, phenomenonTime, resultTime, resultJson);
    }

    protected void bindPreparedObservation(PreparedStatement ps, PreparedObservation observation) throws SQLException {
        ps.setLong(1, observation.id);
        ps.setLong(2, observation.dataStreamId);
        if (observation.foiId != null) {
            ps.setLong(3, observation.foiId);
        } else {
            ps.setNull(3, Types.BIGINT);
        }
        if (observation.phenomenonTime != null) {
            UtcJdbcTime.setInstant(ps, 4, observation.phenomenonTime);
        } else {
            ps.setNull(4, Types.TIMESTAMP);
        }
        if (observation.resultTime != null) {
            UtcJdbcTime.setInstant(ps, 5, observation.resultTime);
        } else {
            ps.setNull(5, Types.TIMESTAMP);
        }
        ps.setString(6, observation.resultJson);
    }

    /** Allows the tiered store to protect immutable archive-owned hot rows. */
    protected String preparedObservationUpsertQuery() {
        return queryBuilder.insertObsPreparedQuery();
    }

    /**
     * Verify that every logical observation now contains the submitted value.
     * The returned/stored ID can differ from the newly requested sequential ID
     * when this was an idempotent replay or a last-write-wins correction.
     */
    protected void validateInsertOutcomes(Connection connection, List<PreparedObservation> observations)
            throws SQLException {
        if (observations.isEmpty())
            return;
        Map<ObservationNaturalKey, PreparedObservation> expectedByKey = new LinkedHashMap<>();
        // LinkedHashMap replacement intentionally selects the last submitted
        // value for a repeated natural key, matching the batch coalescer and
        // OSH's reference store semantics.
        for (PreparedObservation observation : observations)
            expectedByKey.put(ObservationNaturalKey.of(observation), observation);
        for (PreparedObservation expected : expectedByKey.values())
            validateInsertOutcome(connection, expected);
    }

    protected long validateInsertOutcome(Connection connection, PreparedObservation expected) throws SQLException {
        try (PreparedStatement natural = connection.prepareStatement(
                queryBuilder.findResultByUniqueFieldsQuery())) {
            natural.setLong(1, expected.dataStreamId);
            if (expected.foiId == null) natural.setNull(2, Types.BIGINT); else natural.setLong(2, expected.foiId);
            if (expected.phenomenonTime == null) natural.setNull(3, Types.TIMESTAMP); else UtcJdbcTime.setInstant(natural, 3, expected.phenomenonTime);
            if (expected.resultTime == null) natural.setNull(4, Types.TIMESTAMP); else UtcJdbcTime.setInstant(natural, 4, expected.resultTime);
            try (ResultSet result = natural.executeQuery()) {
                if (!result.next())
                    throw new ObservationConflictException(
                            "Observation upsert produced no row for requested ID " + expected.id);
                if (!JsonParser.parseString(expected.resultJson).equals(
                        JsonParser.parseString(result.getString(2))))
                    throw new ObservationConflictException(
                            "Observation upsert did not retain the submitted result for requested ID " + expected.id);
                return result.getLong(1);
            }
        }
    }

    /** Resolve a logical observation already present in PostgreSQL, if any. */
    protected Long findStoredObservationId(PreparedObservation observation) {
        try (Connection connection = connectionManager.getConnection();
             PreparedStatement natural = connection.prepareStatement(
                     queryBuilder.findByUniqueFieldsQuery())) {
            natural.setLong(1, observation.dataStreamId);
            if (observation.foiId == null) natural.setNull(2, Types.BIGINT); else natural.setLong(2, observation.foiId);
            if (observation.phenomenonTime == null) natural.setNull(3, Types.TIMESTAMP); else UtcJdbcTime.setInstant(natural, 3, observation.phenomenonTime);
            if (observation.resultTime == null) natural.setNull(4, Types.TIMESTAMP); else UtcJdbcTime.setInstant(natural, 4, observation.resultTime);
            try (ResultSet result = natural.executeQuery()) {
                return result.next() ? result.getLong(1) : null;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot resolve existing observation identity", error);
        }
    }

    /** Read the committed high-water mark used by the batch store's collision fast path. */
    protected Instant getStoredPhenomenonTimeHighWater(long dataStreamId) {
        try (Connection connection = connectionManager.getConnection();
             PreparedStatement statement = connection.prepareStatement(
                     "SELECT max(phenomenontime) FROM " + getDatastoreName() + " WHERE datastreamid=?")) {
            statement.setLong(1, dataStreamId);
            try (ResultSet result = statement.executeQuery()) {
                return result.next() ? UtcJdbcTime.getInstant(result, 1) : null;
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot read observation high-water mark", error);
        }
    }

    protected static final class ObservationNaturalKey {
        final long dataStreamId;
        final Long foiId;
        final Instant phenomenonTime;
        final Instant resultTime;

        private ObservationNaturalKey(long dataStreamId, Long foiId, Instant phenomenonTime, Instant resultTime) {
            this.dataStreamId = dataStreamId;
            this.foiId = foiId;
            this.phenomenonTime = phenomenonTime;
            this.resultTime = resultTime;
        }

        static ObservationNaturalKey of(PreparedObservation observation) {
            return new ObservationNaturalKey(observation.dataStreamId, observation.foiId,
                    observation.phenomenonTime, observation.resultTime);
        }

        @Override
        public boolean equals(Object value) {
            if (!(value instanceof ObservationNaturalKey other))
                return false;
            return dataStreamId == other.dataStreamId && Objects.equals(foiId, other.foiId) &&
                    Objects.equals(phenomenonTime, other.phenomenonTime) &&
                    Objects.equals(resultTime, other.resultTime);
        }

        @Override
        public int hashCode() {
            return Objects.hash(dataStreamId, foiId, phenomenonTime, resultTime);
        }
    }

    static class TimeParams {
        Range<Instant> phenomenonTimeRange;
        Range<Instant> resultTimeRange;
        boolean currentTimeOnly;
        boolean latestResultOnly;


        TimeParams(ObsFilter filter) {
            // get phenomenon time range
            phenomenonTimeRange = filter.getPhenomenonTime() != null ?
                    filter.getPhenomenonTime().getRange() : PostgisUtils.ALL_TIMES_RANGE;

            // get result time range
            resultTimeRange = filter.getResultTime() != null ?
                    filter.getResultTime().getRange() : PostgisUtils.ALL_TIMES_RANGE;

            latestResultOnly = filter.getResultTime() != null && filter.getResultTime().isLatestTime();
            currentTimeOnly = filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isCurrentTime();
        }
    }

    @Override
    public Stream<ObsStats> getStatistics(ObsStatsQuery query) {
        var filter = query.getObsFilter();
        var timeParams = new TimeParams(filter);

        Set<Long> dsIds = dataStreamStore.getDataStreamsIdsByTimeRange(timeParams.phenomenonTimeRange.lowerEndpoint(),
                timeParams.phenomenonTimeRange.upperEndpoint());
        return dsIds.stream().map((dsId) -> {
            try (Connection connection = this.connectionManager.getConnection()) {
                ParameterizedQuery queryStr = queryBuilder.parameterizedStatsQueryByDataStream(query, dsId);
                try (PreparedStatement statement = connection.prepareStatement(queryStr.sql())) {
                    queryStr.bind(statement);
                    logger.debug(queryStr.sql());
                    try (ResultSet resultSet = statement.executeQuery()) {
                        long start = timeParams.phenomenonTimeRange.lowerEndpoint().getEpochSecond();
                        long end = timeParams.phenomenonTimeRange.upperEndpoint().getEpochSecond();
                        long t = start;
                        long dt = query.getHistogramBinSize().getSeconds();
                        int numBins = (int) Math.ceil((double) (end - start) / dt);
                        int[] counts = new int[numBins];

                        while (resultSet.next()) {
                            int count = resultSet.getInt("count");
                            long intervalAliasSeconds = resultSet.getLong("interval_alias");
                            int binIdx = (int) Math.ceil((intervalAliasSeconds - t) / query.getHistogramBinSize().getSeconds());
                            counts[binIdx] = count;
                        }
                        return new ObsStats.Builder()
                                .withDataStreamID(BigId.fromLong(idScope, dsId))
//                            .withFoiID(foiID)
                                .withObsCountByTime(counts)
                                .withPhenomenonTimeRange(
                                        TimeExtent.period(
                                                Range.closed(Instant.parse("1000-01-01T00:00:00Z"), Instant.parse("3000-01-01T00:00:00Z"))
                                        ))
                                .withResultTimeRange(TimeExtent.period(
                                        Range.closed(Instant.parse("1000-01-01T00:00:00Z"), Instant.parse("3000-01-01T00:00:00Z"))
                                ))
                                .withTotalObsCount(countMatchingEntries(filter))
                                .build();
                    }
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        });
    }

    @Override
    public void linkTo(IFoiStore foiStore) {
        this.foiStore = foiStore;
        super.linkTo(foiStore);
    }

    @Override
    public void linkTo(ISystemDescStore systemDescStore) {
        this.systemDescStore = systemDescStore;
        super.linkTo(systemDescStore);
    }

    @Override
    public IObsData get(Object o) {
        if (!(o instanceof BigId)) {
            throw new UnsupportedOperationException("Get operation is not supported with argument != BigId key, got=" + o.getClass());
        }
        BigId key = (BigId) o;
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.selectByIdQuery())) {
                preparedStatement.setLong(1, key.getIdAsLong());
                ResultSet resultSet = preparedStatement.executeQuery();
                if (resultSet.next()) {
                    ObsData.Builder obsDataBuilder = new ObsData.Builder();

                    BigId dataStreamId = BigId.fromLong(idScope, resultSet.getLong(String.valueOf(DATASTREAM_ID)));
                    obsDataBuilder = obsDataBuilder.withDataStream(dataStreamId);

                    long foiId = resultSet.getLong(String.valueOf(FOI_ID));
                    if (!resultSet.wasNull()) {
                        obsDataBuilder = obsDataBuilder.withFoi(BigId.fromLong(idScope, foiId));
                    }

                    Instant phenomenonTime = UtcJdbcTime.getInstant(resultSet, String.valueOf(PHENOMENON_TIME));
                    if (phenomenonTime != null)
                        obsDataBuilder = obsDataBuilder.withPhenomenonTime(phenomenonTime);
                    Instant resultTime = UtcJdbcTime.getInstant(resultSet, String.valueOf(RESULT_TIME));
                    if (resultTime != null)
                        obsDataBuilder = obsDataBuilder.withResultTime(resultTime);

                    // get datastream schema from datastreamId
                    IDataStreamInfo dataStreamInfo = getDataStreamInfo(dataStreamId);
                    DataBlock dataBlock = SerializerUtils.readDataBlockFromJson(dataStreamInfo.getRecordStructure(), resultSet.getString(String.valueOf(RESULT)));
                    return obsDataBuilder.withResult(dataBlock).build();
                } else {
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }



    @Override
    public IObsData put(BigId key, IObsData obs) {
        IObsData oldObs = this.get(key);
        if (oldObs == null)
            throw new UnsupportedOperationException("Put can only be used to update existing entries");

        try (Connection connection = this.connectionManager.getConnection()) {

            Long targetId = key.getIdAsLong();

            try (PreparedStatement findStmt = connection.prepareStatement(queryBuilder.findByUniqueFieldsQuery())) {

                findStmt.setLong(1, obs.getDataStreamID().getIdAsLong());

                if (obs.hasFoi()) {
                    findStmt.setLong(2, obs.getFoiID().getIdAsLong());
                } else {
                    findStmt.setNull(2, Types.BIGINT);
                }

                if (obs.getPhenomenonTime() != null) {
                    UtcJdbcTime.setInstant(findStmt, 3, obs.getPhenomenonTime());
                } else {
                    findStmt.setNull(3, Types.TIMESTAMP);
                }

                if (obs.getResultTime() != null) {
                    UtcJdbcTime.setInstant(findStmt, 4, obs.getResultTime());
                } else {
                    findStmt.setNull(4, Types.TIMESTAMP);
                }

                try (ResultSet rs = findStmt.executeQuery()) {
                    if (rs.next()) {
                        long foundId = rs.getLong("id");
                        // If conflict exists, use that row instead
                        targetId = foundId;
                    }
                }
            }

            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.updateByIdQuery())) {

                preparedStatement.setLong(1, obs.getDataStreamID().getIdAsLong());

                if (obs.hasFoi()) {
                    preparedStatement.setLong(2, obs.getFoiID().getIdAsLong());
                } else {
                    preparedStatement.setNull(2, Types.BIGINT);
                }

                if (obs.getPhenomenonTime() != null) {
                    UtcJdbcTime.setInstant(preparedStatement, 3, obs.getPhenomenonTime());
                } else {
                    preparedStatement.setNull(3, Types.TIMESTAMP);
                }

                if (obs.getResultTime() != null) {
                    UtcJdbcTime.setInstant(preparedStatement, 4, obs.getResultTime());
                } else {
                    preparedStatement.setNull(4, Types.TIMESTAMP);
                }

                IDataStreamInfo dataStreamInfo = getDataStreamInfo(obs.getDataStreamID());
                String serializedBlock = SerializerUtils.writeDataBlockToJson(
                        dataStreamInfo.getRecordStructure(),
                        dataStreamInfo.getRecordEncoding(),
                        obs.getResult()
                );

                PGobject jsonObject = new PGobject();
                jsonObject.setType("json");
                jsonObject.setValue(serializedBlock);

                preparedStatement.setObject(5, jsonObject);

                preparedStatement.setLong(6, targetId);

                int rows = preparedStatement.executeUpdate();
                if (rows == 0) {
                    throw new IllegalStateException("Update affected 0 rows for key: " + targetId);
                }
            }

        } catch (Exception e) {
            throw new IllegalStateException("Cannot update obs", e);
        }

        return obs;
    }

    @Override
    public IObsData remove(Object o) {
        if (!(o instanceof BigId)) {
            throw new UnsupportedOperationException("Remove operation is not supported with argument != BigId key, got=" + o.getClass());
        }
        BigId key = (BigId) o;
        IObsData data = this.get(o);

        logger.debug("Remove Obs with key={}", key);
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement preparedStatement = connection.prepareStatement(queryBuilder.removeByIdQuery())) {
                preparedStatement.setLong(1, key.getIdAsLong());
                preparedStatement.executeUpdate();
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return data;
    }

    TimeExtent getDataStreamPhenomenonTimeRange(long dataStreamID) {
        Instant[] timeRange = new Instant[]{Instant.MAX, Instant.MIN};
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(queryBuilder.getPhenomenonTimeRangeByDataStreamIdQuery())) {
                statement.setLong(1, dataStreamID);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        Instant min = UtcJdbcTime.getInstant(resultSet, "min");
                        Instant max = UtcJdbcTime.getInstant(resultSet, "max");
                        if (min != null) {
                            timeRange[0] = min;
                        }
                        if (max != null) {
                            timeRange[1] = max;
                        }

                        if (timeRange[0] == Instant.MAX || timeRange[1] == Instant.MIN)
                            return null;
                        else
                            return TimeExtent.period(timeRange[0], timeRange[1]);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Map<Long, TimeExtent> getDataStreamPhenomenonTimeRanges(List<Long> dataStreamIds) {
        Map<Long, TimeExtent> result = new HashMap<>();

        if (dataStreamIds == null || dataStreamIds.isEmpty())
            return result;

        String sql = queryBuilder.getPhenomenonTimeRangeByDataStreamIdsQuery();

        try (Connection conn = connectionManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            Array sqlArray = conn.createArrayOf("bigint", dataStreamIds.toArray());
            ps.setArray(1, sqlArray);
            logger.debug("Getting datastream phenomenon time ranges \n{}", ps);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("datastreamID");
                    Instant min = UtcJdbcTime.getInstant(rs, "min");
                    Instant max = UtcJdbcTime.getInstant(rs, "max");

                    if (min != null && max != null) {
                        result.put(id,
                                TimeExtent.period(min, max)
                        );
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }


    TimeExtent getDataStreamResultTimeRange(long dataStreamID) {
        Instant[] timeRange = new Instant[]{Instant.MAX, Instant.MIN};
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(queryBuilder.getResultTimeRangeByDataStreamIdQuery())) {
                statement.setLong(1, dataStreamID);
                logger.debug(queryBuilder.getResultTimeRangeByDataStreamIdQuery());
                try (ResultSet resultSet = statement.executeQuery()) {
                    while (resultSet.next()) {
                        Instant min = UtcJdbcTime.getInstant(resultSet, "min");
                        Instant max = UtcJdbcTime.getInstant(resultSet, "max");
                        if (min != null) {
                            timeRange[0] = min;
                        }
                        if (max != null) {
                            timeRange[1] = max;
                        }

                        if (timeRange[0] == Instant.MAX || timeRange[1] == Instant.MIN)
                            return null;
                        else
                            return TimeExtent.period(timeRange[0], timeRange[1]);
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public Map<Long, TimeExtent> getDataStreamResultTimeRanges(List<Long> dataStreamIds) {
        Map<Long, TimeExtent> result = new HashMap<>();

        if (dataStreamIds == null || dataStreamIds.isEmpty())
            return result;

        String sql = queryBuilder.getResultTimeRangeByDataStreamIdsQuery();

        try (Connection conn = connectionManager.getConnection();
             PreparedStatement ps = conn.prepareStatement(sql)) {

            Array sqlArray = conn.createArrayOf("bigint", dataStreamIds.toArray());
            ps.setArray(1, sqlArray);

            logger.debug("Getting datastream phenomenon time ranges \n{}", ps);

            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    long id = rs.getLong("datastreamID");
                    Instant min = UtcJdbcTime.getInstant(rs, "min");
                    Instant max = UtcJdbcTime.getInstant(rs, "max");

                    if (min != null && max != null) {
                        result.put(id,
                                TimeExtent.period(min, max)
                        );
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }

    @Override
    public long countMatchingEntries(ObsFilter filter) {
        try (Connection connection = this.connectionManager.getConnection()) {
            ParameterizedQuery query = queryBuilder.createParameterizedSelectEntriesCountQuery(filter);
            logger.debug(query.sql());
            try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
                query.bind(statement);
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (resultSet.next()) {
                        return resultSet.getLong(1);
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return 0;
    }

    @Override
    public long removeEntries(ObsFilter filter) {
        logger.debug("Remove Obs with filter={}", filter.toString());
        ParameterizedQuery query = queryBuilder.createParameterizedRemoveEntriesQuery(filter);
        logger.debug(query.sql());
        try (Connection connection = this.connectionManager.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement(query.sql())) {
                query.bind(statement);
                return statement.executeUpdate();
            } catch (Exception ex) {
                connection.rollback();
                throw ex;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected void initUidHashIdProvider() {
        idProvider = PostgisUtils.getObsHashIdProvider(212158449);
    }
}
