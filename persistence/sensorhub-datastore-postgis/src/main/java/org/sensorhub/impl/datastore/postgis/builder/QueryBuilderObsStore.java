/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.builder;

import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.obs.ObsStatsQuery;
import org.sensorhub.impl.datastore.postgis.builder.filter.datastream.DataStreamFilterQuery;
import org.sensorhub.impl.datastore.postgis.builder.query.obs.RemoveEntriesObsQuery;
import org.sensorhub.impl.datastore.postgis.builder.query.obs.SelectEntriesObsQuery;
import org.sensorhub.impl.datastore.postgis.builder.query.stats.StatsObsQuery;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField.GEOMETRY;
import static org.sensorhub.api.datastore.feature.IFeatureStoreBase.FeatureField.VALID_TIME;
import static org.sensorhub.api.datastore.obs.IObsStore.ObsField.*;

public class QueryBuilderObsStore extends QueryBuilder {

    public final static String OBS_STORE_TABLE_NAME = "obs";
    private final boolean genericPayloadIndexEnabled;

    public QueryBuilderObsStore() {
        this(OBS_STORE_TABLE_NAME, true);
    }

    public QueryBuilderObsStore(String tableName) {
        this(tableName, true);
    }

    /**
     * @param genericPayloadIndexEnabled false only for a fresh OSCAR
     *        PARQUET_PRIMARY table. Purpose-built event indexes remain because
     *        mutable occupancy/alarm workflow rows stay in PostgreSQL.
     */
    public QueryBuilderObsStore(String tableName, boolean genericPayloadIndexEnabled) {
        super(tableName);
        this.genericPayloadIndexEnabled = genericPayloadIndexEnabled;
    }

    public String[] createInitializationQueries() {
        List<String> queries = new ArrayList<>();
        queries.add(createTableQuery());
        if (genericPayloadIndexEnabled)
            queries.add(createDataIndexQuery());
        queries.add(createDataStreamIndexQuery());
        queries.add(createPhenomenonTimeIndexQuery());
        queries.add(createPhenomenonTimeDataStreamIdIndexQuery());
        queries.add(createResultTimeIndexQuery());
        queries.add(createResultTimeDataStreamIdIndexQuery());
        queries.add(createFoiIndexQuery());
        queries.add(createUniqueConstraint());
        return queries.toArray(String[]::new);
    }

    public String createTableQuery() {
        return "CREATE TABLE IF NOT EXISTS " + this.getStoreTableName() +
                " (" +
                "id bigint PRIMARY KEY," +
                DATASTREAM_ID + " bigint, " +
                FOI_ID + " bigint," +
                PHENOMENON_TIME + " TIMESTAMP," +
                RESULT_TIME + " TIMESTAMP," +
                RESULT + " JSONB" +
                ")";
    }

    /** Marker used only by OSCAR's tiered store; harmless on ordinary stores. */
    public String addArchiveGenerationColumnQuery() {
        return "ALTER TABLE " + getStoreTableName() + " ADD COLUMN IF NOT EXISTS archivegenerationid uuid";
    }

    public String createUnarchivedTimeIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + getStoreTableName() +
                "_unarchived_time_idx ON " + getStoreTableName() +
                " (phenomenontime, id) WHERE archivegenerationid IS NULL";
    }

    public String createArchiveGenerationIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + getStoreTableName() +
                "_archive_generation_idx ON " + getStoreTableName() +
                " (archivegenerationid, id) WHERE archivegenerationid IS NOT NULL";
    }

    /**
     * Covering partial index for unadjudicated alarm counts across tiered
     * datastreams so {@code count_alarm} executes as an index-only scan without
     * random heap fetches.
     */
    public String createTieredUnadjudicatedAlarmIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + getStoreTableName() +
                "_unadjudicated_alarm_idx ON " + getStoreTableName() +
                " (" + DATASTREAM_ID + ", " + RESULT_TIME + ") " +
                "INCLUDE (archivegenerationid) " +
                "WHERE ((" + RESULT + " @> '{\"gammaAlarm\": true}'::jsonb OR " +
                RESULT + " @> '{\"neutronAlarm\": true}'::jsonb) AND " +
                RESULT + " @> '{\"adjudicatedIdsCount\": 0}'::jsonb)";
    }

    public String createDataIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_data_idx on "+this.getStoreTableName()+" USING GIN("+RESULT+")";
    }

    public String createResultPathOpsIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_result_path_ops_idx on "+
                this.getStoreTableName()+" USING GIN("+RESULT+" jsonb_path_ops)";
    }

    public String createOccupancyCountIndexQuery() {
        return createNumericResultIndexQuery("occupancyCount", "occupancy_count");
    }

    public String createMaxGammaIndexQuery() {
        return createNumericResultIndexQuery("maxGamma", "max_gamma");
    }

    public String createMaxNeutronIndexQuery() {
        return createNumericResultIndexQuery("maxNeutron", "max_neutron");
    }

    public String[] createEventFilterIndexQueries() {
        return new String[]{
                createResultPathOpsIndexQuery(),
                createOccupancyCountIndexQuery(),
                createMaxGammaIndexQuery(),
                createMaxNeutronIndexQuery(),
                createKeysetPhenomenonTimeAscIndexQuery(),
                createKeysetPhenomenonTimeDescIndexQuery(),
                createKeysetDataStreamPhenomenonTimeAscIndexQuery(),
                createKeysetDataStreamPhenomenonTimeDescIndexQuery(),
                createKeysetDataStreamResultTimeIndexQuery(),
                createNonAlarmKeysetPhenomenonTimeDescIndexQuery()
        };
    }

    /**
     * Ordered partial index for non-alarm occupancy paging ({@code gammaAlarm=false
     * AND neutronAlarm=false ORDER BY phenomenontime DESC NULLS FIRST, id ASC})
     * so Event Log's lowest-priority bucket walks the newest matching rows
     * directly instead of scanning and top-N sorting every historical occupancy
     * heap page via GIN {@code BitmapAnd}.
     */
    public String createNonAlarmKeysetPhenomenonTimeDescIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() +
                "_non_alarm_ptime_desc_idx on " +
                this.getStoreTableName() + " (" + PHENOMENON_TIME + " DESC NULLS FIRST, id ASC) " +
                "INCLUDE (" + DATASTREAM_ID + ", " + RESULT_TIME + ") " +
                "WHERE (" + RESULT + " @> '{\"gammaAlarm\": false}'::jsonb AND " +
                RESULT + " @> '{\"neutronAlarm\": false}'::jsonb)";
    }

    /**
     * Keeps PostgreSQL's visibility map current on insert-heavy observation
     * tables so index-only scans and counts do not degrade into hundreds of
     * thousands of heap page fetches between default 20%-scale autovacuums.
     */
    public String configureAutovacuumQuery() {
        return "ALTER TABLE " + this.getStoreTableName() + " SET (" +
                "autovacuum_vacuum_insert_scale_factor = 0.01, " +
                "autovacuum_vacuum_insert_threshold = 5000, " +
                "autovacuum_vacuum_scale_factor = 0.02, " +
                "autovacuum_vacuum_threshold = 2000)";
    }

    public String createKeysetPhenomenonTimeAscIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() + "_phenomenon_time_id_nulls_first_idx on " +
                this.getStoreTableName() + " (" + PHENOMENON_TIME + " ASC NULLS FIRST, id ASC)";
    }

    public String createKeysetPhenomenonTimeDescIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() + "_phenomenon_time_desc_id_nulls_first_idx on " +
                this.getStoreTableName() + " (" + PHENOMENON_TIME + " DESC NULLS FIRST, id ASC)";
    }

    public String createKeysetDataStreamPhenomenonTimeAscIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() + "_datastream_phenomenon_time_id_nulls_first_idx on " +
                this.getStoreTableName() + " (" + DATASTREAM_ID + " ASC, " + PHENOMENON_TIME + " ASC NULLS FIRST, id ASC)";
    }

    public String createKeysetDataStreamPhenomenonTimeDescIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() + "_datastream_phenomenon_time_desc_id_nulls_first_idx on " +
                this.getStoreTableName() + " (" + DATASTREAM_ID + " ASC, " + PHENOMENON_TIME + " DESC NULLS FIRST, id ASC)";
    }

    public String createKeysetDataStreamResultTimeIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS " + this.getStoreTableName() + "_datastream_result_time_id_idx on " +
                this.getStoreTableName() + " (" + DATASTREAM_ID + " ASC, " + RESULT_TIME + " ASC, id ASC)";
    }

    private String createNumericResultIndexQuery(String propertyName, String indexName) {
        String jsonValue = RESULT+"->>'"+propertyName+"'";
        String jsonProperty = RESULT+"->'"+propertyName+"'";
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_result_"+indexName+"_idx on "+
                this.getStoreTableName()+" ("+DATASTREAM_ID+", (("+jsonValue+")::numeric)) "+
                "WHERE jsonb_typeof("+jsonProperty+") = 'number'";
    }

    public String createDataStreamIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_datastream_idx on "+this.getStoreTableName()+" ("+DATASTREAM_ID+")";
    }

    public String createPhenomenonTimeDataStreamIdIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_datastream_id_phenomenon_time_idx on "+this.getStoreTableName()+" ("+ DATASTREAM_ID + ", " + PHENOMENON_TIME +")";
    }

    public String createPhenomenonTimeIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_phenomenon_time_idx on "+this.getStoreTableName()+" ("+ PHENOMENON_TIME +")";
    }

    public String createResultTimeDataStreamIdIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_datastream_id_result_time_idx on "+this.getStoreTableName()+" ("+ DATASTREAM_ID + ", " + RESULT_TIME +")";
    }

    public String createResultTimeIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_result_time_idx on "+this.getStoreTableName()+" ("+ RESULT_TIME +")";
    }

    public String createFoiIndexQuery() {
        return "CREATE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_foi_idx on "+this.getStoreTableName()+" ("+ FOI_ID +")";
    }

    public String insertObsQuery() {
        return "INSERT INTO "+this.getStoreTableName()+" " +
                "(id,"+DATASTREAM_ID+", "+FOI_ID+", "+PHENOMENON_TIME+", "+RESULT_TIME+", "+RESULT+") VALUES (${1},${2},${3},${4},${5},${6}) "+
                naturalKeyUpsertClause();
    }

    public String insertObsPreparedQuery() {
        return "INSERT INTO "+this.getStoreTableName()+" " +
                "(id,"+DATASTREAM_ID+", "+FOI_ID+", "+PHENOMENON_TIME+", "+RESULT_TIME+", "+RESULT+") VALUES (?::int8,?::int8,?::int8,?::timestamp,?::timestamp,?::jsonb) "+
                naturalKeyUpsertClause();
    }

    /**
     * Tiered rows already assigned to an immutable generation cannot be
     * changed in place. The store writes a generation-owned overlay for such
     * conflicts before committing the batch.
     */
    public String insertTieredObsPreparedQuery() {
        return "INSERT INTO "+this.getStoreTableName()+" " +
                "(id,"+DATASTREAM_ID+", "+FOI_ID+", "+PHENOMENON_TIME+", "+RESULT_TIME+", "+RESULT+") VALUES (?::int8,?::int8,?::int8,?::timestamp,?::timestamp,?::jsonb) "+
                naturalKeyUpsertClause("(" + getStoreTableName() + ".archivegenerationid IS NULL OR " +
                        "NOT EXISTS (SELECT 1 FROM oscar_archive_generation generation " +
                        "WHERE generation.id=" + getStoreTableName() + ".archivegenerationid " +
                        "AND generation.state IN ('ACTIVE','SEALED'))) AND ");
    }

    /**
     * Match the observation identity used by OSH's reference MVStore:
     * datastream, FOI, phenomenon time and result time identify one logical
     * observation. Replaying the same value is a no-op; a corrected value for
     * that identity replaces the result while retaining the original row ID.
     */
    private String naturalKeyUpsertClause() {
        return naturalKeyUpsertClause("");
    }

    private String naturalKeyUpsertClause(String updateGuard) {
        return "ON CONFLICT (" + DATASTREAM_ID + ", COALESCE(" + FOI_ID +
                ", -1::bigint), COALESCE(" + PHENOMENON_TIME +
                ", '-infinity'::timestamp), COALESCE(" + RESULT_TIME +
                ", '-infinity'::timestamp)) DO UPDATE SET " + RESULT + "=EXCLUDED." + RESULT +
                " WHERE " + updateGuard + getStoreTableName() + "." + RESULT +
                " IS DISTINCT FROM EXCLUDED." + RESULT;
    }

    public String createUniqueConstraint() {
        // PostgreSQL considers NULL values distinct in a normal unique index.
        // Most OSCAR observations have no result time, so the former index did
        // not actually enforce the documented natural observation key. A new
        // index name also makes existing installations add the corrected
        // constraint instead of retaining the old definition via IF NOT EXISTS.
        return "CREATE UNIQUE INDEX IF NOT EXISTS "+this.getStoreTableName()+"_unique_constraint_v2 on "+
                this.getStoreTableName()+" (" + DATASTREAM_ID + ", COALESCE(" + FOI_ID +
                ", -1::bigint), COALESCE(" + PHENOMENON_TIME +
                ", '-infinity'::timestamp), COALESCE(" + RESULT_TIME +
                ", '-infinity'::timestamp))";
    }

    /** Online variant used to upgrade an already populated observation table. */
    public String createUniqueConstraintConcurrently() {
        return createUniqueConstraint().replace("CREATE UNIQUE INDEX IF NOT EXISTS",
                "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS");
    }

    public String uniqueConstraintName() {
        return getStoreTableName() + "_unique_constraint_v2";
    }

    public String updateByIdQuery() {
        return "UPDATE " + this.getStoreTableName() + " SET " +
                DATASTREAM_ID + " = ?, " +
                FOI_ID + " = ?, " +
                PHENOMENON_TIME + " = ?, " +
                RESULT_TIME + " = ?, " +
                RESULT + " = ? " +
                "WHERE id = ?";
    }

    public String findByUniqueFieldsQuery() {
        return naturalKeyLookupQuery("id");
    }

    /**
     * Resolve the stored value used to verify an upsert. The predicate must
     * retain the same COALESCE expressions as the natural-key unique index;
     * {@code IS NOT DISTINCT FROM} is NULL-safe but does not let a PostgreSQL
     * generic prepared plan use the full expression key.
     */
    public String findResultByUniqueFieldsQuery() {
        return naturalKeyLookupQuery("id, " + RESULT + "::text");
    }

    private String naturalKeyLookupQuery(String projection) {
        return "SELECT " + projection + " FROM " + this.getStoreTableName() + " WHERE " +
                DATASTREAM_ID + " = ? AND " +
                "COALESCE(" + FOI_ID + ", -1::bigint) = COALESCE(?::bigint, -1::bigint) AND " +
                "COALESCE(" + PHENOMENON_TIME + ", '-infinity'::timestamp) = " +
                "COALESCE(?::timestamp, '-infinity'::timestamp) AND " +
                "COALESCE(" + RESULT_TIME + ", '-infinity'::timestamp) = " +
                "COALESCE(?::timestamp, '-infinity'::timestamp) LIMIT 1";
    }

    public String getPhenomenonTimeRangeByDataStreamIdQuery(long dataStreamID) {
        return getPhenomenonTimeRangeByDataStreamIdQuery();
    }

    public String getPhenomenonTimeRangeByDataStreamIdQuery() {
        return "SELECT Min("+PHENOMENON_TIME+"),Max("+PHENOMENON_TIME+") FROM "+this.getStoreTableName()+" WHERE "+DATASTREAM_ID+" = ?";
    }

    public String getPhenomenonTimeRangeByDataStreamIdsQuery() {
        return "SELECT " + DATASTREAM_ID + ", " +
                "       MIN(" + PHENOMENON_TIME + ") AS min, " +
                "       MAX(" + PHENOMENON_TIME + ") AS max " +
                "FROM " + getStoreTableName() +
                " WHERE " + DATASTREAM_ID + " = ANY (?) " +
                "GROUP BY " + DATASTREAM_ID;
    }

    public String getBinCountByPhenomenontime(long seconds, List<Long> dsIds, List<Long> foiIds) {
        StringBuilder idsQuery = new StringBuilder();
        for(int i=0;i < dsIds.size();i++) {
            idsQuery.append(" datastreamid = ").append(dsIds.get(i));
            if(i < dsIds.size()) {
                idsQuery.append(" AND ");
            }
        }
        // foiIds filter
        idsQuery.append(" AND ");
        for(int i=0;i < foiIds.size();i++) {
            idsQuery.append(" foiid = ").append(foiIds.get(i));
            if(i < foiIds.size()) {
                idsQuery.append(" AND ");
            }
        }
        return "SELECT COUNT(*), floor((extract('epoch' from "+PHENOMENON_TIME+") / "+seconds+" )) * "+seconds+" as interval_alias " +
                " WHERE "+idsQuery.toString() +
                " FROM "+this.getStoreTableName()+" GROUP BY interval_alias order BY interval_alias";
    }

    public String getResultTimeRangeByDataStreamIdQuery(long dataStreamID) {
        return getResultTimeRangeByDataStreamIdQuery();
    }

    public String getResultTimeRangeByDataStreamIdQuery() {
        return "SELECT Min("+RESULT_TIME+"),Max("+RESULT_TIME+") FROM "+this.getStoreTableName()+" WHERE "+DATASTREAM_ID+" = ?";
    }

    public String getResultTimeRangeByDataStreamIdsQuery() {
        return "SELECT " + DATASTREAM_ID + ", " +
                "       MIN(" + RESULT_TIME + ") AS min, " +
                "       MAX(" + RESULT_TIME + ") AS max " +
                "FROM " + getStoreTableName() +
                " WHERE " + DATASTREAM_ID + " = ANY (?) " +
                "GROUP BY " + DATASTREAM_ID;
    }

    public String countByPhenomenonTimeRangeQuery(Instant min, Instant max) {
            String minTimestamp = min.toString();
            if(min == Instant.MIN) {
                minTimestamp = "-infinity";
            }
            String maxTimestamp = max.toString();
            if(max == Instant.MAX) {
                maxTimestamp = "infinity";
            }
            String sb = "tsrange((" + this.getStoreTableName() + "."+PHENOMENON_TIME+")::timestamp," +
                    " (" + this.getStoreTableName()  + "."+PHENOMENON_TIME+")::timestamp)" +
                    " && '[" + minTimestamp + "," + maxTimestamp + "]'";

        return "SELECT COUNT(*) FROM "+this.getStoreTableName()+" "+sb;
    }

    public String createSelectEntriesQuery(ObsFilter filter, Set<IObsStore.ObsField> fields) {
        return createParameterizedSelectEntriesQuery(filter, fields).sql();
    }

    public ParameterizedQuery createParameterizedSelectEntriesQuery(ObsFilter filter, Set<IObsStore.ObsField> fields) {
        return createParameterizedSelectEntriesQuery(filter, fields, false);
    }

    public ParameterizedQuery createParameterizedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields, boolean parentTableOnly) {
        return createParameterizedSelectEntriesQuery(filter, fields, true, parentTableOnly);
    }

    /**
     * Builds the normal observation selection without presentation-only
     * ordering. This is safe only when the caller supplies an equivalent outer
     * order or consumes rows as an order-independent aggregate. Ordering that
     * is semantically required by a composed {@code DISTINCT ON} selector is
     * deliberately retained inside the subquery.
     */
    public ParameterizedQuery createParameterizedUnorderedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields) {
        return createParameterizedUnorderedSelectEntriesQuery(filter, fields, false, new String[0]);
    }

    public ParameterizedQuery createParameterizedUnorderedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields, boolean parentTableOnly) {
        return createParameterizedUnorderedSelectEntriesQuery(filter, fields, parentTableOnly, new String[0]);
    }

    public ParameterizedQuery createParameterizedUnorderedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields, String... additionalSelectColumns) {
        return createParameterizedUnorderedSelectEntriesQuery(filter, fields, false, additionalSelectColumns);
    }

    public ParameterizedQuery createParameterizedUnorderedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields, boolean parentTableOnly,
            String... additionalSelectColumns) {
        if ((filter.getPhenomenonTime() != null && filter.getPhenomenonTime().isLatestTime()) ||
                (filter.getResultTime() != null && filter.getResultTime().isLatestTime())) {
            throw new IllegalArgumentException(
                    "Latest observation selection requires its DISTINCT ON ordering");
        }
        return createParameterizedSelectEntriesQuery(
                filter, fields, false, parentTableOnly, additionalSelectColumns);
    }

    private ParameterizedQuery createParameterizedSelectEntriesQuery(
            ObsFilter filter, Set<IObsStore.ObsField> fields, boolean ordered,
            boolean parentTableOnly, String... additionalSelectColumns) {
        SelectEntriesObsQuery selectEntriesObsQuery = new SelectEntriesObsQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .linkTo(this.dataStreamStore)
                .linkTo(this.foiStore)
                .withFields(fields)
                .withAdditionalSelectFields(additionalSelectColumns)
                .withObsFilter(filter)
                .parentTableOnly(parentTableOnly)
                .withLimit(filter.getLimit())
                .withoutOrderByIf(!ordered)
                .build();
        return selectEntriesObsQuery.toParameterizedQuery();
    }

    /**
     * Returns whether the latest observation for each explicitly selected data
     * stream can be read with the optimized lateral query.
     *
     * <p>The generic latest-result query uses {@code DISTINCT ON} across all
     * matching observations. For a multi-stream request that makes PostgreSQL
     * scan and sort each stream's history before it can return one row. The
     * lateral form below performs one bounded index probe per stream instead.
     * Keep the eligibility deliberately narrow so every other filter retains
     * the existing query semantics.</p>
     */
    public boolean supportsLatestResultPerDataStreamQuery(ObsFilter filter) {
        if (filter == null ||
                filter.getDataStreamFilter() == null ||
                !DataStreamFilterQuery.hasOnlyInternalIds(filter.getDataStreamFilter())) {
            return false;
        }
        boolean latestResult = filter.getResultTime() != null &&
                filter.getResultTime().isLatestTime() &&
                filter.getPhenomenonTime() == null;
        boolean latestPhenomenon = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().isLatestTime() &&
                filter.getResultTime() == null;
        if (!latestResult && !latestPhenomenon) {
            return false;
        }

        return filter.getPhenomenonLocation() == null &&
                filter.getFoiFilter() == null &&
                (filter.getInternalIDs() == null || filter.getInternalIDs().isEmpty()) &&
                filter.getCQLFilter() == null &&
                filter.getValuePredicate() == null;
    }

    /**
     * Builds an index-probe query for the latest result or phenomenon time of
     * each requested data stream. The existing {@code (dataStreamID, resultTime)}
     * and {@code (dataStreamID, phenomenonTime)} indexes can be scanned backwards
     * for each fixed stream ID and stop after one row.
     *
     * <p>The two small branches preserve the datastore's null and tie rules
     * without asking PostgreSQL to sort a stream's complete history. The first
     * branch finds the newest non-null timestamp; the second supplies the
     * greatest ID only when a stream contains no non-null timestamp. The
     * outer two-row choice implements {@code NULLS LAST}, and the final order
     * matches the observation store's phenomenon-time order so a tiered hot/
     * cold merge receives correctly ordered inputs.</p>
     */
    public ParameterizedQuery createParameterizedLatestResultPerDataStreamQuery(
            ObsFilter filter,
            Set<IObsStore.ObsField> fields) {
        return createParameterizedLatestResultPerDataStreamQuery(filter, fields, false);
    }

    public ParameterizedQuery createParameterizedLatestResultPerDataStreamQuery(
            ObsFilter filter,
            Set<IObsStore.ObsField> fields,
            boolean parentTableOnly) {
        if (!supportsLatestResultPerDataStreamQuery(filter)) {
            throw new IllegalArgumentException("Filter is not eligible for the latest-result-per-stream query");
        }
        boolean latestPhenomenon = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().isLatestTime();
        String timeColumn = latestPhenomenon ? PHENOMENON_TIME.toString() : RESULT_TIME.toString();
        boolean descending = filter.getPhenomenonTime() != null &&
                filter.getPhenomenonTime().descendingOrder();

        List<Long> dataStreamIds = filter.getDataStreamFilter().getInternalIDs().stream()
                .map(id -> id.getIdAsLong())
                // The legacy IN predicate collapses local IDs that differ only
                // by BigId scope. unnest would otherwise probe and return the
                // same local stream more than once.
                .distinct()
                .collect(Collectors.toList());
        String placeholders = dataStreamIds.stream()
                .map(id -> "?")
                .collect(Collectors.joining(","));
        List<Object> parameters = new ArrayList<>(dataStreamIds);

        String selectedColumns;
        if (fields != null && !fields.isEmpty()) {
            List<String> selectedFields = fields.stream()
                    .map(Object::toString)
                    .distinct()
                    .sorted()
                    .collect(Collectors.toCollection(ArrayList::new));
            selectedFields.add("id");
            selectedColumns = selectedFields.stream()
                    .map(field -> "latest." + field)
                    .collect(Collectors.joining(","));
        } else {
            selectedColumns = "latest.*";
        }

        String relation = (parentTableOnly ? "ONLY " : "") + this.getStoreTableName();
        StringBuilder sql = new StringBuilder("SELECT ")
                .append(selectedColumns)
                .append(" FROM unnest(ARRAY[")
                .append(placeholders)
                .append("]::bigint[]) AS requested(dataStreamID)")
                .append(" CROSS JOIN LATERAL (SELECT candidate.* FROM ((SELECT non_null.* FROM ")
                .append(relation)
                .append(" non_null WHERE non_null.dataStreamID = requested.dataStreamID")
                .append(" AND non_null.").append(timeColumn).append(" IS NOT NULL")
                .append(" ORDER BY non_null.").append(timeColumn).append(" DESC, non_null.id DESC")
                // IteratorResultSet detects an outer LIMIT textually when it
                // decides whether to add paging. FETCH keeps this inner bound
                // from being mistaken for the caller's outer result limit.
                .append(" FETCH FIRST 1 ROW ONLY) UNION ALL (SELECT null_only.* FROM ")
                .append(relation)
                .append(" null_only WHERE null_only.dataStreamID = requested.dataStreamID")
                .append(" AND null_only.").append(timeColumn).append(" IS NULL")
                .append(" ORDER BY null_only.id DESC FETCH FIRST 1 ROW ONLY)) candidate")
                .append(" ORDER BY candidate.").append(timeColumn).append(" DESC NULLS LAST, candidate.id DESC")
                .append(" FETCH FIRST 1 ROW ONLY) latest")
                .append(" ORDER BY latest.phenomenonTime ")
                .append(descending ? "DESC" : "ASC")
                .append(" NULLS FIRST, latest.id ASC");

        if (filter.getLimit() != Long.MAX_VALUE) {
            sql.append(" LIMIT ").append(filter.getLimit());
        }

        return new ParameterizedQuery(sql.toString(), parameters);
    }

    public String createSelectEntriesCountQuery(ObsFilter filter) {
        return createParameterizedSelectEntriesCountQuery(filter).sql();
    }

    public ParameterizedQuery createParameterizedSelectEntriesCountQuery(ObsFilter filter) {
        return createParameterizedSelectEntriesCountQuery(filter, false, null);
    }

    public ParameterizedQuery createParameterizedSelectEntriesCountQuery(
            ObsFilter filter, boolean parentTableOnly, String extraWhereClause, Object... extraParameters) {
        SelectEntriesObsQuery selectEntriesObsQuery = new SelectEntriesObsQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .linkTo(this.dataStreamStore)
                .linkTo(this.foiStore)
                .withObsFilter(filter)
                .parentTableOnly(parentTableOnly)
                .withRawCondition(extraWhereClause, extraParameters)
                .build();
        return selectEntriesObsQuery.toParameterizedCountQuery();
    }

    public String statsQueryByFoi(ObsStatsQuery obsStatsQuery, long foiId) {
        StatsObsQuery statsObsQuery = new StatsObsQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .linkTo(this.dataStreamStore)
                .linkTo(this.foiStore)
                .withFoi(foiId)
                .withObsStatsFilter(obsStatsQuery)
                .withLimit(obsStatsQuery.getLimit())
                .build();
        return statsObsQuery.toQuery();
    }

    public String statsQueryByDataStream(ObsStatsQuery obsStatsQuery, long dsId) {
        return parameterizedStatsQueryByDataStream(obsStatsQuery, dsId).sql();
    }

    public ParameterizedQuery parameterizedStatsQueryByDataStream(ObsStatsQuery obsStatsQuery, long dsId) {
        StatsObsQuery statsObsQuery = new StatsObsQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .linkTo(this.dataStreamStore)
                .linkTo(this.foiStore)
                .withDataStream(dsId)
                .withObsStatsFilter(obsStatsQuery)
                .withLimit(obsStatsQuery.getLimit())
                .build();
        return statsObsQuery.toParameterizedQuery();
    }

    public String createRemoveEntriesQuery(ObsFilter filter) {
        return createParameterizedRemoveEntriesQuery(filter).sql();
    }

    public ParameterizedQuery createParameterizedRemoveEntriesQuery(ObsFilter filter) {
        RemoveEntriesObsQuery removeEntriesObsQuery = new RemoveEntriesObsQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .linkTo(this.dataStreamStore)
                .linkTo(this.foiStore)
                .withObsFilter(filter)
                .build();
        return removeEntriesObsQuery.toParameterizedQuery();
    }
}
