/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis;

import com.google.common.collect.Range;
import org.junit.Test;
import org.sensorhub.api.common.BigId;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.feature.IFoiStore;
import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.api.datastore.obs.IObsStore;
import org.sensorhub.api.datastore.obs.ObsFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderCommandStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderDataStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderObsStore;
import org.sensorhub.impl.datastore.postgis.builder.QueryBuilderSystemDescStore;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Proxy;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class TestParameterizedQueryBuilder {
    private static final String INJECTION = "camera') OR 1=1 --";

    @Test
    public void testDataStreamFullTextFilterIsParameterized() {
        DataStreamFilter filter = new DataStreamFilter.Builder()
                .withKeywords(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderDataStreamStore("datastreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("~* ?"));
        assertEquals("(" + INJECTION + ")", query.parameters().get(0));
    }

    @Test
    public void testCommandStreamFullTextFilterIsParameterized() {
        CommandStreamFilter filter = new CommandStreamFilter.Builder()
                .withKeywords(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderCommandStreamStore("commandstreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("~* ?"));
        assertEquals("(" + INJECTION + ")", query.parameters().get(0));
    }

    @Test
    public void testSystemUidFilterIsParameterized() {
        SystemFilter filter = new SystemFilter.Builder()
                .withUniqueIDs(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderSystemDescStore("systems")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("ILIKE ?"));
        assertEquals("%" + INJECTION, query.parameters().get(0));
    }

    @Test
    public void testFeatureParentLookupIsBoundedAndParameterized() {
        String query = new QueryBuilderSystemDescStore("systems").selectParentByIdQuery();

        assertEquals("SELECT parentid FROM systems WHERE id = ? " +
                "ORDER BY lower(validTime) DESC LIMIT 1", query);
    }

    @Test
    public void testDataStreamOutputNameIsParameterized() {
        DataStreamFilter filter = new DataStreamFilter.Builder()
                .withOutputNames(INJECTION)
                .build();

        ParameterizedQuery query = new QueryBuilderDataStreamStore("datastreams")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("in (?)"));
        assertEquals(INJECTION, query.parameters().get(0));
    }

    @Test
    public void testObsCqlStringLiteralIsParameterized() {
        ObsFilter filter = new ObsFilter.Builder()
                .withCQLFilter("name = '" + INJECTION.replace("'", "''") + "'")
                .build();

        ParameterizedQuery query = new QueryBuilderObsStore("obs")
                .createParameterizedSelectEntriesQuery(filter, null);

        assertSqlDoesNotContainInjection(query);
        assertTrue(query.sql().contains("result @> ?::jsonb"));
        assertTrue(query.parameters().get(0).toString().contains(INJECTION));

        ParameterizedQuery partialIndexable = new QueryBuilderObsStore("obs")
                .createParameterizedSelectEntriesCountQuery(new ObsFilter.Builder()
                        .withCQLFilter("(gammaAlarm = true OR neutronAlarm = true) AND adjudicatedIdsCount = 0")
                        .build());
        assertTrue(partialIndexable.sql().contains("result @> '{\"gammaAlarm\": true}'::jsonb"));
        assertTrue(partialIndexable.sql().contains("result @> '{\"neutronAlarm\": true}'::jsonb"));
        assertTrue(partialIndexable.sql().contains("result @> '{\"adjudicatedIdsCount\": 0}'::jsonb"));
        assertTrue(partialIndexable.parameters().isEmpty());
    }

    @Test
    public void testObsCqlOrderedComparisonsUseTypedInstantOrNumericPaths() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        String offsetCutoff = "2026-01-02T06:00:00.581+18:00";
        String canonicalCutoff = "2026-01-01T12:00:00.581Z";

        for (String operator : List.of("<", "<=", ">", ">=")) {
            ParameterizedQuery temporal = builder.createParameterizedSelectEntriesQuery(
                    new ObsFilter.Builder()
                            .withCQLFilter("occurredAt " + operator + " '" + offsetCutoff + "'")
                            .build(),
                    null);

            assertTrue(temporal.sql().contains("jsonb_typeof(result->'occurredAt') = 'string'"));
            assertTrue(temporal.sql().contains("result->>'occurredAt' ~ '^[0-9]{4}-"));
            assertTrue(temporal.sql().contains("(\\.[0-9]{1,9})?"));
            assertTrue(temporal.sql().contains(
                    "pg_input_is_valid(result->>'occurredAt', 'timestamp with time zone')"));
            assertTrue(temporal.sql().contains(
                    "pg_input_is_valid(CAST(? AS text), 'timestamp with time zone')"));
            assertTrue(temporal.sql().contains(
                    "(result->>'occurredAt')::timestamptz " + operator + " CAST(? AS timestamptz)"));
            assertTrue(temporal.sql().contains("ELSE FALSE END"));
            assertEquals(List.of(canonicalCutoff, canonicalCutoff), temporal.parameters());
        }

        ParameterizedQuery numeric = builder.createParameterizedSelectEntriesQuery(
                new ObsFilter.Builder().withCQLFilter("value >= 10").build(), null);
        assertTrue(numeric.sql().contains("jsonb_typeof(result->'value') = 'number'"));
        assertTrue(numeric.sql().contains("(result->>'value')::numeric >= ?::numeric"));
        assertEquals("10", numeric.parameters().get(0).toString());

        ParameterizedQuery quotedNumeric = builder.createParameterizedSelectEntriesQuery(
                new ObsFilter.Builder().withCQLFilter("value >= '10'").build(), null);
        assertTrue(quotedNumeric.sql().contains("(result->>'value')::numeric >= ?::numeric"));
        assertEquals(List.of("10"), quotedNumeric.parameters());

        ParameterizedQuery malformed = builder.createParameterizedSelectEntriesQuery(
                new ObsFilter.Builder().withCQLFilter("occurredAt > 'not-a-time'").build(), null);
        assertTrue(malformed.sql().contains("WHERE FALSE"));
        assertTrue(malformed.parameters().isEmpty());
        assertFalse(malformed.sql().contains("occurredAt') >"));

        for (String unsupported : List.of("today", "infinity", "2026-01-01",
                "0000-01-01T00:00:00Z")) {
            ParameterizedQuery rejected = builder.createParameterizedSelectEntriesQuery(
                    new ObsFilter.Builder()
                            .withCQLFilter("occurredAt > '" + unsupported + "'")
                            .build(), null);
            assertTrue(rejected.sql().contains("WHERE FALSE"));
            assertTrue(rejected.parameters().isEmpty());
        }
    }

    @Test
    public void testObservationTimeBoundsArePreservedForSelectAndRemove() {
        Instant start = Instant.parse("2026-01-01T00:00:00Z");
        Instant end = Instant.parse("2026-01-02T00:00:00Z");
        ObsFilter halfOpen = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withRange(Range.closedOpen(start, end))
                        .build())
                .build();
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");

        ParameterizedQuery select = builder.createParameterizedSelectEntriesQuery(halfOpen, null);
        ParameterizedQuery remove = builder.createParameterizedRemoveEntriesQuery(halfOpen);

        for (ParameterizedQuery query : Arrays.asList(select, remove)) {
            assertTrue(query.sql().contains("obs.phenomenonTime >= ?::timestamp"));
            assertTrue(query.sql().contains("obs.phenomenonTime < ?::timestamp"));
            assertFalse(query.sql().contains("obs.phenomenonTime <= ?::timestamp"));
            assertEquals(2, query.parameters().size());
        }

        ObsFilter inverse = new ObsFilter.Builder()
                .withResultTime(new TemporalFilter.Builder()
                        .withRange(Range.openClosed(start, end))
                        .build())
                .build();
        ParameterizedQuery inverseRemove = builder.createParameterizedRemoveEntriesQuery(inverse);
        assertTrue(inverseRemove.sql().contains("obs.resultTime > ?::timestamp"));
        assertTrue(inverseRemove.sql().contains("obs.resultTime <= ?::timestamp"));
    }

    @Test
    public void testObservationOrderIsStableAndCanBeRemovedForOuterKeysetPaging() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter ascending = new ObsFilter.Builder().build();
        ParameterizedQuery ordered = builder.createParameterizedSelectEntriesQuery(
                ascending, Set.of(IObsStore.ObsField.PHENOMENON_TIME));
        ParameterizedQuery unordered = builder.createParameterizedUnorderedSelectEntriesQuery(
                ascending, Set.of(IObsStore.ObsField.PHENOMENON_TIME));

        assertTrue(ordered.sql().endsWith(
                "ORDER BY obs.phenomenonTime ASC NULLS FIRST, obs.id ASC"));
        assertFalse(unordered.sql().contains(" ORDER BY "));

        ObsFilter descending = new ObsFilter.Builder()
                .withPhenomenonTime(new TemporalFilter.Builder()
                        .withAllTimes()
                        .descendingOrder(true)
                        .build())
                .build();
        assertTrue(builder.createParameterizedSelectEntriesQuery(descending, null).sql().endsWith(
                "ORDER BY obs.phenomenonTime DESC NULLS FIRST, obs.id ASC"));

        ObsFilter latest = new ObsFilter.Builder().withLatestResult().build();
        assertThrows(IllegalArgumentException.class,
                () -> builder.createParameterizedUnorderedSelectEntriesQuery(latest, null));

        ParameterizedQuery withTableOid = builder.createParameterizedUnorderedSelectEntriesQuery(
                ascending, Set.of(IObsStore.ObsField.PHENOMENON_TIME), "tableoid");
        assertTrue(withTableOid.sql().contains("obs.id,obs.tableoid FROM obs"));
        assertFalse(withTableOid.sql().contains(" ORDER BY "));
        assertThrows(IllegalArgumentException.class,
                () -> builder.createParameterizedUnorderedSelectEntriesQuery(
                        ascending, Set.of(IObsStore.ObsField.PHENOMENON_TIME), "tableoid; DROP TABLE obs"));
    }

    @Test
    public void testOuterKeysetRetainsNestedDataStreamVersionOrdering() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        builder.linkTo(dataStreamStoreNamed("datastreams"));

        List<TemporalFilter> semanticVersions = List.of(
                new TemporalFilter.Builder().withLatestTime().build(),
                new TemporalFilter.Builder().withCurrentTime().build());
        for (TemporalFilter validTime : semanticVersions) {
            ObsFilter filter = new ObsFilter.Builder()
                    .withDataStreams(new DataStreamFilter.Builder()
                            .withOutputNames("gammaCounts")
                            .withValidTime(validTime)
                            .build())
                    .build();

            String sql = builder.createParameterizedUnorderedSelectEntriesQuery(
                    filter, Set.of(IObsStore.ObsField.PHENOMENON_TIME)).sql();

            assertTrue(sql.contains("DISTINCT on ((datastreams.data->>'name'),"));
            assertTrue("DISTINCT ON version selection must retain its semantic order",
                    sql.contains(" ORDER BY datastreams.data->>'name', " +
                            "(datastreams.data->'system@id'->'internalID'->'id')::bigint"));
        }
    }

    @Test
    public void testExactFoiIdsAvoidVersionMultiplyingJoin() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        builder.linkTo(foiStoreNamed("fois"));

        ObsFilter filter = new ObsFilter.Builder()
                .withFois(BigId.NONE, BigId.fromLong(1, 7))
                .build();
        ParameterizedQuery select = builder.createParameterizedSelectEntriesQuery(filter, null);
        ParameterizedQuery count = builder.createParameterizedSelectEntriesCountQuery(filter);

        for (ParameterizedQuery query : List.of(select, count)) {
            assertFalse("Exact FOI IDs must not join temporal FOI versions",
                    query.sql().contains("JOIN fois"));
            assertTrue(query.sql().contains("(obs.foiID IS NULL OR obs.foiID = (?))"));
            assertEquals(List.of(7L), query.parameters());
        }
    }

    private static IDataStreamStore dataStreamStoreNamed(String tableName) {
        return (IDataStreamStore)Proxy.newProxyInstance(
                IDataStreamStore.class.getClassLoader(),
                new Class<?>[]{IDataStreamStore.class},
                (proxy, method, arguments) -> {
                    if ("getDatastoreName".equals(method.getName()))
                        return tableName;
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static IFoiStore foiStoreNamed(String tableName) {
        return (IFoiStore)Proxy.newProxyInstance(
                IFoiStore.class.getClassLoader(),
                new Class<?>[]{IFoiStore.class},
                (proxy, method, arguments) -> {
                    if ("getDatastoreName".equals(method.getName()))
                        return tableName;
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    @Test
    public void testObsEventFilterIndexesMatchGeneratedPredicates() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        String indexes = String.join("\n", builder.createEventFilterIndexQueries());

        assertTrue(indexes.contains("USING GIN(result jsonb_path_ops)"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'occupancyCount')::numeric))"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'maxGamma')::numeric))"));
        assertTrue(indexes.contains("(dataStreamID, ((result->>'maxNeutron')::numeric))"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'occupancyCount') = 'number'"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'maxGamma') = 'number'"));
        assertTrue(indexes.contains("WHERE jsonb_typeof(result->'maxNeutron') = 'number'"));
        assertTrue(indexes.contains("_non_alarm_ptime_desc_idx"));
        assertTrue(indexes.contains("WHERE (result @> '{\"gammaAlarm\": false}'::jsonb AND result @> '{\"neutronAlarm\": false}'::jsonb)"));
        assertTrue(Arrays.stream(builder.createEventFilterIndexQueries())
                .allMatch(query -> query.startsWith("CREATE INDEX IF NOT EXISTS")));

        String alarmIdx = builder.createTieredUnadjudicatedAlarmIndexQuery();
        assertTrue(alarmIdx.startsWith("CREATE INDEX IF NOT EXISTS obs_unadjudicated_alarm_idx ON obs"));
        assertTrue(alarmIdx.contains("INCLUDE (archivegenerationid)"));
        assertTrue(alarmIdx.contains("\"adjudicatedIdsCount\": 0"));
    }

    @Test
    public void testObservationInsertUsesStableIdempotentNaturalKeyUpsert() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");

        for (String query : Arrays.asList(builder.insertObsQuery(), builder.insertObsPreparedQuery())) {
            assertTrue(query.startsWith("INSERT INTO obs"));
            assertTrue(query.contains("ON CONFLICT (dataStreamID, COALESCE(foiID, -1::bigint)"));
            assertTrue(query.contains("DO UPDATE SET result=EXCLUDED.result"));
            assertTrue(query.endsWith("WHERE obs.result IS DISTINCT FROM EXCLUDED.result"));
            assertFalse(query.contains("SET id="));
        }

        String naturalKeyLookup = builder.findByUniqueFieldsQuery();
        assertTrue(naturalKeyLookup.contains("dataStreamID = ?"));
        assertTrue(naturalKeyLookup.contains(
                "COALESCE(foiID, -1::bigint) = COALESCE(?::bigint, -1::bigint)"));
        assertTrue(naturalKeyLookup.contains(
                "COALESCE(phenomenonTime, '-infinity'::timestamp) = " +
                        "COALESCE(?::timestamp, '-infinity'::timestamp)"));
        assertTrue(naturalKeyLookup.contains(
                "COALESCE(resultTime, '-infinity'::timestamp) = " +
                        "COALESCE(?::timestamp, '-infinity'::timestamp)"));
        assertTrue(naturalKeyLookup.endsWith("LIMIT 1"));
        assertFalse(naturalKeyLookup.contains("IS NOT DISTINCT FROM"));

        String resultLookup = builder.findResultByUniqueFieldsQuery();
        assertTrue(resultLookup.startsWith("SELECT id, result::text FROM obs WHERE "));
        assertEquals(naturalKeyLookup.substring(naturalKeyLookup.indexOf(" WHERE ")),
                resultLookup.substring(resultLookup.indexOf(" WHERE ")));
    }

    @Test
    public void testLatestResultsForExplicitDataStreamsUseParameterizedLateralProbes() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(
                        BigId.fromLong(2, 101),
                        BigId.fromLong(2, 202),
                        BigId.fromLong(2, 303))
                .withLatestResult()
                .withLimit(2)
                .build();

        assertTrue(builder.supportsLatestResultPerDataStreamQuery(filter));

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(
                filter,
                Set.of(IObsStore.ObsField.PHENOMENON_TIME, IObsStore.ObsField.RESULT));

        assertTrue(query.sql().contains("unnest(ARRAY[?,?,?]::bigint[])"));
        assertTrue(query.sql().contains("CROSS JOIN LATERAL"));
        assertTrue(query.sql().contains("non_null.dataStreamID = requested.dataStreamID"));
        assertTrue(query.sql().contains("null_only.dataStreamID = requested.dataStreamID"));
        assertTrue(query.sql().contains("non_null.resultTime IS NOT NULL ORDER BY non_null.resultTime DESC, non_null.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().contains("null_only.resultTime IS NULL ORDER BY null_only.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().contains("ORDER BY candidate.resultTime DESC NULLS LAST, candidate.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().endsWith("ORDER BY latest.phenomenonTime ASC NULLS FIRST, latest.id ASC LIMIT 2"));
        assertTrue(query.sql().startsWith("SELECT latest.phenomenonTime,latest.result,latest.id"));
        assertEquals(Arrays.asList(101L, 202L, 303L), query.parameters());
    }

    @Test
    public void testLatestResultOptimizationFallsBackForBroaderFilters() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        BigId dataStreamId = BigId.fromLong(2, 101);

        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(dataStreamId)
                .build()));
        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(new DataStreamFilter.Builder()
                        .withOutputNames("connectionStatus")
                        .build())
                .withLatestResult()
                .build()));
        assertFalse(builder.supportsLatestResultPerDataStreamQuery(new ObsFilter.Builder()
                .withDataStreams(dataStreamId)
                .withLatestResult()
                .withCQLFilter("isConnected = true")
                .build()));
    }

    @Test
    public void testLatestResultQueryLeavesUnboundedPagingToIterator() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(2, 101), BigId.fromLong(2, 202))
                .withLatestResult()
                .build();

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(filter, null);

        assertTrue(query.sql().contains("FETCH FIRST 1 ROW ONLY"));
        assertFalse(query.sql().contains(" LIMIT "));
    }

    @Test
    public void testLatestResultQueryDeduplicatesLocalIdsAcrossScopes() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(
                        BigId.fromLong(1, 101),
                        BigId.fromLong(2, 101),
                        BigId.fromLong(2, 202))
                .withLatestResult()
                .build();

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(filter, null);

        assertTrue(query.sql().contains("unnest(ARRAY[?,?]::bigint[])"));
        assertEquals(Arrays.asList(101L, 202L), query.parameters());
    }

    @Test
    public void testLatestPhenomenonPerDataStreamUsesBoundedLateralProbe() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(2, 101), BigId.fromLong(2, 202))
                .withPhenomenonTime()
                .withLatestTime()
                .descendingOrder(true)
                .done()
                .build();

        assertTrue(builder.supportsLatestResultPerDataStreamQuery(filter));

        ParameterizedQuery query = builder.createParameterizedLatestResultPerDataStreamQuery(filter, null);
        assertTrue(query.sql().contains("non_null.phenomenonTime IS NOT NULL ORDER BY non_null.phenomenonTime DESC, non_null.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().contains("null_only.phenomenonTime IS NULL ORDER BY null_only.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().contains("ORDER BY candidate.phenomenonTime DESC NULLS LAST, candidate.id DESC FETCH FIRST 1 ROW ONLY"));
        assertTrue(query.sql().endsWith("ORDER BY latest.phenomenonTime DESC NULLS FIRST, latest.id ASC"));
        assertEquals(Arrays.asList(101L, 202L), query.parameters());
    }

    @Test
    public void testDataStreamSelectKeysQueryProjectsOnlyId() {
        QueryBuilderDataStreamStore builder = new QueryBuilderDataStreamStore("obs_datastreams");
        DataStreamFilter filter = new DataStreamFilter.Builder()
                .withOutputNames("gammaCount")
                .build();

        ParameterizedQuery query = builder.createParameterizedSelectKeysQuery(filter);
        assertTrue(query.sql().startsWith("SELECT obs_datastreams.id FROM obs_datastreams"));
        assertFalse(query.sql().contains("obs_datastreams.*"));
        assertEquals(Arrays.asList("gammaCount"), query.parameters());
    }

    @Test
    public void testParentTableOnlyQueriesUseFromOnlyClauseAndConfigureAutovacuum() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter filter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(2, 101), BigId.fromLong(2, 202))
                .withCQLFilter("gammaAlarm = false AND neutronAlarm = false")
                .withLimit(15)
                .build();

        ParameterizedQuery selectOnly = builder.createParameterizedSelectEntriesQuery(filter, null, true);
        assertTrue(selectOnly.sql().contains(" FROM ONLY obs "));
        assertFalse(selectOnly.sql().contains(" FROM obs "));

        ParameterizedQuery unorderedOnly = builder.createParameterizedUnorderedSelectEntriesQuery(
                new ObsFilter.Builder().withDataStreams(BigId.fromLong(2, 101)).build(),
                Set.of(IObsStore.ObsField.PHENOMENON_TIME),
                true,
                "tableoid");
        assertTrue(unorderedOnly.sql().contains("obs.id,obs.tableoid FROM ONLY obs "));

        ParameterizedQuery countOnly = builder.createParameterizedSelectEntriesCountQuery(
                filter, true, "obs.archivegenerationid IS NULL");
        assertTrue(countOnly.sql().startsWith("SELECT COUNT(*) FROM ONLY obs WHERE "));
        assertTrue(countOnly.sql().contains("(obs.archivegenerationid IS NULL)"));

        ObsFilter latestFilter = new ObsFilter.Builder()
                .withDataStreams(BigId.fromLong(2, 101), BigId.fromLong(2, 202))
                .withLatestResult()
                .build();
        ParameterizedQuery latestOnly = builder.createParameterizedLatestResultPerDataStreamQuery(
                latestFilter, null, true);
        assertTrue(latestOnly.sql().contains("FROM ONLY obs non_null"));
        assertTrue(latestOnly.sql().contains("FROM ONLY obs null_only"));

        String autovacuum = builder.configureAutovacuumQuery();
        assertTrue(autovacuum.startsWith("ALTER TABLE obs SET ("));
        assertTrue(autovacuum.contains("autovacuum_vacuum_insert_scale_factor = 0.01"));
        assertTrue(autovacuum.contains("autovacuum_vacuum_insert_threshold = 5000"));
    }

    @Test
    public void testStringArrayElementCqlMatchingForPostgisAndArchivedJson() {
        QueryBuilderObsStore builder = new QueryBuilderObsStore("obs");
        ObsFilter emlFilter = new ObsFilter.Builder()
                .withCQLFilter("adjudicatedIds = 'eml-non-threat'")
                .build();
        ParameterizedQuery emlQuery = builder.createParameterizedSelectEntriesQuery(emlFilter, null);
        assertTrue(emlQuery.sql().contains("(result @> ?::jsonb OR result @> ?::jsonb)"));
        assertEquals(List.of(
                "{\"adjudicatedIds\": \"eml-non-threat\"}",
                "{\"adjudicatedIds\": [\"eml-non-threat\"]}"
        ), emlQuery.parameters());

        String emlOccupancyJson = "{\"gammaAlarm\":true,\"adjudicatedIdsCount\":2,\"adjudicatedIds\":[\"eml-non-threat\",\"21c3abcd\"]}";
        String manualOccupancyJson = "{\"gammaAlarm\":true,\"adjudicatedIdsCount\":1,\"adjudicatedIds\":[\"21c3abcd\"]}";
        String unadjudicatedJson = "{\"gammaAlarm\":true,\"adjudicatedIdsCount\":0}";

        assertTrue(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                emlFilter.getCQLFilter(), emlOccupancyJson));
        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                emlFilter.getCQLFilter(), manualOccupancyJson));
        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                emlFilter.getCQLFilter(), unadjudicatedJson));
        assertTrue(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.hasSelectiveDuckDbPrefilter(
                emlFilter.getCQLFilter()));

        ObsFilter notEmlFilter = new ObsFilter.Builder()
                .withCQLFilter("adjudicatedIdsCount > 0 AND NOT (adjudicatedIds = 'eml-non-threat')")
                .build();
        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                notEmlFilter.getCQLFilter(), emlOccupancyJson));
        assertTrue(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                notEmlFilter.getCQLFilter(), manualOccupancyJson));
        assertFalse(org.sensorhub.impl.datastore.postgis.builder.filter.JsonCqlEvaluator.matches(
                notEmlFilter.getCQLFilter(), unadjudicatedJson));
    }

    private void assertSqlDoesNotContainInjection(ParameterizedQuery query) {
        assertFalse(query.sql().contains(INJECTION));
        assertTrue(query.parameters().stream().anyMatch(parameter -> parameter.toString().contains(INJECTION)));
    }
}
