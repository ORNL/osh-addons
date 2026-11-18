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

import org.sensorhub.api.datastore.obs.DataStreamFilter;
import org.sensorhub.api.datastore.obs.IDataStreamStore;
import org.sensorhub.impl.datastore.postgis.builder.query.datastream.SelectEntriesDataStreamQuery;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;

import java.time.Instant;
import java.util.Set;

public class QueryBuilderDataStreamStore extends QueryBuilder {
    public final static String DATASTREAM_TABLE_NAME = "datastreams";

    public QueryBuilderDataStreamStore() {
        this(DATASTREAM_TABLE_NAME);
    }

    public QueryBuilderDataStreamStore(String tableName) {
        super(tableName);
    }

    public String createTableQuery() {
        return "CREATE TABLE " + this.getStoreTableName() + " " +
                "(id BIGSERIAL PRIMARY KEY," +
                "min_phenomenon_time TIMESTAMP," +
                "max_phenomenon_time TIMESTAMP," +
                "min_result_time TIMESTAMP," +
                "max_result_time TIMESTAMP," +
                "data JSONB)";
    }

    public String createIndexQuery() {
        return "CREATE INDEX " + this.getStoreTableName() + "_data_idx on " + this.getStoreTableName() + " USING GIN(data) WITH (fastupdate = off)";
    }

    public String createUniqueIndexQuery() {
        return "CREATE UNIQUE INDEX " + this.getStoreTableName() + "_data_output_idx ON " + this.getStoreTableName()
                + " USING BTREE((data->'name'), (data->'system@id'), (data->'validTime'))";
    }

    public String createDateRangeIndexQuery() {
        return "CREATE INDEX "+this.getStoreTableName()+"_date_range_idx ON " + this.getStoreTableName() +
                " USING gist (int8range( " +
                "(data->'validTime'->'begin')::bigint, " +
                "(data->'validTime'->'end')::bigint" +
                ") )";
    }

    public String createValidTimeBeginIndexQuery() {
        return "CREATE INDEX "+this.getStoreTableName()+"_date_range_begin_idx ON " + this.getStoreTableName() + " USING GIN((data->'validTime'->'begin')) WITH (fastupdate = off)";
    }

    public String createValidTimeEndIndexQuery() {
        return "CREATE INDEX "+this.getStoreTableName()+"_date_range_end_idx ON " + this.getStoreTableName() + " USING GIN((data->'validTime'->'end')) WITH (fastupdate = off)";
    }

    public String createTrigramExtensionQuery() {
        return "CREATE EXTENSION IF NOT EXISTS pg_trgm";
    }

    public String createTrigramDescriptionFullTextIndexQuery() {
        return "CREATE INDEX "+this.getStoreTableName()+"_desc_full_text_datastream_idx ON  " + this.getStoreTableName() + " USING GIN ((data->'recordSchema'->>'description') gin_trgm_ops) WITH (fastupdate = off)";
    }

    //SELECT * from test_obs_datastreams where to_tsvector(data->'recordStruct'->'description') @@ to_tsquery('video | Air');
    public String createDescriptionFullTextIndexQuery() {
        return "CREATE INDEX "+this.getStoreTableName()+"_desc_full_text_datastream_idx ON " + this.getStoreTableName() + " USING " +
                "GIN (to_tsvector('english', data->'recordSchema'->'description')) WITH (fastupdate = off)";
    }

    public String createSchemaFieldsIndexQuery() {
        return "CREATE INDEX " + this.getStoreTableName() + "_fields_gin_idx ON " + this.getStoreTableName() + " USING GIN ((data->'recordSchema'->'fields')) WITH (fastupdate = off)";
    }

    public String updateByIdQuery() {
        return "UPDATE " + this.getStoreTableName() + " SET data = ? WHERE id = ?";
    }

    public String updateValidTimeByIdQuery() {
        return "UPDATE " + this.getStoreTableName() + " SET data = jsonb_set(data, '{validTime}'::text[], to_jsonb(?), true) WHERE id = ?";
    }

    public String insertInfoQuery() {
        return "INSERT INTO " + this.getStoreTableName() + " (data) SELECT (?) ";
    }

    public String getAllDataStreams(Instant min, Instant max) {
        return getAllDataStreams();
    }

    public String getAllDataStreams() {
        return "SELECT id FROM "+this.getStoreTableName()+" WHERE tsrange((data->'validTime'->>'begin')::timestamp," +
                "(data->'validTime'->>'end')::timestamp) <@ ?::tsrange";
    }

    public String createMinMaxTimeTriggerQuery() {
        // Trigger functions are schema-wide. Give each observation store its
        // own function so starting a second database cannot silently redirect
        // range maintenance to the first store's datastream table.
        String functionName = "update_datastream_ranges_" +
                Integer.toUnsignedString(obsStore.getDatastoreName().hashCode(), 16);
        return "ALTER TABLE " + this.getStoreTableName() + " SET (" +
                "fillfactor = 50, " +
                "toast_tuple_target = 128, " +
                "autovacuum_vacuum_scale_factor = 0.0, " +
                "autovacuum_vacuum_threshold = 100, " +
                "autovacuum_analyze_scale_factor = 0.0, " +
                "autovacuum_analyze_threshold = 100);\n" +
                "ALTER INDEX IF EXISTS " + this.getStoreTableName() + "_data_idx SET (fastupdate = off);\n" +
                "ALTER INDEX IF EXISTS " + this.getStoreTableName() + "_date_range_begin_idx SET (fastupdate = off);\n" +
                "ALTER INDEX IF EXISTS " + this.getStoreTableName() + "_date_range_end_idx SET (fastupdate = off);\n" +
                "ALTER INDEX IF EXISTS " + this.getStoreTableName() + "_desc_full_text_datastream_idx SET (fastupdate = off);\n" +
                "ALTER INDEX IF EXISTS " + this.getStoreTableName() + "_fields_gin_idx SET (fastupdate = off);\n" +
                "DROP TRIGGER IF EXISTS obs_update_datastream_ranges ON " + obsStore.getDatastoreName() + ";\n" +
                "CREATE OR REPLACE FUNCTION " + functionName + "()\n" +
                "RETURNS TRIGGER AS $$\n" +
                "BEGIN\n" +
                "    IF NOT EXISTS (SELECT 1 FROM inserted_observations) THEN\n" +
                "        RETURN NULL;\n" +
                "    END IF;\n" +
                "    UPDATE " + this.getStoreTableName() + " target\n" +
                "    SET \n" +
                "        min_phenomenon_time = LEAST(COALESCE(target.min_phenomenon_time, ranges.min_phenomenon_time), ranges.min_phenomenon_time),\n" +
                "        max_phenomenon_time = GREATEST(COALESCE(target.max_phenomenon_time, ranges.max_phenomenon_time), ranges.max_phenomenon_time),\n" +
                "        min_result_time = LEAST(COALESCE(target.min_result_time, ranges.min_result_time), ranges.min_result_time),\n" +
                "        max_result_time = GREATEST(COALESCE(target.max_result_time, ranges.max_result_time), ranges.max_result_time)\n" +
                "    FROM (\n" +
                "        SELECT dataStreamID, MIN(phenomenonTime) min_phenomenon_time, MAX(phenomenonTime) max_phenomenon_time,\n" +
                "               MIN(resultTime) min_result_time, MAX(resultTime) max_result_time\n" +
                "        FROM inserted_observations GROUP BY dataStreamID\n" +
                "    ) ranges\n" +
                "    WHERE target.id = ranges.dataStreamID\n" +
                "      AND (\n" +
                "        (ranges.min_phenomenon_time IS NOT NULL AND (target.min_phenomenon_time IS NULL OR ranges.min_phenomenon_time < target.min_phenomenon_time))\n" +
                "        OR (ranges.max_phenomenon_time IS NOT NULL AND (target.max_phenomenon_time IS NULL OR ranges.max_phenomenon_time >= target.max_phenomenon_time + interval '60 seconds'))\n" +
                "        OR (ranges.min_result_time IS NOT NULL AND (target.min_result_time IS NULL OR ranges.min_result_time < target.min_result_time))\n" +
                "        OR (ranges.max_result_time IS NOT NULL AND (target.max_result_time IS NULL OR ranges.max_result_time >= target.max_result_time + interval '60 seconds'))\n" +
                "      );\n" +
                "    RETURN NULL;\n" +
                "END;\n" +
                "$$ LANGUAGE plpgsql;\n" +
                "\n" +
                "CREATE TRIGGER obs_update_datastream_ranges\n" +
                "AFTER INSERT ON " + obsStore.getDatastoreName() + "\n" +
                "REFERENCING NEW TABLE AS inserted_observations\n" +
                "FOR EACH STATEMENT\n" +
                "EXECUTE FUNCTION " + functionName + "()";
    }

    public String createIdIndex() {
        return "CREATE INDEX " + this.getStoreTableName() + "_id_idx ON " +
                this.getStoreTableName() + "(id)";
    }

    public String migrateLegacyDailyFileDataStreamSchemaQuery() {
        return "UPDATE " + this.getStoreTableName() + "\n" +
                "SET data = jsonb_set(\n" +
                "    jsonb_set(\n" +
                "        jsonb_set(\n" +
                "            data - 'validTime',\n" +
                "            '{recordSchema,fields,1,name}', '\"rpmMessage\"'::jsonb, false\n" +
                "        ),\n" +
                "        '{recordSchema,fields,1,label}', '\"RPM Message\"'::jsonb, false\n" +
                "    ),\n" +
                "    '{recordSchema,fields,1,definition}', '\"http://www.opengis.net/def/RPMMessage\"'::jsonb, false\n" +
                ")\n" +
                "WHERE data->>'outputName' = 'dailyFile'\n" +
                "  AND (data->'recordSchema'->'fields'->1->>'name') IN ('rapiscanMessage', 'aspectMessage')";
    }

    public String createSelectEntriesQuery(DataStreamFilter filter, Set<IDataStreamStore.DataStreamInfoField> fields) {
        return createParameterizedSelectEntriesQuery(filter, fields).sql();
    }

    public ParameterizedQuery createParameterizedSelectEntriesQuery(DataStreamFilter filter, Set<IDataStreamStore.DataStreamInfoField> fields) {
        SelectEntriesDataStreamQuery selectEntriesDataStreamQuery = new SelectEntriesDataStreamQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .withFields(fields)
                .withDataStreamFilter(filter)
                .build();
        return selectEntriesDataStreamQuery.toParameterizedQuery();
    }

    public ParameterizedQuery createParameterizedSelectKeysQuery(DataStreamFilter filter) {
        SelectEntriesDataStreamQuery selectEntriesDataStreamQuery = new SelectEntriesDataStreamQuery.Builder()
                .tableName(this.getStoreTableName())
                .linkTo(this.systemStore)
                .withOnlyId()
                .withDataStreamFilter(filter)
                .build();
        return selectEntriesDataStreamQuery.toParameterizedQuery();
    }

}
