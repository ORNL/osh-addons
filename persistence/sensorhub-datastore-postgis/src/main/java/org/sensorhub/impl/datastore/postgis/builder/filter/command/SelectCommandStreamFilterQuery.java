/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2023 Georobotix. All Rights Reserved.

 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.builder.filter.command;

import org.sensorhub.api.datastore.FullTextFilter;
import org.sensorhub.api.datastore.TemporalFilter;
import org.sensorhub.api.datastore.command.CommandFilter;
import org.sensorhub.api.datastore.command.CommandStreamFilter;
import org.sensorhub.api.datastore.system.SystemFilter;
import org.sensorhub.impl.datastore.postgis.builder.filter.FilterQuery;
import org.sensorhub.impl.datastore.postgis.builder.generator.SelectFilterQueryGenerator;
import org.sensorhub.impl.datastore.postgis.utils.PostgisUtils;
import org.vast.util.Asserts;

import java.util.SortedSet;
import java.util.stream.Collectors;

public class SelectCommandStreamFilterQuery extends FilterQuery<SelectFilterQueryGenerator> {

    public SelectCommandStreamFilterQuery(String tableName, SelectFilterQueryGenerator filterQueryGenerator) {
        super(tableName, filterQueryGenerator);
    }

    public SelectFilterQueryGenerator build(CommandStreamFilter filter) {
        this.handleInternalIDs(filter.getInternalIDs());
        this.handleControlInputNames(filter.getControlInputNames());
        this.handleValidTimeFilter(filter.getValidTimeFilter());
        this.handleFullTextFilter(filter.getFullTextFilter());
        this.handleCommandFilter(filter.getCommandFilter());
        this.handleSystemFilter(filter.getSystemFilter());
        this.handleTaskableProperties(filter.getTaskableProperties());
        return this.filterQueryGenerator;
    }

    protected  void handleControlInputNames(SortedSet<String> names) {
        if (names != null && !names.isEmpty()) {
            addCondition("("+this.tableName+".data->>'controlInputName') in (" + placeholders(names.size()) + ")");
            names.forEach(this::addParameter);
        }
    }

    protected void handleValidTimeFilter(TemporalFilter temporalFilter) {
        if (temporalFilter != null) {
            if (temporalFilter.isLatestTime()) {
                filterQueryGenerator.addDistinct("(" + tableName + ".data->>'name')");
                filterQueryGenerator.addDistinct("(" + tableName + ".data->'system@id'->'internalID'->'id')::bigint");
                filterQueryGenerator.addOrderBy(tableName + ".data->>'name'");
                filterQueryGenerator.addOrderBy("(" + tableName + ".data->'system@id'->'internalID'->'id')::bigint");
                filterQueryGenerator.addOrderBy("COALESCE((" + tableName + ".data->'validTime'->>'end')::timestamp, 'infinity'::timestamp) DESC");
                filterQueryGenerator.addOrderBy(tableName + ".id DESC");
            }
            else if (temporalFilter.isCurrentTime()) {
                filterQueryGenerator.addDistinct("(" + tableName + ".data->>'name')");
                filterQueryGenerator.addDistinct("(" + tableName + ".data->'system@id'->'internalID'->'id')::bigint");
                filterQueryGenerator.addOrderBy(tableName + ".data->>'name'");
                filterQueryGenerator.addOrderBy("(" + tableName + ".data->'system@id'->'internalID'->'id')::bigint");
                filterQueryGenerator.addOrderBy("(" + tableName + ".data->'validTime'->>'begin')::timestamptz DESC ");
                String sb = "(" +
                        tableName + ".data->'validTime' IS NULL " +
                        "OR (" +
                        tableName + ".data->'validTime'->'begin' IS NOT NULL " +
                        "AND (" + tableName + ".data->'validTime'->>'begin')::timestamptz <= now() " +
                        "AND ((" + tableName + ".data->'validTime'->>'end') IS NULL " +
                        "OR (" + tableName + ".data->'validTime'->>'end')::timestamptz >= now())" +
                        ")" +
                        ")";
                addCondition(sb);
            }
            else {
                String min = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMin());
                String max = PostgisUtils.checkAndGetValidInstant(temporalFilter.getMax());

                String sb = "(" +
                        tableName + ".data->'validTime' IS NULL " +
                        "OR tsrange((" +
                        tableName + ".data->'validTime'->>'begin')::timestamp, (" +
                        tableName + ".data->'validTime'->>'end')::timestamp) " +
                        PostgisUtils.getOperator(temporalFilter) + " " +
                        "?::tsrange" +
                        ")";
                addCondition(sb);
                addParameter("[" + min + "," + max + "]");
            }
        }
    }

    protected void handleFullTextFilter(FullTextFilter fullTextFilter) {
        if (fullTextFilter != null) {
            String sb = "(" + this.tableName + ".data->'recordSchema'->>'description') ~* ?";
            addCondition(sb);
            addParameter("(" + fullTextFilter.getKeywords().stream().collect(Collectors.joining("|")) + ")");
        }
    }

    protected void handleCommandFilter(CommandFilter commandFilter) {
        if(commandFilter != null) {
            // create join
            Asserts.checkNotNull(this.commandTableName, "commandTableName should not be null");

            this.filterQueryGenerator.addInnerJoin(this.commandTableName+" ON "+this.tableName+".id = "+this.commandTableName+".commandstreamid");
            SelectCommandFilterQuery commandFilterQuery = new SelectCommandFilterQuery(this.commandTableName, filterQueryGenerator);
            commandFilterQuery.setCommandStreamTableName(this.tableName);
            this.filterQueryGenerator = commandFilterQuery.build(commandFilter);
        }
    }

    protected void handleSystemFilter(SystemFilter systemFilter) {
        if(systemFilter != null) {
                // otherwise
            if (systemFilter.getInternalIDs() != null || systemFilter.getUniqueIDs() != null) {
                // handle UNIQUE IDS
                if (systemFilter.getUniqueIDs() != null && !systemFilter.getUniqueIDs().isEmpty()) {
                    String sb = "(" + tableName + ".data->'system@id'->>'uniqueID') in (" +
                            placeholders(systemFilter.getUniqueIDs().size()) + ")";
                    addCondition(sb);
                    systemFilter.getUniqueIDs().forEach(this::addParameter);
                }

                // handle internal IDS without temporal-version row multiplication from JOIN sys
                if (systemFilter.getInternalIDs() != null && !systemFilter.getInternalIDs().isEmpty()) {
                    StringBuilder sb = new StringBuilder("(");
                    String idPlaceholders = placeholders(systemFilter.getInternalIDs().size());

                    if (systemFilter.includeMembers() && sysDescTableName != null) {
                        sb.append("(").append(tableName)
                                .append(".data->'system@id'->'internalID'->>'id')::bigint IN (SELECT s.id FROM ")
                                .append(sysDescTableName).append(" s WHERE s.id IN (").append(idPlaceholders)
                                .append(") OR s.parentid IN (").append(idPlaceholders).append("))");
                        systemFilter.getInternalIDs().forEach(bigId -> addParameter(bigId.getIdAsLong()));
                        systemFilter.getInternalIDs().forEach(bigId -> addParameter(bigId.getIdAsLong()));
                    } else {
                        sb.append("(").append(tableName)
                                .append(".data->'system@id'->'internalID'->>'id')::bigint IN (")
                                .append(idPlaceholders).append(")");
                        systemFilter.getInternalIDs().forEach(bigId -> addParameter(bigId.getIdAsLong()));
                    }

                    sb.append(")");
                    addCondition(sb.toString());
                }
            }
            if (systemFilter.getParentFilter() != null || systemFilter.getProcedureFilter() != null
                    || systemFilter.getDataStreamFilter() != null || systemFilter.getFullTextFilter() != null
                    || systemFilter.getLocationFilter() != null || systemFilter.getValidTime() != null) {
                throw new IllegalStateException("No linked system store");
            }
        }
    }

    protected void handleTaskableProperties(SortedSet<String> properties) {
        if(properties != null) {
            StringBuilder jsonPath = new StringBuilder();
            jsonPath.append("$.** ? (");
            boolean first=true;
            for(String property : properties) {
                if(!first) {
                    // prepend operator
                    jsonPath.append(" || ");
                }
                jsonPath.append("@ == \"").append(escapeJsonString(property)).append("\"");
                first = false;
            }
            jsonPath.append(")");
            addCondition("jsonb_path_exists(" + tableName + ".data, ?::jsonpath)");
            addParameter(jsonPath.toString());
        }
    }

    private String escapeJsonString(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
