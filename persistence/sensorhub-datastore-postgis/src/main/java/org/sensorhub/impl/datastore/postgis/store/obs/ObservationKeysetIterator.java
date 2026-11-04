/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.

 Copyright (C) 2026 Botts Innovative Research, Inc. All Rights Reserved.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.obs;

import org.postgresql.PGStatement;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Bounded observation iterator using a stable {@code (phenomenontime,id)}
 * continuation instead of OFFSET. Each page releases its connection before
 * returning data so a tiered hot/cold merge also works with a one-connection
 * pool.
 */
final class ObservationKeysetIterator<T> implements Iterator<T>, AutoCloseable {
    private final ParameterizedQuery query;
    private final ConnectionManager connections;
    private final int maxPageSize;
    private int currentPageSize;
    private final long resultLimit;
    private final boolean descending;
    private final Function<ResultSet, T> parser;
    private final Predicate<T> predicate;
    private final boolean predicateRequiresPostFiltering;
    private final ArrayDeque<T> ready;

    private Instant lastPhenomenonTime;
    private long lastId;
    private boolean hasContinuation;
    private boolean exhausted;
    private boolean closed;
    private long accepted;

    ObservationKeysetIterator(ParameterizedQuery query,
                              ConnectionManager connections,
                              int pageSize,
                              long resultLimit,
                              boolean descending,
                              Function<ResultSet, T> parser,
                              Predicate<T> predicate,
                              boolean predicateRequiresPostFiltering) {
        this(query, connections, pageSize, pageSize, resultLimit, descending,
                parser, predicate, predicateRequiresPostFiltering);
    }

    ObservationKeysetIterator(ParameterizedQuery query,
                              ConnectionManager connections,
                              int initialPageSize,
                              int maxPageSize,
                              long resultLimit,
                              boolean descending,
                              Function<ResultSet, T> parser,
                              Predicate<T> predicate,
                              boolean predicateRequiresPostFiltering) {
        if (initialPageSize <= 0 || maxPageSize <= 0)
            throw new IllegalArgumentException("Observation page size must be positive");
        if (resultLimit < 0)
            throw new IllegalArgumentException("Observation result limit cannot be negative");
        this.query = query;
        this.connections = connections;
        this.maxPageSize = maxPageSize;
        this.currentPageSize = Math.min(initialPageSize, maxPageSize);
        this.resultLimit = resultLimit;
        this.descending = descending;
        this.parser = parser;
        this.predicate = predicate;
        this.predicateRequiresPostFiltering = predicateRequiresPostFiltering;
        this.ready = new ArrayDeque<>(Math.min(this.currentPageSize, 1_024));
        this.exhausted = resultLimit == 0;
    }

    @Override
    public boolean hasNext() {
        if (!ready.isEmpty())
            return true;
        if (closed || exhausted)
            return false;
        while (ready.isEmpty() && !exhausted)
            fetchPage();
        return !ready.isEmpty();
    }

    @Override
    public T next() {
        if (!hasNext())
            throw new NoSuchElementException();
        return ready.removeFirst();
    }

    private void fetchPage() {
        long remaining = resultLimit == Long.MAX_VALUE ? Long.MAX_VALUE : resultLimit - accepted;
        if (remaining <= 0) {
            exhausted = true;
            return;
        }
        // A SQL-safe query can stop at the exact caller bound. A Java value
        // predicate must continue reading raw rows until enough matches have
        // been accepted or the source is exhausted.
        int pageTarget = currentPageSize;
        int requested = predicateRequiresPostFiltering
                ? pageTarget
                : (int) Math.min(pageTarget, remaining);
        String pageSql = pageSql(query.sql(), hasContinuation, lastPhenomenonTime,
                descending);
        int fetched = 0;
        try (Connection connection = connections.getConnection();
             PreparedStatement statement = connection.prepareStatement(pageSql)) {
            connection.setReadOnly(true);
            disableGenericPlanPromotion(statement);
            query.bind(statement);
            int parameter = query.parameters().size() + 1;
            if (hasContinuation) {
                if (lastPhenomenonTime == null) {
                    statement.setLong(parameter++, lastId);
                } else {
                    UtcJdbcTime.setInstant(statement, parameter++, lastPhenomenonTime);
                    UtcJdbcTime.setInstant(statement, parameter++, lastPhenomenonTime);
                    statement.setLong(parameter++, lastId);
                }
            }
            statement.setInt(parameter, requested);
            try (ResultSet result = statement.executeQuery()) {
                while (result.next()) {
                    fetched++;
                    lastPhenomenonTime = UtcJdbcTime.getInstant(result, "phenomenontime");
                    lastId = result.getLong("id");
                    hasContinuation = true;
                    T value = parser.apply(result);
                    if (predicate.test(value)) {
                        ready.addLast(value);
                        accepted++;
                        if (accepted >= resultLimit)
                            break;
                    }
                }
            }
        } catch (SQLException error) {
            throw new IllegalStateException("Cannot read keyset observation page", error);
        }
        if (fetched < requested || accepted >= resultLimit) {
            exhausted = true;
        } else if (currentPageSize < maxPageSize) {
            currentPageSize = (int) Math.min((long) maxPageSize, (long) currentPageSize * 2L);
        }
    }

    /**
     * Keyset query shapes are reused with very different stream-ID sets and time windows. After
     * pgJDBC's normal threshold PostgreSQL may otherwise choose one generic server plan, which can
     * turn a selective retention page into a global observation-time-index scan. Keep only these
     * variable read pages unnamed; ordinary ingest and stable queries retain server preparation
     * and binary transfer.
     */
    static void disableGenericPlanPromotion(PreparedStatement statement) throws SQLException {
        statement.unwrap(PGStatement.class).setPrepareThreshold(0);
    }

    static String pageSql(String baseSql, boolean hasContinuation,
                          Instant lastPhenomenonTime, boolean descending) {
        StringBuilder sql = new StringBuilder("SELECT page.* FROM (")
                .append(baseSql)
                .append(") page");
        if (hasContinuation) {
            if (lastPhenomenonTime == null) {
                sql.append(" WHERE ((page.phenomenontime IS NULL AND page.id>?) ")
                        .append("OR page.phenomenontime IS NOT NULL)");
            } else {
                sql.append(" WHERE page.phenomenontime IS NOT NULL AND (")
                        .append("page.phenomenontime")
                        .append(descending ? "<?" : ">?")
                        .append(" OR (page.phenomenontime=? AND page.id>?))");
            }
        }
        return sql.append(" ORDER BY page.phenomenontime ")
                .append(descending ? "DESC" : "ASC")
                .append(" NULLS FIRST, page.id ASC LIMIT ?")
                .toString();
    }

    @Override
    public void close() {
        closed = true;
        ready.clear();
    }
}
