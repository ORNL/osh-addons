/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.obs;

import org.junit.Assume;
import org.junit.Test;
import org.postgresql.PGStatement;
import org.sensorhub.impl.datastore.postgis.archive.UtcJdbcTime;
import org.sensorhub.impl.datastore.postgis.builder.ParameterizedQuery;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestObservationKeysetIterator {
    private static final String BASE = "SELECT obs.phenomenontime,obs.id FROM obs WHERE datastreamid=?";

    @Test
    public void firstPageHasStableAscendingOrderAndNoOffset() {
        String sql = ObservationKeysetIterator.pageSql(BASE, false, null, false);

        assertEquals("SELECT page.* FROM (" + BASE + ") page " +
                "ORDER BY page.phenomenontime ASC NULLS FIRST, page.id ASC LIMIT ?", sql);
        assertFalse(sql.contains("OFFSET"));
    }

    @Test
    public void nullTimeContinuationFinishesNullsThenMovesToTimedRows() {
        String sql = ObservationKeysetIterator.pageSql(BASE, true, null, false);

        assertTrue(sql.contains("page.phenomenontime IS NULL AND page.id>?"));
        assertTrue(sql.contains("OR page.phenomenontime IS NOT NULL"));
        assertFalse(sql.contains("OFFSET"));
    }

    @Test
    public void timedContinuationUsesDirectionAndStableIdTieBreak() {
        Instant cursor = Instant.parse("2026-01-01T00:00:00Z");
        String ascending = ObservationKeysetIterator.pageSql(BASE, true, cursor, false);
        String descending = ObservationKeysetIterator.pageSql(BASE, true, cursor, true);

        assertTrue(ascending.contains("page.phenomenontime>?"));
        assertTrue(descending.contains("page.phenomenontime<?"));
        assertTrue(ascending.contains("page.phenomenontime=? AND page.id>?"));
        assertTrue(descending.endsWith(
                "ORDER BY page.phenomenontime DESC NULLS FIRST, page.id ASC LIMIT ?"));
    }

    @Test
    public void variableKeysetPagesDisableServerPreparePromotion() throws Exception {
        AtomicInteger threshold = new AtomicInteger(Integer.MIN_VALUE);
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                TestObservationKeysetIterator.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class, PGStatement.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("unwrap")) {
                        assertEquals(PGStatement.class, args[0]);
                        return proxy;
                    }
                    if (method.getName().equals("setPrepareThreshold")) {
                        threshold.set((Integer) args[0]);
                        return null;
                    }
                    throw new AssertionError("Unexpected proxy method: " + method.getName());
                });

        ObservationKeysetIterator.disableGenericPlanPromotion(statement);

        assertEquals(0, threshold.get());
    }

    @Test
    public void minResultTimeBoundExtractsConcreteLowerBoundsAndSkipsLatestOrUnbounded() {
        Instant lower = Instant.parse("2026-11-04T01:04:36.850Z");
        Instant upper = Instant.parse("2026-11-04T01:04:45.883Z");

        org.junit.Assert.assertNull(OscarTieredObsStore.minResultTimeBound(null));
        org.junit.Assert.assertNull(OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withPhenomenonTimeDuring(lower, upper)
                        .build()));
        org.junit.Assert.assertNull(OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withLatestResult()
                        .build()));
        org.junit.Assert.assertNull(OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withResultTime(new org.sensorhub.api.datastore.TemporalFilter.Builder()
                                .withAllTimes()
                                .build())
                        .build()));
        org.junit.Assert.assertNull(OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withResultTime(new org.sensorhub.api.datastore.TemporalFilter.Builder()
                                .withRange(Instant.MIN, upper)
                                .build())
                        .build()));

        assertEquals(lower, OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withResultTimeDuring(lower, upper)
                        .build()));
        assertEquals(lower, OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withResultTime(new org.sensorhub.api.datastore.TemporalFilter.Builder()
                                .withSingleValue(lower)
                                .build())
                        .build()));
        org.junit.Assert.assertNotNull(OscarTieredObsStore.minResultTimeBound(
                new org.sensorhub.api.datastore.obs.ObsFilter.Builder()
                        .withResultTime(new org.sensorhub.api.datastore.TemporalFilter.Builder()
                                .withCurrentTime()
                                .build())
                        .build()));
    }

    @Test
    public void sparsePostFilterCrossesTwentyThousandEqualAndNullTimestampsBothDirections()
            throws Exception {
        Assume.assumeTrue("OSCAR_IT_ARCHIVE_DIR enables the disposable PostgreSQL probe",
                System.getenv("OSCAR_IT_ARCHIVE_DIR") != null);
        String table = "codex_keyset_it_" + UUID.randomUUID().toString().replace("-", "");
        int rowCount = 20_005;
        long equalIdBase = 100_000L;
        long equalFoiBase = 200_000L;
        long nullIdBase = 300_000L;
        long nullFoiBase = 400_000L;
        Instant equalTime = Instant.parse("2026-02-01T00:00:00Z");

        try (Connection setup = directConnection(); Statement statement = setup.createStatement()) {
            statement.execute("CREATE TABLE " + table +
                    " (id bigint PRIMARY KEY, foiid bigint NOT NULL, phenomenontime timestamp)");
            insertRows(setup, table, equalIdBase, equalFoiBase, equalTime, rowCount);
            insertRows(setup, table, nullIdBase, nullFoiBase, null, rowCount);
        }

        ConnectionManager manager = new ConnectionManager(
                "localhost:5432", "gis", "postgres", "postgres");
        try {
            for (boolean descending : List.of(false, true)) {
                assertSparseRows(manager, table, "phenomenontime IS NOT NULL", descending,
                        Set.of(equalFoiBase + 1, equalFoiBase + rowCount),
                        List.of(equalIdBase + 1, equalIdBase + rowCount));
                assertSparseRows(manager, table, "phenomenontime IS NULL", descending,
                        Set.of(nullFoiBase + 1, nullFoiBase + rowCount),
                        List.of(nullIdBase + 1, nullIdBase + rowCount));
            }
        } finally {
            manager.close();
            try (Connection cleanup = directConnection(); Statement statement = cleanup.createStatement()) {
                statement.execute("DROP TABLE IF EXISTS " + table);
            }
        }
    }

    private static void insertRows(Connection connection, String table, long idBase, long foiBase,
                                   Instant phenomenonTime, int rowCount) throws Exception {
        String sql = "INSERT INTO " + table +
                " SELECT ? + series, ? + series, ? FROM generate_series(1, ?) series";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setLong(1, idBase);
            statement.setLong(2, foiBase);
            if (phenomenonTime == null)
                statement.setTimestamp(3, null);
            else
                UtcJdbcTime.setInstant(statement, 3, phenomenonTime);
            statement.setInt(4, rowCount);
            assertEquals(rowCount, statement.executeUpdate());
        }
    }

    private static void assertSparseRows(ConnectionManager manager, String table, String condition,
                                         boolean descending, Set<Long> matchingFois,
                                         List<Long> expectedIds) {
        ParameterizedQuery query = new ParameterizedQuery(
                "SELECT id,foiid,phenomenontime FROM " + table + " WHERE " + condition,
                List.of());
        List<Long> actualIds = new ArrayList<>();
        try (ObservationKeysetIterator<Row> rows = new ObservationKeysetIterator<>(
                query, manager, 20_000, 2, descending,
                TestObservationKeysetIterator::readRow,
                row -> matchingFois.contains(row.foiId()), true)) {
            while (rows.hasNext())
                actualIds.add(rows.next().id());
        }
        assertEquals(expectedIds, actualIds);
    }

    private static Row readRow(ResultSet result) {
        try {
            return new Row(result.getLong("id"), result.getLong("foiid"),
                    UtcJdbcTime.getInstant(result, "phenomenontime"));
        } catch (SQLException error) {
            throw new IllegalStateException(error);
        }
    }

    private static Connection directConnection() throws SQLException {
        return DriverManager.getConnection(
                "jdbc:postgresql://localhost:5432/gis", "postgres", "postgres");
    }

    private record Row(long id, long foiId, Instant phenomenonTime) {}
}
