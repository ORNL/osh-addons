package org.sensorhub.impl.datastore.postgis.connection;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ConnectionManager {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    public enum BatchStatus {
        SUCCESS,        // code >= 0
        SUCCESS_UNKNOWN, // code == -2
        FAILED           // code == -3
    }

    private static final int DRIVER_LOGIN_TIMEOUT_SECONDS = 5 * 60;
    private static final int MAX_POOL_SIZE = 8;
    private static final int MIN_IDLE_CONNECTIONS = 0;
    private static final long CONNECTION_TIMEOUT_MS = 30_000L;
    private static final long MAX_LIFETIME_MS = 15 * 60 * 1000L;
    private static final long KEEPALIVE_TIME_MS = 5 * 60 * 1000L;
    private static final long IDLE_TIMEOUT_MS = 2 * 60 * 1000L;

    private final String url;
    private final String dbName;
    private final String login;
    private final String password;
    private final HikariDataSource hikariDataSourceInstance;
    private int batchSize = 100;
    protected final ConcurrentLinkedDeque<String> batchList = new ConcurrentLinkedDeque<>();
    protected final ReentrantLock transactionLock = new ReentrantLock();

    /**
     * Use separate ThreadSafeBatchExecutor to execute batch queries.
     *
     * @param url database host:port or equivalent connection target
     * @param dbName database name
     * @param login database username
     * @param password database password
     */
    public ConnectionManager(String url, String dbName, String login, String password) {
        this.url = url;
        this.dbName = dbName;
        this.login = login;
        this.password = password;
        DriverManager.setLoginTimeout(DRIVER_LOGIN_TIMEOUT_SECONDS);
        this.hikariDataSourceInstance = createHikariDataSource();
    }

    public void enableBatch(int batchSize) {
        this.batchSize = batchSize;
    }

    /** Runtime pool bound used by constrained deployments and deadlock probes. */
    public void setMaximumPoolSize(int maximumPoolSize) {
        if (maximumPoolSize < 1)
            throw new IllegalArgumentException("PostgreSQL pool size must be positive");
        hikariDataSourceInstance.setMaximumPoolSize(maximumPoolSize);
    }

    private HikariDataSource createHikariDataSource() {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl("jdbc:postgresql://" + url + "/" + dbName);
        config.setUsername(login);
        config.setPassword(password);

        // Reduce steady-state idle connection footprint across multiple pools.
        config.setMaximumPoolSize(MAX_POOL_SIZE);
        config.setMinimumIdle(MIN_IDLE_CONNECTIONS);
        config.setConnectionTimeout(CONNECTION_TIMEOUT_MS);
        config.setMaxLifetime(MAX_LIFETIME_MS);
        config.setKeepaliveTime(KEEPALIVE_TIME_MS);
        config.setIdleTimeout(IDLE_TIMEOUT_MS);

        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        // Allow the PostgreSQL driver to turn a PreparedStatement batch into
        // a bounded multi-values INSERT.  Besides reducing protocol overhead,
        // this lets statement-level range maintenance run once per batch
        // instead of once per observation.
        config.addDataSourceProperty("reWriteBatchedInserts", "true");
        config.addDataSourceProperty("tcpKeepAlive", "true");
        config.addDataSourceProperty("socketTimeout", "300");
        config.addDataSourceProperty("networkTimeout", "60");
        // Bounds only PostgreSQL's separate query-cancellation signal. It does
        // not impose a statement timeout on normal application queries.
        config.addDataSourceProperty("cancelSignalTimeout", "2");
        config.setAutoCommit(true);

        return new HikariDataSource(config);
    }

    public Connection getConnection() {
        try {
            Connection connection = hikariDataSourceInstance.getConnection();
            return CancellableConnectionScope.track(
                    connection, () -> hikariDataSourceInstance.evictConnection(connection));
        } catch (CancellationException ex) {
            throw ex;
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
    }

    /**
     * Opens a thread-bound operation that can abort only the PostgreSQL
     * connections borrowed by that operation. The static scope deliberately
     * spans connection-manager instances so linked observation and metadata
     * stores participate in one logical cancellation boundary.
     */
    public static ICancellableDataStore.CancellableOperation beginCancellableOperation() {
        return CancellableConnectionScope.open();
    }

    /**
     * Adds a private non-PostgreSQL JDBC statement to the current cancellable
     * datastore operation. With no active scope the original statement is
     * returned unchanged.
     */
    public static <S extends Statement> S trackCancellableStatement(
            S statement, String description) {
        return CancellableConnectionScope.trackStandaloneStatement(statement, description);
    }

    /**
     * Throws when the operation bound to the current thread has been
     * cancelled.  Use this around iteration that continues after a tracked
     * JDBC statement call has returned, such as a materialized result set.
     */
    public static void throwIfCancellableOperationCancelled() {
        CancellableConnectionScope.throwIfCancellationRequested();
    }

    public static Runnable captureCancellableOperationCheck() {
        return CancellableConnectionScope.captureCancellationCheck();
    }

    public static boolean hasActiveCancellableOperation() {
        return CancellableConnectionScope.isActive();
    }

    public void addBatch(String sqlQuery) {
        batchList.add(sqlQuery);
    }

    public void close() {
        try {
            if (hikariDataSourceInstance != null) {
                hikariDataSourceInstance.close();
            }
        } catch (Exception ex) {
            throw new IllegalStateException("Cannot close Batch connection");
        }
    }

    public void tryCommit() {
        if (batchList.size() >= getBatchSize()) {
            this.commit();
        }
    }

    protected void commitBatch() {
        if (batchList.isEmpty()) {
            return;
        }

        List<String> queries;
        synchronized (batchList) {
            queries = new ArrayList<>(batchList);
            batchList.clear();
        }

        int[] rows = null;
        transactionLock.lock();
        try (Connection connection = this.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                for (String sqlQuery : queries) {
                    statement.addBatch(sqlQuery);
                }
                rows = statement.executeBatch();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            transactionLock.unlock();
        }

        if (rows != null) {
            Map<BatchStatus, Long> summary = Arrays.stream(rows)
                    .mapToObj(ConnectionManager::classify)
                    .collect(Collectors.groupingBy(s -> s, Collectors.counting()));
            long success = summary.getOrDefault(BatchStatus.SUCCESS, 0L);
            long successUnknown = summary.getOrDefault(BatchStatus.SUCCESS_UNKNOWN, 0L);
            long failed = summary.getOrDefault(BatchStatus.FAILED, 0L);
            log.info("Batch execution: SUCCESS={}, SUCCESS_UNKNOWN={}, FAILED={}", success, successUnknown, failed);
        }
    }

    public void commit() {
        try {
            this.commitBatch();
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new IllegalStateException("Cannot commit Batch connection");
        }
    }

    public int getBatchSize() {
        return batchSize;
    }

    protected static BatchStatus classify(int code) {
        if (code >= 0) {
            return BatchStatus.SUCCESS;
        }
        if (code == Statement.SUCCESS_NO_INFO) {
            return BatchStatus.SUCCESS_UNKNOWN;
        }
        return BatchStatus.FAILED; // Statement.EXECUTE_FAILED (-3)
    }
}
