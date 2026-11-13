package org.sensorhub.impl.datastore.postgis.connection;

import org.sensorhub.api.datastore.ICancellableDataStore;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-bound cancellation scope shared by every PostgreSQL connection
 * manager and private JDBC resources participating in the same logical
 * datastore operation. Sharing the thread-local scope is important because
 * one observation query can borrow connections from linked metadata stores
 * and open a DuckDB archive session.
 */
final class CancellableConnectionScope implements ICancellableDataStore.CancellableOperation {
    private static final long DEFAULT_CANCELLATION_TIMEOUT_MILLIS = 2_500L;
    private static final int MAX_CANCELLATION_THREADS = 64;
    private static final ThreadLocal<CancellableConnectionScope> CURRENT = new ThreadLocal<>();
    private static final Executor DIRECT_EXECUTOR = Runnable::run;
    private static final AtomicInteger CANCELLATION_THREAD_SEQUENCE = new AtomicInteger();
    private static final ThreadPoolExecutor CANCELLATION_EXECUTOR = new ThreadPoolExecutor(
            0,
            MAX_CANCELLATION_THREADS,
            30L,
            TimeUnit.SECONDS,
            new SynchronousQueue<>(),
            runnable -> {
                Thread thread = new Thread(runnable,
                        "postgis-cancellation-" + CANCELLATION_THREAD_SEQUENCE.incrementAndGet());
                thread.setDaemon(true);
                return thread;
            },
            new ThreadPoolExecutor.AbortPolicy());

    private final Thread owner = Thread.currentThread();
    private final long cancellationTimeoutNanos;
    private final Object cancellationLock = new Object();
    private final AtomicBoolean cancelled = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<TrackedConnection> connections = ConcurrentHashMap.newKeySet();
    private final Set<TrackedStandaloneStatement> standaloneStatements = ConcurrentHashMap.newKeySet();
    private boolean cancellationAttempted;
    private IllegalStateException cancellationFailure;
    private volatile IllegalStateException closeFailure;

    private CancellableConnectionScope(long cancellationTimeout, TimeUnit unit) {
        if (cancellationTimeout <= 0)
            throw new IllegalArgumentException("Datastore cancellation timeout must be positive");
        this.cancellationTimeoutNanos = unit.toNanos(cancellationTimeout);
    }

    static CancellableConnectionScope open() {
        return open(DEFAULT_CANCELLATION_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
    }

    static boolean isActive() {
        return CURRENT.get() != null;
    }

    static CancellableConnectionScope open(long cancellationTimeout, TimeUnit unit) {
        if (CURRENT.get() != null)
            throw new IllegalStateException("A cancellable datastore operation is already active on this thread");
        CancellableConnectionScope scope = new CancellableConnectionScope(cancellationTimeout, unit);
        CURRENT.set(scope);
        return scope;
    }

    static Connection track(Connection connection) {
        return track(connection, null);
    }

    static Connection track(Connection connection, Runnable evictionHook) {
        CancellableConnectionScope scope = CURRENT.get();
        return scope == null ? connection : scope.trackConnection(connection, evictionHook);
    }

    /**
     * Tracks a JDBC statement whose connection is private to a non-PostgreSQL
     * operation, such as one bounded DuckDB archive session.  The returned
     * proxy unregisters itself on close and participates in the same
     * operation-wide cancellation deadline as PostgreSQL resources.
     */
    static <S extends Statement> S trackStandaloneStatement(S statement, String description) {
        if (statement == null)
            throw new IllegalArgumentException("Cancellable statement cannot be null");
        if (description == null || !description.matches("[A-Za-z0-9][A-Za-z0-9 -]{0,63}"))
            throw new IllegalArgumentException("Unsafe cancellable statement description");
        CancellableConnectionScope scope = CURRENT.get();
        return scope == null ? statement : scope.trackStandalone(statement, description);
    }

    /**
     * Provides a cooperative cancellation point for work performed after a
     * tracked JDBC call has returned.  JDBC statements can hand callers a
     * materialized {@link java.sql.ResultSet}; cancelling the statement then
     * need not interrupt iteration over those already-produced rows.
     */
    static void throwIfCancellationRequested() {
        CancellableConnectionScope scope = CURRENT.get();
        if (scope != null && scope.cancelled.get())
            throw new CancellationException("Datastore operation was cancelled");
    }

    static Runnable captureCancellationCheck() {
        CancellableConnectionScope scope = CURRENT.get();
        if (scope == null)
            return () -> {};
        return () -> {
            if (scope.cancelled.get())
                throw new CancellationException("Datastore operation was cancelled");
        };
    }

    private Connection trackConnection(Connection connection, Runnable evictionHook) {
        TrackedConnection tracked = new TrackedConnection(connection, evictionHook);
        connections.add(tracked);
        if (cancelled.get()) {
            IllegalStateException failure = runBoundedActions(
                    List.of(new CancellationAction(
                            "abort and close a connection acquired after cancellation",
                            () -> {
                                IllegalStateException cleanupFailure = null;
                                try {
                                    tracked.abort();
                                } catch (RuntimeException error) {
                                    cleanupFailure = appendFailure(
                                            "Could not clean up a connection acquired after cancellation",
                                            cleanupFailure, error);
                                }
                                try {
                                    tracked.close();
                                } catch (RuntimeException error) {
                                    cleanupFailure = appendFailure(
                                            "Could not clean up a connection acquired after cancellation",
                                            cleanupFailure, error);
                                }
                                if (cleanupFailure != null)
                                    throw cleanupFailure;
                            })),
                    "Could not clean up a connection acquired after cancellation");
            recordCancellationFailure(failure);
            CancellationException cancellation =
                    new CancellationException("Datastore operation was cancelled");
            if (failure != null)
                cancellation.addSuppressed(failure);
            throw cancellation;
        }
        return tracked.proxy();
    }

    @Override
    public void cancel() {
        IllegalStateException terminalCloseFailure = closeFailure;
        if (closed.get()) {
            if (terminalCloseFailure != null)
                throw terminalCloseFailure;
            return;
        }

        cancelled.set(true);
        synchronized (cancellationLock) {
            if (!cancellationAttempted) {
                List<CancellationAction> actions = new ArrayList<>();
                for (TrackedStandaloneStatement statement : new ArrayList<>(standaloneStatements))
                    statement.addCancellationAction(actions);
                for (TrackedConnection connection : new ArrayList<>(connections))
                    connection.addCancellationActions(actions);
                cancellationFailure = runBoundedActions(
                        actions, "Could not cancel every scoped datastore resource");
                cancellationAttempted = true;
            }
            if (cancellationFailure != null)
                throw cancellationFailure;
        }
    }

    @Override
    public boolean isCancellationRequested() {
        return cancelled.get();
    }

    @Override
    public void close() {
        if (Thread.currentThread() != owner)
            throw new IllegalStateException("Cancellable datastore operation must be closed by its owning thread");
        if (closed.get()) {
            if (closeFailure != null)
                throw closeFailure;
            return;
        }
        if (CURRENT.get() != this)
            throw new IllegalStateException("Cancellable datastore operation is not current on this thread");

        IllegalStateException failure = null;
        if (cancelled.get()) {
            try {
                cancel();
            } catch (RuntimeException error) {
                failure = appendFailure(
                        "Cancellable datastore operation ended with resource failures",
                        failure, error);
            }
        }

        boolean closeFailed = false;
        for (TrackedStandaloneStatement statement : new ArrayList<>(standaloneStatements)) {
            try {
                statement.close();
            } catch (RuntimeException error) {
                closeFailed = true;
                failure = appendFailure(
                        "Cancellable datastore operation ended with resource failures",
                        failure, error);
            }
        }
        for (TrackedConnection connection : new ArrayList<>(connections)) {
            try {
                connection.close();
            } catch (RuntimeException error) {
                closeFailed = true;
                failure = appendFailure(
                        "Cancellable datastore operation ended with resource failures",
                        failure, error);
            }
        }

        if (closeFailed) {
            cancelled.set(true);
            try {
                cancel();
            } catch (RuntimeException error) {
                failure = appendFailure(
                        "Cancellable datastore operation ended with resource failures",
                        failure, error);
            }
            // A JDBC abort normally makes a second Hikari close cheap and
            // deterministic. Always try it so a failed first close cannot
            // leave a pool entry silently attached to this thread-local scope.
            for (TrackedConnection connection : new ArrayList<>(connections)) {
                try {
                    connection.close();
                } catch (RuntimeException error) {
                    failure = appendFailure(
                            "Cancellable datastore operation ended with resource failures",
                            failure, error);
                }
            }
            for (TrackedStandaloneStatement statement : new ArrayList<>(standaloneStatements)) {
                try {
                    statement.close();
                } catch (RuntimeException error) {
                    failure = appendFailure(
                            "Cancellable datastore operation ended with resource failures",
                            failure, error);
                }
            }
        }

        closeFailure = failure;
        closed.set(true);
        CURRENT.remove();
        if (failure != null)
            throw failure;
    }

    private void recordCancellationFailure(IllegalStateException failure) {
        if (failure == null)
            return;
        synchronized (cancellationLock) {
            cancellationFailure = appendFailure(
                    "Could not cancel every scoped datastore resource",
                    cancellationFailure, failure);
        }
    }

    private IllegalStateException runBoundedActions(
            List<CancellationAction> actions, String failureMessage) {
        if (actions.isEmpty())
            return null;

        long startedAt = System.nanoTime();
        ExecutorCompletionService<Void> completion =
                new ExecutorCompletionService<>(CANCELLATION_EXECUTOR);
        Map<Future<Void>, String> pending = new HashMap<>();
        IllegalStateException failure = null;

        for (CancellationAction action : actions) {
            try {
                Future<Void> future = completion.submit(() -> {
                    action.runnable.run();
                    return null;
                });
                pending.put(future, action.description);
            } catch (RejectedExecutionException error) {
                failure = appendFailure(failureMessage, failure,
                        new IllegalStateException(
                                "Cancellation worker capacity was exhausted while trying to "
                                        + action.description, error));
            }
        }

        boolean interrupted = false;
        while (!pending.isEmpty()) {
            // Subtraction from one monotonic origin remains correct across
            // System.nanoTime() wraparound for every practical timeout.
            long remaining = cancellationTimeoutNanos - (System.nanoTime() - startedAt);
            if (remaining <= 0)
                break;
            Future<Void> completed;
            try {
                completed = completion.poll(remaining, TimeUnit.NANOSECONDS);
            } catch (InterruptedException error) {
                interrupted = true;
                failure = appendFailure(failureMessage, failure,
                        new IllegalStateException(
                                "Interrupted while waiting for datastore cancellation", error));
                break;
            }
            if (completed == null)
                break;
            String description = pending.remove(completed);
            failure = collectCompletedAction(failureMessage, failure, completed, description);
        }

        for (Map.Entry<Future<Void>, String> entry : pending.entrySet()) {
            Future<Void> future = entry.getKey();
            if (future.isDone()) {
                failure = collectCompletedAction(
                        failureMessage, failure, future, entry.getValue());
                continue;
            }
            future.cancel(true);
            failure = appendFailure(failureMessage, failure,
                    new IllegalStateException(
                            "Timed out while trying to " + entry.getValue()
                                    + " (shared cancellation deadline: "
                                    + TimeUnit.NANOSECONDS.toMillis(cancellationTimeoutNanos)
                                    + " ms)"));
        }
        if (interrupted)
            Thread.currentThread().interrupt();
        return failure;
    }

    private static IllegalStateException collectCompletedAction(
            String failureMessage,
            IllegalStateException failure,
            Future<Void> completed,
            String description) {
        try {
            completed.get();
            return failure;
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            return appendFailure(failureMessage, failure,
                    new IllegalStateException(
                            "Interrupted while collecting result for " + description, error));
        } catch (ExecutionException error) {
            return appendFailure(failureMessage, failure,
                    new IllegalStateException(
                            "Failed to " + description, error.getCause()));
        } catch (CancellationException error) {
            return appendFailure(failureMessage, failure,
                    new IllegalStateException(
                            "Cancellation worker was stopped while trying to " + description, error));
        }
    }

    private static IllegalStateException appendFailure(
            String message, IllegalStateException failure, Throwable error) {
        IllegalStateException result = failure == null
                ? new IllegalStateException(message) : failure;
        if (error != result)
            result.addSuppressed(error);
        return result;
    }

    private static final class CancellationAction {
        private final String description;
        private final Runnable runnable;

        private CancellationAction(String description, Runnable runnable) {
            this.description = description;
            this.runnable = runnable;
        }
    }

    private <S extends Statement> S trackStandalone(S statement, String description) {
        TrackedStandaloneStatement tracked = new TrackedStandaloneStatement(statement, description);
        standaloneStatements.add(tracked);
        if (cancelled.get()) {
            IllegalStateException failure = runBoundedActions(
                    List.of(new CancellationAction(
                            "cancel and close " + description + " acquired after cancellation",
                            () -> {
                                IllegalStateException cleanupFailure = null;
                                try {
                                    tracked.cancel();
                                } catch (RuntimeException error) {
                                    cleanupFailure = appendFailure(
                                            "Could not clean up a statement acquired after cancellation",
                                            cleanupFailure, error);
                                }
                                try {
                                    tracked.close();
                                } catch (RuntimeException error) {
                                    cleanupFailure = appendFailure(
                                            "Could not clean up a statement acquired after cancellation",
                                            cleanupFailure, error);
                                }
                                if (cleanupFailure != null)
                                    throw cleanupFailure;
                            })),
                    "Could not clean up a statement acquired after cancellation");
            recordCancellationFailure(failure);
            CancellationException cancellation =
                    new CancellationException("Datastore operation was cancelled");
            if (failure != null)
                cancellation.addSuppressed(failure);
            throw cancellation;
        }
        return tracked.proxy();
    }

    private final class TrackedStandaloneStatement {
        private final Statement delegate;
        private final String description;
        private final Statement proxy;
        private final AtomicBoolean cancelAttempted = new AtomicBoolean();
        private volatile boolean statementClosed;
        private volatile boolean statementCancelled;

        TrackedStandaloneStatement(Statement delegate, String description) {
            this.delegate = delegate;
            this.description = description;
            Class<?> statementType = delegate instanceof CallableStatement
                    ? CallableStatement.class
                    : delegate instanceof PreparedStatement
                    ? PreparedStatement.class : Statement.class;
            this.proxy = (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(), new Class<?>[]{statementType},
                    (ignored, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return switch (method.getName()) {
                                case "equals" -> ignored == args[0];
                                case "hashCode" -> System.identityHashCode(ignored);
                                case "toString" -> "Scoped[" + delegate + "]";
                                default -> throw new UnsupportedOperationException(method.getName());
                            };
                        }
                        if (method.getName().equals("close") && method.getParameterCount() == 0) {
                            close();
                            return null;
                        }
                        if (cancelled.get())
                            throw new CancellationException("Datastore operation was cancelled");
                        try {
                            return method.invoke(delegate, args);
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
        }

        @SuppressWarnings("unchecked")
        <S extends Statement> S proxy() {
            return (S) proxy;
        }

        void addCancellationAction(List<CancellationAction> actions) {
            actions.add(new CancellationAction(
                    "cancel scoped " + description + " " + identity(delegate), this::cancel));
        }

        void cancel() {
            if (statementClosed || statementCancelled)
                return;
            if (!cancelAttempted.compareAndSet(false, true))
                return;
            try {
                delegate.cancel();
                statementCancelled = true;
            } catch (SQLException error) {
                throw new IllegalStateException("Could not cancel scoped " + description, error);
            }
        }

        synchronized void close() {
            if (statementClosed)
                return;
            try {
                delegate.close();
                statementClosed = true;
                standaloneStatements.remove(this);
            } catch (SQLException error) {
                throw new IllegalStateException("Could not close scoped " + description, error);
            }
        }
    }

    private final class TrackedConnection {
        private final Connection delegate;
        private final Runnable evictionHook;
        private final Connection proxy;
        private final Set<TrackedStatement> statements = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean abortAttempted = new AtomicBoolean();
        private final AtomicBoolean evicted = new AtomicBoolean();
        private volatile boolean connectionClosed;
        private volatile boolean connectionAborted;

        TrackedConnection(Connection delegate, Runnable evictionHook) {
            this.delegate = delegate;
            this.evictionHook = evictionHook;
            this.proxy = (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (ignored, method, args) -> {
                        if (method.getDeclaringClass() == Object.class) {
                            return switch (method.getName()) {
                                case "equals" -> ignored == args[0];
                                case "hashCode" -> System.identityHashCode(ignored);
                                case "toString" -> "Scoped[" + delegate + "]";
                                default -> throw new UnsupportedOperationException(method.getName());
                            };
                        }
                        if (method.getName().equals("close") && method.getParameterCount() == 0) {
                            close();
                            return null;
                        }
                        if (cancelled.get())
                            throw new CancellationException("Datastore operation was cancelled");
                        try {
                            Object result = method.invoke(delegate, args);
                            return result instanceof Statement statement
                                    ? trackStatement(statement) : result;
                        } catch (InvocationTargetException error) {
                            throw error.getCause();
                        }
                    });
        }

        Connection proxy() {
            return proxy;
        }

        void addCancellationActions(List<CancellationAction> actions) {
            List<TrackedStatement> statementSnapshot = new ArrayList<>(statements);
            CountDownLatch statementSignals = new CountDownLatch(statementSnapshot.size());
            for (TrackedStatement statement : statementSnapshot)
                statement.addCancellationAction(actions, statementSignals);
            actions.add(new CancellationAction(
                    "abort scoped PostgreSQL connection " + identity(delegate), () -> {
                        boolean interrupted = false;
                        try {
                            // Give concurrently-issued Statement.cancel calls
                            // a short head start, then force the connection
                            // abort while the one operation-wide deadline is
                            // still in force. This preserves graceful cancel
                            // in the usual case without serializing resources.
                            long gracefulWait = Math.min(
                                    TimeUnit.MILLISECONDS.toNanos(250L),
                                    Math.max(1L, cancellationTimeoutNanos / 4L));
                            try {
                                statementSignals.await(gracefulWait, TimeUnit.NANOSECONDS);
                            } catch (InterruptedException ignored) {
                                interrupted = true;
                            }
                            abort();
                        } finally {
                            if (interrupted)
                                Thread.currentThread().interrupt();
                        }
                    }));
        }

        private void evict() {
            if (evictionHook != null && evicted.compareAndSet(false, true)) {
                try {
                    evictionHook.run();
                } catch (RuntimeException ignored) {
                }
            }
        }

        void abort() {
            if (connectionClosed || connectionAborted)
                return;
            if (!abortAttempted.compareAndSet(false, true))
                return;
            try {
                // This method itself is invoked on a daemon cancellation
                // worker. A driver that runs the supplied command inline can
                // therefore never pin the caller past the shared deadline.
                delegate.abort(DIRECT_EXECUTOR);
                connectionAborted = true;
            } catch (SQLException error) {
                throw new IllegalStateException("Could not abort scoped PostgreSQL connection", error);
            } finally {
                evict();
            }
        }

        synchronized void close() {
            if (connectionClosed)
                return;
            if (cancelled.get() || connectionAborted)
                evict();
            IllegalStateException failure = null;
            for (TrackedStatement statement : new ArrayList<>(statements)) {
                try {
                    statement.close();
                } catch (RuntimeException error) {
                    failure = appendFailure(
                            "Could not close scoped PostgreSQL connection resources",
                            failure, error);
                }
            }
            try {
                delegate.close();
                connectionClosed = true;
                connections.remove(this);
            } catch (SQLException error) {
                // Hikari can evict a physically aborted pool entry and still
                // rethrow a rollback error. Remove an entry only when JDBC
                // confirms it is closed, but never hide that close failure
                // from the operation owner.
                if (connectionAborted) {
                    try {
                        if (delegate.isClosed()) {
                            connectionClosed = true;
                            connections.remove(this);
                        }
                    } catch (SQLException stateError) {
                        error.addSuppressed(stateError);
                    }
                }
                failure = appendFailure(
                        "Could not close scoped PostgreSQL connection resources",
                        failure,
                        new IllegalStateException(
                                "Could not close scoped PostgreSQL connection", error));
            }
            if (failure != null)
                throw failure;
        }

        private Statement trackStatement(Statement statement) {
            TrackedStatement tracked = new TrackedStatement(statement);
            statements.add(tracked);
            if (cancelled.get()) {
                IllegalStateException failure = runBoundedActions(
                        List.of(new CancellationAction(
                                "cancel and close a statement acquired after cancellation",
                                () -> {
                                    IllegalStateException cleanupFailure = null;
                                    try {
                                        tracked.cancel();
                                    } catch (RuntimeException error) {
                                        cleanupFailure = appendFailure(
                                                "Could not clean up a statement acquired after cancellation",
                                                cleanupFailure, error);
                                    }
                                    try {
                                        abort();
                                    } catch (RuntimeException error) {
                                        cleanupFailure = appendFailure(
                                                "Could not clean up a statement acquired after cancellation",
                                                cleanupFailure, error);
                                    }
                                    try {
                                        tracked.close();
                                    } catch (RuntimeException error) {
                                        cleanupFailure = appendFailure(
                                                "Could not clean up a statement acquired after cancellation",
                                                cleanupFailure, error);
                                    }
                                    if (cleanupFailure != null)
                                        throw cleanupFailure;
                                })),
                        "Could not clean up a statement acquired after cancellation");
                recordCancellationFailure(failure);
                CancellationException cancellation =
                        new CancellationException("Datastore operation was cancelled");
                if (failure != null)
                    cancellation.addSuppressed(failure);
                throw cancellation;
            }
            return tracked.proxy();
        }

        private final class TrackedStatement {
            private final Statement delegate;
            private final Statement proxy;
            private final AtomicBoolean cancelAttempted = new AtomicBoolean();
            private volatile boolean statementClosed;
            private volatile boolean statementCancelled;

            TrackedStatement(Statement delegate) {
                this.delegate = delegate;
                Class<?> statementType = delegate instanceof CallableStatement
                        ? CallableStatement.class
                        : delegate instanceof PreparedStatement
                        ? PreparedStatement.class : Statement.class;
                this.proxy = (Statement) Proxy.newProxyInstance(
                        Statement.class.getClassLoader(), new Class<?>[]{statementType},
                        (ignored, method, args) -> {
                            if (method.getDeclaringClass() == Object.class) {
                                return switch (method.getName()) {
                                    case "equals" -> ignored == args[0];
                                    case "hashCode" -> System.identityHashCode(ignored);
                                    case "toString" -> "Scoped[" + delegate + "]";
                                    default -> throw new UnsupportedOperationException(method.getName());
                                };
                            }
                            if (method.getName().equals("close") && method.getParameterCount() == 0) {
                                close();
                                return null;
                            }
                            if (cancelled.get())
                                throw new CancellationException("Datastore operation was cancelled");
                            if (method.getName().equals("getConnection")
                                    && method.getParameterCount() == 0)
                                return TrackedConnection.this.proxy;
                            try {
                                return method.invoke(delegate, args);
                            } catch (InvocationTargetException error) {
                                throw error.getCause();
                            }
                        });
            }

            Statement proxy() {
                return proxy;
            }

            void addCancellationAction(
                    List<CancellationAction> actions, CountDownLatch completionSignal) {
                actions.add(new CancellationAction(
                        "cancel scoped PostgreSQL statement " + identity(delegate), () -> {
                            try {
                                cancel();
                            } finally {
                                completionSignal.countDown();
                            }
                        }));
            }

            void cancel() {
                if (statementClosed || statementCancelled)
                    return;
                if (!cancelAttempted.compareAndSet(false, true))
                    return;
                try {
                    delegate.cancel();
                    statementCancelled = true;
                } catch (SQLException error) {
                    throw new IllegalStateException("Could not cancel scoped PostgreSQL statement", error);
                }
            }

            synchronized void close() {
                if (statementClosed)
                    return;
                try {
                    delegate.close();
                    statementClosed = true;
                    statements.remove(this);
                } catch (SQLException error) {
                    throw new IllegalStateException("Could not close scoped PostgreSQL statement", error);
                }
            }
        }
    }

    private static String identity(Object resource) {
        return Integer.toHexString(System.identityHashCode(resource));
    }
}
