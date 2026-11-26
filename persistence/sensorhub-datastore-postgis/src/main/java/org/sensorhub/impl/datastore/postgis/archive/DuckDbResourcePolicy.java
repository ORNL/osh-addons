package org.sensorhub.impl.datastore.postgis.archive;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;

/** Opens a bounded DuckDB connection with an operation-private spill directory. */
final class DuckDbResourcePolicy {
    private static final Set<PosixFilePermission> PRIVATE_DIRECTORY_PERMISSIONS =
            PosixFilePermissions.fromString("rwx------");
    private static final Set<PosixFilePermission> PRIVATE_LOCK_FILE_PERMISSIONS =
            PosixFilePermissions.fromString("rw-------");
    static final String ROOT_LOCK_FILE_NAME = ".oscar-duckdb.lock";
    private static final Pattern SAFE_OPERATION = Pattern.compile("[a-z][a-z0-9-]{0,31}");
    private static final int CANONICAL_UUID_LENGTH = 36;
    private static final Object TEMPORARY_ROOT_INITIALIZATION_LOCK = new Object();
    private static final Map<Path, RootLock> LOCKED_TEMPORARY_ROOTS = new HashMap<>();
    private static final AdmissionController ADMISSION = new AdmissionController();

    private DuckDbResourcePolicy() {
    }

    static Session open(TieredArchiveConfig config, String operation) throws Exception {
        return open(config, operation, false);
    }

    /**
     * Opens a bounded DuckDB session backed by a session-private database file
     * inside {@code operationDirectory} so large staged tables and external
     * sorts transparently spill within {@code memory_limit} and are removed
     * atomically when the session closes.
     */
    static Session openFileBacked(TieredArchiveConfig config, String operation) throws Exception {
        return open(config, operation, true);
    }

    private static Session open(TieredArchiveConfig config, String operation, boolean fileBacked) throws Exception {
        config.validate();
        if (operation == null || !SAFE_OPERATION.matcher(operation).matches())
            throw new IllegalArgumentException("Unsafe DuckDB operation name");

        Path archiveRoot = Path.of(config.rootDirectory).toAbsolutePath().normalize();
        Path temporaryRoot = Path.of(config.duckDbTemporaryDirectory).toAbsolutePath().normalize();
        requireDisjointRoots(archiveRoot, temporaryRoot);
        AdmissionPermit permit = ADMISSION.acquire(
                config.duckDbMaximumConcurrentSessions,
                config.duckDbSessionAcquireTimeoutSeconds,
                operation);
        Connection connection = null;
        Path realTemporaryRoot = null;
        Path operationDirectory = null;
        try {
            preparePrivateDirectory(temporaryRoot);
            realTemporaryRoot = temporaryRoot.toRealPath();
            Path realArchiveRoot = Files.exists(archiveRoot, LinkOption.NOFOLLOW_LINKS) ?
                    archiveRoot.toRealPath() : archiveRoot;
            requireDisjointRoots(realArchiveRoot, realTemporaryRoot);
            initializeTemporaryRoot(realTemporaryRoot);
            operationDirectory = createOperationDirectory(realTemporaryRoot, operation);

            String jdbcUrl = fileBacked
                    ? "jdbc:duckdb:" + operationDirectory.resolve("work.duckdb")
                    : "jdbc:duckdb:";
            connection = DriverManager.getConnection(jdbcUrl);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SET memory_limit='" + config.duckDbMemoryLimitMegabytes + "MB'");
                statement.execute("SET threads=" + config.duckDbThreads);
                statement.execute("SET temp_directory='" + sqlLiteral(operationDirectory.toString()) + "'");
                statement.execute("SET max_temp_directory_size='" +
                        config.duckDbMaximumTemporaryMegabytes + "MB'");
                // Global insertion-order preservation is unnecessary for the
                // archive's explicitly ordered writes/reads and can retain a
                // large extra buffer during near-cap operations.
                statement.execute("SET preserve_insertion_order=false");
                statement.execute("SET parquet_metadata_cache=true");
            }
            return new Session(connection, realTemporaryRoot, operationDirectory, permit);
        } catch (Exception | Error error) {
            closeQuietly(connection, error);
            if (realTemporaryRoot != null && operationDirectory != null)
                deleteOperationDirectory(realTemporaryRoot, operationDirectory, error);
            permit.close();
            throw error;
        }
    }

    /**
     * Creates a private 0700 scratch directory inside {@code duckDbTemporaryDirectory}
     * without occupying a DuckDB session permit. Recognized directories left
     * behind after a hard crash are reclaimed automatically on next startup.
     */
    static ScratchDirectory openScratchDirectory(TieredArchiveConfig config, String operation) throws Exception {
        config.validate();
        if (operation == null || !SAFE_OPERATION.matcher(operation).matches())
            throw new IllegalArgumentException("Unsafe DuckDB operation name");
        Path archiveRoot = Path.of(config.rootDirectory).toAbsolutePath().normalize();
        Path temporaryRoot = Path.of(config.duckDbTemporaryDirectory).toAbsolutePath().normalize();
        requireDisjointRoots(archiveRoot, temporaryRoot);
        preparePrivateDirectory(temporaryRoot);
        Path realTemporaryRoot = temporaryRoot.toRealPath();
        Path realArchiveRoot = Files.exists(archiveRoot, LinkOption.NOFOLLOW_LINKS) ?
                archiveRoot.toRealPath() : archiveRoot;
        requireDisjointRoots(realArchiveRoot, realTemporaryRoot);
        initializeTemporaryRoot(realTemporaryRoot);
        Path dir = createOperationDirectory(realTemporaryRoot, operation);
        return new ScratchDirectory(realTemporaryRoot, dir);
    }

    private static Path createOperationDirectory(Path realTemporaryRoot, String operation) throws IOException {
        Path operationDirectory = realTemporaryRoot.resolve(
                operation + '-' + UUID.randomUUID()).normalize();
        if (!operationDirectory.getParent().equals(realTemporaryRoot))
            throw new IllegalStateException(
                    "DuckDB operation directory escaped its configured parent");
        if (supportsPosix(realTemporaryRoot))
            Files.createDirectory(operationDirectory,
                    PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY_PERMISSIONS));
        else
            Files.createDirectory(operationDirectory);
        BasicFileAttributes attributes = Files.readAttributes(
                operationDirectory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink())
            throw new IllegalStateException(
                    "DuckDB operation directory creation was redirected");
        requirePrivatePosixDirectory(operationDirectory);
        return operationDirectory;
    }

    private static void preparePrivateDirectory(Path directory) throws IOException {
        if (supportsPosix(nearestExistingAncestor(directory)))
            Files.createDirectories(directory,
                    PosixFilePermissions.asFileAttribute(PRIVATE_DIRECTORY_PERMISSIONS));
        else
            Files.createDirectories(directory);
        BasicFileAttributes attributes = Files.readAttributes(
                directory, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isDirectory() || attributes.isSymbolicLink())
            throw new IllegalStateException("DuckDB temporary directory is not a real directory: " + directory);
        requirePrivatePosixDirectory(directory);
    }

    private static Path nearestExistingAncestor(Path path) {
        Path current = path;
        while (current != null && !Files.exists(current, LinkOption.NOFOLLOW_LINKS))
            current = current.getParent();
        if (current == null)
            throw new IllegalStateException("DuckDB temporary directory has no existing filesystem ancestor");
        return current;
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(
                path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
    }

    private static void requirePrivatePosixDirectory(Path directory) throws IOException {
        if (!supportsPosix(directory))
            return;
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                directory, LinkOption.NOFOLLOW_LINKS);
        if (!permissions.equals(PRIVATE_DIRECTORY_PERMISSIONS))
            throw new IllegalStateException(
                    "DuckDB temporary directory must have POSIX mode 0700: " + directory);
    }

    private static void requireDisjointRoots(Path archiveRoot, Path temporaryRoot) {
        if (archiveRoot.equals(temporaryRoot) || archiveRoot.startsWith(temporaryRoot) ||
                temporaryRoot.startsWith(archiveRoot))
            throw new IllegalArgumentException(
                    "DuckDB temporary directory and immutable archive root must be disjoint");
    }

    /**
     * Exclusively claims the canonical temporary root for this classloader,
     * then reclaims operation directories left by a prior owner.  The OS lock
     * remains held for the lifetime of this classloader so another process or
     * classloader cannot mistake a live directory for stale state.
     */
    private static void initializeTemporaryRoot(Path realTemporaryRoot) throws IOException {
        synchronized (TEMPORARY_ROOT_INITIALIZATION_LOCK) {
            if (LOCKED_TEMPORARY_ROOTS.containsKey(realTemporaryRoot))
                return;

            RootLock rootLock = acquireRootLock(realTemporaryRoot);
            try {
                try (var children = Files.newDirectoryStream(realTemporaryRoot)) {
                    for (Path child : children) {
                        BasicFileAttributes attributes = Files.readAttributes(
                                child, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
                        if (!attributes.isDirectory() || attributes.isSymbolicLink() ||
                                !isRecognizedOperationDirectory(child.getFileName().toString()))
                            continue;
                        deleteOperationDirectoryStrict(realTemporaryRoot, child);
                    }
                }
                LOCKED_TEMPORARY_ROOTS.put(realTemporaryRoot, rootLock);
            } catch (IOException | RuntimeException | Error error) {
                try {
                    rootLock.close();
                } catch (Throwable closeError) {
                    error.addSuppressed(closeError);
                }
                throw error;
            }
        }
    }

    private static RootLock acquireRootLock(Path realTemporaryRoot) throws IOException {
        Path lockFile = realTemporaryRoot.resolve(ROOT_LOCK_FILE_NAME)
                .toAbsolutePath().normalize();
        if (!lockFile.getParent().equals(realTemporaryRoot))
            throw new IOException("Unsafe DuckDB temporary root lock path: " + lockFile);

        FileChannel channel = null;
        FileLock lock = null;
        try {
            preflightRootLockFile(lockFile);
            Set<OpenOption> options = Set.of(
                    StandardOpenOption.CREATE,
                    StandardOpenOption.READ,
                    StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS);
            if (supportsPosix(realTemporaryRoot))
                channel = FileChannel.open(lockFile, options,
                        PosixFilePermissions.asFileAttribute(PRIVATE_LOCK_FILE_PERMISSIONS));
            else
                channel = FileChannel.open(lockFile, options);
            requirePrivateRegularLockFile(lockFile);
            try {
                lock = channel.tryLock();
            } catch (OverlappingFileLockException unavailable) {
                throw new IOException(
                        "DuckDB temporary root is already owned in this JVM: " +
                                realTemporaryRoot, unavailable);
            }
            if (lock == null)
                throw new IOException(
                        "DuckDB temporary root is already owned by another process: " +
                                realTemporaryRoot);
            requirePrivateRegularLockFile(lockFile);
            return new RootLock(channel, lock);
        } catch (IOException | RuntimeException | Error error) {
            if (lock != null) {
                try {
                    lock.release();
                } catch (Throwable releaseError) {
                    error.addSuppressed(releaseError);
                }
            }
            if (channel != null) {
                try {
                    channel.close();
                } catch (Throwable closeError) {
                    error.addSuppressed(closeError);
                }
            }
            throw error;
        }
    }

    private static void preflightRootLockFile(Path lockFile) throws IOException {
        try {
            requirePrivateRegularLockFile(lockFile);
        } catch (NoSuchFileException absent) {
            // A positively absent lease is created atomically with mode 0600
            // by the subsequent no-follow channel open.
        }
    }

    private static void requirePrivateRegularLockFile(Path lockFile) throws IOException {
        BasicFileAttributes attributes = Files.readAttributes(
                lockFile, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile() || attributes.isSymbolicLink())
            throw new IOException(
                    "DuckDB temporary root lock must be a real regular file: " + lockFile);
        if (!supportsPosix(lockFile))
            return;
        Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(
                lockFile, LinkOption.NOFOLLOW_LINKS);
        if (!permissions.equals(PRIVATE_LOCK_FILE_PERMISSIONS))
            throw new IOException(
                    "DuckDB temporary root lock must have POSIX mode 0600: " + lockFile);
    }

    private static boolean isRecognizedOperationDirectory(String name) {
        int separator = name.length() - CANONICAL_UUID_LENGTH - 1;
        if (separator <= 0 || name.charAt(separator) != '-')
            return false;
        String operation = name.substring(0, separator);
        if (!SAFE_OPERATION.matcher(operation).matches())
            return false;
        String candidateUuid = name.substring(separator + 1);
        try {
            return UUID.fromString(candidateUuid).toString().equals(candidateUuid);
        } catch (IllegalArgumentException invalidUuid) {
            return false;
        }
    }

    private static void deleteOperationDirectory(Path realTemporaryRoot, Path operationDirectory,
                                                 Throwable primary) {
        try {
            deleteOperationDirectoryStrict(realTemporaryRoot, operationDirectory);
        } catch (Throwable cleanupError) {
            primary.addSuppressed(cleanupError);
        }
    }

    private static void deleteOperationDirectoryStrict(Path realTemporaryRoot, Path operationDirectory)
            throws IOException {
        Path normalized = operationDirectory.toAbsolutePath().normalize();
        if (!normalized.getParent().equals(realTemporaryRoot) || normalized.equals(realTemporaryRoot))
            throw new IllegalStateException("Refusing unsafe DuckDB temporary cleanup: " + normalized);
        // Files.exists() also returns false when existence cannot be
        // determined. Only a positively confirmed absence may make cleanup a
        // no-op; inaccessible/indeterminate paths must fail visibly below.
        if (Files.notExists(normalized, LinkOption.NOFOLLOW_LINKS))
            return;
        BasicFileAttributes rootAttributes = Files.readAttributes(
                normalized, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!rootAttributes.isDirectory() || rootAttributes.isSymbolicLink())
            throw new IllegalStateException(
                    "Refusing redirected DuckDB temporary cleanup: " + normalized);
        Files.walkFileTree(normalized, new SimpleFileVisitor<>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path directory, IOException error) throws IOException {
                if (error != null)
                    throw error;
                Files.delete(directory);
                return FileVisitResult.CONTINUE;
            }
        });
    }

    private static void closeQuietly(Connection connection, Throwable primary) {
        if (connection == null)
            return;
        try {
            connection.close();
        } catch (Throwable closeError) {
            primary.addSuppressed(closeError);
        }
    }

    private static String sqlLiteral(String value) {
        return value.replace("'", "''");
    }

    private static final class RootLock implements AutoCloseable {
        private final FileChannel channel;
        private final FileLock lock;

        private RootLock(FileChannel channel, FileLock lock) {
            this.channel = channel;
            this.lock = lock;
        }

        @Override
        public void close() throws IOException {
            IOException failure = null;
            try {
                lock.release();
            } catch (IOException error) {
                failure = error;
            }
            try {
                channel.close();
            } catch (IOException error) {
                if (failure == null)
                    failure = error;
                else
                    failure.addSuppressed(error);
            }
            if (failure != null)
                throw failure;
        }
    }

    static final class Session implements AutoCloseable {
        private final Connection connection;
        private final Path temporaryRoot;
        private final Path operationDirectory;
        private final AdmissionPermit permit;
        private boolean closed;

        private Session(Connection connection, Path temporaryRoot, Path operationDirectory,
                        AdmissionPermit permit) {
            this.connection = connection;
            this.temporaryRoot = temporaryRoot;
            this.operationDirectory = operationDirectory;
            this.permit = permit;
        }

        Connection connection() {
            return connection;
        }

        Path operationDirectory() {
            return operationDirectory;
        }

        boolean shouldYield() {
            return permit.shouldYield();
        }

        boolean hasWaitingRequests() {
            return permit.hasWaitingRequests();
        }

        @Override
        public synchronized void close() {
            if (closed)
                return;
            closed = true;
            Throwable failure = null;
            boolean connectionFailed = false;
            try {
                connection.close();
            } catch (Throwable error) {
                failure = error;
                connectionFailed = true;
            }
            if (failure == null)
                failure = new IllegalStateException("DuckDB temporary cleanup failed");
            int priorSuppressed = failure.getSuppressed().length;
            deleteOperationDirectory(temporaryRoot, operationDirectory, failure);
            boolean cleanupFailed = failure.getSuppressed().length != priorSuppressed;
            try {
                if (cleanupFailed || connectionFailed)
                    throw new IllegalStateException("Cannot close bounded DuckDB session", failure);
            } finally {
                permit.close();
            }
        }
    }

    static final class ScratchDirectory implements AutoCloseable {
        private final Path temporaryRoot;
        private final Path directory;
        private boolean closed;

        private ScratchDirectory(Path temporaryRoot, Path directory) {
            this.temporaryRoot = temporaryRoot;
            this.directory = directory;
        }

        Path path() {
            return directory;
        }

        @Override
        public synchronized void close() {
            if (closed)
                return;
            closed = true;
            Throwable failure = new IllegalStateException("DuckDB scratch cleanup failed");
            int priorSuppressed = failure.getSuppressed().length;
            deleteOperationDirectory(temporaryRoot, directory, failure);
            if (failure.getSuppressed().length != priorSuppressed)
                throw new IllegalStateException("Cannot close DuckDB scratch directory", failure);
        }
    }

    /**
     * Fair process-wide admission.  Every active and waiting request keeps its
     * configured cap registered, so independently configured stores cannot
     * evade the smallest live cap.  FIFO admission within priority classes
     * prevents a stream of short queries from starving an exporter, while
     * reserving at least one session for interactive/lifecycle work whenever
     * the effective global cap exceeds one.
     */
    static final long ANTI_STARVATION_AGING_NANOS = TimeUnit.SECONDS.toNanos(5);

    static void setNanoClockForTesting(LongSupplier clock) {
        ADMISSION.setNanoClock(clock);
    }

    static boolean hasInteractiveWaiters() {
        return ADMISSION.hasInteractiveWaiters();
    }

    static int waitingCountForTesting() {
        return ADMISSION.waitingCount();
    }

    private static final class AdmissionController {
        private final ReentrantLock lock = new ReentrantLock(true);
        private final Condition capacityChanged = lock.newCondition();
        private final java.util.LinkedList<AdmissionRequest> waiting = new java.util.LinkedList<>();
        private final TreeMap<Integer, Integer> registeredLimits = new TreeMap<>();
        private volatile LongSupplier nanoClock = System::nanoTime;
        private int active;
        private int activeBackgroundCount;

        void setNanoClock(LongSupplier clock) {
            this.nanoClock = clock == null ? System::nanoTime : clock;
        }

        private static boolean isHighPriorityWaiter(String operation) {
            return "point-query".equals(operation)
                    || "archive-export".equals(operation)
                    || "parity-verify".equals(operation)
                    || "hot-delete".equals(operation)
                    || "export".equals(operation)
                    || "export-verify".equals(operation);
        }

        static boolean isBackgroundCountOperation(String operation) {
            return "cold-cql-count".equals(operation)
                    || "cold-cql-id-count".equals(operation)
                    || "archive-compaction".equals(operation)
                    || "compaction-verify".equals(operation);
        }

        private void enqueueRequest(AdmissionRequest request) {
            long now = request.enqueuedAtNanos();
            int minInsertAt = 0;
            int index = 0;
            for (AdmissionRequest queued : waiting) {
                if (now - queued.enqueuedAtNanos() >= ANTI_STARVATION_AGING_NANOS) {
                    minInsertAt = index + 1;
                }
                index++;
            }
            if ("point-query".equals(request.operation())) {
                int insertAt = 0;
                for (AdmissionRequest queued : waiting) {
                    if (insertAt >= minInsertAt && !isHighPriorityWaiter(queued.operation())) {
                        waiting.add(insertAt, request);
                        return;
                    }
                    insertAt++;
                }
            } else if (!isBackgroundCountOperation(request.operation())) {
                int insertAt = 0;
                for (AdmissionRequest queued : waiting) {
                    if (insertAt >= minInsertAt && isBackgroundCountOperation(queued.operation())) {
                        waiting.add(insertAt, request);
                        return;
                    }
                    insertAt++;
                }
            }
            waiting.addLast(request);
        }

        private AdmissionRequest firstEligibleWaiter(int globalCap) {
            if (active >= globalCap)
                return null;
            int backgroundCap = Math.max(1, globalCap - 1);
            for (AdmissionRequest queued : waiting) {
                if (!isBackgroundCountOperation(queued.operation()) || activeBackgroundCount < backgroundCap)
                    return queued;
            }
            return null;
        }

        boolean hasWaitingRequests() {
            lock.lock();
            try {
                return !waiting.isEmpty();
            } finally {
                lock.unlock();
            }
        }

        boolean hasInteractiveWaiters() {
            lock.lock();
            try {
                for (AdmissionRequest queued : waiting) {
                    if (!isBackgroundCountOperation(queued.operation()))
                        return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        int waitingCount() {
            lock.lock();
            try {
                return waiting.size();
            } finally {
                lock.unlock();
            }
        }

        boolean shouldBackgroundYield(boolean backgroundPermit) {
            if (!backgroundPermit)
                return false;
            lock.lock();
            try {
                if (waiting.isEmpty())
                    return false;
                int globalCap = registeredLimits.isEmpty() ? Integer.MAX_VALUE : registeredLimits.firstKey();
                if (globalCap <= 1)
                    return true;
                for (AdmissionRequest queued : waiting) {
                    if (!isBackgroundCountOperation(queued.operation()))
                        return true;
                }
                return false;
            } finally {
                lock.unlock();
            }
        }

        AdmissionPermit acquire(int limit, int timeoutSeconds, String operation) throws Exception {
            long remaining = TimeUnit.SECONDS.toNanos(timeoutSeconds);
            boolean background = isBackgroundCountOperation(operation);
            try {
                lock.lockInterruptibly();
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                throw new SQLTransientException(
                        "Interrupted while waiting for DuckDB session capacity for operation " +
                                operation, interrupted);
            }
            AdmissionRequest request = new AdmissionRequest(operation, nanoClock.getAsLong());
            try {
                enqueueRequest(request);
                addLimit(limit);
                while (firstEligibleWaiter(registeredLimits.firstKey()) != request) {
                    if (remaining <= 0) {
                        waiting.remove(request);
                        removeLimit(limit);
                        capacityChanged.signalAll();
                        throw new SQLTransientException(
                                "DuckDB session capacity is busy for operation " + operation +
                                        " after " + timeoutSeconds + " seconds");
                    }
                    remaining = capacityChanged.awaitNanos(remaining);
                }
                waiting.remove(request);
                active++;
                if (background)
                    activeBackgroundCount++;
                capacityChanged.signalAll();
                return new AdmissionPermit(this, limit, background);
            } catch (InterruptedException interrupted) {
                waiting.remove(request);
                removeLimit(limit);
                capacityChanged.signalAll();
                Thread.currentThread().interrupt();
                throw new SQLTransientException(
                        "Interrupted while waiting for DuckDB session capacity for operation " +
                                operation, interrupted);
            } finally {
                lock.unlock();
            }
        }

        private void release(int limit, boolean background) {
            lock.lock();
            try {
                if (active <= 0 || (background && activeBackgroundCount <= 0))
                    throw new IllegalStateException("DuckDB admission permit released without an active session");
                active--;
                if (background)
                    activeBackgroundCount--;
                removeLimit(limit);
                capacityChanged.signalAll();
            } finally {
                lock.unlock();
            }
        }

        private void addLimit(int limit) {
            registeredLimits.merge(limit, 1, Integer::sum);
        }

        private void removeLimit(int limit) {
            Integer count = registeredLimits.get(limit);
            if (count == null)
                throw new IllegalStateException("DuckDB admission limit registration is missing");
            if (count == 1)
                registeredLimits.remove(limit);
            else
                registeredLimits.put(limit, count - 1);
        }
    }

    private static final class AdmissionRequest {
        private final String operation;
        private final long enqueuedAtNanos;

        private AdmissionRequest(String operation, long enqueuedAtNanos) {
            this.operation = operation;
            this.enqueuedAtNanos = enqueuedAtNanos;
        }

        private String operation() {
            return operation;
        }

        private long enqueuedAtNanos() {
            return enqueuedAtNanos;
        }
    }

    private static final class AdmissionPermit implements AutoCloseable {
        private final AdmissionController controller;
        private final int limit;
        private final boolean background;
        private boolean closed;

        private AdmissionPermit(AdmissionController controller, int limit, boolean background) {
            this.controller = controller;
            this.limit = limit;
            this.background = background;
        }

        boolean shouldYield() {
            synchronized (this) {
                if (closed)
                    return false;
            }
            return controller.shouldBackgroundYield(background);
        }

        boolean hasWaitingRequests() {
            synchronized (this) {
                if (closed)
                    return false;
            }
            return controller.hasWaitingRequests();
        }

        @Override
        public synchronized void close() {
            if (closed)
                return;
            closed = true;
            controller.release(limit, background);
        }
    }
}
