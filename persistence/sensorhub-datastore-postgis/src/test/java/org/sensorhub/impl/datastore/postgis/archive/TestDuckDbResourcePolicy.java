package org.sensorhub.impl.datastore.postgis.archive;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.sensorhub.api.datastore.ICancellableDataStore;
import org.sensorhub.impl.datastore.postgis.connection.ConnectionManager;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.ResultSet;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TestDuckDbResourcePolicy {
    @Rule
    public TemporaryFolder temporary = new TemporaryFolder();

    @Test
    public void appliesEveryBoundAndRemovesOperationDirectory() throws Exception {
        Path temporaryRoot = temporary.getRoot().toPath().resolve("duckdb-spill");
        TieredArchiveConfig config = config(temporaryRoot);
        Path operationDirectory;

        try (DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(config, "near-cap")) {
            operationDirectory = session.operationDirectory();
            assertEquals(temporaryRoot.toRealPath(), operationDirectory.getParent());
            assertTrue(Files.isDirectory(operationDirectory));
            if (supportsPosix(operationDirectory)) {
                assertEquals(PosixFilePermissions.fromString("rwx------"),
                        Files.getPosixFilePermissions(operationDirectory, LinkOption.NOFOLLOW_LINKS));
                assertEquals(PosixFilePermissions.fromString("rwx------"),
                        Files.getPosixFilePermissions(temporaryRoot, LinkOption.NOFOLLOW_LINKS));
            }
            try (Statement statement = session.connection().createStatement()) {
                assertEquals(1, numericSetting(statement, "threads"));
                assertTrue(byteSetting(statement, "memory_limit") <= 64L * 1024 * 1024);
                assertTrue(byteSetting(statement, "max_temp_directory_size") <= 96L * 1024 * 1024);
                assertEquals(operationDirectory.toString(), textSetting(statement, "temp_directory"));

                // The generated values plus the hash sort are deliberately
                // close to the 64 MiB memory cap. The operation must finish
                // under the configured one-thread/96 MiB spill envelope.
                try (ResultSet rows = statement.executeQuery(
                        "SELECT count(*) FROM (SELECT i, repeat('x', 192) payload " +
                                "FROM range(0, 120000) values(i) ORDER BY hash(i), payload) bounded")) {
                    assertTrue(rows.next());
                    assertEquals(120000L, rows.getLong(1));
                }
            }
        }

        assertFalse(Files.exists(operationDirectory));
        assertTrue(Files.isDirectory(temporaryRoot));
        assertOnlyRootLock(temporaryRoot);
    }

    @Test
    public void concurrentOperationsReceiveDifferentConfinedDirectories() throws Exception {
        Path temporaryRoot = temporary.newFolder("separate-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        Path firstDirectory;
        Path secondDirectory;
        try (DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(config, "cold-query");
             DuckDbResourcePolicy.Session second = DuckDbResourcePolicy.open(config, "cold-query")) {
            firstDirectory = first.operationDirectory();
            secondDirectory = second.operationDirectory();
            assertNotEquals(firstDirectory, secondDirectory);
            assertEquals(temporaryRoot.toRealPath(), firstDirectory.getParent());
            assertEquals(temporaryRoot.toRealPath(), secondDirectory.getParent());
        }
        assertFalse(Files.exists(firstDirectory));
        assertFalse(Files.exists(secondDirectory));
        assertOnlyRootLock(temporaryRoot);
    }

    @Test
    public void firstOpenReclaimsOnlyRecognizedPriorProcessDirectories() throws Exception {
        Path temporaryRoot = temporary.newFolder("stale-spill").toPath();
        Path staleEmpty = temporaryRoot.resolve(
                "cold-query-11111111-1111-4111-8111-111111111111");
        Path stalePopulated = temporaryRoot.resolve(
                "metadata-query-22222222-2222-4222-8222-222222222222");
        Path unrelated = temporaryRoot.resolve("operator-notes");
        Path unsafePrefix = temporaryRoot.resolve(
                "Cold-query-33333333-3333-4333-8333-333333333333");
        Path uppercaseUuid = temporaryRoot.resolve(
                "cold-query-44444444-4444-4444-8444-44444444444A");
        Path recognizedRegularFile = temporaryRoot.resolve(
                "cold-query-55555555-5555-4555-8555-555555555555");
        Files.createDirectory(staleEmpty);
        Files.createDirectories(stalePopulated.resolve("nested"));
        Files.writeString(stalePopulated.resolve("nested/spill.tmp"), "stale spill");
        Files.createDirectory(unrelated);
        Files.writeString(unrelated.resolve("keep"), "operator data");
        Files.createDirectory(unsafePrefix);
        Files.createDirectory(uppercaseUuid);
        Files.writeString(recognizedRegularFile, "not an operation directory");

        Path liveDirectory;
        try (DuckDbResourcePolicy.Session session =
                     DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query")) {
            liveDirectory = session.operationDirectory();
            assertFalse(Files.exists(staleEmpty, LinkOption.NOFOLLOW_LINKS));
            assertFalse(Files.exists(stalePopulated, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(liveDirectory, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(unrelated, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(unsafePrefix, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(uppercaseUuid, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isRegularFile(recognizedRegularFile, LinkOption.NOFOLLOW_LINKS));
        }

        assertFalse(Files.exists(liveDirectory, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.exists(unrelated.resolve("keep"), LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isDirectory(unsafePrefix, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isDirectory(uppercaseUuid, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.isRegularFile(recognizedRegularFile, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    public void firstOpenPreservesRecognizedNameSymbolicLinkWithoutFollowingIt()
            throws Exception {
        Path temporaryRoot = temporary.newFolder("linked-stale-spill").toPath();
        Path outside = temporary.newFolder("linked-stale-target").toPath();
        Path proof = Files.writeString(outside.resolve("proof"), "must survive");
        Path linkedOperation = temporaryRoot.resolve(
                "cold-query-66666666-6666-4666-8666-666666666666");
        try {
            Files.createSymbolicLink(linkedOperation, outside);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }

        try (DuckDbResourcePolicy.Session ignored =
                     DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query")) {
            assertTrue(Files.isSymbolicLink(linkedOperation));
            assertTrue(Files.exists(proof, LinkOption.NOFOLLOW_LINKS));
        }

        assertTrue(Files.isSymbolicLink(linkedOperation));
        assertTrue(Files.exists(proof, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    public void staleCleanupFailurePreventsSessionAdmissionUntilCleanupCanSucceed()
            throws Exception {
        Path temporaryRoot = temporary.newFolder("stale-cleanup-failure-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwx------"));
        Path stale = temporaryRoot.resolve(
                "cold-query-77777777-7777-4777-8777-777777777777");
        Files.createDirectory(stale);
        Files.writeString(stale.resolve("spill.tmp"), "cannot remove yet");
        Files.setPosixFilePermissions(stale,
                PosixFilePermissions.fromString("r-x------"));

        try {
            assertThrows(java.io.IOException.class,
                    () -> DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query"));
            assertTrue(Files.exists(stale, LinkOption.NOFOLLOW_LINKS));
        } finally {
            Files.setPosixFilePermissions(stale,
                    PosixFilePermissions.fromString("rwx------"));
        }

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query")) {
            assertFalse(Files.exists(stale, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(recovered.operationDirectory(), LinkOption.NOFOLLOW_LINKS));
        }
    }

    @Test
    public void externallyHeldRootLockPreventsCleanupUntilReleased() throws Exception {
        Path temporaryRoot = temporary.newFolder("externally-locked-spill").toPath();
        Path stale = temporaryRoot.resolve(
                "cold-query-89999999-9999-4999-8999-999999999999");
        Path externallyLive = temporaryRoot.resolve(
                "metadata-query-90000000-0000-4000-8000-000000000000");
        Path staleProof = Files.createDirectories(stale.resolve("nested"))
                .resolve("spill.tmp");
        Files.writeString(staleProof, "prior process spill");
        Path liveProof = Files.writeString(
                Files.createDirectory(externallyLive).resolve("active.tmp"),
                "other process live spill");
        Path lockFile = temporaryRoot.resolve(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME);
        if (supportsPosix(lockFile.getParent()))
            Files.createFile(lockFile,
                    PosixFilePermissions.asFileAttribute(
                            PosixFilePermissions.fromString("rw-------")));
        else
            Files.createFile(lockFile);

        Path readyFile = temporaryRoot.resolve(".external-lock-ready");
        Process lockHolder = startExternalLockHolder(lockFile, readyFile);
        try {
            long readyDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!Files.exists(readyFile, LinkOption.NOFOLLOW_LINKS) &&
                    lockHolder.isAlive() && System.nanoTime() < readyDeadline)
                Thread.sleep(10L);
            assertTrue("External DuckDB root lock holder did not become ready",
                    Files.isRegularFile(readyFile, LinkOption.NOFOLLOW_LINKS));
            Files.delete(readyFile);

            IOException unavailable = assertThrows(IOException.class,
                    () -> DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query"));
            assertTrue(unavailable.getMessage().contains("another process"));
            assertTrue(Files.exists(staleProof, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.exists(liveProof, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isRegularFile(lockFile, LinkOption.NOFOLLOW_LINKS));
        } finally {
            lockHolder.getOutputStream().close();
            if (!lockHolder.waitFor(5, TimeUnit.SECONDS)) {
                lockHolder.destroyForcibly();
                assertTrue("External DuckDB root lock holder did not exit",
                        lockHolder.waitFor(5, TimeUnit.SECONDS));
            }
        }
        assertEquals("External DuckDB root lock holder failed", 0, lockHolder.exitValue());

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(config(temporaryRoot), "cold-query")) {
            assertFalse(Files.exists(stale, LinkOption.NOFOLLOW_LINKS));
            assertFalse(Files.exists(externallyLive, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.isDirectory(
                    recovered.operationDirectory(), LinkOption.NOFOLLOW_LINKS));
        }
        assertOnlyRootLock(temporaryRoot);
    }

    @Test
    public void unsafeExistingRootLockFailsClosedBeforeStaleCleanup() throws Exception {
        Path linkedRoot = temporary.newFolder("linked-root-lock-spill").toPath();
        Path linkedStale = linkedRoot.resolve(
                "cold-query-91111111-1111-4111-8111-111111111111");
        Path linkedProof = Files.writeString(
                Files.createDirectory(linkedStale).resolve("spill.tmp"), "must survive");
        Path lockTarget = Files.writeString(
                temporary.getRoot().toPath().resolve("lock-target"), "unrelated");
        Path linkedLock = linkedRoot.resolve(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME);
        try {
            Files.createSymbolicLink(linkedLock, lockTarget);
            assertThrows(IOException.class,
                    () -> DuckDbResourcePolicy.open(config(linkedRoot), "cold-query"));
            assertTrue(Files.isSymbolicLink(linkedLock));
            assertTrue(Files.exists(linkedProof, LinkOption.NOFOLLOW_LINKS));
            assertTrue(Files.exists(lockTarget, LinkOption.NOFOLLOW_LINKS));
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            // The POSIX permission case below still exercises fail-closed
            // validation on filesystems without symbolic links.
        }

        Path broadRoot = temporary.newFolder("broad-root-lock-spill").toPath();
        if (!supportsPosix(broadRoot))
            return;
        Files.setPosixFilePermissions(broadRoot,
                PosixFilePermissions.fromString("rwx------"));
        Path broadStale = broadRoot.resolve(
                "cold-query-92222222-2222-4222-8222-222222222222");
        Path broadProof = Files.writeString(
                Files.createDirectory(broadStale).resolve("spill.tmp"), "must survive");
        Path broadLock = broadRoot.resolve(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME);
        Files.createFile(broadLock,
                PosixFilePermissions.asFileAttribute(
                        PosixFilePermissions.fromString("rw-r--r--")));

        IOException broadFailure = assertThrows(IOException.class,
                () -> DuckDbResourcePolicy.open(config(broadRoot), "cold-query"));
        assertTrue(broadFailure.getMessage().contains("POSIX mode 0600"));
        assertTrue(Files.exists(broadProof, LinkOption.NOFOLLOW_LINKS));
        assertEquals(PosixFilePermissions.fromString("rw-r--r--"),
                Files.getPosixFilePermissions(broadLock, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    public void fifoRootLockFailsPromptlyBeforeStaleCleanup() throws Exception {
        Path temporaryRoot = temporary.newFolder("fifo-root-lock-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwx------"));
        Path stale = temporaryRoot.resolve(
                "cold-query-93333333-3333-4333-8333-333333333333");
        Path staleProof = Files.writeString(
                Files.createDirectory(stale).resolve("spill.tmp"), "must survive");
        Path lockFile = temporaryRoot.resolve(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME);
        Process fifoCreator;
        try {
            fifoCreator = new ProcessBuilder("mkfifo", lockFile.toString()).start();
        } catch (IOException unavailable) {
            return;
        }
        if (!fifoCreator.waitFor(5, TimeUnit.SECONDS)) {
            fifoCreator.destroyForcibly();
            fifoCreator.waitFor(5, TimeUnit.SECONDS);
            return;
        }
        if (fifoCreator.exitValue() != 0)
            return;
        Files.setPosixFilePermissions(lockFile,
                PosixFilePermissions.fromString("rw-------"));
        assertFalse(Files.isRegularFile(lockFile, LinkOption.NOFOLLOW_LINKS));

        TieredArchiveConfig config = config(temporaryRoot);
        AtomicBoolean sessionCreated = new AtomicBoolean();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        CountDownLatch finished = new CountDownLatch(1);
        Thread opener = new Thread(() -> {
            try (DuckDbResourcePolicy.Session ignored =
                         DuckDbResourcePolicy.open(config, "cold-query")) {
                sessionCreated.set(true);
            } catch (Throwable error) {
                failure.set(error);
            } finally {
                finished.countDown();
            }
        }, "fifo-root-lock-opener");
        opener.setDaemon(true);

        long started = System.nanoTime();
        opener.start();
        boolean completedPromptly = finished.await(2, TimeUnit.SECONDS);
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        if (!completedPromptly) {
            // Unblock a regressed write-only FIFO open so the daemon does not
            // retain test resources after the bounded assertion fails.
            try (FileChannel ignored = FileChannel.open(
                    lockFile, StandardOpenOption.READ, StandardOpenOption.WRITE,
                    LinkOption.NOFOLLOW_LINKS)) {
                // Opening a POSIX FIFO read/write is nonblocking.
            }
            finished.await(2, TimeUnit.SECONDS);
        }

        assertTrue("FIFO root lease blocked DuckDB admission for " + elapsedMillis + " ms",
                completedPromptly);
        assertFalse(sessionCreated.get());
        assertTrue("Expected a fail-closed FIFO lease error, got " + failure.get(),
                failure.get() instanceof IOException);
        assertTrue(failure.get().getMessage().contains("real regular file"));
        assertTrue(Files.exists(staleProof, LinkOption.NOFOLLOW_LINKS));
        assertTrue(Files.exists(lockFile, LinkOption.NOFOLLOW_LINKS));
    }

    @Test
    public void concurrentFirstOpensCannotReapNewLiveOperationDirectories() throws Exception {
        Path temporaryRoot = temporary.newFolder("concurrent-first-open-spill").toPath();
        Path stale = temporaryRoot.resolve(
                "cold-query-88888888-8888-4888-8888-888888888888");
        Files.createDirectory(stale);
        Files.writeString(stale.resolve("spill.tmp"), "prior process");
        TieredArchiveConfig config = config(temporaryRoot);
        int workerCount = 4;
        config.duckDbMaximumConcurrentSessions = workerCount;
        config.duckDbSessionAcquireTimeoutSeconds = 10;

        ExecutorService workers = Executors.newFixedThreadPool(workerCount);
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch opened = new CountDownLatch(workerCount);
        CountDownLatch release = new CountDownLatch(1);
        ConcurrentLinkedQueue<Path> liveDirectories = new ConcurrentLinkedQueue<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int worker = 0; worker < workerCount; worker++) {
            int workerNumber = worker;
            futures.add(workers.submit(() -> {
                boolean signalled = false;
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    try (DuckDbResourcePolicy.Session session =
                                 DuckDbResourcePolicy.open(config, "cold-query")) {
                        liveDirectories.add(session.operationDirectory());
                        opened.countDown();
                        signalled = true;
                        assertTrue(release.await(10, TimeUnit.SECONDS));
                    }
                } catch (Throwable error) {
                    workerFailure.compareAndSet(null, new AssertionError(
                            "Concurrent DuckDB opener " + workerNumber + " failed", error));
                } finally {
                    if (!signalled)
                        opened.countDown();
                }
            }));
        }

        start.countDown();
        try {
            assertTrue("Concurrent DuckDB sessions did not open",
                    opened.await(15, TimeUnit.SECONDS));
            assertNull(workerFailure.get());
            assertFalse(Files.exists(stale, LinkOption.NOFOLLOW_LINKS));
            assertEquals(workerCount, liveDirectories.size());
            for (Path liveDirectory : liveDirectories)
                assertTrue("A concurrent first opener reaped a live session directory",
                        Files.isDirectory(liveDirectory, LinkOption.NOFOLLOW_LINKS));
        } finally {
            release.countDown();
            workers.shutdown();
            assertTrue("Concurrent DuckDB opener threads did not exit",
                    workers.awaitTermination(15, TimeUnit.SECONDS));
        }
        for (Future<?> future : futures)
            future.get();
        assertNull(workerFailure.get());
        assertOnlyRootLock(temporaryRoot);
    }

    @Test
    public void scopedCancellationInterruptsLongQueryAndRemovesScratchDirectory()
            throws Exception {
        Path temporaryRoot = temporary.newFolder("cancelled-query-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        AtomicReference<ICancellableDataStore.CancellableOperation> operation =
                new AtomicReference<>();
        AtomicReference<Path> operationDirectory = new AtomicReference<>();
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        AtomicBoolean completedNormally = new AtomicBoolean();
        CountDownLatch queryReady = new CountDownLatch(1);
        CountDownLatch workerExited = new CountDownLatch(1);

        Thread worker = new Thread(() -> {
            try (ICancellableDataStore.CancellableOperation scope =
                         ConnectionManager.beginCancellableOperation();
                 DuckDbResourcePolicy.Session session =
                         DuckDbResourcePolicy.open(config, "metadata-query");
                 Statement statement = ConnectionManager.trackCancellableStatement(
                         session.connection().createStatement(), "DuckDB metadata query")) {
                operation.set(scope);
                operationDirectory.set(session.operationDirectory());
                queryReady.countDown();
                try (ResultSet rows = statement.executeQuery(
                        "SELECT sum(hash(i)) FROM range(0, 1000000000000) values(i)")) {
                    rows.next();
                }
                completedNormally.set(true);
            } catch (Throwable error) {
                workerFailure.set(error);
            } finally {
                workerExited.countDown();
            }
        }, "long-cold-statistics-query");
        worker.setDaemon(true);
        worker.start();

        long elapsedMillis;
        try {
            assertTrue("The DuckDB statement must be ready before cancellation",
                    queryReady.await(5, TimeUnit.SECONDS));
            Thread.sleep(200L);
            long started = System.nanoTime();
            operation.get().cancel();
            assertTrue("DuckDB cancellation must release the statistics worker inside the stop budget",
                    workerExited.await(5, TimeUnit.SECONDS));
            elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        } finally {
            ICancellableDataStore.CancellableOperation active = operation.get();
            if (worker.isAlive() && active != null) {
                try {
                    active.cancel();
                } catch (RuntimeException ignored) {
                    // Preserve the original assertion while still requesting
                    // best-effort cleanup of a failed cancellation fixture.
                }
            }
            worker.interrupt();
            worker.join(5_000L);
        }

        assertFalse("The cancelled statistics worker must terminate", worker.isAlive());
        assertFalse("The cancelled cold query must not complete normally", completedNormally.get());
        assertTrue("The in-flight DuckDB query must observe cancellation",
                workerFailure.get() != null);
        assertTrue("DuckDB cancellation must preserve headroom inside the 5 second statistics stop budget: "
                + elapsedMillis + " ms", elapsedMillis < 4_000L);
        assertFalse("Closing the cancelled session must remove its private scratch directory",
                Files.exists(operationDirectory.get(), LinkOption.NOFOLLOW_LINKS));
        assertOnlyRootLock(temporaryRoot);
    }

    @Test
    public void processWideAdmissionFailsBusyThenRecoversAfterClose() throws Exception {
        Path temporaryRoot = temporary.newFolder("admission-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        config.duckDbMaximumConcurrentSessions = 1;
        config.duckDbSessionAcquireTimeoutSeconds = 1;

        DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(config, "cold-query");
        try {
            long started = System.nanoTime();
            SQLTransientException busy = assertThrows(SQLTransientException.class,
                    () -> DuckDbResourcePolicy.open(config, "archive-export"));
            assertTrue(busy.getMessage().contains("capacity is busy"));
            assertTrue(System.nanoTime() - started >=
                    java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(750));
        } finally {
            first.close();
        }

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(config, "archive-export")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    @Test
    public void smallestRegisteredLimitConstrainsDifferentlyConfiguredStores() throws Exception {
        Path temporaryRoot = temporary.newFolder("mixed-admission-spill").toPath();
        TieredArchiveConfig broad = config(temporaryRoot);
        broad.duckDbMaximumConcurrentSessions = 2;
        TieredArchiveConfig strict = config(temporaryRoot);
        strict.duckDbMaximumConcurrentSessions = 1;
        strict.duckDbSessionAcquireTimeoutSeconds = 1;

        try (DuckDbResourcePolicy.Session first = DuckDbResourcePolicy.open(broad, "cold-query");
             DuckDbResourcePolicy.Session second = DuckDbResourcePolicy.open(broad, "archive-export")) {
            assertThrows(SQLTransientException.class,
                    () -> DuckDbResourcePolicy.open(strict, "archive-verify"));
        }

        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(strict, "archive-verify")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    @Test
    public void backgroundCqlCountReservesInteractiveSessionAndYieldsToNonBackgroundWaiters() throws Exception {
        Path temporaryRoot = temporary.newFolder("background-isolation-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 1;

        try (DuckDbResourcePolicy.Session firstCount = DuckDbResourcePolicy.open(config, "cold-cql-count")) {
            assertFalse("Background count must not yield while no interactive waiter is queued",
                    firstCount.shouldYield());

            // A second concurrent cold-cql-count must NOT consume the reserved interactive slot.
            SQLTransientException secondCountBlocked = assertThrows(SQLTransientException.class,
                    () -> DuckDbResourcePolicy.open(config, "cold-cql-count"));
            assertTrue(secondCountBlocked.getMessage().contains("capacity is busy"));

            // An interactive cold-query must immediately acquire the reserved 2nd slot even while
            // a second cold-cql-count waiter is queued ahead of it.
            CountDownLatch countWaiterStarted = new CountDownLatch(1);
            CountDownLatch countWaiterDone = new CountDownLatch(1);
            AtomicReference<Throwable> countWaiterError = new AtomicReference<>();
            TieredArchiveConfig longerWait = config(temporaryRoot);
            longerWait.duckDbMaximumConcurrentSessions = 2;
            longerWait.duckDbSessionAcquireTimeoutSeconds = 5;

            Thread secondCountThread = new Thread(() -> {
                countWaiterStarted.countDown();
                try (DuckDbResourcePolicy.Session secondCount =
                             DuckDbResourcePolicy.open(longerWait, "cold-cql-count")) {
                    assertTrue(Files.isDirectory(secondCount.operationDirectory()));
                } catch (Throwable err) {
                    countWaiterError.set(err);
                } finally {
                    countWaiterDone.countDown();
                }
            }, "queued-background-count");
            secondCountThread.setDaemon(true);
            secondCountThread.start();
            assertTrue(countWaiterStarted.await(2, TimeUnit.SECONDS));
            Thread.sleep(75L);

            long queryStart = System.nanoTime();
            try (DuckDbResourcePolicy.Session interactiveQuery =
                         DuckDbResourcePolicy.open(longerWait, "cold-query")) {
                long acquireMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - queryStart);
                assertTrue("Interactive cold-query must bypass capped cold-cql-count waiter immediately, took "
                        + acquireMs + " ms", acquireMs < 750L);

                // While both slots are held (1 cold-cql-count + 1 cold-query) and another cold-query
                // queues, firstCount.shouldYield() must flip true so multi-batch counts yield cleanly.
                CountDownLatch secondQueryQueued = new CountDownLatch(1);
                CountDownLatch secondQueryAcquired = new CountDownLatch(1);
                Thread secondInteractiveThread = new Thread(() -> {
                    secondQueryQueued.countDown();
                    try (DuckDbResourcePolicy.Session secondInteractive =
                                 DuckDbResourcePolicy.open(longerWait, "cold-query")) {
                        secondQueryAcquired.countDown();
                    } catch (Throwable ignored) {
                    }
                }, "second-interactive-query");
                secondInteractiveThread.setDaemon(true);
                secondInteractiveThread.start();
                assertTrue(secondQueryQueued.await(2, TimeUnit.SECONDS));
                long yieldDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
                while (!firstCount.shouldYield() && System.nanoTime() < yieldDeadline) {
                    Thread.sleep(10L);
                }
                assertTrue("Active cold-cql-count must signal cooperative yield when cold-query is waiting",
                        firstCount.shouldYield());

                // Releasing firstCount must admit the waiting cold-query AHEAD of the waiting cold-cql-count.
                firstCount.close();
                assertTrue("Waiting cold-query must acquire ahead of queued cold-cql-count",
                        secondQueryAcquired.await(2, TimeUnit.SECONDS));
                secondInteractiveThread.join(2_000L);
            }

            assertTrue("Queued cold-cql-count must succeed after interactive queries complete",
                    countWaiterDone.await(3, TimeUnit.SECONDS));
            assertEquals(null, countWaiterError.get());
        }
    }

    @Test
    public void rejectsDisabledResourceBoundsAndLinkedTemporaryRoot() throws Exception {
        Path temporaryRoot = temporary.newFolder("bad-spill").toPath();
        TieredArchiveConfig invalidBounds = config(temporaryRoot);
        invalidBounds.duckDbMemoryLimitMegabytes = 15;
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(invalidBounds, "cold-query"));

        Path target = temporary.newFolder("real-spill").toPath();
        Path link = temporary.getRoot().toPath().resolve("linked-spill");
        try {
            Files.createSymbolicLink(link, target);
        } catch (UnsupportedOperationException | java.nio.file.FileSystemException unsupported) {
            return;
        }
        TieredArchiveConfig linkedConfig = config(link);
        assertThrows(IllegalStateException.class,
                () -> DuckDbResourcePolicy.open(linkedConfig, "cold-query"));
    }

    @Test
    public void rejectsOverlappingArchiveAndTemporaryRoots() throws Exception {
        Path temporaryRoot = temporary.newFolder("overlap-spill").toPath();
        TieredArchiveConfig config = config(temporaryRoot);
        config.rootDirectory = temporaryRoot.resolve("archive-inside-spill").toString();
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));

        config.rootDirectory = temporaryRoot.resolveSibling("archive-parent").toString();
        config.duckDbTemporaryDirectory = Path.of(config.rootDirectory).resolve("spill-inside-archive").toString();
        assertThrows(IllegalArgumentException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));
    }

    @Test
    public void rejectsExistingPosixTemporaryRootWithBroadPermissions() throws Exception {
        Path temporaryRoot = temporary.newFolder("broad-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwxr-xr-x"));
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = temporaryRoot.resolveSibling("broad-archive").toString();
        config.duckDbTemporaryDirectory = temporaryRoot.toString();
        assertThrows(IllegalStateException.class,
                () -> DuckDbResourcePolicy.open(config, "cold-query"));
    }

    @Test
    public void closeSurfacesOperationDirectoryCleanupFailure() throws Exception {
        Path temporaryRoot = temporary.newFolder("cleanup-failure-spill").toPath();
        if (!supportsPosix(temporaryRoot))
            return;
        Files.setPosixFilePermissions(temporaryRoot,
                PosixFilePermissions.fromString("rwx------"));
        DuckDbResourcePolicy.Session session = DuckDbResourcePolicy.open(
                config(temporaryRoot), "cold-query");
        Path operationDirectory = session.operationDirectory();
        Files.writeString(operationDirectory.resolve("cleanup-proof"), "must be removed");
        Files.setPosixFilePermissions(operationDirectory,
                PosixFilePermissions.fromString("r-x------"));
        try {
            IllegalStateException failure = assertThrows(
                    IllegalStateException.class, session::close);
            assertTrue(failure.getMessage().contains("Cannot close bounded DuckDB session"));
            assertTrue(Files.exists(operationDirectory, LinkOption.NOFOLLOW_LINKS));
        } finally {
            Files.setPosixFilePermissions(operationDirectory,
                    PosixFilePermissions.fromString("rwx------"));
            Files.deleteIfExists(operationDirectory.resolve("cleanup-proof"));
            Files.deleteIfExists(operationDirectory);
        }

        TieredArchiveConfig recovery = config(temporaryRoot);
        recovery.duckDbMaximumConcurrentSessions = 1;
        recovery.duckDbSessionAcquireTimeoutSeconds = 1;
        try (DuckDbResourcePolicy.Session recovered =
                     DuckDbResourcePolicy.open(recovery, "cold-query")) {
            assertTrue(Files.isDirectory(recovered.operationDirectory()));
        }
    }

    private static TieredArchiveConfig config(Path temporaryRoot) throws Exception {
        if (Files.exists(temporaryRoot) && supportsPosix(temporaryRoot))
            Files.setPosixFilePermissions(temporaryRoot,
                    PosixFilePermissions.fromString("rwx------"));
        TieredArchiveConfig config = new TieredArchiveConfig();
        config.rootDirectory = temporaryRoot.resolveSibling(
                temporaryRoot.getFileName() + "-archive").toString();
        config.duckDbMemoryLimitMegabytes = 64;
        config.duckDbThreads = 1;
        config.duckDbMaximumTemporaryMegabytes = 96;
        config.duckDbMaximumConcurrentSessions = 2;
        config.duckDbSessionAcquireTimeoutSeconds = 2;
        config.duckDbTemporaryDirectory = temporaryRoot.toString();
        return config;
    }

    private static Process startExternalLockHolder(Path lockFile, Path readyFile)
            throws IOException {
        String executableName = System.getProperty("os.name").startsWith("Windows") ?
                "java.exe" : "java";
        Path java = Path.of(System.getProperty("java.home"), "bin", executableName);
        return new ProcessBuilder(
                java.toString(),
                "-cp", System.getProperty("java.class.path"),
                ExternalRootLockHolder.class.getName(),
                lockFile.toString(),
                readyFile.toString())
                .redirectErrorStream(true)
                .start();
    }

    public static final class ExternalRootLockHolder {
        private ExternalRootLockHolder() {
        }

        public static void main(String[] arguments) throws Exception {
            Path lockFile = Path.of(arguments[0]);
            Path readyFile = Path.of(arguments[1]);
            try (FileChannel channel = FileChannel.open(
                        lockFile, StandardOpenOption.WRITE, LinkOption.NOFOLLOW_LINKS);
                 FileLock ignored = channel.lock()) {
                Files.writeString(readyFile, "ready", StandardOpenOption.CREATE_NEW);
                System.in.read();
            }
        }
    }

    private static void assertOnlyRootLock(Path temporaryRoot) throws Exception {
        Path expected = temporaryRoot.resolve(DuckDbResourcePolicy.ROOT_LOCK_FILE_NAME);
        try (var children = Files.list(temporaryRoot)) {
            assertEquals("DuckDB scratch must contain only its root lease",
                    List.of(expected), children.toList());
        }
        assertTrue(Files.isRegularFile(expected, LinkOption.NOFOLLOW_LINKS));
        assertFalse(Files.isSymbolicLink(expected));
        if (supportsPosix(expected))
            assertEquals(PosixFilePermissions.fromString("rw-------"),
                    Files.getPosixFilePermissions(expected, LinkOption.NOFOLLOW_LINKS));
    }

    private static boolean supportsPosix(Path path) {
        return Files.getFileAttributeView(
                path, PosixFileAttributeView.class, LinkOption.NOFOLLOW_LINKS) != null;
    }

    private static long numericSetting(Statement statement, String name) throws Exception {
        return Long.parseLong(textSetting(statement, name));
    }

    private static long byteSetting(Statement statement, String name) throws Exception {
        String value = textSetting(statement, name).trim().toUpperCase(java.util.Locale.ROOT);
        long multiplier = 1;
        if (value.endsWith("KIB")) {
            multiplier = 1024L;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("MIB")) {
            multiplier = 1024L * 1024;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("GIB")) {
            multiplier = 1024L * 1024 * 1024;
            value = value.substring(0, value.length() - 3);
        } else if (value.endsWith("KB")) {
            multiplier = 1000L;
            value = value.substring(0, value.length() - 2);
        } else if (value.endsWith("MB")) {
            multiplier = 1000L * 1000;
            value = value.substring(0, value.length() - 2);
        } else if (value.endsWith("GB")) {
            multiplier = 1000L * 1000 * 1000;
            value = value.substring(0, value.length() - 2);
        }
        return new java.math.BigDecimal(value.trim())
                .multiply(java.math.BigDecimal.valueOf(multiplier)).longValue();
    }

    private static String textSetting(Statement statement, String name) throws Exception {
        try (ResultSet result = statement.executeQuery("SELECT current_setting('" + name + "')")) {
            assertTrue(result.next());
            return result.getString(1);
        }
    }
}
