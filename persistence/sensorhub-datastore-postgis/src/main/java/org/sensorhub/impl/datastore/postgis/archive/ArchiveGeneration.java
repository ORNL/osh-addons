package org.sensorhub.impl.datastore.postgis.archive;

import java.nio.file.Path;
import java.time.Instant;
import java.util.UUID;

/** Immutable catalog projection used by the query planner and lifecycle tools. */
public record ArchiveGeneration(
        UUID id,
        String profile,
        Instant start,
        Instant end,
        Path objectPath,
        String sha256,
        long rowCount,
        long minimumId,
        long maximumId,
        String state,
        boolean parityVerified,
        Instant maxResultTime,
        Instant minResultTime) {

    public ArchiveGeneration(
            UUID id,
            String profile,
            Instant start,
            Instant end,
            Path objectPath,
            String sha256,
            long rowCount,
            long minimumId,
            long maximumId,
            String state,
            boolean parityVerified,
            Instant maxResultTime) {
        this(id, profile, start, end, objectPath, sha256, rowCount,
                minimumId, maximumId, state, parityVerified, maxResultTime, null);
    }

    public ArchiveGeneration(
            UUID id,
            String profile,
            Instant start,
            Instant end,
            Path objectPath,
            String sha256,
            long rowCount,
            long minimumId,
            long maximumId,
            String state,
            boolean parityVerified) {
        this(id, profile, start, end, objectPath, sha256, rowCount,
                minimumId, maximumId, state, parityVerified, null, null);
    }
}
