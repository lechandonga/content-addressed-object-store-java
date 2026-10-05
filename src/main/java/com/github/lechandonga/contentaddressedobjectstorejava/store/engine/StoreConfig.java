package com.github.lechandonga.contentaddressedobjectstorejava.store.engine;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Configuration knobs for {@link LocalObjectStore}. Every value has a safe
 * default; only the data directory is expected to be overridden in practice.
 *
 * @param dataDir             root directory holding all on-disk state
 * @param gcGracePeriod       minimum age a blob must reach before garbage
 *                            collection is allowed to reclaim it. Protects
 *                            blobs that are staged-but-not-yet-committed while
 *                            a concurrent GC pass is scanning.
 * @param tempSweepMinAge     minimum age of staging files swept at startup
 *                            (files younger than this may belong to a still
 *                            running writer sharing the directory)
 */
public record StoreConfig(Path dataDir, Duration gcGracePeriod, Duration tempSweepMinAge) {

    public static final Duration DEFAULT_GC_GRACE = Duration.ofMinutes(10);
    public static final Duration DEFAULT_TEMP_SWEEP_MIN_AGE = Duration.ofMinutes(10);

    public StoreConfig {
        if (dataDir == null) {
            throw new IllegalArgumentException("dataDir must not be null");
        }
        if (gcGracePeriod == null) {
            gcGracePeriod = DEFAULT_GC_GRACE;
        }
        if (tempSweepMinAge == null) {
            tempSweepMinAge = DEFAULT_TEMP_SWEEP_MIN_AGE;
        }
    }

    public StoreConfig(Path dataDir) {
        this(dataDir, DEFAULT_GC_GRACE, DEFAULT_TEMP_SWEEP_MIN_AGE);
    }
}
