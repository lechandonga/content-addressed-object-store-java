package com.github.lechandonga.contentaddressedobjectstorejava.store.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Externalized configuration under the {@code objectstore.*} prefix.
 */
@ConfigurationProperties(prefix = "objectstore")
public class ObjectStoreProperties {

    /** Root data directory for blobs, metadata, locks, staging and quarantine. */
    private Path dataDir = Path.of("data", "object-store");

    /** Minimum age of unreferenced blobs before GC may reclaim them. */
    private Duration gcGracePeriod = Duration.ofMinutes(10);

    /** Minimum age of staging files swept at startup. */
    private Duration tempSweepMinAge = Duration.ofMinutes(10);

    public Path getDataDir() {
        return dataDir;
    }

    public void setDataDir(Path dataDir) {
        this.dataDir = dataDir;
    }

    public Duration getGcGracePeriod() {
        return gcGracePeriod;
    }

    public void setGcGracePeriod(Duration gcGracePeriod) {
        this.gcGracePeriod = gcGracePeriod;
    }

    public Duration getTempSweepMinAge() {
        return tempSweepMinAge;
    }

    public void setTempSweepMinAge(Duration tempSweepMinAge) {
        this.tempSweepMinAge = tempSweepMinAge;
    }
}
