package com.github.lechandonga.contentaddressedobjectstorejava.store.engine;

import com.github.lechandonga.contentaddressedobjectstorejava.store.api.ObjectVersion;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/** Jackson-serializable shape of an object's metadata file. */
final class MetadataFile {
    public String objectId;
    public long latestVersion;
    public List<Entry> versions = new ArrayList<>();

    static final class Entry {
        public long version;
        public String contentHash;
        public long size;
        public String committedAt;

        Entry() {
        }

        Entry(long version, String contentHash, long size, Instant committedAt) {
            this.version = version;
            this.contentHash = contentHash;
            this.size = size;
            this.committedAt = committedAt.toString();
        }

        ObjectVersion toApi() {
            return new ObjectVersion(version, contentHash, size, Instant.parse(committedAt));
        }
    }
}
