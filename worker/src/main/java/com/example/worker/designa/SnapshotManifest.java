package com.example.worker.designa;

import java.io.Serializable;
import java.util.List;

/**
 * Metadata describing a published immutable SQLite snapshot in object storage.
 */
public class SnapshotManifest implements Serializable {
    private static final long serialVersionUID = 1L;

    public long version;
    public String objectKey;
    public String sha256;
    public int bookCount;
    public List<Integer> bookIds;
    public String publishedBy;
    public long publishedAtMs;

    public SnapshotManifest() {
    }

    public SnapshotManifest(long version,
                            String objectKey,
                            String sha256,
                            int bookCount,
                            List<Integer> bookIds,
                            String publishedBy,
                            long publishedAtMs) {
        this.version = version;
        this.objectKey = objectKey;
        this.sha256 = sha256;
        this.bookCount = bookCount;
        this.bookIds = bookIds;
        this.publishedBy = publishedBy;
        this.publishedAtMs = publishedAtMs;
    }
}
