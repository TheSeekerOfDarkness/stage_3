package com.example.worker.backend;

/**
 * Result of verifying whether a worker's search role can actively serve queries.
 */
public final class ReadinessStatus {
    public final boolean ready;
    public final String reason;
    public final long indexVersion;
    public final int indexedBookCount;

    public ReadinessStatus(boolean ready, String reason, long indexVersion, int indexedBookCount) {
        this.ready = ready;
        this.reason = reason;
        this.indexVersion = indexVersion;
        this.indexedBookCount = indexedBookCount;
    }
}
