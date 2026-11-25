package com.example.worker.designa;

/**
 * Distributed claim and leader lock abstraction for Design A coordination.
 */
public interface LeaderElector {
    boolean tryClaimCrawl(int bookId, String workerId);

    boolean tryAcquirePublisherLock();

    boolean renewPublisherLock();

    void releasePublisherLock();

    String currentPublisherLeader();
}
