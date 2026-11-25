package com.example.worker.backend;

import com.example.worker.WorkerRole;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Abstraction implemented by Design A (local SQLite snapshots), Design B (Hazelcast),
 * and the in-memory test backend.
 */
public interface ClusterBackend {
    String designMode();

    void start() throws Exception;

    void stop();

    void onRolesChanged(Set<WorkerRole> activeRoles);

    String enqueueCrawlTask(int bookId, String runId, String sourceUrl) throws Exception;

    List<Map<String, Object>> enqueueCrawlBatch(List<Integer> bookIds, String runId, String sourceUrlTemplate) throws Exception;

    Map<String, Object> executeDirectIngest(int bookId, String runId, String sourceUrl) throws Exception;

    Map<String, Object> executeDirectIndex(int bookId, String runId) throws Exception;

    SearchResponseData search(List<String> distinctTerms,
                              String author,
                              String language,
                              Integer year,
                              int limit,
                              int offset) throws Exception;

    ReadinessStatus checkSearchReadiness();

    Map<String, Object> clusterDiagnostics();

    Map<String, Object> executeDistributedDemo(String payload) throws Exception;

    Map<String, Object> integritySummary();
}
