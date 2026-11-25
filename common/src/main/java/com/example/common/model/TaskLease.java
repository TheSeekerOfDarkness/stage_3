package com.example.common.model;

import java.io.Serializable;

/**
 * Lease record tracking an in-flight crawl or index task for failure takeover.
 */
public class TaskLease implements Serializable {
    private static final long serialVersionUID = 1L;

    public String taskId;
    public String taskType;
    public int bookId;
    public String runId;
    public String ownerWorkerId;
    public String ownerMemberUuid;
    public long claimedAtMs;
    public String payloadJson;

    public TaskLease() {
    }

    public TaskLease(String taskId, String taskType, int bookId, String runId,
                     String ownerWorkerId, String ownerMemberUuid, long claimedAtMs, String payloadJson) {
        this.taskId = taskId;
        this.taskType = taskType;
        this.bookId = bookId;
        this.runId = runId;
        this.ownerWorkerId = ownerWorkerId;
        this.ownerMemberUuid = ownerMemberUuid;
        this.claimedAtMs = claimedAtMs;
        this.payloadJson = payloadJson;
    }
}
