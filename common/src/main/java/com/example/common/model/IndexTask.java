package com.example.common.model;

import java.io.Serializable;

/**
 * Work item emitted only after the document is durably stored and ready for indexing.
 */
public class IndexTask implements Serializable {
    private static final long serialVersionUID = 1L;

    public String taskId;
    public int bookId;
    public String runId;
    public String docSha256;
    public String headerKey;
    public String bodyKey;
    public int attempt;
    public long createdAtMs;

    public IndexTask() {
    }

    public IndexTask(String taskId, int bookId, String runId, String docSha256,
                     String headerKey, String bodyKey, int attempt, long createdAtMs) {
        this.taskId = taskId;
        this.bookId = bookId;
        this.runId = runId;
        this.docSha256 = docSha256;
        this.headerKey = headerKey;
        this.bodyKey = bodyKey;
        this.attempt = attempt;
        this.createdAtMs = createdAtMs;
    }
}
