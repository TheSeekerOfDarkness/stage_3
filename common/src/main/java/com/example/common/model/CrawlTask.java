package com.example.common.model;

import java.io.Serializable;

/**
 * Work item representing a book download and split task for an ingest worker.
 */
public class CrawlTask implements Serializable {
    private static final long serialVersionUID = 1L;

    public String taskId;
    public int bookId;
    public String runId;
    public String sourceUrl;
    public int attempt;
    public long createdAtMs;

    public CrawlTask() {
    }

    public CrawlTask(String taskId, int bookId, String runId, String sourceUrl, int attempt, long createdAtMs) {
        this.taskId = taskId;
        this.bookId = bookId;
        this.runId = runId;
        this.sourceUrl = sourceUrl;
        this.attempt = attempt;
        this.createdAtMs = createdAtMs;
    }
}
