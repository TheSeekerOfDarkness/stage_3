package com.example.common.model;

import java.io.Serializable;
import java.util.List;

/**
 * Immutable per-book indexing output merged into snapshots (Design A).
 */
public class IndexDelta implements Serializable {
    private static final long serialVersionUID = 1L;

    public Book book;
    public List<String> distinctTerms;
    public String docSha256;
    public String indexedBy;
    public String taskId;
    public String runId;
    public long indexedAtMs;

    public IndexDelta() {
    }

    public IndexDelta(Book book, List<String> distinctTerms, String docSha256,
                      String indexedBy, String taskId, String runId, long indexedAtMs) {
        this.book = book;
        this.distinctTerms = distinctTerms;
        this.docSha256 = docSha256;
        this.indexedBy = indexedBy;
        this.taskId = taskId;
        this.runId = runId;
        this.indexedAtMs = indexedAtMs;
    }
}
