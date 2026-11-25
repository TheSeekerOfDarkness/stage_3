package com.example.common.model;

import java.io.Serializable;

/**
 * Durable document payload stored after ingestion and verified by SHA-256 checksum.
 */
public class DocumentRecord implements Serializable {
    private static final long serialVersionUID = 1L;

    public int bookId;
    public String header;
    public String body;
    public String docSha256;
    public String sourcePath;
    public String ingestedBy;
    public String taskId;
    public String runId;
    public long ingestedAtMs;

    public DocumentRecord() {
    }

    public DocumentRecord(int bookId, String header, String body, String docSha256,
                          String sourcePath, String ingestedBy, String taskId, String runId, long ingestedAtMs) {
        this.bookId = bookId;
        this.header = header;
        this.body = body;
        this.docSha256 = docSha256;
        this.sourcePath = sourcePath;
        this.ingestedBy = ingestedBy;
        this.taskId = taskId;
        this.runId = runId;
        this.ingestedAtMs = ingestedAtMs;
    }
}
