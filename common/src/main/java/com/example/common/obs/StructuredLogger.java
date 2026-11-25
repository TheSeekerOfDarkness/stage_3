package com.example.common.obs;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Thread-safe JSONL append-only event logger for node-by-task, discovery, and chaos evidence.
 */
public class StructuredLogger implements AutoCloseable {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final String workerId;
    private final String host;
    private final BufferedWriter writer;

    public StructuredLogger(Path logPath, String workerId, String host) throws IOException {
        this.workerId = workerId;
        this.host = host;
        Path parent = logPath.toAbsolutePath().getParent();
        if (parent != null) {
            Files.createDirectories(parent);
        }
        this.writer = Files.newBufferedWriter(
            logPath,
            StandardCharsets.UTF_8,
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.APPEND
        );
    }

    public synchronized void log(String event, Map<String, Object> fields) {
        Map<String, Object> record = new LinkedHashMap<>();
        record.put("ts_ms", System.currentTimeMillis());
        record.put("worker_id", workerId);
        record.put("host", host);
        record.put("event", event);
        if (fields != null) {
            for (Map.Entry<String, Object> entry : fields.entrySet()) {
                if (entry.getValue() != null) {
                    record.put(entry.getKey(), entry.getValue());
                }
            }
        }
        try {
            writer.write(GSON.toJson(record));
            writer.newLine();
            writer.flush();
        } catch (IOException e) {
            System.err.println("[StructuredLogger] Failed to write log line: " + e.getMessage());
        }
    }

    @Override
    public synchronized void close() throws IOException {
        writer.close();
    }
}
