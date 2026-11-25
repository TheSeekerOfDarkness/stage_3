package com.example.common;

import com.example.common.model.*;
import com.example.common.obs.Checksums;
import com.example.common.obs.RequestIds;
import com.example.common.obs.StructuredLogger;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.*;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ObservabilityAndModelsTest {

    @TempDir
    Path tempDir;

    @Test
    void checksumsAreDeterministicAcrossStringBytesAndFile() throws IOException {
        String content = "Alice was beginning to get very tired.";
        Path file = tempDir.resolve("sample.txt");
        Files.writeString(file, content, StandardCharsets.UTF_8);

        String fromString = Checksums.sha256Hex(content);
        String fromBytes = Checksums.sha256Hex(content.getBytes(StandardCharsets.UTF_8));
        String fromFile = Checksums.sha256Hex(file);

        assertEquals(64, fromString.length());
        assertEquals(fromString, fromBytes);
        assertEquals(fromString, fromFile);

        String docHash1 = Checksums.documentSha256("Title: Alice", content);
        String docHash2 = Checksums.documentSha256("Title: Alice", content + " ");
        assertNotEquals(docHash1, docHash2);
    }

    @Test
    void requestIdsGenerateAndPreserveIncomingHeaders() {
        assertTrue(RequestIds.newTaskId("crawl").startsWith("crawl-"));
        assertTrue(RequestIds.newRequestId().startsWith("req-"));
        assertEquals("ext-123", RequestIds.resolveOrCreate(" ext-123 "));
        assertTrue(RequestIds.resolveOrCreate("   ").startsWith("req-"));
    }

    @Test
    void structuredLoggerWritesValidJsonlLines() throws IOException {
        Path logFile = tempDir.resolve("logs/worker-1.jsonl");
        try (StructuredLogger logger = new StructuredLogger(logFile, "worker-1", "192.168.1.10")) {
            logger.log("crawl_done", Map.of("task_id", "crawl-1", "book_id", 11, "latency_ms", 42));
            logger.log("search_served", Map.of("request_id", "req-abc", "total_matches", 3));
        }

        List<String> lines = Files.readAllLines(logFile, StandardCharsets.UTF_8);
        assertEquals(2, lines.size());

        Gson gson = new Gson();
        Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
        Map<String, Object> first = gson.fromJson(lines.get(0), mapType);
        assertEquals("worker-1", first.get("worker_id"));
        assertEquals("192.168.1.10", first.get("host"));
        assertEquals("crawl_done", first.get("event"));
        assertEquals("crawl-1", first.get("task_id"));
        assertNotNull(first.get("ts_ms"));
    }

    @Test
    void allModelsSupportJavaSerializationRoundTrip() throws IOException, ClassNotFoundException {
        Book book = new Book(11, "Alice", "Lewis Carroll", "en", 1865, "books/11", "w1", 2L, "abc");
        SearchResult sr = book.toSearchResult();
        DocumentRecord doc = new DocumentRecord(11, "H", "B", "abc", "books/11", "w1", "t1", "r1", 100L);
        CrawlTask crawlTask = new CrawlTask("t1", 11, "r1", "http://mirror/11", 1, 100L);
        IndexTask indexTask = new IndexTask("t2", 11, "r1", "abc", "h/11", "b/11", 1, 101L);
        TaskLease lease = new TaskLease("t2", "index", 11, "r1", "w1", "uuid-1", 102L, "{}");
        IndexDelta delta = new IndexDelta(book, List.of("alice", "wonderland"), "abc", "w1", "t2", "r1", 103L);

        List<Serializable> objects = List.of(book, sr, doc, crawlTask, indexTask, lease, delta);
        for (Serializable original : objects) {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                oos.writeObject(original);
            }
            try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
                Object restored = ois.readObject();
                assertEquals(original.getClass(), restored.getClass());
            }
        }
    }
}
