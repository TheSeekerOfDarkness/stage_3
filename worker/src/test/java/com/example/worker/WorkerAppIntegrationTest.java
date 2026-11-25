package com.example.worker;

import com.example.common.obs.StructuredLogger;
import com.example.worker.backend.InMemoryBackend;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.OutputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.junit.jupiter.api.Assertions.*;

class WorkerAppIntegrationTest {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("unchecked")
    void supportsDynamicRolesConsulReadinessAndFullPipeline() throws Exception {
        List<String> consulEvents = new CopyOnWriteArrayList<>();

        HttpServer mockConsul = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockConsul.createContext("/v1/agent/service/register", exchange -> {
            consulEvents.add("register");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        mockConsul.createContext("/v1/agent/service/deregister/", exchange -> {
            consulEvents.add("deregister");
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        mockConsul.start();

        String sampleBook = """
            Title: Alice's Adventures in Wonderland
            Author: Lewis Carroll
            Release Date: June 2008 [EBook #11]
            Language: English
            *** START OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            Alice was beginning to get very tired of sitting by her sister on the bank.
            *** END OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            """;

        HttpServer mockGutenberg = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockGutenberg.createContext("/books/", exchange -> {
            byte[] bytes = sampleBook.getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, bytes.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(bytes);
            }
        });
        mockGutenberg.start();

        Path logPath = tempDir.resolve("logs/worker-test.jsonl");
        WorkerConfig config = WorkerConfig.fromMap(Map.of(
            "WORKER_ID", "w-test-1",
            "HOST_NAME", "lab-host-1",
            "HOST_IP", "127.0.0.1",
            "PORT", "0",
            "DESIGN_MODE", "in-memory",
            "WORKER_ROLES", "search",
            "DRAIN_DELAY_MS", "10",
            "CONSUL_ENABLED", "true",
            "CONSUL_URL", "http://127.0.0.1:" + mockConsul.getAddress().getPort(),
            "GUTENBERG_URL_TEMPLATE", "http://127.0.0.1:" + mockGutenberg.getAddress().getPort() + "/books/%d"
        ));

        HttpClient client = HttpClient.newHttpClient();

        try (StructuredLogger logger = new StructuredLogger(logPath, config.workerId, config.hostName);
             WorkerApp app = new WorkerApp(config, new InMemoryBackend(config, logger), logger)) {
            app.start();
            String base = "http://127.0.0.1:" + app.getPort();

            // 1. Health, info, and initial readiness (search enabled)
            HttpResponse<String> health = get(client, base + "/internal/health");
            assertEquals(200, health.statusCode());

            HttpResponse<String> ready1 = get(client, base + "/internal/ready");
            assertEquals(200, ready1.statusCode());
            assertTrue(consulEvents.contains("register"));

            HttpResponse<String> info = get(client, base + "/internal/info");
            assertEquals(200, info.statusCode());
            Map<String, Object> infoBody = GSON.fromJson(info.body(), MAP_TYPE);
            assertEquals("w-test-1", infoBody.get("worker_id"));
            assertEquals(List.of("search"), infoBody.get("roles"));

            // 2. Drain -> 503 + deregister, Resume -> 200 + register
            assertEquals(200, post(client, base + "/internal/drain", "{}").statusCode());
            assertEquals(503, get(client, base + "/internal/ready").statusCode());
            assertEquals(503, get(client, base + "/search?q=alice").statusCode());
            assertTrue(consulEvents.contains("deregister"));

            assertEquals(200, post(client, base + "/internal/resume", "{}").statusCode());
            assertEquals(200, get(client, base + "/internal/ready").statusCode());

            // 3. Ingest role disabled initially -> 503
            assertEquals(503, post(client, base + "/internal/ingest/11", "{}").statusCode());

            // 4. Enable ingest & index dynamically, drop search -> ready becomes 503
            HttpResponse<String> setRoles1 = post(
                client, base + "/internal/roles", "{\"roles\":[\"ingest\",\"index\"]}"
            );
            assertEquals(200, setRoles1.statusCode());
            assertEquals(503, get(client, base + "/internal/ready").statusCode());

            // 5. Execute direct ingest -> index
            HttpResponse<String> ingestResp = post(client, base + "/internal/ingest/11", "{\"run_id\":\"run-p2\"}");
            assertEquals(200, ingestResp.statusCode());
            Map<String, Object> ingestMap = GSON.fromJson(ingestResp.body(), MAP_TYPE);
            assertEquals("downloaded", ingestMap.get("status"));

            HttpResponse<String> indexResp = post(client, base + "/internal/index/11", "{\"run_id\":\"run-p2\"}");
            assertEquals(200, indexResp.statusCode());
            Map<String, Object> indexMap = GSON.fromJson(indexResp.body(), MAP_TYPE);
            assertEquals("indexed", indexMap.get("status"));

            // 6. Re-enable search role -> ready 200 and search matches Stage 2 JSON + headers
            assertEquals(
                200,
                post(client, base + "/internal/roles", "{\"roles\":[\"ingest\",\"index\",\"search\"]}").statusCode()
            );
            assertEquals(200, get(client, base + "/internal/ready").statusCode());

            HttpRequest searchReq = HttpRequest.newBuilder()
                .uri(URI.create(base + "/search?q=alice&language=en&author=Carroll&year=2008"))
                .header("X-Request-Id", "req-custom-42")
                .GET()
                .build();
            HttpResponse<String> searchResp = client.send(searchReq, HttpResponse.BodyHandlers.ofString());
            assertEquals(200, searchResp.statusCode());
            assertEquals("req-custom-42", searchResp.headers().firstValue("X-Request-Id").orElse(null));
            assertEquals("w-test-1@lab-host-1", searchResp.headers().firstValue("X-Served-By").orElse(null));
            assertEquals("1", searchResp.headers().firstValue("X-Index-Version").orElse(null));

            Map<String, Object> searchBody = GSON.fromJson(searchResp.body(), MAP_TYPE);
            assertEquals(1, ((Number) searchBody.get("total_matches")).intValue());
            List<Map<String, Object>> results = (List<Map<String, Object>>) searchBody.get("results");
            assertEquals(1, results.size());
            assertEquals(11, ((Number) results.get(0).get("book_id")).intValue());

            // 7. Integrity summary & JSONL logs exist
            HttpResponse<String> integrity = get(client, base + "/internal/integrity");
            assertEquals(200, integrity.statusCode());
            Map<String, Object> integrityBody = GSON.fromJson(integrity.body(), MAP_TYPE);
            assertEquals(1, ((Number) integrityBody.get("document_count")).intValue());
            assertEquals(1, ((Number) integrityBody.get("indexed_book_count")).intValue());

            List<String> logLines = Files.readAllLines(logPath, StandardCharsets.UTF_8);
            assertTrue(logLines.stream().anyMatch(l -> l.contains("\"crawl_done\"")));
            assertTrue(logLines.stream().anyMatch(l -> l.contains("\"index_done\"")));
            assertTrue(logLines.stream().anyMatch(l -> l.contains("\"search_served\"")));
            assertTrue(logLines.stream().anyMatch(l -> l.contains("\"role_changed\"")));
        } finally {
            mockConsul.stop(0);
            mockGutenberg.stop(0);
        }
    }

    private static HttpResponse<String> get(HttpClient client, String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(HttpClient client, String url, String json) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
            .uri(URI.create(url))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
            .build();
        return client.send(req, HttpResponse.BodyHandlers.ofString());
    }
}
