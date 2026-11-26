package com.example.worker.designb;

import com.example.common.model.CrawlTask;
import com.example.common.model.TaskLease;
import com.example.common.obs.RequestIds;
import com.example.common.obs.StructuredLogger;
import com.example.worker.WorkerApp;
import com.example.worker.WorkerConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hazelcast.map.IMap;
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
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

class DesignBBackendIntegrationTest {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("unchecked")
    void formsClusterDistributesPartitionsExecutesAcrossMembersAndRecoversOnCrash() throws Exception {
        AtomicInteger book11Downloads = new AtomicInteger(0);
        AtomicInteger book84Downloads = new AtomicInteger(0);
        AtomicInteger book2701Downloads = new AtomicInteger(0);

        String book11 = """
            Title: Alice's Adventures in Wonderland
            Author: Lewis Carroll
            Release Date: June 2008 [EBook #11]
            Language: English
            *** START OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            Alice followed the white rabbit down the rabbit hole in wonderland.
            *** END OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            """;

        String book84 = """
            Title: Frankenstein
            Author: Mary Shelley
            Release Date: October 1993 [EBook #84]
            Language: English
            *** START OF THE PROJECT GUTENBERG EBOOK FRANKENSTEIN ***
            Victor created a monster during a stormy night in the laboratory.
            *** END OF THE PROJECT GUTENBERG EBOOK FRANKENSTEIN ***
            """;

        String book2701 = """
            Title: Moby-Dick; or, The Whale
            Author: Herman Melville
            Release Date: July 2001 [EBook #2701]
            Language: English
            *** START OF THE PROJECT GUTENBERG EBOOK MOBY-DICK; OR, THE WHALE ***
            Call me Ishmael. The white whale swam across the ocean during the night.
            *** END OF THE PROJECT GUTENBERG EBOOK MOBY-DICK; OR, THE WHALE ***
            """;

        HttpServer mockGutenberg = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockGutenberg.createContext("/books/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            byte[] payload;
            int status = 200;
            if (path.endsWith("/11")) {
                book11Downloads.incrementAndGet();
                payload = book11.getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/84")) {
                book84Downloads.incrementAndGet();
                payload = book84.getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/2701")) {
                book2701Downloads.incrementAndGet();
                payload = book2701.getBytes(StandardCharsets.UTF_8);
            } else {
                status = 404;
                payload = "Not Found".getBytes(StandardCharsets.UTF_8);
            }
            exchange.sendResponseHeaders(status, payload.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(payload);
            }
        });
        mockGutenberg.start();

        String clusterName = "test-hz-" + System.currentTimeMillis();
        String members = "127.0.0.1:15701,127.0.0.1:15702,127.0.0.1:15703";
        String template = "http://127.0.0.1:" + mockGutenberg.getAddress().getPort() + "/books/%d";

        WorkerConfig cfg1 = buildConfig("w-b1", "host-1", 15701, clusterName, members, template);
        WorkerConfig cfg2 = buildConfig("w-b2", "host-2", 15702, clusterName, members, template);
        WorkerConfig cfg3 = buildConfig("w-b3", "host-3", 15703, clusterName, members, template);

        HttpClient client = HttpClient.newHttpClient();
        try (StructuredLogger log1 = new StructuredLogger(tempDir.resolve("logs/b1.jsonl"), "w-b1", "host-1");
             StructuredLogger log2 = new StructuredLogger(tempDir.resolve("logs/b2.jsonl"), "w-b2", "host-2");
             StructuredLogger log3 = new StructuredLogger(tempDir.resolve("logs/b3.jsonl"), "w-b3", "host-3")) {

            DesignBBackend b1 = new DesignBBackend(cfg1, log1);
            DesignBBackend b2 = new DesignBBackend(cfg2, log2);
            DesignBBackend b3 = new DesignBBackend(cfg3, log3);

            WorkerApp app1 = new WorkerApp(cfg1, b1, log1);
            WorkerApp app2 = new WorkerApp(cfg2, b2, log2);
            WorkerApp app3 = new WorkerApp(cfg3, b3, log3);

            app1.start();
            app2.start();
            app3.start();

            try {
                String base1 = "http://127.0.0.1:" + app1.getPort();
                String base2 = "http://127.0.0.1:" + app2.getPort();
                String base3 = "http://127.0.0.1:" + app3.getPort();

                // 1. Verify 3-member cluster and 271 partitions distributed across all 3 members
                awaitCondition(15000, () -> {
                    HttpResponse<String> diagResp = get(client, base1 + "/internal/cluster/partitions");
                    if (diagResp.statusCode() != 200) return false;
                    Map<String, Object> diag = GSON.fromJson(diagResp.body(), MAP_TYPE);
                    if (((Number) diag.get("member_count")).intValue() != 3) return false;
                    if (((Number) diag.get("partition_count")).intValue() != 271) return false;
                    List<Map<String, Object>> mList = (List<Map<String, Object>>) diag.get("members");
                    return mList.stream().allMatch(m -> ((Number) m.get("owned_partitions")).intValue() > 0);
                });

                // 2. Crawl & index books 11 and 84 -> immediately visible across node 3
                assertEquals(202, post(client, base1 + "/internal/tasks/crawl", "{\"book_id\":11,\"run_id\":\"run-b1\"}").statusCode());
                assertEquals(202, post(client, base1 + "/internal/tasks/crawl", "{\"book_id\":84,\"run_id\":\"run-b1\"}").statusCode());

                awaitCondition(10000, () -> {
                    HttpResponse<String> r = get(client, base3 + "/search?q=white&language=en");
                    if (r.statusCode() != 200) return false;
                    Map<String, Object> body = GSON.fromJson(r.body(), MAP_TYPE);
                    HttpResponse<String> r2 = get(client, base3 + "/search?q=monster&author=Shelley&year=1993");
                    if (r2.statusCode() != 200) return false;
                    Map<String, Object> body2 = GSON.fromJson(r2.body(), MAP_TYPE);
                    return ((Number) body.get("total_matches")).intValue() == 1
                        && ((Number) body2.get("total_matches")).intValue() == 1;
                });

                // Duplicate ingest on w-b2 does not re-download
                HttpResponse<String> dup = post(client, base2 + "/internal/ingest/11", "{\"run_id\":\"run-b1\"}");
                assertEquals(200, dup.statusCode());
                Map<String, Object> dupMap = GSON.fromJson(dup.body(), MAP_TYPE);
                assertEquals("already_available", dupMap.get("status"));
                assertEquals(1, book11Downloads.get());
                assertEquals(1, book84Downloads.get());

                // 3. Distributed IExecutorService execution across all 3 members
                HttpResponse<String> execResp = post(client, base2 + "/internal/cluster/exec-demo", "{\"payload\":\"ping\"}");
                assertEquals(200, execResp.statusCode());
                Map<String, Object> execBody = GSON.fromJson(execResp.body(), MAP_TYPE);
                assertEquals(3, ((Number) execBody.get("member_count")).intValue());
                List<Map<String, Object>> executions = (List<Map<String, Object>>) execBody.get("executions");
                Set<String> executedWorkers = executions.stream()
                    .map(e -> String.valueOf(e.get("worker_id")))
                    .collect(Collectors.toSet());
                assertEquals(Set.of("w-b1", "w-b2", "w-b3"), executedWorkers);

                // 4. Abruptly crash w-b1 while holding an in-flight CrawlTask lease for Book 2701
                String deadUuid = b1.getLocalMemberUuid();
                CrawlTask orphanedTask = new CrawlTask(
                    RequestIds.newTaskId("crawl"), 2701, "run-b-crash", null, 1, System.currentTimeMillis()
                );
                TaskLease orphanedLease = new TaskLease(
                    orphanedTask.taskId,
                    "crawl",
                    2701,
                    "run-b-crash",
                    "w-b1",
                    deadUuid,
                    System.currentTimeMillis(),
                    GSON.toJson(orphanedTask)
                );
                IMap<Integer, String> claims = b1.getHazelcastInstance().getMap("crawl_claims");
                IMap<String, TaskLease> leases = b1.getHazelcastInstance().getMap("in_flight_tasks");
                claims.put(2701, "w-b1");
                leases.put(orphanedLease.taskId, orphanedLease);

                b1.terminateAbruptly();
                app1.close();

                // Surviving nodes must keep serving 11 & 84 AND automatically take over + index 2701
                awaitCondition(15000, () -> {
                    HttpResponse<String> r = get(client, base3 + "/search?q=white&limit=10&offset=0");
                    if (r.statusCode() != 200) return false;
                    Map<String, Object> body = GSON.fromJson(r.body(), MAP_TYPE);
                    return ((Number) body.get("total_matches")).intValue() == 2;
                });

                HttpResponse<String> search2701 = get(client, base2 + "/search?q=ishmael&author=Melville");
                assertEquals(200, search2701.statusCode());
                Map<String, Object> body2701 = GSON.fromJson(search2701.body(), MAP_TYPE);
                assertEquals(1, ((Number) body2701.get("total_matches")).intValue());
                assertEquals(1, book2701Downloads.get());

                // 5. Verify zero data loss & full integrity across surviving 2-node cluster
                HttpResponse<String> integrityResp = get(client, base3 + "/internal/integrity");
                assertEquals(200, integrityResp.statusCode());
                Map<String, Object> integrity = GSON.fromJson(integrityResp.body(), MAP_TYPE);
                assertEquals(2, ((Number) integrity.get("member_count")).intValue());
                assertEquals(3, ((Number) integrity.get("document_count")).intValue());
                assertEquals(3, ((Number) integrity.get("indexed_book_count")).intValue());
                assertEquals(List.of(11.0, 84.0, 2701.0), integrity.get("indexed_book_ids"));
            } finally {
                app1.close();
                app2.close();
                app3.close();
            }
        } finally {
            mockGutenberg.stop(0);
        }
    }

    private WorkerConfig buildConfig(String workerId,
                                     String hostName,
                                     int hazelcastPort,
                                     String clusterName,
                                     String members,
                                     String gutenbergTemplate) {
        return WorkerConfig.fromMap(Map.ofEntries(
            Map.entry("WORKER_ID", workerId),
            Map.entry("HOST_NAME", hostName),
            Map.entry("HOST_IP", "127.0.0.1"),
            Map.entry("PORT", "0"),
            Map.entry("DESIGN_MODE", "design-b"),
            Map.entry("WORKER_ROLES", "ingest,index,search"),
            Map.entry("CRAWL_THREADS", "1"),
            Map.entry("INDEX_THREADS", "1"),
            Map.entry("DRAIN_DELAY_MS", "10"),
            Map.entry("CONSUL_ENABLED", "false"),
            Map.entry("HAZELCAST_CLUSTER_NAME", clusterName),
            Map.entry("HAZELCAST_PORT", String.valueOf(hazelcastPort)),
            Map.entry("HAZELCAST_JOIN_MODE", "tcpip"),
            Map.entry("HAZELCAST_MEMBERS", members),
            Map.entry("HAZELCAST_BACKUP_COUNT", "1"),
            Map.entry("GUTENBERG_URL_TEMPLATE", gutenbergTemplate)
        ));
    }

    private static HttpResponse<String> get(HttpClient client, String url) throws Exception {
        return client.send(HttpRequest.newBuilder().uri(URI.create(url)).GET().build(),
            HttpResponse.BodyHandlers.ofString());
    }

    private static HttpResponse<String> post(HttpClient client, String url, String json) throws Exception {
        return client.send(
            HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(json, StandardCharsets.UTF_8))
                .build(),
            HttpResponse.BodyHandlers.ofString()
        );
    }

    @FunctionalInterface
    private interface ThrowingCondition {
        boolean check() throws Exception;
    }

    private static void awaitCondition(long timeoutMs, ThrowingCondition condition) throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMs;
        Throwable last = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                if (condition.check()) {
                    return;
                }
            } catch (Throwable t) {
                last = t;
            }
            Thread.sleep(100);
        }
        fail("Condition not met within " + timeoutMs + "ms" + (last != null ? ": " + last.getMessage() : ""));
    }
}
