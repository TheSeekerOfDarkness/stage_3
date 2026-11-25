package com.example.worker.designa;

import com.example.common.model.CrawlTask;
import com.example.common.model.IndexTask;
import com.example.common.obs.StructuredLogger;
import com.example.worker.WorkerApp;
import com.example.worker.WorkerConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
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
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

class DesignABackendIntegrationTest {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("unchecked")
    void publishesLocalSqliteSnapshotsPreventsDuplicateCrawlsAndFailsOverPublisher() throws Exception {
        AtomicInteger book11Downloads = new AtomicInteger(0);
        AtomicInteger book84Downloads = new AtomicInteger(0);

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

        Path objectStoreRoot = tempDir.resolve("minio-local");
        ObjectStorage sharedStorage = new LocalDirObjectStorage(objectStoreRoot);
        InMemoryTaskBroker sharedBroker = new InMemoryTaskBroker();
        InMemoryLeaderElector sharedElector = new InMemoryLeaderElector();

        String gutenbergTemplate = "http://127.0.0.1:" + mockGutenberg.getAddress().getPort() + "/books/%d";

        WorkerConfig cfg1 = WorkerConfig.fromMap(Map.ofEntries(
            Map.entry("WORKER_ID", "w-a1"),
            Map.entry("HOST_NAME", "host-1"),
            Map.entry("HOST_IP", "127.0.0.1"),
            Map.entry("PORT", "0"),
            Map.entry("DESIGN_MODE", "design-a"),
            Map.entry("WORKER_ROLES", "ingest,index,search"),
            Map.entry("CRAWL_THREADS", "1"),
            Map.entry("INDEX_THREADS", "1"),
            Map.entry("PUBLISH_INTERVAL_MS", "100"),
            Map.entry("DRAIN_DELAY_MS", "10"),
            Map.entry("CONSUL_ENABLED", "false"),
            Map.entry("DATA_DIR", tempDir.resolve("worker-data").toString()),
            Map.entry("GUTENBERG_URL_TEMPLATE", gutenbergTemplate)
        ));
        WorkerConfig cfg2 = WorkerConfig.fromMap(Map.ofEntries(
            Map.entry("WORKER_ID", "w-a2"),
            Map.entry("HOST_NAME", "host-2"),
            Map.entry("HOST_IP", "127.0.0.1"),
            Map.entry("PORT", "0"),
            Map.entry("DESIGN_MODE", "design-a"),
            Map.entry("WORKER_ROLES", "ingest,index,search"),
            Map.entry("CRAWL_THREADS", "1"),
            Map.entry("INDEX_THREADS", "1"),
            Map.entry("PUBLISH_INTERVAL_MS", "100"),
            Map.entry("DRAIN_DELAY_MS", "10"),
            Map.entry("CONSUL_ENABLED", "false"),
            Map.entry("DATA_DIR", tempDir.resolve("worker-data").toString()),
            Map.entry("GUTENBERG_URL_TEMPLATE", gutenbergTemplate)
        ));

        HttpClient client = HttpClient.newHttpClient();
        try (StructuredLogger log1 = new StructuredLogger(tempDir.resolve("logs/w1.jsonl"), "w-a1", "host-1");
             StructuredLogger log2 = new StructuredLogger(tempDir.resolve("logs/w2.jsonl"), "w-a2", "host-2")) {

            DesignABackend backend1 = new DesignABackend(
                cfg1, log1, sharedStorage, sharedBroker, new ScopedLeaderElector("w-a1", sharedElector)
            );
            DesignABackend backend2 = new DesignABackend(
                cfg2, log2, sharedStorage, sharedBroker, new ScopedLeaderElector("w-a2", sharedElector)
            );

            WorkerApp app1 = new WorkerApp(cfg1, backend1, log1);
            WorkerApp app2 = new WorkerApp(cfg2, backend2, log2);
            app1.start();
            app2.start();

            try {
                String base1 = "http://127.0.0.1:" + app1.getPort();
                String base2 = "http://127.0.0.1:" + app2.getPort();

                // Enqueue book 11 crawl -> auto index -> snapshot v1 -> synced to app2
                HttpResponse<String> enq11 = post(
                    client, base1 + "/internal/tasks/crawl", "{\"book_id\":11,\"run_id\":\"run-a1\"}"
                );
                assertEquals(202, enq11.statusCode());

                awaitCondition(8000, () -> {
                    HttpResponse<String> r2 = get(client, base2 + "/search?q=wonderland&author=Carroll");
                    if (r2.statusCode() != 200) return false;
                    Map<String, Object> body = GSON.fromJson(r2.body(), MAP_TYPE);
                    return ((Number) body.get("total_matches")).intValue() == 1
                        && "1".equals(r2.headers().firstValue("X-Index-Version").orElse("0"));
                });

                // Duplicate crawl must not re-download from Gutenberg
                HttpResponse<String> dup = post(
                    client, base2 + "/internal/ingest/11", "{\"run_id\":\"run-a1\"}"
                );
                assertEquals(200, dup.statusCode());
                Map<String, Object> dupBody = GSON.fromJson(dup.body(), MAP_TYPE);
                assertEquals("already_available", dupBody.get("status"));
                assertEquals(1, book11Downloads.get());

                // Kill/stop worker-1 (publisher leader) -> worker-2 must acquire lock, bootstrap v1, and publish v2
                app1.close();

                HttpResponse<String> enq84 = post(
                    client, base2 + "/internal/tasks/crawl", "{\"book_id\":84,\"run_id\":\"run-a2\"}"
                );
                assertEquals(202, enq84.statusCode());

                awaitCondition(8000, () -> {
                    HttpResponse<String> r = get(client, base2 + "/search?q=monster&language=en");
                    if (r.statusCode() != 200) return false;
                    Map<String, Object> body = GSON.fromJson(r.body(), MAP_TYPE);
                    return ((Number) body.get("total_matches")).intValue() == 1
                        && "2".equals(r.headers().firstValue("X-Index-Version").orElse("0"));
                });

                // Verify v1 book 11 is still present in v2 after publisher failover
                HttpResponse<String> verify11 = get(client, base2 + "/search?q=alice");
                assertEquals(200, verify11.statusCode());
                Map<String, Object> verify11Body = GSON.fromJson(verify11.body(), MAP_TYPE);
                assertEquals(1, ((Number) verify11Body.get("total_matches")).intValue());
                assertEquals(1, book84Downloads.get());

                // Verify integrity summary
                HttpResponse<String> integrity = get(client, base2 + "/internal/integrity");
                assertEquals(200, integrity.statusCode());
                Map<String, Object> intBody = GSON.fromJson(integrity.body(), MAP_TYPE);
                assertEquals(2, ((Number) intBody.get("document_count")).intValue());
                assertEquals(2, ((Number) intBody.get("indexed_book_count")).intValue());
                assertEquals(List.of(11.0, 84.0), intBody.get("indexed_book_ids"));
            } finally {
                app1.close();
                app2.close();
            }
        } finally {
            sharedBroker.close();
            mockGutenberg.stop(0);
        }
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
            Thread.sleep(50);
        }
        fail("Condition not met within " + timeoutMs + "ms" + (last != null ? ": " + last.getMessage() : ""));
    }

    private static final class LocalDirObjectStorage implements ObjectStorage {
        private final Path root;

        LocalDirObjectStorage(Path root) {
            this.root = root;
        }

        @Override
        public void ensureBucket(String bucket) throws IOException {
            Files.createDirectories(root.resolve(bucket));
        }

        @Override
        public boolean exists(String bucket, String key) {
            return Files.exists(root.resolve(bucket).resolve(key));
        }

        @Override
        public synchronized void putBytes(String bucket, String key, byte[] data, String contentType) throws IOException {
            Path target = root.resolve(bucket).resolve(key);
            Files.createDirectories(target.getParent());
            Files.write(target, data);
        }

        @Override
        public void putString(String bucket, String key, String content, String contentType) throws IOException {
            putBytes(bucket, key, content.getBytes(StandardCharsets.UTF_8), contentType);
        }

        @Override
        public synchronized void putFile(String bucket, String key, Path sourceFile, String contentType) throws IOException {
            Path target = root.resolve(bucket).resolve(key);
            Files.createDirectories(target.getParent());
            Files.copy(sourceFile, target, StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public byte[] getBytes(String bucket, String key) throws IOException {
            Path target = root.resolve(bucket).resolve(key);
            if (!Files.exists(target)) {
                throw new NoSuchElementException(bucket + "/" + key);
            }
            return Files.readAllBytes(target);
        }

        @Override
        public String getString(String bucket, String key) throws IOException {
            return new String(getBytes(bucket, key), StandardCharsets.UTF_8);
        }

        @Override
        public void downloadToFile(String bucket, String key, Path destinationFile) throws IOException {
            Path target = root.resolve(bucket).resolve(key);
            if (!Files.exists(target)) {
                throw new NoSuchElementException(bucket + "/" + key);
            }
            Files.createDirectories(destinationFile.toAbsolutePath().getParent());
            Files.copy(target, destinationFile, StandardCopyOption.REPLACE_EXISTING);
        }

        @Override
        public List<String> listKeys(String bucket, String prefix) throws IOException {
            Path bucketDir = root.resolve(bucket);
            if (!Files.exists(bucketDir)) {
                return Collections.emptyList();
            }
            String safePrefix = prefix != null ? prefix : "";
            try (Stream<Path> walk = Files.walk(bucketDir)) {
                return walk.filter(Files::isRegularFile)
                    .map(p -> bucketDir.relativize(p).toString().replace("\\", "/"))
                    .filter(k -> k.startsWith(safePrefix))
                    .sorted()
                    .toList();
            }
        }
    }

    private static final class InMemoryTaskBroker implements TaskBroker {
        private final BlockingQueue<CrawlTask> crawlQ = new LinkedBlockingQueue<>();
        private final BlockingQueue<IndexTask> indexQ = new LinkedBlockingQueue<>();
        private final List<Consumer<SnapshotManifest>> listeners = new CopyOnWriteArrayList<>();

        @Override
        public void start() {
        }

        @Override
        public void publishCrawlTask(CrawlTask task) {
            crawlQ.offer(task);
        }

        @Override
        public void publishIndexTask(IndexTask task) {
            indexQ.offer(task);
        }

        @Override
        public void publishSnapshotNotification(SnapshotManifest manifest) {
            for (Consumer<SnapshotManifest> listener : listeners) {
                listener.accept(manifest);
            }
        }

        @Override
        public AutoCloseable startCrawlConsumer(int concurrency, TaskHandler<CrawlTask> handler) {
            return startPool(crawlQ, concurrency, handler);
        }

        @Override
        public AutoCloseable startIndexConsumer(int concurrency, TaskHandler<IndexTask> handler) {
            return startPool(indexQ, concurrency, handler);
        }

        @Override
        public AutoCloseable subscribeSnapshotNotifications(Consumer<SnapshotManifest> listener) {
            listeners.add(listener);
            return () -> listeners.remove(listener);
        }

        private <T> AutoCloseable startPool(BlockingQueue<T> queue, int concurrency, TaskHandler<T> handler) {
            ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, concurrency));
            for (int i = 0; i < Math.max(1, concurrency); i++) {
                pool.submit(() -> {
                    while (!Thread.currentThread().isInterrupted()) {
                        try {
                            T item = queue.poll(50, TimeUnit.MILLISECONDS);
                            if (item != null) {
                                handler.handle(item);
                            }
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            break;
                        } catch (Exception ignored) {
                        }
                    }
                });
            }
            return pool::shutdownNow;
        }

        @Override
        public void close() {
            listeners.clear();
        }
    }

    private static final class InMemoryLeaderElector {
        final Map<Integer, String> claims = new ConcurrentHashMap<>();
        final AtomicReference<String> publisher = new AtomicReference<>();
    }

    private static final class ScopedLeaderElector implements LeaderElector {
        private final String workerId;
        private final InMemoryLeaderElector shared;

        ScopedLeaderElector(String workerId, InMemoryLeaderElector shared) {
            this.workerId = workerId;
            this.shared = shared;
        }

        @Override
        public boolean tryClaimCrawl(int bookId, String claimant) {
            return shared.claims.putIfAbsent(bookId, claimant) == null;
        }

        @Override
        public boolean tryAcquirePublisherLock() {
            String cur = shared.publisher.get();
            return workerId.equals(cur) || shared.publisher.compareAndSet(null, workerId);
        }

        @Override
        public boolean renewPublisherLock() {
            return workerId.equals(shared.publisher.get());
        }

        @Override
        public void releasePublisherLock() {
            shared.publisher.compareAndSet(workerId, null);
        }

        @Override
        public String currentPublisherLeader() {
            return shared.publisher.get();
        }
    }
}
