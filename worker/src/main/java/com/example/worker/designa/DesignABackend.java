package com.example.worker.designa;

import com.example.common.model.Book;
import com.example.common.model.CrawlTask;
import com.example.common.model.IndexDelta;
import com.example.common.model.IndexTask;
import com.example.common.net.GutenbergDownloader;
import com.example.common.obs.Checksums;
import com.example.common.obs.RequestIds;
import com.example.common.obs.StructuredLogger;
import com.example.common.text.BookSplitter;
import com.example.common.text.HeaderParser;
import com.example.common.text.Tokenizer;
import com.example.worker.WorkerConfig;
import com.example.worker.WorkerRole;
import com.example.worker.backend.ClusterBackend;
import com.example.worker.backend.ReadinessStatus;
import com.example.worker.backend.SearchResponseData;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/**
 * Design A backend: Complete local SQLite index copies distributed from MinIO snapshots,
 * coordinated with RabbitMQ queues and Consul KV/session locks.
 */
public class DesignABackend implements ClusterBackend {
    public static final String DATALAKE_BUCKET = "datalake";
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    public static final class StoredDocumentMeta {
        public int bookId;
        public String docSha256;
        public String headerKey;
        public String bodyKey;
        public String sourcePath;
        public String ingestedBy;
        public String taskId;
        public String runId;
        public long ingestedAtMs;
    }

    private final WorkerConfig config;
    private final StructuredLogger logger;
    private final ObjectStorage storage;
    private final TaskBroker broker;
    private final LeaderElector leaderElector;
    private final GutenbergDownloader downloader;
    private final SnapshotPublisher snapshotPublisher;

    private final Path localSnapshotsDir;
    private final AtomicReference<LocalSqliteSearcher> currentSearcher =
        new AtomicReference<>(LocalSqliteSearcher.empty());
    private final Object snapshotSyncLock = new Object();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile Set<WorkerRole> currentRoles = Collections.emptySet();

    private AutoCloseable crawlConsumerHandle;
    private AutoCloseable indexConsumerHandle;
    private AutoCloseable snapshotSubHandle;
    private ScheduledExecutorService snapshotPollExecutor;

    public static DesignABackend createDefault(WorkerConfig config, StructuredLogger logger) {
        ObjectStorage storage = new MinioObjectStorage(
            config.minioEndpoint, config.minioAccessKey, config.minioSecretKey
        );
        TaskBroker broker = new RabbitMqTaskBroker(config);
        LeaderElector elector = new ConsulCoordinator(config);
        return new DesignABackend(config, logger, storage, broker, elector);
    }

    public DesignABackend(WorkerConfig config,
                          StructuredLogger logger,
                          ObjectStorage storage,
                          TaskBroker broker,
                          LeaderElector leaderElector) {
        this.config = config;
        this.logger = logger;
        this.storage = storage;
        this.broker = broker;
        this.leaderElector = leaderElector;
        this.downloader = new GutenbergDownloader(
            config.gutenbergUrlTemplate,
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build(),
            Duration.ofSeconds(15),
            3,
            100
        );
        this.snapshotPublisher = new SnapshotPublisher(config, storage, broker, leaderElector, logger);
        this.localSnapshotsDir = Paths.get(config.dataDir, config.workerId, "snapshots");
    }

    @Override
    public String designMode() {
        return "design-a";
    }

    @Override
    public synchronized void start() throws Exception {
        Files.createDirectories(localSnapshotsDir);
        storage.ensureBucket(DATALAKE_BUCKET);
        storage.ensureBucket(SnapshotPublisher.INDEX_DELTAS_BUCKET);
        storage.ensureBucket(SnapshotPublisher.SNAPSHOTS_BUCKET);
        broker.start();
        started.set(true);
        syncRoleLoops();
    }

    @Override
    public synchronized void stop() {
        started.set(false);
        closeQuietly(crawlConsumerHandle);
        crawlConsumerHandle = null;
        closeQuietly(indexConsumerHandle);
        indexConsumerHandle = null;
        snapshotPublisher.stop();
        closeQuietly(snapshotSubHandle);
        snapshotSubHandle = null;
        shutdownPollExecutor();
        currentSearcher.set(LocalSqliteSearcher.empty());
        broker.close();
    }

    @Override
    public synchronized void onRolesChanged(Set<WorkerRole> activeRoles) {
        this.currentRoles = activeRoles == null ? Collections.emptySet() : Set.copyOf(activeRoles);
        if (started.get()) {
            try {
                syncRoleLoops();
            } catch (Exception e) {
                log("role_sync_error", Map.of("error", safe(e.getMessage())));
            }
        }
    }

    private void syncRoleLoops() throws Exception {
        if (currentRoles.contains(WorkerRole.INGEST)) {
            if (crawlConsumerHandle == null) {
                crawlConsumerHandle = broker.startCrawlConsumer(config.crawlThreads, this::handleCrawlTask);
            }
        } else {
            closeQuietly(crawlConsumerHandle);
            crawlConsumerHandle = null;
        }

        if (currentRoles.contains(WorkerRole.INDEX)) {
            if (indexConsumerHandle == null) {
                indexConsumerHandle = broker.startIndexConsumer(config.indexThreads, this::handleIndexTask);
            }
            snapshotPublisher.start();
        } else {
            closeQuietly(indexConsumerHandle);
            indexConsumerHandle = null;
            snapshotPublisher.stop();
        }

        if (currentRoles.contains(WorkerRole.SEARCH)) {
            if (snapshotSubHandle == null) {
                snapshotSubHandle = broker.subscribeSnapshotNotifications(this::onSnapshotNotification);
            }
            if (snapshotPollExecutor == null || snapshotPollExecutor.isShutdown()) {
                snapshotPollExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "snapshot-sync-" + config.workerId);
                    t.setDaemon(true);
                    return t;
                });
                snapshotPollExecutor.scheduleWithFixedDelay(() -> {
                    if (started.get() && currentRoles.contains(WorkerRole.SEARCH)) {
                        syncLatestSnapshotQuietly();
                    }
                }, 0L, 2000L, TimeUnit.MILLISECONDS);
            }
        } else {
            closeQuietly(snapshotSubHandle);
            snapshotSubHandle = null;
            shutdownPollExecutor();
        }
    }

    @Override
    public String enqueueCrawlTask(int bookId, String runId, String sourceUrl) throws Exception {
        String taskId = RequestIds.newTaskId("crawl");
        CrawlTask task = new CrawlTask(taskId, bookId, safe(runId), sourceUrl, 1, System.currentTimeMillis());
        broker.publishCrawlTask(task);
        return taskId;
    }

    @Override
    public List<Map<String, Object>> enqueueCrawlBatch(List<Integer> bookIds,
                                                       String runId,
                                                       String sourceUrlTemplate) throws Exception {
        List<Map<String, Object>> tasks = new ArrayList<>();
        for (int bookId : bookIds) {
            String url = null;
            if (sourceUrlTemplate != null && !sourceUrlTemplate.isBlank()) {
                url = sourceUrlTemplate.contains("%d")
                    ? String.format(sourceUrlTemplate, bookId)
                    : sourceUrlTemplate + "/" + bookId;
            }
            String taskId = enqueueCrawlTask(bookId, runId, url);
            tasks.add(Map.of("book_id", bookId, "task_id", taskId));
        }
        return tasks;
    }

    @Override
    public Map<String, Object> executeDirectIngest(int bookId, String runId, String sourceUrl) throws Exception {
        String taskId = RequestIds.newTaskId("crawl");
        return doCrawl(bookId, runId, sourceUrl, taskId, false);
    }

    @Override
    public Map<String, Object> executeDirectIndex(int bookId, String runId) throws Exception {
        String taskId = RequestIds.newTaskId("index");
        return doIndex(bookId, runId, taskId);
    }

    private void handleCrawlTask(CrawlTask task) throws Exception {
        log("crawl_claimed", Map.of(
            "task_id", task.taskId,
            "book_id", task.bookId,
            "run_id", safe(task.runId),
            "role", "ingest",
            "attempt", task.attempt
        ));
        doCrawl(task.bookId, task.runId, task.sourceUrl, task.taskId, true);
    }

    private Map<String, Object> doCrawl(int bookId,
                                        String runId,
                                        String sourceUrl,
                                        String taskId,
                                        boolean emitIndexTask) throws Exception {
        long startNs = System.nanoTime();
        String metaKey = "books/" + bookId + ".meta.json";
        String headerKey = "books/" + bookId + ".header.txt";
        String bodyKey = "books/" + bookId + ".body.txt";

        if (storage.exists(DATALAKE_BUCKET, metaKey)) {
            StoredDocumentMeta existing = GSON.fromJson(
                storage.getString(DATALAKE_BUCKET, metaKey), StoredDocumentMeta.class
            );
            if (emitIndexTask && existing != null) {
                publishIndexWork(existing, runId);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "already_available");
            resp.put("doc_sha256", existing != null ? existing.docSha256 : "");
            resp.put("worker_id", config.workerId);
            resp.put("task_id", taskId);
            return resp;
        }

        boolean claimed = leaderElector.tryClaimCrawl(bookId, config.workerId);
        if (!claimed) {
            if (storage.exists(DATALAKE_BUCKET, metaKey)) {
                StoredDocumentMeta existing = GSON.fromJson(
                    storage.getString(DATALAKE_BUCKET, metaKey), StoredDocumentMeta.class
                );
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("book_id", bookId);
                resp.put("status", "already_available");
                resp.put("doc_sha256", existing != null ? existing.docSha256 : "");
                resp.put("worker_id", config.workerId);
                resp.put("task_id", taskId);
                return resp;
            }
            // Another worker claimed this book ID concurrently and is in-flight
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "claimed_by_peer");
            resp.put("worker_id", config.workerId);
            resp.put("task_id", taskId);
            return resp;
        }

        String rawText = (sourceUrl != null && !sourceUrl.isBlank())
            ? downloader.downloadFromUrl(sourceUrl, bookId)
            : downloader.download(bookId);

        BookSplitter.SplitResult split = BookSplitter.split(rawText);
        String sha = Checksums.documentSha256(split.header, split.body);
        String sourcePath = "minio://" + DATALAKE_BUCKET + "/books/" + bookId;

        storage.putString(DATALAKE_BUCKET, headerKey, split.header, "text/plain; charset=utf-8");
        storage.putString(DATALAKE_BUCKET, bodyKey, split.body, "text/plain; charset=utf-8");

        StoredDocumentMeta meta = new StoredDocumentMeta();
        meta.bookId = bookId;
        meta.docSha256 = sha;
        meta.headerKey = headerKey;
        meta.bodyKey = bodyKey;
        meta.sourcePath = sourcePath;
        meta.ingestedBy = config.workerId;
        meta.taskId = taskId;
        meta.runId = safe(runId);
        meta.ingestedAtMs = System.currentTimeMillis();
        storage.putString(DATALAKE_BUCKET, metaKey, GSON.toJson(meta), "application/json");

        if (emitIndexTask) {
            publishIndexWork(meta, runId);
        }

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        log("crawl_done", Map.of(
            "task_id", taskId,
            "run_id", safe(runId),
            "role", "ingest",
            "book_id", bookId,
            "doc_sha256", sha,
            "latency_ms", latencyMs,
            "status", "downloaded"
        ));

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("book_id", bookId);
        resp.put("status", "downloaded");
        resp.put("doc_sha256", sha);
        resp.put("path", sourcePath);
        resp.put("worker_id", config.workerId);
        resp.put("task_id", taskId);
        return resp;
    }

    private void publishIndexWork(StoredDocumentMeta meta, String runId) throws Exception {
        IndexTask indexTask = new IndexTask(
            RequestIds.newTaskId("index"),
            meta.bookId,
            safe(runId),
            meta.docSha256,
            meta.headerKey,
            meta.bodyKey,
            1,
            System.currentTimeMillis()
        );
        broker.publishIndexTask(indexTask);
    }

    private void handleIndexTask(IndexTask task) throws Exception {
        log("index_claimed", Map.of(
            "task_id", task.taskId,
            "book_id", task.bookId,
            "run_id", safe(task.runId),
            "role", "index",
            "attempt", task.attempt
        ));
        doIndex(task.bookId, task.runId, task.taskId);
    }

    private Map<String, Object> doIndex(int bookId, String runId, String taskId) throws Exception {
        long startNs = System.nanoTime();
        String metaKey = "books/" + bookId + ".meta.json";
        if (!storage.exists(DATALAKE_BUCKET, metaKey)) {
            throw new NoSuchElementException("Book " + bookId + " not found in datalake");
        }

        StoredDocumentMeta meta = GSON.fromJson(
            storage.getString(DATALAKE_BUCKET, metaKey), StoredDocumentMeta.class
        );
        String header = storage.getString(DATALAKE_BUCKET, meta.headerKey);
        String body = storage.getString(DATALAKE_BUCKET, meta.bodyKey);

        String actualSha = Checksums.documentSha256(header, body);
        if (meta.docSha256 != null && !meta.docSha256.isBlank() && !meta.docSha256.equals(actualSha)) {
            throw new IOException("Document SHA-256 mismatch for book " + bookId);
        }

        Book book = HeaderParser.parse(bookId, header, meta.sourcePath);
        book.indexedBy = config.workerId;
        book.docSha256 = actualSha;

        List<String> terms = Tokenizer.tokenizeDistinctSorted(body);
        IndexDelta delta = new IndexDelta(
            book,
            terms,
            actualSha,
            config.workerId,
            taskId,
            safe(runId),
            System.currentTimeMillis()
        );
        storage.putString(
            SnapshotPublisher.INDEX_DELTAS_BUCKET,
            "deltas/" + bookId + ".json",
            GSON.toJson(delta),
            "application/json"
        );

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        log("index_done", Map.of(
            "task_id", taskId,
            "run_id", safe(runId),
            "role", "index",
            "book_id", bookId,
            "doc_sha256", actualSha,
            "term_count", terms.size(),
            "latency_ms", latencyMs,
            "status", "delta_written"
        ));

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("book_id", bookId);
        resp.put("status", "delta_written");
        resp.put("doc_sha256", actualSha);
        resp.put("term_count", terms.size());
        resp.put("worker_id", config.workerId);
        resp.put("task_id", taskId);
        return resp;
    }

    public SnapshotManifest triggerPublishNow() throws Exception {
        return snapshotPublisher.publishOnceNow();
    }

    public void syncLatestSnapshot() throws Exception {
        if (!storage.exists(SnapshotPublisher.SNAPSHOTS_BUCKET, SnapshotPublisher.LATEST_MANIFEST_KEY)) {
            return;
        }
        String json = storage.getString(
            SnapshotPublisher.SNAPSHOTS_BUCKET, SnapshotPublisher.LATEST_MANIFEST_KEY
        );
        SnapshotManifest manifest = GSON.fromJson(json, SnapshotManifest.class);
        if (manifest != null) {
            syncToManifest(manifest);
        }
    }

    private void syncLatestSnapshotQuietly() {
        try {
            syncLatestSnapshot();
        } catch (Exception e) {
            log("snapshot_sync_error", Map.of("error", safe(e.getMessage())));
        }
    }

    private void onSnapshotNotification(SnapshotManifest manifest) {
        if (!started.get() || !currentRoles.contains(WorkerRole.SEARCH)) {
            return;
        }
        try {
            syncToManifest(manifest);
        } catch (Exception e) {
            log("snapshot_sync_error", Map.of("error", safe(e.getMessage())));
        }
    }

    private void syncToManifest(SnapshotManifest manifest) throws Exception {
        synchronized (snapshotSyncLock) {
            LocalSqliteSearcher active = currentSearcher.get();
            if (manifest.version <= active.version) {
                return;
            }

            Path tmpFile = localSnapshotsDir.resolve("index-v" + manifest.version + ".db.tmp");
            Path finalFile = localSnapshotsDir.resolve("index-v" + manifest.version + ".db");

            storage.downloadToFile(SnapshotPublisher.SNAPSHOTS_BUCKET, manifest.objectKey, tmpFile);
            String actualSha = Checksums.sha256Hex(tmpFile);
            if (!actualSha.equals(manifest.sha256)) {
                Files.deleteIfExists(tmpFile);
                throw new IOException("Snapshot SHA-256 mismatch for version " + manifest.version);
            }

            Files.move(tmpFile, finalFile, StandardCopyOption.REPLACE_EXISTING);
            LocalSqliteSearcher next = LocalSqliteSearcher.open(
                finalFile, manifest.version, manifest.sha256, manifest.bookCount
            );
            if (!next.isHealthy()) {
                throw new IOException("Downloaded SQLite snapshot failed health query: v" + manifest.version);
            }

            currentSearcher.set(next);
            cleanupOldSnapshots(manifest.version);

            log("snapshot_loaded", Map.of(
                "role", "search",
                "index_version", manifest.version,
                "book_count", manifest.bookCount,
                "snapshot_sha256", manifest.sha256
            ));
        }
    }

    private void cleanupOldSnapshots(long currentVersion) {
        try (Stream<Path> files = Files.list(localSnapshotsDir)) {
            List<Path> dbs = files
                .filter(p -> p.getFileName().toString().startsWith("index-v") && p.getFileName().toString().endsWith(".db"))
                .sorted(Comparator.comparingLong(DesignABackend::extractVersion).reversed())
                .toList();
            for (int i = 2; i < dbs.size(); i++) {
                Path old = dbs.get(i);
                if (extractVersion(old) < currentVersion) {
                    Files.deleteIfExists(old);
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static long extractVersion(Path path) {
        String name = path.getFileName().toString();
        try {
            int start = "index-v".length();
            int end = name.indexOf(".db");
            return Long.parseLong(name.substring(start, end));
        } catch (Exception e) {
            return -1L;
        }
    }

    @Override
    public SearchResponseData search(List<String> distinctTerms,
                                     String author,
                                     String language,
                                     Integer year,
                                     int limit,
                                     int offset) throws Exception {
        return currentSearcher.get().search(distinctTerms, author, language, year, limit, offset);
    }

    @Override
    public ReadinessStatus checkSearchReadiness() {
        if (!started.get()) {
            return new ReadinessStatus(false, "not_started", 0L, 0);
        }
        LocalSqliteSearcher searcher = currentSearcher.get();
        if (searcher.version < config.minRequiredIndexVersion) {
            return new ReadinessStatus(
                false,
                "waiting_for_index_version_" + config.minRequiredIndexVersion,
                searcher.version,
                searcher.bookCount
            );
        }
        if (!searcher.isHealthy()) {
            return new ReadinessStatus(false, "sqlite_unhealthy", searcher.version, searcher.bookCount);
        }
        return new ReadinessStatus(true, "ready", searcher.version, searcher.bookCount);
    }

    @Override
    public Map<String, Object> clusterDiagnostics() {
        LocalSqliteSearcher searcher = currentSearcher.get();
        Map<String, Object> diag = new LinkedHashMap<>();
        diag.put("design_mode", designMode());
        diag.put("worker_id", config.workerId);
        diag.put("local_index_version", searcher.version);
        diag.put("local_indexed_books", searcher.bookCount);
        diag.put("local_snapshot_sha256", searcher.sha256);
        diag.put("is_publisher_leader", snapshotPublisher.isLeader());
        diag.put("publisher_leader", leaderElector.currentPublisherLeader());
        return diag;
    }

    @Override
    public Map<String, Object> executeDistributedDemo(String payload) {
        return Map.of(
            "executed_on", config.workerId,
            "host", config.hostName,
            "design_mode", designMode(),
            "payload", payload != null ? payload : ""
        );
    }

    @Override
    public Map<String, Object> integritySummary() {
        Map<String, String> docChecksums = new LinkedHashMap<>();
        try {
            for (String key : storage.listKeys(DATALAKE_BUCKET, "books/")) {
                if (!key.endsWith(".meta.json")) {
                    continue;
                }
                StoredDocumentMeta meta = GSON.fromJson(
                    storage.getString(DATALAKE_BUCKET, key), StoredDocumentMeta.class
                );
                if (meta != null) {
                    docChecksums.put(String.valueOf(meta.bookId), meta.docSha256);
                }
            }
        } catch (Exception ignored) {
        }

        LocalSqliteSearcher searcher = currentSearcher.get();
        List<Integer> indexedBookIds = searcher.listIndexedBookIds();

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("design_mode", designMode());
        summary.put("worker_id", config.workerId);
        summary.put("index_version", searcher.version);
        summary.put("snapshot_sha256", searcher.sha256);
        summary.put("document_count", docChecksums.size());
        summary.put("indexed_book_count", indexedBookIds.size());
        summary.put("document_checksums", docChecksums);
        summary.put("indexed_book_ids", indexedBookIds);
        return summary;
    }

    private void shutdownPollExecutor() {
        if (snapshotPollExecutor != null) {
            snapshotPollExecutor.shutdownNow();
            try {
                snapshotPollExecutor.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            snapshotPollExecutor = null;
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception ignored) {
            }
        }
    }

    private void log(String event, Map<String, Object> fields) {
        if (logger != null) {
            logger.log(event, fields);
        }
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }
}
