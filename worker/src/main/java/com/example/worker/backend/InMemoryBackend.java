package com.example.worker.backend;

import com.example.common.model.Book;
import com.example.common.model.CrawlTask;
import com.example.common.model.DocumentRecord;
import com.example.common.model.IndexTask;
import com.example.common.model.QueryMatcher;
import com.example.common.net.GutenbergDownloader;
import com.example.common.obs.Checksums;
import com.example.common.obs.RequestIds;
import com.example.common.obs.StructuredLogger;
import com.example.common.text.BookSplitter;
import com.example.common.text.HeaderParser;
import com.example.common.text.Tokenizer;
import com.example.worker.WorkerConfig;
import com.example.worker.WorkerRole;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe in-memory ClusterBackend for testing and local verification.
 */
public class InMemoryBackend implements ClusterBackend {
    private final WorkerConfig config;
    private final StructuredLogger logger;
    private final GutenbergDownloader downloader;

    private final ConcurrentHashMap<Integer, DocumentRecord> documents = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, Book> books = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Set<Integer>> invertedIndex = new ConcurrentHashMap<>();
    private final AtomicLong indexVersion = new AtomicLong(0L);

    private final BlockingQueue<CrawlTask> crawlQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<IndexTask> indexQueue = new LinkedBlockingQueue<>();

    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile Set<WorkerRole> currentRoles = Collections.emptySet();
    private ExecutorService ingestExecutor;
    private ExecutorService indexExecutor;

    public InMemoryBackend(WorkerConfig config, StructuredLogger logger) {
        this.config = config;
        this.logger = logger;
        this.downloader = new GutenbergDownloader(
            config.gutenbergUrlTemplate,
            HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NORMAL)
                .build(),
            Duration.ofSeconds(15),
            3,
            50
        );
    }

    @Override
    public String designMode() {
        return "in-memory";
    }

    @Override
    public synchronized void start() {
        started.set(true);
        syncExecutors();
    }

    @Override
    public synchronized void stop() {
        started.set(false);
        shutdownExecutor(ingestExecutor);
        ingestExecutor = null;
        shutdownExecutor(indexExecutor);
        indexExecutor = null;
    }

    @Override
    public synchronized void onRolesChanged(Set<WorkerRole> activeRoles) {
        this.currentRoles = activeRoles == null ? Collections.emptySet() : Set.copyOf(activeRoles);
        if (started.get()) {
            syncExecutors();
        }
    }

    private void syncExecutors() {
        if (currentRoles.contains(WorkerRole.INGEST)) {
            if (ingestExecutor == null || ingestExecutor.isShutdown()) {
                ingestExecutor = Executors.newFixedThreadPool(config.crawlThreads);
                for (int i = 0; i < config.crawlThreads; i++) {
                    ingestExecutor.submit(this::runIngestLoop);
                }
            }
        } else {
            shutdownExecutor(ingestExecutor);
            ingestExecutor = null;
        }

        if (currentRoles.contains(WorkerRole.INDEX)) {
            if (indexExecutor == null || indexExecutor.isShutdown()) {
                indexExecutor = Executors.newFixedThreadPool(config.indexThreads);
                for (int i = 0; i < config.indexThreads; i++) {
                    indexExecutor.submit(this::runIndexLoop);
                }
            }
        } else {
            shutdownExecutor(indexExecutor);
            indexExecutor = null;
        }
    }

    private void runIngestLoop() {
        while (started.get() && !Thread.currentThread().isInterrupted() && currentRoles.contains(WorkerRole.INGEST)) {
            try {
                CrawlTask task = crawlQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                log("crawl_claimed", Map.of(
                    "task_id", task.taskId,
                    "book_id", task.bookId,
                    "run_id", safe(task.runId),
                    "role", "ingest"
                ));
                Map<String, Object> ingested = doIngest(task.bookId, task.runId, task.sourceUrl, task.taskId);
                String sha = (String) ingested.get("doc_sha256");
                IndexTask indexTask = new IndexTask(
                    RequestIds.newTaskId("index"),
                    task.bookId,
                    task.runId,
                    sha,
                    "books/" + task.bookId + ".header.txt",
                    "books/" + task.bookId + ".body.txt",
                    1,
                    System.currentTimeMillis()
                );
                indexQueue.offer(indexTask);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log("crawl_error", Map.of("error", safe(e.getMessage()), "role", "ingest"));
            }
        }
    }

    private void runIndexLoop() {
        while (started.get() && !Thread.currentThread().isInterrupted() && currentRoles.contains(WorkerRole.INDEX)) {
            try {
                IndexTask task = indexQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                log("index_claimed", Map.of(
                    "task_id", task.taskId,
                    "book_id", task.bookId,
                    "run_id", safe(task.runId),
                    "role", "index"
                ));
                doIndex(task.bookId, task.runId, task.taskId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log("index_error", Map.of("error", safe(e.getMessage()), "role", "index"));
            }
        }
    }

    @Override
    public String enqueueCrawlTask(int bookId, String runId, String sourceUrl) {
        String taskId = RequestIds.newTaskId("crawl");
        CrawlTask task = new CrawlTask(taskId, bookId, safe(runId), sourceUrl, 1, System.currentTimeMillis());
        crawlQueue.offer(task);
        return taskId;
    }

    @Override
    public List<Map<String, Object>> enqueueCrawlBatch(List<Integer> bookIds, String runId, String sourceUrlTemplate) {
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
        return doIngest(bookId, runId, sourceUrl, taskId);
    }

    private Map<String, Object> doIngest(int bookId, String runId, String sourceUrl, String taskId) throws Exception {
        long startNs = System.nanoTime();
        DocumentRecord existing = documents.get(bookId);
        if (existing != null) {
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "already_available");
            resp.put("doc_sha256", existing.docSha256);
            resp.put("worker_id", config.workerId);
            resp.put("task_id", taskId);
            return resp;
        }

        String rawText = (sourceUrl != null && !sourceUrl.isBlank())
            ? downloader.downloadFromUrl(sourceUrl, bookId)
            : downloader.download(bookId);

        BookSplitter.SplitResult split = BookSplitter.split(rawText);
        String sha = Checksums.documentSha256(split.header, split.body);
        String sourcePath = "memory/books/" + bookId;
        DocumentRecord record = new DocumentRecord(
            bookId,
            split.header,
            split.body,
            sha,
            sourcePath,
            config.workerId,
            taskId,
            safe(runId),
            System.currentTimeMillis()
        );
        documents.putIfAbsent(bookId, record);

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

    @Override
    public Map<String, Object> executeDirectIndex(int bookId, String runId) throws Exception {
        String taskId = RequestIds.newTaskId("index");
        return doIndex(bookId, runId, taskId);
    }

    private Map<String, Object> doIndex(int bookId, String runId, String taskId) {
        long startNs = System.nanoTime();
        DocumentRecord doc = documents.get(bookId);
        if (doc == null) {
            throw new NoSuchElementException("Book " + bookId + " not found in document store");
        }

        Book parsed = HeaderParser.parse(bookId, doc.header, doc.sourcePath);
        Set<String> terms = Tokenizer.tokenizeDistinct(doc.body);
        for (String term : terms) {
            invertedIndex.computeIfAbsent(term, ignored -> ConcurrentHashMap.newKeySet()).add(bookId);
        }

        long version = indexVersion.incrementAndGet();
        parsed.indexedBy = config.workerId;
        parsed.indexVersion = version;
        parsed.docSha256 = doc.docSha256;
        books.put(bookId, parsed);

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        log("index_done", Map.of(
            "task_id", taskId,
            "run_id", safe(runId),
            "role", "index",
            "book_id", bookId,
            "doc_sha256", doc.docSha256,
            "index_version", version,
            "term_count", terms.size(),
            "latency_ms", latencyMs,
            "status", "indexed"
        ));

        Map<String, Object> resp = new LinkedHashMap<>();
        resp.put("book_id", bookId);
        resp.put("status", "indexed");
        resp.put("index_version", version);
        resp.put("term_count", terms.size());
        resp.put("worker_id", config.workerId);
        resp.put("task_id", taskId);
        return resp;
    }

    @Override
    public SearchResponseData search(List<String> distinctTerms,
                                     String author,
                                     String language,
                                     Integer year,
                                     int limit,
                                     int offset) {
        long version = indexVersion.get();
        if (distinctTerms == null || distinctTerms.isEmpty()) {
            return new SearchResponseData(0, Collections.emptyList(), version);
        }

        Set<Integer> intersection = null;
        for (String term : distinctTerms) {
            Set<Integer> posting = invertedIndex.get(term);
            if (posting == null || posting.isEmpty()) {
                return new SearchResponseData(0, Collections.emptyList(), version);
            }
            if (intersection == null) {
                intersection = new HashSet<>(posting);
            } else {
                intersection.retainAll(posting);
                if (intersection.isEmpty()) {
                    return new SearchResponseData(0, Collections.emptyList(), version);
                }
            }
        }

        List<Book> candidates = new ArrayList<>();
        for (Integer bookId : intersection) {
            Book b = books.get(bookId);
            if (b != null) {
                candidates.add(b);
            }
        }

        QueryMatcher.PageResult page = QueryMatcher.filterSortAndPaginate(
            candidates, author, language, year, limit, offset
        );
        return new SearchResponseData(page.totalMatches, page.results, version);
    }

    @Override
    public ReadinessStatus checkSearchReadiness() {
        boolean isStarted = started.get();
        return new ReadinessStatus(
            isStarted,
            isStarted ? "ready" : "not_started",
            indexVersion.get(),
            books.size()
        );
    }

    @Override
    public Map<String, Object> clusterDiagnostics() {
        Map<String, Object> diag = new LinkedHashMap<>();
        diag.put("design_mode", designMode());
        diag.put("worker_id", config.workerId);
        diag.put("documents", documents.size());
        diag.put("indexed_books", books.size());
        diag.put("terms", invertedIndex.size());
        diag.put("index_version", indexVersion.get());
        return diag;
    }

    @Override
    public Map<String, Object> executeDistributedDemo(String payload) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("executed_on", config.workerId);
        result.put("host", config.hostName);
        result.put("design_mode", designMode());
        result.put("payload", payload != null ? payload : "");
        return result;
    }

    @Override
    public Map<String, Object> integritySummary() {
        Map<String, String> docChecksums = new LinkedHashMap<>();
        documents.keySet().stream().sorted().forEach(id -> {
            DocumentRecord rec = documents.get(id);
            if (rec != null) {
                docChecksums.put(String.valueOf(id), rec.docSha256);
            }
        });

        Map<String, Long> bookVersions = new LinkedHashMap<>();
        books.keySet().stream().sorted().forEach(id -> {
            Book b = books.get(id);
            if (b != null) {
                bookVersions.put(String.valueOf(id), b.indexVersion);
            }
        });

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("design_mode", designMode());
        summary.put("worker_id", config.workerId);
        summary.put("index_version", indexVersion.get());
        summary.put("document_count", docChecksums.size());
        summary.put("indexed_book_count", bookVersions.size());
        summary.put("document_checksums", docChecksums);
        summary.put("indexed_book_versions", bookVersions);
        return summary;
    }

    private void shutdownExecutor(ExecutorService exec) {
        if (exec != null) {
            exec.shutdownNow();
            try {
                exec.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
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
