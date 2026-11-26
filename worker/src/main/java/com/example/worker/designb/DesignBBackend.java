package com.example.worker.designb;

import com.example.common.model.Book;
import com.example.common.model.CrawlTask;
import com.example.common.model.DocumentRecord;
import com.example.common.model.IndexTask;
import com.example.common.model.QueryMatcher;
import com.example.common.model.TaskLease;
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
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.collection.IQueue;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.map.IMap;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import java.io.IOException;
import java.net.http.HttpClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Design B backend: Embedded Hazelcast 5.5.0 distributed in-memory index,
 * atomic EntryProcessor posting updates, and MembershipListener task failover.
 */
public class DesignBBackend implements ClusterBackend {
    private static final int MAX_ATTEMPTS = 3;
    private static final int ENTRY_PROCESSOR_CHUNK_SIZE = 2000;
    private static final String PUBLISHED_VERSION_KEY = "published_version";
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final WorkerConfig config;
    private final StructuredLogger logger;
    private final GutenbergDownloader downloader;

    private HazelcastInstance hazelcast;
    private IMap<Integer, DocumentRecord> documents;
    private IMap<Integer, Book> books;
    private IMap<String, Set<Integer>> invertedIndex;
    private IMap<Integer, String> crawlClaims;
    private IMap<String, TaskLease> inFlightTasks;
    private IMap<String, Long> clusterMeta;
    private IQueue<CrawlTask> crawlQueue;
    private IQueue<IndexTask> indexQueue;
    private UUID membershipListenerId;

    private final AtomicBoolean started = new AtomicBoolean(false);
    private volatile Set<WorkerRole> currentRoles = Collections.emptySet();
    private ExecutorService ingestExecutor;
    private ExecutorService indexExecutor;

    public DesignBBackend(WorkerConfig config, StructuredLogger logger) {
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
            100
        );
    }

    @Override
    public String designMode() {
        return "design-b";
    }

    public HazelcastInstance getHazelcastInstance() {
        return hazelcast;
    }

    public String getLocalMemberUuid() {
        if (hazelcast == null) {
            return "";
        }
        return hazelcast.getCluster().getLocalMember().getUuid().toString();
    }

    @Override
    public synchronized void start() {
        if (started.get()) {
            return;
        }
        this.hazelcast = Hazelcast.newHazelcastInstance(HazelcastConfigBuilder.build(config));
        this.documents = hazelcast.getMap("documents");
        this.books = hazelcast.getMap("books");
        this.invertedIndex = hazelcast.getMap("inverted_index");
        this.crawlClaims = hazelcast.getMap("crawl_claims");
        this.inFlightTasks = hazelcast.getMap("in_flight_tasks");
        this.clusterMeta = hazelcast.getMap("cluster_meta");
        this.crawlQueue = hazelcast.getQueue("crawl_queue");
        this.indexQueue = hazelcast.getQueue("index_queue");

        this.membershipListenerId = hazelcast.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
                log("cluster_member_added", Map.of(
                    "member_uuid", membershipEvent.getMember().getUuid().toString(),
                    "address", membershipEvent.getMember().getAddress().toString()
                ));
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                onMemberRemoved(membershipEvent.getMember());
            }
        });

        started.set(true);
        syncRoleLoops();
    }

    @Override
    public synchronized void stop() {
        started.set(false);
        shutdownExecutor(ingestExecutor);
        ingestExecutor = null;
        shutdownExecutor(indexExecutor);
        indexExecutor = null;
        if (hazelcast != null) {
            try {
                if (membershipListenerId != null) {
                    hazelcast.getCluster().removeMembershipListener(membershipListenerId);
                }
            } catch (Exception ignored) {
            }
            try {
                hazelcast.shutdown();
            } catch (Exception ignored) {
            }
            hazelcast = null;
        }
    }

    /**
     * Simulates an abrupt node failure (no graceful partition migration) for fault-tolerance tests.
     */
    public synchronized void terminateAbruptly() {
        started.set(false);
        shutdownExecutor(ingestExecutor);
        ingestExecutor = null;
        shutdownExecutor(indexExecutor);
        indexExecutor = null;
        if (hazelcast != null) {
            try {
                hazelcast.getLifecycleService().terminate();
            } catch (Exception ignored) {
            }
            hazelcast = null;
        }
    }

    @Override
    public synchronized void onRolesChanged(Set<WorkerRole> activeRoles) {
        this.currentRoles = activeRoles == null ? Collections.emptySet() : Set.copyOf(activeRoles);
        if (started.get()) {
            syncRoleLoops();
        }
    }

    private void syncRoleLoops() {
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
            CrawlTask task = null;
            TaskLease lease = null;
            try {
                task = crawlQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                lease = new TaskLease(
                    task.taskId,
                    "crawl",
                    task.bookId,
                    safe(task.runId),
                    config.workerId,
                    getLocalMemberUuid(),
                    System.currentTimeMillis(),
                    GSON.toJson(task)
                );
                inFlightTasks.put(lease.taskId, lease);

                log("crawl_claimed", Map.of(
                    "task_id", task.taskId,
                    "book_id", task.bookId,
                    "run_id", safe(task.runId),
                    "role", "ingest",
                    "attempt", task.attempt
                ));
                doCrawl(task.bookId, task.runId, task.sourceUrl, task.taskId, true);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log("crawl_error", Map.of(
                    "task_id", task != null ? task.taskId : "",
                    "book_id", task != null ? task.bookId : 0,
                    "error", safe(e.getMessage()),
                    "role", "ingest"
                ));
                if (task != null && task.attempt < MAX_ATTEMPTS && started.get()) {
                    task.attempt += 1;
                    crawlQueue.offer(task);
                }
            } finally {
                if (lease != null && started.get()) {
                    try {
                        inFlightTasks.remove(lease.taskId, lease);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private void runIndexLoop() {
        while (started.get() && !Thread.currentThread().isInterrupted() && currentRoles.contains(WorkerRole.INDEX)) {
            IndexTask task = null;
            TaskLease lease = null;
            try {
                task = indexQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }
                lease = new TaskLease(
                    task.taskId,
                    "index",
                    task.bookId,
                    safe(task.runId),
                    config.workerId,
                    getLocalMemberUuid(),
                    System.currentTimeMillis(),
                    GSON.toJson(task)
                );
                inFlightTasks.put(lease.taskId, lease);

                log("index_claimed", Map.of(
                    "task_id", task.taskId,
                    "book_id", task.bookId,
                    "run_id", safe(task.runId),
                    "role", "index",
                    "attempt", task.attempt
                ));
                doIndex(task.bookId, task.runId, task.taskId);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                log("index_error", Map.of(
                    "task_id", task != null ? task.taskId : "",
                    "book_id", task != null ? task.bookId : 0,
                    "error", safe(e.getMessage()),
                    "role", "index"
                ));
                if (task != null && task.attempt < MAX_ATTEMPTS && started.get()) {
                    task.attempt += 1;
                    indexQueue.offer(task);
                }
            } finally {
                if (lease != null && started.get()) {
                    try {
                        inFlightTasks.remove(lease.taskId, lease);
                    } catch (Exception ignored) {
                    }
                }
            }
        }
    }

    private void onMemberRemoved(Member departed) {
        if (!started.get() || hazelcast == null || !hazelcast.getLifecycleService().isRunning()) {
            return;
        }
        String deadUuid = departed.getUuid().toString();
        log("cluster_member_removed", Map.of(
            "member_uuid", deadUuid,
            "address", departed.getAddress().toString()
        ));

        try {
            List<TaskLease> snapshot = new ArrayList<>(inFlightTasks.values());
            for (TaskLease lease : snapshot) {
                if (!deadUuid.equals(lease.ownerMemberUuid)) {
                    continue;
                }
                if (!inFlightTasks.remove(lease.taskId, lease)) {
                    continue;
                }

                if ("crawl".equals(lease.taskType)) {
                    CrawlTask task = GSON.fromJson(lease.payloadJson, CrawlTask.class);
                    if (task != null && !documents.containsKey(task.bookId)) {
                        crawlClaims.remove(task.bookId, lease.ownerWorkerId);
                        task.attempt += 1;
                        crawlQueue.offer(task);
                        log("task_requeued", Map.of(
                            "task_id", task.taskId,
                            "task_type", "crawl",
                            "book_id", task.bookId,
                            "run_id", safe(task.runId),
                            "dead_member_uuid", deadUuid,
                            "takeover_worker_id", config.workerId
                        ));
                    }
                } else if ("index".equals(lease.taskType)) {
                    IndexTask task = GSON.fromJson(lease.payloadJson, IndexTask.class);
                    if (task != null) {
                        task.attempt += 1;
                        indexQueue.offer(task);
                        log("task_requeued", Map.of(
                            "task_id", task.taskId,
                            "task_type", "index",
                            "book_id", task.bookId,
                            "run_id", safe(task.runId),
                            "dead_member_uuid", deadUuid,
                            "takeover_worker_id", config.workerId
                        ));
                    }
                }
            }
        } catch (Exception e) {
            log("takeover_error", Map.of("error", safe(e.getMessage()), "dead_member_uuid", deadUuid));
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
        return doCrawl(bookId, runId, sourceUrl, taskId, false);
    }

    @Override
    public Map<String, Object> executeDirectIndex(int bookId, String runId) throws Exception {
        String taskId = RequestIds.newTaskId("index");
        return doIndex(bookId, runId, taskId);
    }

    private Map<String, Object> doCrawl(int bookId,
                                        String runId,
                                        String sourceUrl,
                                        String taskId,
                                        boolean emitIndexTask) throws Exception {
        long startNs = System.nanoTime();
        DocumentRecord existing = documents.get(bookId);
        if (existing != null) {
            if (emitIndexTask) {
                emitIndexWork(existing, runId);
            }
            Map<String, Object> resp = new LinkedHashMap<>();
            resp.put("book_id", bookId);
            resp.put("status", "already_available");
            resp.put("doc_sha256", existing.docSha256);
            resp.put("worker_id", config.workerId);
            resp.put("task_id", taskId);
            return resp;
        }

        boolean claimed = crawlClaims.putIfAbsent(bookId, config.workerId) == null;
        if (!claimed) {
            DocumentRecord concurrent = documents.get(bookId);
            if (concurrent != null) {
                Map<String, Object> resp = new LinkedHashMap<>();
                resp.put("book_id", bookId);
                resp.put("status", "already_available");
                resp.put("doc_sha256", concurrent.docSha256);
                resp.put("worker_id", config.workerId);
                resp.put("task_id", taskId);
                return resp;
            }
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
        String sourcePath = "hazelcast://documents/" + bookId;
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
        documents.put(bookId, record);

        if (emitIndexTask) {
            emitIndexWork(record, runId);
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

    private void emitIndexWork(DocumentRecord doc, String runId) {
        IndexTask indexTask = new IndexTask(
            RequestIds.newTaskId("index"),
            doc.bookId,
            safe(runId),
            doc.docSha256,
            "documents/" + doc.bookId + ".header",
            "documents/" + doc.bookId + ".body",
            1,
            System.currentTimeMillis()
        );
        indexQueue.offer(indexTask);
    }

    private Map<String, Object> doIndex(int bookId, String runId, String taskId) throws Exception {
        long startNs = System.nanoTime();
        DocumentRecord doc = documents.get(bookId);
        if (doc == null) {
            throw new NoSuchElementException("Book " + bookId + " not found in distributed documents map");
        }

        String actualSha = Checksums.documentSha256(doc.header, doc.body);
        if (doc.docSha256 != null && !doc.docSha256.isBlank() && !doc.docSha256.equals(actualSha)) {
            throw new IOException("Document SHA-256 mismatch for book " + bookId);
        }

        Book parsed = HeaderParser.parse(bookId, doc.header, doc.sourcePath);
        List<String> terms = new ArrayList<>(Tokenizer.tokenizeDistinct(doc.body));
        AddBookToPostingProcessor processor = new AddBookToPostingProcessor(bookId);
        for (int i = 0; i < terms.size(); i += ENTRY_PROCESSOR_CHUNK_SIZE) {
            int end = Math.min(terms.size(), i + ENTRY_PROCESSOR_CHUNK_SIZE);
            Set<String> chunk = new HashSet<>(terms.subList(i, end));
            invertedIndex.executeOnKeys(chunk, processor);
        }

        long version = clusterMeta.executeOnKey(PUBLISHED_VERSION_KEY, new IncrementVersionProcessor());
        parsed.indexedBy = config.workerId;
        parsed.indexVersion = version;
        parsed.docSha256 = actualSha;
        // Commit metadata last so search only observes fully indexed books
        books.put(bookId, parsed);

        long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
        log("index_done", Map.of(
            "task_id", taskId,
            "run_id", safe(runId),
            "role", "index",
            "book_id", bookId,
            "doc_sha256", actualSha,
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
        long version = currentPublishedVersion();
        if (distinctTerms == null || distinctTerms.isEmpty()) {
            return new SearchResponseData(0, Collections.emptyList(), version);
        }

        Map<String, Set<Integer>> postings = invertedIndex.getAll(new HashSet<>(distinctTerms));
        Set<Integer> intersection = null;
        for (String term : distinctTerms) {
            Set<Integer> posting = postings.get(term);
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

        Map<Integer, Book> committed = books.getAll(intersection);
        QueryMatcher.PageResult page = QueryMatcher.filterSortAndPaginate(
            committed.values(), author, language, year, limit, offset
        );
        return new SearchResponseData(page.totalMatches, page.results, version);
    }

    @Override
    public ReadinessStatus checkSearchReadiness() {
        if (!started.get() || hazelcast == null || !hazelcast.getLifecycleService().isRunning()) {
            return new ReadinessStatus(false, "hazelcast_not_running", 0L, 0);
        }
        try {
            long version = currentPublishedVersion();
            int bookCount = books.size();
            if (version < config.minRequiredIndexVersion) {
                return new ReadinessStatus(
                    false,
                    "waiting_for_index_version_" + config.minRequiredIndexVersion,
                    version,
                    bookCount
                );
            }
            return new ReadinessStatus(true, "ready", version, bookCount);
        } catch (Exception e) {
            return new ReadinessStatus(false, "hazelcast_error", 0L, 0);
        }
    }

    @Override
    public Map<String, Object> clusterDiagnostics() {
        Map<String, Object> diag = new LinkedHashMap<>();
        diag.put("design_mode", designMode());
        diag.put("worker_id", config.workerId);
        diag.put("cluster_name", config.hazelcastClusterName);
        diag.put("backup_count", config.hazelcastBackupCount);

        if (hazelcast == null || !hazelcast.getLifecycleService().isRunning()) {
            diag.put("running", false);
            return diag;
        }

        PartitionService partitionService = hazelcast.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();
        Map<String, Integer> ownedCounts = new HashMap<>();
        for (Partition p : partitions) {
            Member owner = p.getOwner();
            if (owner != null) {
                String uuid = owner.getUuid().toString();
                ownedCounts.put(uuid, ownedCounts.getOrDefault(uuid, 0) + 1);
            }
        }

        List<Map<String, Object>> membersList = new ArrayList<>();
        for (Member m : hazelcast.getCluster().getMembers()) {
            String uuid = m.getUuid().toString();
            Map<String, Object> entry = new LinkedHashMap<>();
            entry.put("uuid", uuid);
            entry.put("address", m.getAddress().toString());
            entry.put("worker_id", m.getAttribute("worker_id"));
            entry.put("host_name", m.getAttribute("host_name"));
            entry.put("owned_partitions", ownedCounts.getOrDefault(uuid, 0));
            membersList.add(entry);
        }

        List<Map<String, Object>> sampleOwners = new ArrayList<>();
        int count = 0;
        for (String term : invertedIndex.keySet()) {
            if (count >= 20) {
                break;
            }
            Partition p = partitionService.getPartition(term);
            Member owner = p.getOwner();
            Map<String, Object> sample = new LinkedHashMap<>();
            sample.put("term", term);
            sample.put("partition_id", p.getPartitionId());
            if (owner != null) {
                sample.put("owner_uuid", owner.getUuid().toString());
                sample.put("owner_address", owner.getAddress().toString());
                sample.put("owner_worker_id", owner.getAttribute("worker_id"));
            }
            sampleOwners.add(sample);
            count++;
        }

        diag.put("running", true);
        diag.put("member_count", membersList.size());
        diag.put("partition_count", partitions.size());
        diag.put("cluster_safe", partitionService.isClusterSafe());
        diag.put("local_member_uuid", getLocalMemberUuid());
        diag.put("members", membersList);
        diag.put("sample_term_owners", sampleOwners);
        diag.put("documents", documents.size());
        diag.put("indexed_books", books.size());
        diag.put("terms", invertedIndex.size());
        diag.put("index_version", currentPublishedVersion());
        diag.put("in_flight_tasks", inFlightTasks.size());
        return diag;
    }

    @Override
    public Map<String, Object> executeDistributedDemo(String payload) throws Exception {
        if (hazelcast == null || !hazelcast.getLifecycleService().isRunning()) {
            throw new IllegalStateException("Hazelcast cluster is not running");
        }
        IExecutorService exec = hazelcast.getExecutorService("search-exec");
        Map<Member, Future<Map<String, Object>>> futures = exec.submitToAllMembers(new DistributedDemoCallable(payload));
        List<Map<String, Object>> executions = new ArrayList<>();
        for (Map.Entry<Member, Future<Map<String, Object>>> entry : futures.entrySet()) {
            executions.add(entry.getValue().get(10, TimeUnit.SECONDS));
        }
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("design_mode", designMode());
        result.put("initiator_worker_id", config.workerId);
        result.put("member_count", executions.size());
        result.put("executions", executions);
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
        List<Integer> bookIds = new ArrayList<>();
        books.keySet().stream().sorted().forEach(id -> {
            Book b = books.get(id);
            if (b != null) {
                bookVersions.put(String.valueOf(id), b.indexVersion);
                bookIds.add(id);
            }
        });

        Map<String, Object> summary = new LinkedHashMap<>();
        summary.put("design_mode", designMode());
        summary.put("worker_id", config.workerId);
        summary.put("member_count", hazelcast != null ? hazelcast.getCluster().getMembers().size() : 0);
        summary.put("backup_count", config.hazelcastBackupCount);
        summary.put("index_version", currentPublishedVersion());
        summary.put("document_count", docChecksums.size());
        summary.put("indexed_book_count", bookIds.size());
        summary.put("document_checksums", docChecksums);
        summary.put("indexed_book_versions", bookVersions);
        summary.put("indexed_book_ids", bookIds);
        return summary;
    }

    private long currentPublishedVersion() {
        if (clusterMeta == null) {
            return 0L;
        }
        Long version = clusterMeta.get(PUBLISHED_VERSION_KEY);
        return version == null ? 0L : version;
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
