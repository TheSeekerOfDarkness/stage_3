package com.example.worker;

import com.example.common.net.GutenbergDownloader;
import com.example.common.obs.RequestIds;
import com.example.common.obs.StructuredLogger;
import com.example.common.text.Tokenizer;
import com.example.worker.backend.ClusterBackend;
import com.example.worker.backend.InMemoryBackend;
import com.example.worker.backend.ReadinessStatus;
import com.example.worker.backend.SearchResponseData;
import com.example.worker.designa.DesignABackend;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.javalin.Javalin;
import io.javalin.http.Context;
import java.lang.reflect.Type;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified interchangeable worker process supporting dynamic roles, Consul readiness,
 * and pluggable distributed search cluster backends.
 */
public class WorkerApp implements AutoCloseable {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();

    private final WorkerConfig config;
    private final ClusterBackend backend;
    private final StructuredLogger logger;
    private final ConsulRegistrar consulRegistrar;
    private final RoleManager roleManager;
    private final boolean ownsLogger;

    private final AtomicBoolean lastReadyState = new AtomicBoolean(false);
    private Javalin app;
    private volatile int boundPort = 0;

    public WorkerApp(WorkerConfig config, ClusterBackend backend, StructuredLogger logger) {
        this(config, backend, logger, new ConsulRegistrar(config, logger), false);
    }

    public WorkerApp(WorkerConfig config,
                     ClusterBackend backend,
                     StructuredLogger logger,
                     ConsulRegistrar consulRegistrar,
                     boolean ownsLogger) {
        this.config = config;
        this.backend = backend;
        this.logger = logger;
        this.consulRegistrar = consulRegistrar;
        this.ownsLogger = ownsLogger;
        this.roleManager = new RoleManager(config.initialRoles, backend, consulRegistrar, logger);
    }

    public synchronized void start() throws Exception {
        backend.start();

        app = Javalin.create(javalinConfig -> {
            javalinConfig.http.defaultContentType = "application/json";
            javalinConfig.bundledPlugins.enableCors(cors -> cors.addRule(it -> it.anyHost()));
        });

        registerRoutes(app);
        app.start(config.port);
        this.boundPort = app.port();
        roleManager.initialize(this.boundPort);
    }

    public int getPort() {
        return boundPort;
    }

    public RoleManager getRoleManager() {
        return roleManager;
    }

    private void registerRoutes(Javalin javalin) {
        javalin.get("/search", this::handleSearch);
        javalin.get("/internal/ready", this::handleReady);
        javalin.get("/internal/health", this::handleHealth);
        javalin.get("/internal/info", this::handleInfo);
        javalin.post("/internal/roles", this::handleSetRoles);
        javalin.put("/internal/roles", this::handleSetRoles);
        javalin.post("/internal/drain", this::handleDrain);
        javalin.post("/internal/resume", this::handleResume);
        javalin.post("/internal/tasks/crawl", this::handleEnqueueCrawl);
        javalin.post("/internal/tasks/crawl/batch", this::handleEnqueueCrawlBatch);
        javalin.post("/internal/ingest/{book_id}", this::handleDirectIngest);
        javalin.post("/internal/index/{book_id}", this::handleDirectIndex);
        javalin.get("/internal/integrity", ctx -> ctx.result(GSON.toJson(backend.integritySummary())));
        javalin.get("/internal/cluster/partitions", ctx -> ctx.result(GSON.toJson(backend.clusterDiagnostics())));
        javalin.post("/internal/cluster/exec-demo", this::handleExecDemo);
    }

    private void handleSearch(Context ctx) {
        long startNs = System.nanoTime();
        String requestId = RequestIds.resolveOrCreate(ctx.header("X-Request-Id"));
        ctx.header("X-Request-Id", requestId);
        ctx.header("X-Served-By", config.workerId + "@" + config.hostName);

        if (roleManager.isDraining() || !roleManager.hasRole(WorkerRole.SEARCH)) {
            sendError(ctx, 503, "Worker is not ready to serve search requests");
            return;
        }

        String query = ctx.queryParam("q");
        if (query == null) {
            sendError(ctx, 400, "Query parameter 'q' is required");
            return;
        }

        String author = ctx.queryParam("author");
        String language = ctx.queryParam("language");
        Integer year = null;
        String yearParam = ctx.queryParam("year");
        if (yearParam != null && !yearParam.trim().isEmpty()) {
            try {
                year = Integer.parseInt(yearParam.trim());
            } catch (NumberFormatException e) {
                sendError(ctx, 400, "Parameter 'year' must be a valid integer");
                return;
            }
        }

        int limit = 20;
        int offset = 0;
        String limitParam = ctx.queryParam("limit");
        if (limitParam != null) {
            try {
                limit = Integer.parseInt(limitParam.trim());
            } catch (NumberFormatException e) {
                sendError(ctx, 400, "Parameter 'limit' must be a valid integer");
                return;
            }
            if (limit < 1 || limit > 100) {
                sendError(ctx, 400, "Parameter 'limit' must be between 1 and 100");
                return;
            }
        }

        String offsetParam = ctx.queryParam("offset");
        if (offsetParam != null) {
            try {
                offset = Integer.parseInt(offsetParam.trim());
            } catch (NumberFormatException e) {
                sendError(ctx, 400, "Parameter 'offset' must be a valid integer");
                return;
            }
            if (offset < 0) {
                sendError(ctx, 400, "Parameter 'offset' must be 0 or greater");
                return;
            }
        }

        List<String> distinctTerms = new ArrayList<>(Tokenizer.tokenizeDistinct(query));
        if (distinctTerms.isEmpty()) {
            long version = backend.checkSearchReadiness().indexVersion;
            ctx.header("X-Index-Version", String.valueOf(version));
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("query", query);
            response.put("total_matches", 0);
            response.put("results", Collections.emptyList());
            ctx.result(GSON.toJson(response));
            logSearch(requestId, query, 0, version, startNs);
            return;
        }

        try {
            SearchResponseData result = backend.search(distinctTerms, author, language, year, limit, offset);
            ctx.header("X-Index-Version", String.valueOf(result.indexVersion));

            Map<String, Object> appliedFilters = new LinkedHashMap<>();
            if (author != null) appliedFilters.put("author", author);
            if (language != null) appliedFilters.put("language", language);
            if (year != null) appliedFilters.put("year", year);

            Map<String, Object> response = new LinkedHashMap<>();
            response.put("query", query);
            response.put("filters", appliedFilters);
            response.put("total_matches", result.totalMatches);
            response.put("offset", offset);
            response.put("limit", limit);
            response.put("results", result.results);

            ctx.result(GSON.toJson(response));
            logSearch(requestId, query, result.totalMatches, result.indexVersion, startNs);
        } catch (Exception e) {
            sendError(ctx, 500, "Search query error: " + e.getMessage());
        }
    }

    private void handleReady(Context ctx) {
        boolean draining = roleManager.isDraining();
        boolean searchRole = roleManager.hasRole(WorkerRole.SEARCH);
        ReadinessStatus status = backend.checkSearchReadiness();
        boolean ready = !draining && searchRole && status.ready;

        String reason;
        if (draining) {
            reason = "draining";
        } else if (!searchRole) {
            reason = "search_role_disabled";
        } else {
            reason = status.reason;
        }

        boolean prev = lastReadyState.getAndSet(ready);
        if (prev != ready && logger != null) {
            logger.log("readiness_changed", Map.of(
                "ready", ready,
                "reason", reason,
                "index_version", status.indexVersion,
                "indexed_books", status.indexedBookCount
            ));
        }

        if (!ready) {
            ctx.status(503);
        }

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("ready", ready);
        payload.put("reason", reason);
        payload.put("worker_id", config.workerId);
        payload.put("host", config.hostName);
        payload.put("design_mode", backend.designMode());
        payload.put("index_version", status.indexVersion);
        payload.put("indexed_books", status.indexedBookCount);
        ctx.result(GSON.toJson(payload));
    }

    private void handleHealth(Context ctx) {
        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("status", "alive");
        payload.put("worker_id", config.workerId);
        payload.put("host", config.hostName);
        payload.put("pid", ProcessHandle.current().pid());
        ctx.result(GSON.toJson(payload));
    }

    private void handleInfo(Context ctx) {
        ReadinessStatus status = backend.checkSearchReadiness();
        boolean ready = !roleManager.isDraining() && roleManager.hasRole(WorkerRole.SEARCH) && status.ready;

        Map<String, Object> threads = new LinkedHashMap<>();
        threads.put("crawl", config.crawlThreads);
        threads.put("index", config.indexThreads);
        threads.put("search", config.searchThreads);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("worker_id", config.workerId);
        payload.put("host", config.hostName);
        payload.put("host_ip", config.hostIp);
        payload.put("port", boundPort);
        payload.put("pid", ProcessHandle.current().pid());
        payload.put("design_mode", backend.designMode());
        payload.put("roles", WorkerRole.toWireNames(roleManager.getActiveRoles()));
        payload.put("draining", roleManager.isDraining());
        payload.put("consul_registered", roleManager.isConsulRegistered());
        payload.put("threads", threads);
        payload.put("ready", ready);
        payload.put("index_version", status.indexVersion);
        payload.put("indexed_books", status.indexedBookCount);
        ctx.result(GSON.toJson(payload));
    }

    private void handleSetRoles(Context ctx) {
        Map<String, Object> body = parseJsonMap(ctx);
        if (body == null || !(body.get("roles") instanceof List<?> rawRoles)) {
            sendError(ctx, 400, "JSON body must contain a 'roles' array");
            return;
        }

        EnumSet<WorkerRole> parsed = EnumSet.noneOf(WorkerRole.class);
        try {
            for (Object item : rawRoles) {
                if (item == null) {
                    throw new IllegalArgumentException("Role entry cannot be null");
                }
                parsed.add(WorkerRole.fromString(item.toString()));
            }
        } catch (IllegalArgumentException e) {
            sendError(ctx, 400, e.getMessage());
            return;
        }

        roleManager.setRoles(parsed);
        ReadinessStatus status = backend.checkSearchReadiness();
        boolean ready = !roleManager.isDraining() && roleManager.hasRole(WorkerRole.SEARCH) && status.ready;

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("worker_id", config.workerId);
        response.put("roles", WorkerRole.toWireNames(roleManager.getActiveRoles()));
        response.put("ready", ready);
        ctx.result(GSON.toJson(response));
    }

    private void handleDrain(Context ctx) {
        roleManager.beginDrain();
        ctx.result(GSON.toJson(Map.of(
            "worker_id", config.workerId,
            "draining", true
        )));
    }

    private void handleResume(Context ctx) {
        roleManager.resume();
        ctx.result(GSON.toJson(Map.of(
            "worker_id", config.workerId,
            "draining", false,
            "roles", WorkerRole.toWireNames(roleManager.getActiveRoles())
        )));
    }

    private void handleEnqueueCrawl(Context ctx) {
        Map<String, Object> body = parseJsonMap(ctx);
        if (body == null || body.get("book_id") == null) {
            sendError(ctx, 400, "JSON body must contain 'book_id'");
            return;
        }
        int bookId;
        try {
            bookId = ((Number) body.get("book_id")).intValue();
        } catch (ClassCastException e) {
            sendError(ctx, 400, "'book_id' must be a positive integer");
            return;
        }
        if (bookId <= 0) {
            sendError(ctx, 400, "'book_id' must be a positive integer");
            return;
        }
        String runId = stringValue(body.get("run_id"), "");
        String sourceUrl = stringValue(body.get("source_url"), null);

        try {
            String taskId = backend.enqueueCrawlTask(bookId, runId, sourceUrl);
            ctx.status(202);
            ctx.result(GSON.toJson(Map.of(
                "task_id", taskId,
                "book_id", bookId,
                "run_id", runId,
                "status", "queued"
            )));
        } catch (Exception e) {
            sendError(ctx, 500, "Failed to enqueue crawl task: " + e.getMessage());
        }
    }

    private void handleEnqueueCrawlBatch(Context ctx) {
        Map<String, Object> body = parseJsonMap(ctx);
        if (body == null || !(body.get("book_ids") instanceof List<?> rawIds)) {
            sendError(ctx, 400, "JSON body must contain a 'book_ids' array");
            return;
        }
        List<Integer> bookIds = new ArrayList<>();
        for (Object item : rawIds) {
            if (!(item instanceof Number num) || num.intValue() <= 0) {
                sendError(ctx, 400, "All 'book_ids' entries must be positive integers");
                return;
            }
            bookIds.add(num.intValue());
        }
        String runId = stringValue(body.get("run_id"), "");
        String sourceUrlTemplate = stringValue(body.get("source_url_template"), null);

        try {
            List<Map<String, Object>> tasks = backend.enqueueCrawlBatch(bookIds, runId, sourceUrlTemplate);
            ctx.status(202);
            ctx.result(GSON.toJson(Map.of(
                "count", tasks.size(),
                "run_id", runId,
                "tasks", tasks
            )));
        } catch (Exception e) {
            sendError(ctx, 500, "Failed to enqueue crawl batch: " + e.getMessage());
        }
    }

    private void handleDirectIngest(Context ctx) {
        if (!roleManager.hasRole(WorkerRole.INGEST)) {
            sendError(ctx, 503, "Ingest role is disabled on worker " + config.workerId);
            return;
        }
        int bookId = readBookId(ctx);
        if (bookId == -1) {
            return;
        }
        Map<String, Object> body = parseOptionalJsonMap(ctx);
        String runId = stringValue(body.get("run_id"), "");
        String sourceUrl = stringValue(body.get("source_url"), null);

        try {
            Map<String, Object> result = backend.executeDirectIngest(bookId, runId, sourceUrl);
            ctx.result(GSON.toJson(result));
        } catch (GutenbergDownloader.BookNotFoundException e) {
            sendError(ctx, 404, e.getMessage());
        } catch (IllegalArgumentException e) {
            sendError(ctx, 422, e.getMessage());
        } catch (GutenbergDownloader.DownloaderException e) {
            int code = e.getStatusCode() >= 400 && e.getStatusCode() < 600 ? e.getStatusCode() : 502;
            sendError(ctx, code, e.getMessage());
        } catch (Exception e) {
            sendError(ctx, 500, "Ingest failed for book " + bookId + ": " + e.getMessage());
        }
    }

    private void handleDirectIndex(Context ctx) {
        if (!roleManager.hasRole(WorkerRole.INDEX)) {
            sendError(ctx, 503, "Index role is disabled on worker " + config.workerId);
            return;
        }
        int bookId = readBookId(ctx);
        if (bookId == -1) {
            return;
        }
        Map<String, Object> body = parseOptionalJsonMap(ctx);
        String runId = stringValue(body.get("run_id"), "");

        try {
            Map<String, Object> result = backend.executeDirectIndex(bookId, runId);
            ctx.result(GSON.toJson(result));
        } catch (NoSuchElementException e) {
            sendError(ctx, 404, e.getMessage());
        } catch (Exception e) {
            sendError(ctx, 500, "Index failed for book " + bookId + ": " + e.getMessage());
        }
    }

    private void handleExecDemo(Context ctx) {
        Map<String, Object> body = parseOptionalJsonMap(ctx);
        String payload = stringValue(body.get("payload"), "");
        try {
            ctx.result(GSON.toJson(backend.executeDistributedDemo(payload)));
        } catch (Exception e) {
            sendError(ctx, 500, "Distributed execution demo failed: " + e.getMessage());
        }
    }

    private void logSearch(String requestId, String query, int totalMatches, long indexVersion, long startNs) {
        if (logger != null) {
            long latencyMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNs);
            logger.log("search_served", Map.of(
                "request_id", requestId,
                "role", "search",
                "query", query,
                "total_matches", totalMatches,
                "index_version", indexVersion,
                "latency_ms", latencyMs
            ));
        }
    }

    private static int readBookId(Context ctx) {
        try {
            int bookId = Integer.parseInt(ctx.pathParam("book_id"));
            if (bookId > 0) {
                return bookId;
            }
        } catch (NumberFormatException ignored) {
        }
        sendError(ctx, 400, "book_id must be a positive integer");
        return -1;
    }

    private static Map<String, Object> parseJsonMap(Context ctx) {
        String raw = ctx.body();
        if (raw == null || raw.isBlank()) {
            return null;
        }
        try {
            return GSON.fromJson(raw, MAP_TYPE);
        } catch (Exception e) {
            return null;
        }
    }

    private static Map<String, Object> parseOptionalJsonMap(Context ctx) {
        Map<String, Object> map = parseJsonMap(ctx);
        return map != null ? map : Collections.emptyMap();
    }

    private static String stringValue(Object value, String defaultVal) {
        return value == null ? defaultVal : value.toString();
    }

    private static void sendError(Context ctx, int status, String message) {
        ctx.status(status);
        ctx.result(GSON.toJson(Map.of("error", message != null ? message : "unknown_error")));
    }

    @Override
    public synchronized void close() {
        try {
            roleManager.beginDrain();
            if (config.drainDelayMs > 0) {
                Thread.sleep(config.drainDelayMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (app != null) {
                app.stop();
                app = null;
            }
            backend.stop();
            if (ownsLogger && logger != null) {
                try {
                    logger.close();
                } catch (Exception ignored) {
                }
            }
        }
    }

    public static ClusterBackend createBackend(WorkerConfig config, StructuredLogger logger) {
        return switch (config.designMode) {
            case "in-memory" -> new InMemoryBackend(config, logger);
            case "design-a" -> DesignABackend.createDefault(config, logger);
            case "design-b" -> throw new UnsupportedOperationException("Design B backend is not yet configured");
            default -> throw new IllegalArgumentException("Unknown DESIGN_MODE: " + config.designMode);
        };
    }

    public static void main(String[] args) throws Exception {
        WorkerConfig config = WorkerConfig.fromEnv();
        Path logPath = Paths.get(config.logDir, config.workerId + ".jsonl");
        StructuredLogger logger = new StructuredLogger(logPath, config.workerId, config.hostName);
        ClusterBackend backend = createBackend(config, logger);
        ConsulRegistrar consulRegistrar = new ConsulRegistrar(config, logger);
        WorkerApp workerApp = new WorkerApp(config, backend, logger, consulRegistrar, true);

        Runtime.getRuntime().addShutdownHook(new Thread(workerApp::close, "worker-shutdown"));
        workerApp.start();
    }
}
