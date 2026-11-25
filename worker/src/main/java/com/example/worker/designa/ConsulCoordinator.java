package com.example.worker.designa;

import com.example.worker.WorkerConfig;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;

/**
 * Consul KV and Session-based distributed coordination for Design A crawl claims and publisher lock.
 */
public class ConsulCoordinator implements LeaderElector {
    private static final Gson GSON = new Gson();
    private static final Type MAP_TYPE = new TypeToken<Map<String, Object>>() {}.getType();
    private static final String PUBLISHER_LOCK_KEY = "locks/design-a/publisher";

    private final WorkerConfig config;
    private final HttpClient httpClient;
    private volatile String sessionId;

    public ConsulCoordinator(WorkerConfig config) {
        this(config, HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build());
    }

    public ConsulCoordinator(WorkerConfig config, HttpClient httpClient) {
        this.config = config;
        this.httpClient = httpClient;
    }

    @Override
    public boolean tryClaimCrawl(int bookId, String workerId) {
        if (!config.consulEnabled) {
            return true;
        }
        String url = config.consulUrl + "/v1/kv/claims/crawl/" + bookId + "?cas=0";
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .PUT(HttpRequest.BodyPublishers.ofString(workerId, StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            return resp.statusCode() == 200 && "true".equalsIgnoreCase(resp.body().trim());
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return false;
        }
    }

    @Override
    public synchronized boolean tryAcquirePublisherLock() {
        if (!config.consulEnabled) {
            return true;
        }
        if (sessionId == null && !createSession()) {
            return false;
        }
        String url = config.consulUrl + "/v1/kv/" + PUBLISHER_LOCK_KEY + "?acquire=" + sessionId;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .PUT(HttpRequest.BodyPublishers.ofString(config.workerId, StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() == 200 && "true".equalsIgnoreCase(resp.body().trim())) {
                return true;
            }
            if (resp.statusCode() >= 400) {
                sessionId = null;
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            sessionId = null;
        }
        return false;
    }

    @Override
    public synchronized boolean renewPublisherLock() {
        if (!config.consulEnabled) {
            return true;
        }
        if (sessionId == null) {
            return false;
        }
        String url = config.consulUrl + "/v1/session/renew/" + sessionId;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() == 200) {
                return true;
            }
            sessionId = null;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            sessionId = null;
        }
        return false;
    }

    @Override
    public synchronized void releasePublisherLock() {
        if (!config.consulEnabled || sessionId == null) {
            sessionId = null;
            return;
        }
        String url = config.consulUrl + "/v1/session/destroy/" + sessionId;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
            httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        } finally {
            sessionId = null;
        }
    }

    @Override
    public String currentPublisherLeader() {
        if (!config.consulEnabled) {
            return config.workerId;
        }
        String url = config.consulUrl + "/v1/kv/" + PUBLISHER_LOCK_KEY + "?raw";
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .GET()
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() == 200 && resp.body() != null && !resp.body().isBlank()) {
                return resp.body().trim();
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }

    private boolean createSession() {
        String url = config.consulUrl + "/v1/session/create";
        String body = GSON.toJson(Map.of(
            "Name", "design-a-publisher-" + config.workerId,
            "TTL", "10s",
            "LockDelay", "1s",
            "Behavior", "release"
        ));
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(3))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(body, StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() == 200) {
                Map<String, Object> parsed = GSON.fromJson(resp.body(), MAP_TYPE);
                Object id = parsed != null ? parsed.get("ID") : null;
                if (id != null && !id.toString().isBlank()) {
                    this.sessionId = id.toString();
                    return true;
                }
            }
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
        return false;
    }
}
