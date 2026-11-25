package com.example.worker;

import com.example.common.obs.StructuredLogger;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Registers and deregisters a worker's search endpoint with Consul HTTP Agent API.
 */
public class ConsulRegistrar {
    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final WorkerConfig config;
    private final StructuredLogger logger;
    private final HttpClient httpClient;
    private final String serviceId;
    private volatile boolean registered = false;

    public ConsulRegistrar(WorkerConfig config, StructuredLogger logger) {
        this(
            config,
            logger,
            HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build()
        );
    }

    public ConsulRegistrar(WorkerConfig config, StructuredLogger logger, HttpClient httpClient) {
        this.config = config;
        this.logger = logger;
        this.httpClient = httpClient;
        this.serviceId = "search-" + config.workerId;
    }

    public String getServiceId() {
        return serviceId;
    }

    public boolean isRegistered() {
        return registered;
    }

    public synchronized boolean register(int actualPort) {
        if (!config.consulEnabled) {
            return false;
        }
        Map<String, Object> check = new LinkedHashMap<>();
        check.put("HTTP", "http://" + config.hostIp + ":" + actualPort + "/internal/ready");
        check.put("Interval", config.consulCheckInterval);
        check.put("Timeout", config.consulCheckTimeout);
        check.put("DeregisterCriticalServiceAfter", config.consulDeregisterAfter);

        Map<String, Object> payload = new LinkedHashMap<>();
        payload.put("ID", serviceId);
        payload.put("Name", config.consulServiceName);
        payload.put("Address", config.hostIp);
        payload.put("Port", actualPort);
        payload.put("Tags", List.of(
            config.designMode,
            "worker:" + config.workerId,
            "host:" + config.hostName
        ));
        payload.put("Check", check);

        String url = config.consulUrl + "/v1/agent/service/register";
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(GSON.toJson(payload), StandardCharsets.UTF_8))
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                registered = true;
                log("consul_registered", Map.of(
                    "service_id", serviceId,
                    "service_name", config.consulServiceName,
                    "port", actualPort
                ));
                return true;
            }
            log("consul_error", Map.of(
                "action", "register",
                "service_id", serviceId,
                "status_code", resp.statusCode(),
                "body", resp.body()
            ));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log("consul_error", Map.of(
                "action", "register",
                "service_id", serviceId,
                "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
            ));
        }
        return false;
    }

    public synchronized boolean deregister() {
        if (!config.consulEnabled) {
            registered = false;
            return false;
        }
        String url = config.consulUrl + "/v1/agent/service/deregister/" + serviceId;
        try {
            HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .PUT(HttpRequest.BodyPublishers.noBody())
                .build();
            HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (resp.statusCode() >= 200 && resp.statusCode() < 300) {
                registered = false;
                log("consul_deregistered", Map.of("service_id", serviceId));
                return true;
            }
            log("consul_error", Map.of(
                "action", "deregister",
                "service_id", serviceId,
                "status_code", resp.statusCode(),
                "body", resp.body()
            ));
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            log("consul_error", Map.of(
                "action", "deregister",
                "service_id", serviceId,
                "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
            ));
        }
        return false;
    }

    private void log(String event, Map<String, Object> fields) {
        if (logger != null) {
            logger.log(event, fields);
        }
    }
}
