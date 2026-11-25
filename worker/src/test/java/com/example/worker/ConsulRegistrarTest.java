package com.example.worker;

import com.example.common.obs.StructuredLogger;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.InputStream;
import java.lang.reflect.Type;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConsulRegistrarTest {

    @TempDir
    Path tempDir;

    @Test
    @SuppressWarnings("unchecked")
    void registersWithReadyCheckAndDeregistersCleanly() throws Exception {
        AtomicReference<String> registeredMethod = new AtomicReference<>();
        AtomicReference<String> registeredBody = new AtomicReference<>();
        AtomicReference<String> deregisterPath = new AtomicReference<>();

        HttpServer mockConsul = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockConsul.createContext("/v1/agent/service/register", exchange -> {
            registeredMethod.set(exchange.getRequestMethod());
            try (InputStream is = exchange.getRequestBody()) {
                registeredBody.set(new String(is.readAllBytes(), StandardCharsets.UTF_8));
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        mockConsul.createContext("/v1/agent/service/deregister/", exchange -> {
            deregisterPath.set(exchange.getRequestURI().getPath());
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        mockConsul.start();

        try (StructuredLogger logger = new StructuredLogger(tempDir.resolve("consul.jsonl"), "w1", "host1")) {
            WorkerConfig config = WorkerConfig.fromMap(Map.of(
                "WORKER_ID", "w1",
                "HOST_NAME", "lab-01",
                "HOST_IP", "10.0.0.11",
                "DESIGN_MODE", "in-memory",
                "CONSUL_ENABLED", "true",
                "CONSUL_URL", "http://127.0.0.1:" + mockConsul.getAddress().getPort(),
                "CONSUL_SERVICE_NAME", "search-service",
                "CONSUL_CHECK_INTERVAL", "2s",
                "CONSUL_CHECK_TIMEOUT", "1s",
                "CONSUL_DEREGISTER_AFTER", "30s"
            ));

            ConsulRegistrar registrar = new ConsulRegistrar(config, logger);
            assertTrue(registrar.register(7055));
            assertTrue(registrar.isRegistered());
            assertEquals("PUT", registeredMethod.get());

            Type mapType = new TypeToken<Map<String, Object>>() {}.getType();
            Map<String, Object> payload = new Gson().fromJson(registeredBody.get(), mapType);
            assertEquals("search-w1", payload.get("ID"));
            assertEquals("search-service", payload.get("Name"));
            assertEquals("10.0.0.11", payload.get("Address"));
            assertEquals(7055, ((Number) payload.get("Port")).intValue());
            assertTrue(((List<String>) payload.get("Tags")).containsAll(List.of("in-memory", "worker:w1", "host:lab-01")));

            Map<String, Object> check = (Map<String, Object>) payload.get("Check");
            assertEquals("http://10.0.0.11:7055/internal/ready", check.get("HTTP"));
            assertEquals("2s", check.get("Interval"));
            assertEquals("1s", check.get("Timeout"));
            assertEquals("30s", check.get("DeregisterCriticalServiceAfter"));

            assertTrue(registrar.deregister());
            assertFalse(registrar.isRegistered());
            assertEquals("/v1/agent/service/deregister/search-w1", deregisterPath.get());
        } finally {
            mockConsul.stop(0);
        }
    }
}
