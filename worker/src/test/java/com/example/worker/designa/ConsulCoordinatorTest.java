package com.example.worker.designa;

import com.example.worker.WorkerConfig;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class ConsulCoordinatorTest {

    @Test
    void claimsCrawlOnceAndManagesSessionLockedPublisherLeadership() throws Exception {
        Map<String, String> kvStore = new ConcurrentHashMap<>();
        Set<String> validSessions = ConcurrentHashMap.newKeySet();
        AtomicInteger sessionSeq = new AtomicInteger(0);
        AtomicReference<String> lockHolder = new AtomicReference<>();

        HttpServer mockConsul = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        mockConsul.createContext("/v1/kv/claims/crawl/", exchange -> {
            String key = exchange.getRequestURI().getPath();
            String query = exchange.getRequestURI().getRawQuery();
            String body;
            try (InputStream is = exchange.getRequestBody()) {
                body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
            boolean ok = query != null && query.contains("cas=0") && kvStore.putIfAbsent(key, body) == null;
            byte[] resp = String.valueOf(ok).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });

        mockConsul.createContext("/v1/session/create", exchange -> {
            String sid = "sess-" + sessionSeq.incrementAndGet();
            validSessions.add(sid);
            byte[] resp = ("{\"ID\":\"" + sid + "\"}").getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });

        mockConsul.createContext("/v1/session/renew/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String sid = path.substring(path.lastIndexOf('/') + 1);
            boolean valid = validSessions.contains(sid);
            exchange.sendResponseHeaders(valid ? 200 : 404, -1);
            exchange.close();
        });

        mockConsul.createContext("/v1/session/destroy/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            String sid = path.substring(path.lastIndexOf('/') + 1);
            validSessions.remove(sid);
            lockHolder.set(null);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });

        mockConsul.createContext("/v1/kv/locks/design-a/publisher", exchange -> {
            if ("GET".equals(exchange.getRequestMethod())) {
                String holder = lockHolder.get();
                if (holder == null) {
                    exchange.sendResponseHeaders(404, -1);
                    exchange.close();
                    return;
                }
                byte[] resp = holder.getBytes(StandardCharsets.UTF_8);
                exchange.sendResponseHeaders(200, resp.length);
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(resp);
                }
                return;
            }
            String body;
            try (InputStream is = exchange.getRequestBody()) {
                body = new String(is.readAllBytes(), StandardCharsets.UTF_8);
            }
            String query = exchange.getRequestURI().getRawQuery();
            String sid = (query != null && query.startsWith("acquire="))
                ? query.substring("acquire=".length())
                : "";
            boolean acquired = validSessions.contains(sid) && lockHolder.compareAndSet(null, body);
            byte[] resp = String.valueOf(acquired).getBytes(StandardCharsets.UTF_8);
            exchange.sendResponseHeaders(200, resp.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(resp);
            }
        });
        mockConsul.start();

        try {
            WorkerConfig c1 = WorkerConfig.fromMap(Map.of(
                "WORKER_ID", "w1",
                "CONSUL_ENABLED", "true",
                "CONSUL_URL", "http://127.0.0.1:" + mockConsul.getAddress().getPort()
            ));
            WorkerConfig c2 = WorkerConfig.fromMap(Map.of(
                "WORKER_ID", "w2",
                "CONSUL_ENABLED", "true",
                "CONSUL_URL", "http://127.0.0.1:" + mockConsul.getAddress().getPort()
            ));

            ConsulCoordinator coord1 = new ConsulCoordinator(c1);
            ConsulCoordinator coord2 = new ConsulCoordinator(c2);

            assertTrue(coord1.tryClaimCrawl(11, "w1"));
            assertFalse(coord2.tryClaimCrawl(11, "w2"));

            assertTrue(coord1.tryAcquirePublisherLock());
            assertTrue(coord1.renewPublisherLock());
            assertEquals("w1", coord2.currentPublisherLeader());
            assertFalse(coord2.tryAcquirePublisherLock());

            coord1.releasePublisherLock();
            assertNull(coord2.currentPublisherLeader());
            assertTrue(coord2.tryAcquirePublisherLock());
            assertEquals("w2", coord1.currentPublisherLeader());
        } finally {
            mockConsul.stop(0);
        }
    }
}
