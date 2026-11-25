package com.example.common;

import com.example.common.net.GutenbergDownloader;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class GutenbergDownloaderTest {

    @Test
    void downloadsSuccessfullyAndHandles404And500Retries() throws Exception {
        AtomicInteger flakyCalls = new AtomicInteger(0);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/books/", exchange -> {
            String path = exchange.getRequestURI().getPath();
            byte[] body;
            int status;
            if (path.endsWith("/11")) {
                status = 200;
                body = "BOOK-11-CONTENT".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/404")) {
                status = 404;
                body = "Not Found".getBytes(StandardCharsets.UTF_8);
            } else if (path.endsWith("/500")) {
                int call = flakyCalls.incrementAndGet();
                if (call < 3) {
                    status = 500;
                    body = "Temporary Error".getBytes(StandardCharsets.UTF_8);
                } else {
                    status = 200;
                    body = "RECOVERED-500".getBytes(StandardCharsets.UTF_8);
                }
            } else {
                status = 503;
                body = "Always Failing".getBytes(StandardCharsets.UTF_8);
            }
            exchange.sendResponseHeaders(status, body.length);
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(body);
            }
        });
        server.start();

        try {
            String template = "http://127.0.0.1:" + server.getAddress().getPort() + "/books/%d";
            GutenbergDownloader downloader = new GutenbergDownloader(
                template,
                HttpClient.newHttpClient(),
                Duration.ofSeconds(5),
                3,
                10
            );

            assertEquals("BOOK-11-CONTENT", downloader.download(11));
            assertThrows(GutenbergDownloader.BookNotFoundException.class, () -> downloader.download(404));
            assertEquals("RECOVERED-500", downloader.download(500));
            assertEquals(3, flakyCalls.get());

            GutenbergDownloader.DownloaderException ex = assertThrows(
                GutenbergDownloader.DownloaderException.class,
                () -> downloader.download(999)
            );
            assertEquals(503, ex.getStatusCode());
        } finally {
            server.stop(0);
        }
    }
}
