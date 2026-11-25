package com.example.worker;

import com.example.common.net.GutenbergDownloader;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Immutable runtime configuration for the unified Stage 3 worker.
 */
public final class WorkerConfig {
    public final String workerId;
    public final String hostName;
    public final String hostIp;
    public final int port;
    public final String designMode;
    public final Set<WorkerRole> initialRoles;
    public final int crawlThreads;
    public final int indexThreads;
    public final int searchThreads;
    public final long drainDelayMs;

    public final String logDir;
    public final String dataDir;
    public final String gutenbergUrlTemplate;

    public final boolean consulEnabled;
    public final String consulUrl;
    public final String consulServiceName;
    public final String consulCheckInterval;
    public final String consulCheckTimeout;
    public final String consulDeregisterAfter;

    // Design A settings
    public final String rabbitmqHost;
    public final int rabbitmqPort;
    public final String rabbitmqUser;
    public final String rabbitmqPass;
    public final String minioEndpoint;
    public final String minioAccessKey;
    public final String minioSecretKey;
    public final long publishIntervalMs;
    public final long minRequiredIndexVersion;

    // Design B settings
    public final String hazelcastClusterName;
    public final int hazelcastPort;
    public final String hazelcastJoinMode;
    public final String hazelcastMembers;
    public final String hazelcastPublicAddress;
    public final int hazelcastBackupCount;

    private WorkerConfig(Map<String, String> env) {
        String detectedHost = detectHostName();
        this.hostName = getOrDefault(env, "HOST_NAME", detectedHost);
        this.workerId = getOrDefault(env, "WORKER_ID", "worker-" + this.hostName);
        this.hostIp = getOrDefault(env, "HOST_IP", "127.0.0.1");
        this.port = parseInt(env, "PORT", 7000);
        this.designMode = getOrDefault(env, "DESIGN_MODE", "design-a").toLowerCase(Locale.ROOT);
        this.initialRoles = Collections.unmodifiableSet(
            WorkerRole.parseSet(getOrDefault(env, "WORKER_ROLES", "ingest,index,search"))
        );
        this.crawlThreads = Math.max(1, parseInt(env, "CRAWL_THREADS", 2));
        this.indexThreads = Math.max(1, parseInt(env, "INDEX_THREADS", 2));
        this.searchThreads = Math.max(1, parseInt(env, "SEARCH_THREADS", 8));
        this.drainDelayMs = Math.max(0L, parseLong(env, "DRAIN_DELAY_MS", 500L));

        this.logDir = getOrDefault(env, "LOG_DIR", "logs");
        this.dataDir = getOrDefault(env, "DATA_DIR", "data");
        this.gutenbergUrlTemplate = getOrDefault(env, "GUTENBERG_URL_TEMPLATE", GutenbergDownloader.DEFAULT_URL_TEMPLATE);

        this.consulEnabled = parseBoolean(env, "CONSUL_ENABLED", true);
        this.consulUrl = stripTrailingSlash(getOrDefault(env, "CONSUL_URL", "http://127.0.0.1:8500"));
        this.consulServiceName = getOrDefault(env, "CONSUL_SERVICE_NAME", "search-service");
        this.consulCheckInterval = getOrDefault(env, "CONSUL_CHECK_INTERVAL", "2s");
        this.consulCheckTimeout = getOrDefault(env, "CONSUL_CHECK_TIMEOUT", "1s");
        this.consulDeregisterAfter = getOrDefault(env, "CONSUL_DEREGISTER_AFTER", "30s");

        this.rabbitmqHost = getOrDefault(env, "RABBITMQ_HOST", "127.0.0.1");
        this.rabbitmqPort = parseInt(env, "RABBITMQ_PORT", 5672);
        this.rabbitmqUser = getOrDefault(env, "RABBITMQ_USER", "guest");
        this.rabbitmqPass = getOrDefault(env, "RABBITMQ_PASS", "guest");
        this.minioEndpoint = stripTrailingSlash(getOrDefault(env, "MINIO_ENDPOINT", "http://127.0.0.1:9000"));
        this.minioAccessKey = getOrDefault(env, "MINIO_ACCESS_KEY", "minioadmin");
        this.minioSecretKey = getOrDefault(env, "MINIO_SECRET_KEY", "minioadmin");
        this.publishIntervalMs = Math.max(100L, parseLong(env, "PUBLISH_INTERVAL_MS", 1000L));
        this.minRequiredIndexVersion = Math.max(0L, parseLong(env, "MIN_REQUIRED_INDEX_VERSION", 0L));

        this.hazelcastClusterName = getOrDefault(env, "HAZELCAST_CLUSTER_NAME", "search-cluster");
        this.hazelcastPort = parseInt(env, "HAZELCAST_PORT", 5701);
        this.hazelcastJoinMode = getOrDefault(env, "HAZELCAST_JOIN_MODE", "tcpip").toLowerCase(Locale.ROOT);
        this.hazelcastMembers = getOrDefault(env, "HAZELCAST_MEMBERS", "127.0.0.1:5701");
        this.hazelcastPublicAddress = getOrDefault(env, "HAZELCAST_PUBLIC_ADDRESS", "");
        this.hazelcastBackupCount = Math.max(0, parseInt(env, "HAZELCAST_BACKUP_COUNT", 1));
    }

    public static WorkerConfig fromEnv() {
        return new WorkerConfig(System.getenv());
    }

    public static WorkerConfig fromMap(Map<String, String> values) {
        return new WorkerConfig(values == null ? Collections.emptyMap() : values);
    }

    private static String getOrDefault(Map<String, String> map, String key, String def) {
        String val = map.get(key);
        return (val == null || val.isBlank()) ? def : val.trim();
    }

    private static int parseInt(Map<String, String> map, String key, int def) {
        String val = map.get(key);
        if (val == null || val.isBlank()) {
            return def;
        }
        try {
            return Integer.parseInt(val.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer for " + key + ": " + val, e);
        }
    }

    private static long parseLong(Map<String, String> map, String key, long def) {
        String val = map.get(key);
        if (val == null || val.isBlank()) {
            return def;
        }
        try {
            return Long.parseLong(val.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid long for " + key + ": " + val, e);
        }
    }

    private static boolean parseBoolean(Map<String, String> map, String key, boolean def) {
        String val = map.get(key);
        if (val == null || val.isBlank()) {
            return def;
        }
        return Boolean.parseBoolean(val.trim());
    }

    private static String stripTrailingSlash(String url) {
        if (url.endsWith("/")) {
            return url.substring(0, url.length() - 1);
        }
        return url;
    }

    private static String detectHostName() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            return "localhost";
        }
    }
}
