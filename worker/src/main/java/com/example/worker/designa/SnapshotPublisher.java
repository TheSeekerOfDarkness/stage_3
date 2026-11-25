package com.example.worker.designa;

import com.example.common.model.Book;
import com.example.common.model.IndexDelta;
import com.example.common.obs.Checksums;
import com.example.common.obs.StructuredLogger;
import com.example.worker.WorkerConfig;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Leader-elected SQLite snapshot publisher that merges pending index deltas into
 * versioned SQLite snapshots, uploads them to MinIO, and notifies search replicas.
 */
public class SnapshotPublisher {
    public static final String INDEX_DELTAS_BUCKET = "index-deltas";
    public static final String SNAPSHOTS_BUCKET = "snapshots";
    public static final String LATEST_MANIFEST_KEY = "snapshots/latest.json";

    private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

    private final WorkerConfig config;
    private final ObjectStorage storage;
    private final TaskBroker broker;
    private final LeaderElector leaderElector;
    private final StructuredLogger logger;

    private final Path masterDir;
    private final Path masterDbPath;
    private final String masterJdbcUrl;

    private ScheduledExecutorService scheduler;
    private volatile boolean running = false;
    private volatile boolean isLeader = false;

    public SnapshotPublisher(WorkerConfig config,
                             ObjectStorage storage,
                             TaskBroker broker,
                             LeaderElector leaderElector,
                             StructuredLogger logger) {
        this.config = config;
        this.storage = storage;
        this.broker = broker;
        this.leaderElector = leaderElector;
        this.logger = logger;
        this.masterDir = Paths.get(config.dataDir, config.workerId, "master");
        this.masterDbPath = masterDir.resolve("master.db");
        this.masterJdbcUrl = "jdbc:sqlite:" + masterDbPath.toAbsolutePath() + "?busy_timeout=60000";
    }

    public boolean isLeader() {
        return isLeader;
    }

    public synchronized void start() throws SQLException, IOException {
        if (running) {
            return;
        }
        Files.createDirectories(masterDir);
        ensureSchema();
        running = true;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "snapshot-publisher-" + config.workerId);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            if (!running) {
                return;
            }
            try {
                publishOnceNow();
            } catch (Exception e) {
                if (logger != null) {
                    logger.log("publisher_error", Map.of(
                        "role", "index",
                        "error", e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()
                    ));
                }
            }
        }, 100L, config.publishIntervalMs, TimeUnit.MILLISECONDS);
    }

    public synchronized void stop() {
        running = false;
        if (scheduler != null) {
            scheduler.shutdownNow();
            try {
                scheduler.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            scheduler = null;
        }
        if (isLeader) {
            leaderElector.releasePublisherLock();
            isLeader = false;
        }
    }

    public synchronized SnapshotManifest publishOnceNow() throws Exception {
        if (!ensureLeadership()) {
            return null;
        }

        bootstrapFromRemoteIfNewer();

        List<String> deltaKeys = storage.listKeys(INDEX_DELTAS_BUCKET, "deltas/");
        if (deltaKeys.isEmpty()) {
            return null;
        }

        Map<Integer, String> appliedShas = loadAppliedDeltaShas();
        List<IndexDelta> pending = new ArrayList<>();
        for (String key : deltaKeys) {
            if (!key.endsWith(".json")) {
                continue;
            }
            String json = storage.getString(INDEX_DELTAS_BUCKET, key);
            IndexDelta delta = GSON.fromJson(json, IndexDelta.class);
            if (delta == null || delta.book == null) {
                continue;
            }
            String existingSha = appliedShas.get(delta.book.bookId);
            if (existingSha == null || !existingSha.equals(delta.docSha256)) {
                pending.add(delta);
            }
        }

        if (pending.isEmpty()) {
            return null;
        }

        long newVersion;
        List<Integer> bookIds;
        Path stagingSnapshot = masterDir.resolve("staging-snapshot.db");
        Files.deleteIfExists(stagingSnapshot);

        try (Connection connection = DriverManager.getConnection(masterJdbcUrl)) {
            connection.setAutoCommit(false);
            try {
                long currentVersion = readMasterVersion(connection);
                newVersion = currentVersion + 1;

                for (IndexDelta delta : pending) {
                    applyDelta(connection, delta, newVersion);
                }
                writeMasterVersion(connection, newVersion);
                connection.commit();
            } catch (SQLException e) {
                connection.rollback();
                throw e;
            } finally {
                connection.setAutoCommit(true);
            }

            try (Statement stmt = connection.createStatement()) {
                stmt.execute("PRAGMA wal_checkpoint(TRUNCATE);");
                String escaped = stagingSnapshot.toAbsolutePath().toString().replace("'", "''");
                stmt.execute("VACUUM INTO '" + escaped + "'");
            }
            bookIds = queryBookIds(connection);
        }

        String sha256 = Checksums.sha256Hex(stagingSnapshot);
        String objectKey = "snapshots/index-v" + newVersion + ".db";
        storage.putFile(SNAPSHOTS_BUCKET, objectKey, stagingSnapshot, "application/x-sqlite3");
        Files.deleteIfExists(stagingSnapshot);

        SnapshotManifest manifest = new SnapshotManifest(
            newVersion,
            objectKey,
            sha256,
            bookIds.size(),
            bookIds,
            config.workerId,
            System.currentTimeMillis()
        );
        storage.putString(SNAPSHOTS_BUCKET, LATEST_MANIFEST_KEY, GSON.toJson(manifest), "application/json");
        broker.publishSnapshotNotification(manifest);

        if (logger != null) {
            logger.log("snapshot_published", Map.of(
                "role", "index",
                "index_version", newVersion,
                "merged_deltas", pending.size(),
                "book_count", bookIds.size(),
                "snapshot_sha256", sha256,
                "object_key", objectKey
            ));
        }
        return manifest;
    }

    private boolean ensureLeadership() throws Exception {
        if (isLeader) {
            if (leaderElector.renewPublisherLock()) {
                return true;
            }
            isLeader = false;
        }
        if (leaderElector.tryAcquirePublisherLock()) {
            isLeader = true;
            bootstrapFromRemoteIfNewer();
            return true;
        }
        return false;
    }

    private void bootstrapFromRemoteIfNewer() throws Exception {
        if (!storage.exists(SNAPSHOTS_BUCKET, LATEST_MANIFEST_KEY)) {
            return;
        }
        SnapshotManifest remote = GSON.fromJson(storage.getString(SNAPSHOTS_BUCKET, LATEST_MANIFEST_KEY), SnapshotManifest.class);
        if (remote == null || remote.version <= 0) {
            return;
        }
        long localVersion;
        try (Connection connection = DriverManager.getConnection(masterJdbcUrl)) {
            localVersion = readMasterVersion(connection);
        }
        if (remote.version <= localVersion) {
            return;
        }

        Path downloaded = masterDir.resolve("bootstrap-v" + remote.version + ".db");
        storage.downloadToFile(SNAPSHOTS_BUCKET, remote.objectKey, downloaded);
        String actualSha = Checksums.sha256Hex(downloaded);
        if (!actualSha.equals(remote.sha256)) {
            Files.deleteIfExists(downloaded);
            throw new IOException("Bootstrap snapshot SHA-256 mismatch for version " + remote.version);
        }

        Files.deleteIfExists(masterDir.resolve("master.db-wal"));
        Files.deleteIfExists(masterDir.resolve("master.db-shm"));
        Files.move(downloaded, masterDbPath, StandardCopyOption.REPLACE_EXISTING);
        ensureSchema();

        try (Connection connection = DriverManager.getConnection(masterJdbcUrl)) {
            writeMasterVersion(connection, remote.version);
            reconcileDeltaStateFromRemote(connection, remote.version);
        }
    }

    private void reconcileDeltaStateFromRemote(Connection connection, long remoteVersion) throws Exception {
        Map<Integer, String> existing = new HashMap<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT book_id, doc_sha256 FROM delta_state")) {
            while (rs.next()) {
                existing.put(rs.getInt(1), rs.getString(2));
            }
        }

        List<Integer> bookIds = queryBookIds(connection);
        try (PreparedStatement insert = connection.prepareStatement(
            "INSERT OR IGNORE INTO delta_state (book_id, doc_sha256, index_version) VALUES (?, ?, ?)")) {
            for (int bookId : bookIds) {
                if (existing.containsKey(bookId)) {
                    continue;
                }
                String sha = "";
                String deltaKey = "deltas/" + bookId + ".json";
                if (storage.exists(INDEX_DELTAS_BUCKET, deltaKey)) {
                    IndexDelta delta = GSON.fromJson(storage.getString(INDEX_DELTAS_BUCKET, deltaKey), IndexDelta.class);
                    if (delta != null && delta.docSha256 != null) {
                        sha = delta.docSha256;
                    }
                }
                insert.setInt(1, bookId);
                insert.setString(2, sha);
                insert.setLong(3, remoteVersion);
                insert.addBatch();
            }
            insert.executeBatch();
        }
    }

    private void ensureSchema() throws SQLException {
        try (Connection connection = DriverManager.getConnection(masterJdbcUrl);
             Statement stmt = connection.createStatement()) {
            stmt.execute("PRAGMA journal_mode=WAL;");
            stmt.execute("PRAGMA busy_timeout=60000;");
            stmt.execute("CREATE TABLE IF NOT EXISTS books ("
                + "book_id INTEGER PRIMARY KEY, "
                + "title TEXT NOT NULL, "
                + "author TEXT NOT NULL, "
                + "language TEXT, "
                + "year INTEGER, "
                + "datalake_path TEXT)");
            stmt.execute("CREATE TABLE IF NOT EXISTS inverted_index ("
                + "term TEXT NOT NULL, "
                + "book_id INTEGER NOT NULL, "
                + "PRIMARY KEY (term, book_id))");
            stmt.execute("CREATE INDEX IF NOT EXISTS idx_inverted_book_id ON inverted_index(book_id)");
            stmt.execute("CREATE TABLE IF NOT EXISTS delta_state ("
                + "book_id INTEGER PRIMARY KEY, "
                + "doc_sha256 TEXT NOT NULL, "
                + "index_version INTEGER NOT NULL)");
            stmt.execute("CREATE TABLE IF NOT EXISTS publisher_meta ("
                + "id INTEGER PRIMARY KEY CHECK (id = 1), "
                + "version INTEGER NOT NULL)");
            stmt.execute("INSERT OR IGNORE INTO publisher_meta (id, version) VALUES (1, 0)");
        }
    }

    private Map<Integer, String> loadAppliedDeltaShas() throws SQLException {
        Map<Integer, String> map = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(masterJdbcUrl);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT book_id, doc_sha256 FROM delta_state")) {
            while (rs.next()) {
                map.put(rs.getInt(1), rs.getString(2));
            }
        }
        return map;
    }

    private static long readMasterVersion(Connection connection) throws SQLException {
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT version FROM publisher_meta WHERE id = 1")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    private static void writeMasterVersion(Connection connection, long version) throws SQLException {
        try (PreparedStatement stmt = connection.prepareStatement(
            "INSERT OR REPLACE INTO publisher_meta (id, version) VALUES (1, ?)")) {
            stmt.setLong(1, version);
            stmt.executeUpdate();
        }
    }

    private static void applyDelta(Connection connection, IndexDelta delta, long newVersion) throws SQLException {
        Book book = delta.book;
        try (PreparedStatement saveBook = connection.prepareStatement(
            "INSERT OR REPLACE INTO books (book_id, title, author, language, year, datalake_path) VALUES (?, ?, ?, ?, ?, ?)")) {
            saveBook.setInt(1, book.bookId);
            saveBook.setString(2, book.title);
            saveBook.setString(3, book.author);
            saveBook.setString(4, book.language);
            saveBook.setInt(5, book.year);
            saveBook.setString(6, book.path);
            saveBook.executeUpdate();
        }

        try (PreparedStatement deleteTerms = connection.prepareStatement(
            "DELETE FROM inverted_index WHERE book_id = ?")) {
            deleteTerms.setInt(1, book.bookId);
            deleteTerms.executeUpdate();
        }

        if (delta.distinctTerms != null && !delta.distinctTerms.isEmpty()) {
            try (PreparedStatement insertTerm = connection.prepareStatement(
                "INSERT INTO inverted_index (term, book_id) VALUES (?, ?)")) {
                for (String term : delta.distinctTerms) {
                    insertTerm.setString(1, term);
                    insertTerm.setInt(2, book.bookId);
                    insertTerm.addBatch();
                }
                insertTerm.executeBatch();
            }
        }

        try (PreparedStatement saveState = connection.prepareStatement(
            "INSERT OR REPLACE INTO delta_state (book_id, doc_sha256, index_version) VALUES (?, ?, ?)")) {
            saveState.setInt(1, book.bookId);
            saveState.setString(2, delta.docSha256 != null ? delta.docSha256 : "");
            saveState.setLong(3, newVersion);
            saveState.executeUpdate();
        }
    }

    private static List<Integer> queryBookIds(Connection connection) throws SQLException {
        List<Integer> ids = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT book_id FROM books ORDER BY book_id ASC")) {
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
        }
        return ids;
    }
}
