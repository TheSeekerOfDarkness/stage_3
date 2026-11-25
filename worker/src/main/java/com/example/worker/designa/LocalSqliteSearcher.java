package com.example.worker.designa;

import com.example.common.model.SearchResult;
import com.example.worker.backend.SearchResponseData;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Local read-only SQLite inverted index searcher preserving Stage 2 SQL query semantics.
 */
public class LocalSqliteSearcher {
    public final Path dbPath;
    public final long version;
    public final String sha256;
    public final int bookCount;
    private final String jdbcUrl;

    private LocalSqliteSearcher(Path dbPath, long version, String sha256, int bookCount) {
        this.dbPath = dbPath;
        this.version = version;
        this.sha256 = sha256;
        this.bookCount = bookCount;
        this.jdbcUrl = dbPath != null ? "jdbc:sqlite:" + dbPath.toAbsolutePath() + "?busy_timeout=15000" : null;
    }

    public static LocalSqliteSearcher empty() {
        return new LocalSqliteSearcher(null, 0L, "", 0);
    }

    public static LocalSqliteSearcher open(Path dbPath, long version, String sha256, int bookCount) {
        return new LocalSqliteSearcher(dbPath, version, sha256, bookCount);
    }

    public boolean isHealthy() {
        if (dbPath == null) {
            return version == 0L;
        }
        if (!Files.exists(dbPath)) {
            return false;
        }
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM books")) {
            return rs.next();
        } catch (SQLException e) {
            return false;
        }
    }

    public SearchResponseData search(List<String> terms,
                                     String author,
                                     String language,
                                     Integer year,
                                     int limit,
                                     int offset) throws SQLException {
        if (dbPath == null || terms == null || terms.isEmpty()) {
            return new SearchResponseData(0, Collections.emptyList(), version);
        }

        List<String> distinctTerms = new ArrayList<>(new LinkedHashSet<>(terms));
        if (distinctTerms.isEmpty()) {
            return new SearchResponseData(0, Collections.emptyList(), version);
        }

        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            int totalMatches = countMatches(connection, distinctTerms, author, language, year);
            if (totalMatches == 0) {
                return new SearchResponseData(0, Collections.emptyList(), version);
            }
            List<SearchResult> results = queryPage(connection, distinctTerms, author, language, year, limit, offset);
            return new SearchResponseData(totalMatches, results, version);
        }
    }

    public List<Integer> listIndexedBookIds() {
        if (dbPath == null || !Files.exists(dbPath)) {
            return Collections.emptyList();
        }
        List<Integer> ids = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(jdbcUrl);
             Statement stmt = connection.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT book_id FROM books ORDER BY book_id ASC")) {
            while (rs.next()) {
                ids.add(rs.getInt(1));
            }
        } catch (SQLException ignored) {
        }
        return ids;
    }

    private static List<SearchResult> queryPage(Connection connection,
                                                List<String> distinctTerms,
                                                String author,
                                                String language,
                                                Integer year,
                                                int limit,
                                                int offset) throws SQLException {
        int k = distinctTerms.size();
        String placeholders = buildPlaceholders(k);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT b.book_id, b.title, b.author, b.language, b.year, b.datalake_path ")
            .append("FROM books b ")
            .append("JOIN (")
            .append("  SELECT book_id FROM inverted_index ")
            .append("  WHERE term IN (").append(placeholders).append(") ")
            .append("  GROUP BY book_id ")
            .append("  HAVING COUNT(DISTINCT term) = ? ")
            .append(") m ON b.book_id = m.book_id ");

        appendFilters(sql, author, language, year);
        sql.append("ORDER BY b.book_id ASC");
        if (limit > 0) {
            sql.append(" LIMIT ? OFFSET ?");
        }

        List<SearchResult> results = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            int idx = bindCommonParams(stmt, distinctTerms, author, language, year);
            if (limit > 0) {
                stmt.setInt(idx++, limit);
                stmt.setInt(idx, Math.max(0, offset));
            }
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    results.add(new SearchResult(
                        rs.getInt("book_id"),
                        rs.getString("title"),
                        rs.getString("author"),
                        rs.getString("language"),
                        rs.getInt("year"),
                        rs.getString("datalake_path")
                    ));
                }
            }
        }
        return results;
    }

    private static int countMatches(Connection connection,
                                    List<String> distinctTerms,
                                    String author,
                                    String language,
                                    Integer year) throws SQLException {
        int k = distinctTerms.size();
        String placeholders = buildPlaceholders(k);

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(*) FROM books b JOIN (")
            .append("  SELECT book_id FROM inverted_index ")
            .append("  WHERE term IN (").append(placeholders).append(") ")
            .append("  GROUP BY book_id ")
            .append("  HAVING COUNT(DISTINCT term) = ?")
            .append(") m ON b.book_id = m.book_id ");

        appendFilters(sql, author, language, year);

        try (PreparedStatement stmt = connection.prepareStatement(sql.toString())) {
            bindCommonParams(stmt, distinctTerms, author, language, year);
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    private static String buildPlaceholders(int count) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < count; i++) {
            if (i > 0) sb.append(", ");
            sb.append("?");
        }
        return sb.toString();
    }

    private static void appendFilters(StringBuilder sql, String author, String language, Integer year) {
        if (author != null && !author.isBlank()) {
            sql.append("AND b.author LIKE ? ");
        }
        if (language != null && !language.isBlank()) {
            sql.append("AND b.language = ? ");
        }
        if (year != null) {
            sql.append("AND b.year = ? ");
        }
    }

    private static int bindCommonParams(PreparedStatement stmt,
                                        List<String> distinctTerms,
                                        String author,
                                        String language,
                                        Integer year) throws SQLException {
        int paramIndex = 1;
        for (String term : distinctTerms) {
            stmt.setString(paramIndex++, term);
        }
        stmt.setInt(paramIndex++, distinctTerms.size());
        if (author != null && !author.isBlank()) {
            stmt.setString(paramIndex++, "%" + author.trim() + "%");
        }
        if (language != null && !language.isBlank()) {
            stmt.setString(paramIndex++, language.trim());
        }
        if (year != null) {
            stmt.setInt(paramIndex++, year);
        }
        return paramIndex;
    }
}
