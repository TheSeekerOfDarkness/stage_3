package com.example.common.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;

/**
 * Shared metadata filtering, deterministic sorting, and pagination matching Stage 2 SQLite semantics.
 */
public final class QueryMatcher {

    public static final class PageResult {
        public final int totalMatches;
        public final List<SearchResult> results;

        public PageResult(int totalMatches, List<SearchResult> results) {
            this.totalMatches = totalMatches;
            this.results = results;
        }
    }

    private QueryMatcher() {
    }

    public static boolean matchesFilters(Book book, String author, String language, Integer year) {
        if (book == null) {
            return false;
        }
        if (author != null && !author.isBlank()) {
            if (book.author == null) {
                return false;
            }
            String needle = author.trim().toLowerCase(Locale.ROOT);
            if (!book.author.toLowerCase(Locale.ROOT).contains(needle)) {
                return false;
            }
        }
        if (language != null && !language.isBlank()) {
            if (book.language == null || !book.language.equals(language.trim())) {
                return false;
            }
        }
        if (year != null && book.year != year) {
            return false;
        }
        return true;
    }

    public static PageResult filterSortAndPaginate(Collection<Book> candidates,
                                                   String author,
                                                   String language,
                                                   Integer year,
                                                   int limit,
                                                   int offset) {
        if (candidates == null || candidates.isEmpty()) {
            return new PageResult(0, Collections.emptyList());
        }

        List<Book> matched = new ArrayList<>();
        for (Book book : candidates) {
            if (matchesFilters(book, author, language, year)) {
                matched.add(book);
            }
        }
        matched.sort(Comparator.comparingInt(b -> b.bookId));

        int totalMatches = matched.size();
        int safeOffset = Math.max(0, offset);
        if (safeOffset >= totalMatches) {
            return new PageResult(totalMatches, Collections.emptyList());
        }

        int count = limit > 0 ? Math.min(limit, totalMatches - safeOffset) : (totalMatches - safeOffset);
        List<SearchResult> page = new ArrayList<>(count);
        for (int i = safeOffset; i < safeOffset + count; i++) {
            page.add(matched.get(i).toSearchResult());
        }
        return new PageResult(totalMatches, page);
    }
}
