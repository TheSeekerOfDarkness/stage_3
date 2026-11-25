package com.example.worker.backend;

import com.example.common.model.SearchResult;
import java.util.List;

/**
 * Search query output paired with the index version served.
 */
public final class SearchResponseData {
    public final int totalMatches;
    public final List<SearchResult> results;
    public final long indexVersion;

    public SearchResponseData(int totalMatches, List<SearchResult> results, long indexVersion) {
        this.totalMatches = totalMatches;
        this.results = results;
        this.indexVersion = indexVersion;
    }
}
