package com.example.common;

import com.example.common.model.Book;
import com.example.common.model.QueryMatcher;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryMatcherTest {

    private final List<Book> corpus = List.of(
        new Book(84, "Frankenstein", "Mary Shelley", "en", 1818, "datalake/84"),
        new Book(11, "Alice's Adventures in Wonderland", "Lewis Carroll", "en", 1865, "datalake/11"),
        new Book(2701, "Moby Dick", "Herman Melville", "en", 1851, "datalake/2701"),
        new Book(1342, "Pride and Prejudice", "Jane Austen", "en", 1813, "datalake/1342"),
        new Book(999, "Les Miserables", "Victor Hugo", "fr", 1862, "datalake/999")
    );

    @Test
    void filtersByAuthorCaseInsensitiveSubstringLanguageAndYear() {
        QueryMatcher.PageResult byAuthor = QueryMatcher.filterSortAndPaginate(
            corpus, "shelley", null, null, 20, 0
        );
        assertEquals(1, byAuthor.totalMatches);
        assertEquals(84, byAuthor.results.get(0).book_id);

        QueryMatcher.PageResult byLang = QueryMatcher.filterSortAndPaginate(
            corpus, null, "fr", null, 20, 0
        );
        assertEquals(1, byLang.totalMatches);
        assertEquals(999, byLang.results.get(0).book_id);

        QueryMatcher.PageResult byYear = QueryMatcher.filterSortAndPaginate(
            corpus, null, "en", 1865, 20, 0
        );
        assertEquals(1, byYear.totalMatches);
        assertEquals(11, byYear.results.get(0).book_id);
    }

    @Test
    void sortsByBookIdAscAndPaginatesDeterministically() {
        QueryMatcher.PageResult page1 = QueryMatcher.filterSortAndPaginate(
            corpus, null, "en", null, 2, 0
        );
        assertEquals(4, page1.totalMatches);
        assertEquals(List.of(11, 84), page1.results.stream().map(r -> r.book_id).toList());

        QueryMatcher.PageResult page2 = QueryMatcher.filterSortAndPaginate(
            corpus, null, "en", null, 2, 2
        );
        assertEquals(4, page2.totalMatches);
        assertEquals(List.of(1342, 2701), page2.results.stream().map(r -> r.book_id).toList());

        QueryMatcher.PageResult emptyOffset = QueryMatcher.filterSortAndPaginate(
            corpus, null, "en", null, 2, 10
        );
        assertEquals(4, emptyOffset.totalMatches);
        assertTrue(emptyOffset.results.isEmpty());
    }
}
