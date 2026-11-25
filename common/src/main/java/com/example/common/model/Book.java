package com.example.common.model;

import java.io.Serializable;

/**
 * Metadata and provenance for an indexed book across both distributed designs.
 */
public class Book implements Serializable {
    private static final long serialVersionUID = 1L;

    public int bookId;
    public String title;
    public String author;
    public String language;
    public int year;
    public String path;
    public String indexedBy;
    public long indexVersion;
    public String docSha256;

    public Book() {
    }

    public Book(int bookId, String title, String author, String language, int year, String path) {
        this.bookId = bookId;
        this.title = title;
        this.author = author;
        this.language = language;
        this.year = year;
        this.path = path;
    }

    public Book(int bookId, String title, String author, String language, int year, String path,
                String indexedBy, long indexVersion, String docSha256) {
        this(bookId, title, author, language, year, path);
        this.indexedBy = indexedBy;
        this.indexVersion = indexVersion;
        this.docSha256 = docSha256;
    }

    public SearchResult toSearchResult() {
        return new SearchResult(bookId, title, author, language, year, path);
    }
}
