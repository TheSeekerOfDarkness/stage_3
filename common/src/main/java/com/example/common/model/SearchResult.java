package com.example.common.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Public search result DTO preserving the Stage 2 JSON contract.
 */
public class SearchResult implements Serializable {
    private static final long serialVersionUID = 1L;

    public int book_id;
    public String title;
    public String author;
    public String language;
    public int year;
    public String path;

    public SearchResult() {
    }

    public SearchResult(int bookId, String title, String author, String language, int year, String path) {
        this.book_id = bookId;
        this.title = title;
        this.author = author;
        this.language = language;
        this.year = year;
        this.path = path;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SearchResult that)) return false;
        return book_id == that.book_id
                && year == that.year
                && Objects.equals(title, that.title)
                && Objects.equals(author, that.author)
                && Objects.equals(language, that.language)
                && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(book_id, title, author, language, year, path);
    }

    @Override
    public String toString() {
        return "SearchResult{"
                + "book_id=" + book_id
                + ", title='" + title + '\''
                + ", author='" + author + '\''
                + ", language='" + language + '\''
                + ", year=" + year
                + ", path='" + path + '\''
                + '}';
    }
}
