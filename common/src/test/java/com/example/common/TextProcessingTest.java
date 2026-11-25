package com.example.common;

import com.example.common.model.Book;
import com.example.common.text.BookSplitter;
import com.example.common.text.HeaderParser;
import com.example.common.text.Tokenizer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class TextProcessingTest {

    @Test
    void tokenizerMatchesStage2ParityAndNullSafety() {
        assertTrue(Tokenizer.tokenize(null).isEmpty());
        assertTrue(Tokenizer.tokenize("").isEmpty());
        assertEquals(
            List.of("alice", "s", "adventures", "in", "wonderland", "1865"),
            Tokenizer.tokenize("Alice's Adventures in Wonderland! (1865)")
        );
        assertEquals(
            Set.of("the", "cat", "sat"),
            Tokenizer.tokenizeDistinct("The cat, the CAT sat!")
        );
        assertEquals(
            List.of("cat", "sat", "the"),
            Tokenizer.tokenizeDistinctSorted("The cat, the CAT sat!")
        );
    }

    @Test
    void bookSplitterExtractsHeaderAndBody() {
        String raw = """
            Title: Alice's Adventures in Wonderland
            Author: Lewis Carroll
            *** START OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            Alice was beginning to get very tired.
            *** END OF THE PROJECT GUTENBERG EBOOK ALICE'S ADVENTURES IN WONDERLAND ***
            End notes
            """;

        BookSplitter.SplitResult split = BookSplitter.split(raw);
        assertTrue(split.header.contains("Title: Alice's Adventures in Wonderland"));
        assertEquals("Alice was beginning to get very tired.", split.body);
    }

    @Test
    void bookSplitterRejectsInvalidInputs() {
        assertThrows(IllegalArgumentException.class, () -> BookSplitter.split(null));
        assertThrows(IllegalArgumentException.class, () -> BookSplitter.split("No markers here"));
        assertThrows(
            IllegalArgumentException.class,
            () -> BookSplitter.split(BookSplitter.END_MARKER + "\n" + BookSplitter.START_MARKER)
        );
    }

    @Test
    void headerParserHandlesMultilineTitleReleaseDateAndLanguages() {
        String header = """
            Title: The Adventures of Sherlock Holmes
                and Other Stories
            Author: Arthur Conan Doyle
            Release Date: March 1999 [EBook #1661]
            Language: English
            """;

        Book book = HeaderParser.parse(1661, header, "books/1661");
        assertEquals(1661, book.bookId);
        assertEquals("The Adventures of Sherlock Holmes and Other Stories", book.title);
        assertEquals("Arthur Conan Doyle", book.author);
        assertEquals("en", book.language);
        assertEquals(1999, book.year);
        assertEquals("books/1661", book.path);

        Book unknown = HeaderParser.parse(99, "Language: Klingon\n", "books/99");
        assertEquals("Unknown", unknown.title);
        assertEquals("Unknown", unknown.author);
        assertEquals("unknown", unknown.language);
        assertEquals(0, unknown.year);
    }
}
