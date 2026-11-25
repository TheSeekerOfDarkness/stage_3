package com.example.common.text;

import com.example.common.model.Book;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts book metadata from Project Gutenberg headers with Stage 2 parity.
 */
public final class HeaderParser {
    private static final Map<String, String> LANGUAGE_CODES = Map.of(
        "English", "en",
        "French", "fr",
        "Finnish", "fi",
        "Hungarian", "hu",
        "Portuguese", "pt",
        "Chinese", "zh",
        "Spanish", "es",
        "German", "de",
        "Italian", "it",
        "Latin", "la"
    );

    private static final Pattern YEAR_PATTERN = Pattern.compile("\\d{4}");

    private HeaderParser() {
    }

    public static Book parse(int bookId, String headerText, String path) {
        Book book = new Book();
        book.bookId = bookId;
        book.path = path;
        book.title = "Unknown";
        book.author = "Unknown";
        book.language = "unknown";
        book.year = 0;

        if (headerText == null || headerText.isEmpty()) {
            return book;
        }

        String[] lines = headerText.split("\n");

        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].trim();

            if (line.startsWith("Title:")) {
                StringBuilder title = new StringBuilder(line.substring("Title:".length()).trim());
                int next = i + 1;
                while (next < lines.length && isContinuation(lines[next])) {
                    title.append(" ").append(lines[next].trim());
                    next++;
                }
                book.title = title.toString();
            } else if (line.startsWith("Author:")) {
                book.author = line.substring("Author:".length()).trim();
            } else if (line.startsWith("Language:")) {
                String languageName = line.substring("Language:".length()).trim();
                book.language = LANGUAGE_CODES.getOrDefault(languageName, "unknown");
            } else if (line.toLowerCase().startsWith("release date:")) {
                book.year = findYear(line.substring("release date:".length()));
            }
        }

        return book;
    }

    private static boolean isContinuation(String line) {
        boolean startsWithSpace = line.startsWith(" ") || line.startsWith("\t");
        return startsWithSpace && !line.trim().isEmpty();
    }

    private static int findYear(String text) {
        int bracket = text.indexOf("[");
        if (bracket != -1) {
            text = text.substring(0, bracket);
        }

        Matcher matcher = YEAR_PATTERN.matcher(text);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group());
        }
        return 0;
    }
}
