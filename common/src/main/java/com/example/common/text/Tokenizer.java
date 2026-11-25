package com.example.common.text;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Unified tokenizer matching Stage 2 indexing and search semantics.
 */
public final class Tokenizer {
    private Tokenizer() {
    }

    public static List<String> tokenize(String text) {
        List<String> words = new ArrayList<>();
        if (text == null) {
            return words;
        }

        StringBuilder word = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isLetterOrDigit(c)) {
                word.append(Character.toLowerCase(c));
            } else if (word.length() > 0) {
                words.add(word.toString());
                word.setLength(0);
            }
        }

        if (word.length() > 0) {
            words.add(word.toString());
        }

        return words;
    }

    public static Set<String> tokenizeDistinct(String text) {
        return new LinkedHashSet<>(tokenize(text));
    }

    public static List<String> tokenizeDistinctSorted(String text) {
        List<String> distinct = new ArrayList<>(tokenizeDistinct(text));
        Collections.sort(distinct);
        return distinct;
    }
}
