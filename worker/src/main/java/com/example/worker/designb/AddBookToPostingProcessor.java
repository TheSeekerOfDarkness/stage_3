package com.example.worker.designb;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Partition-atomic EntryProcessor that appends a bookId to a term's posting set.
 */
public class AddBookToPostingProcessor implements EntryProcessor<String, Set<Integer>, Void>, Serializable {
    private static final long serialVersionUID = 1L;

    private final int bookId;

    public AddBookToPostingProcessor(int bookId) {
        this.bookId = bookId;
    }

    @Override
    public Void process(Map.Entry<String, Set<Integer>> entry) {
        Set<Integer> current = entry.getValue();
        Set<Integer> updated = (current == null) ? new HashSet<>() : new HashSet<>(current);
        if (updated.add(bookId)) {
            entry.setValue(updated);
        }
        return null;
    }
}
