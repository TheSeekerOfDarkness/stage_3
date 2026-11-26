package com.example.worker.designb;

import com.hazelcast.map.EntryProcessor;
import java.io.Serializable;
import java.util.Map;

/**
 * Partition-atomic counter increment for the published cluster index version.
 */
public class IncrementVersionProcessor implements EntryProcessor<String, Long, Long>, Serializable {
    private static final long serialVersionUID = 1L;

    @Override
    public Long process(Map.Entry<String, Long> entry) {
        Long current = entry.getValue();
        long next = (current == null ? 0L : current) + 1L;
        entry.setValue(next);
        return next;
    }
}
