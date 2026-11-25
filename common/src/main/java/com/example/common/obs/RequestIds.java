package com.example.common.obs;

import java.util.UUID;

/**
 * Generates and propagates correlation identifiers across distributed tasks and search requests.
 */
public final class RequestIds {
    private RequestIds() {
    }

    public static String newTaskId(String prefix) {
        String safePrefix = (prefix == null || prefix.isBlank()) ? "task" : prefix.trim();
        return safePrefix + "-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    public static String newRequestId() {
        return "req-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12);
    }

    public static String resolveOrCreate(String incomingHeader) {
        if (incomingHeader != null && !incomingHeader.isBlank()) {
            return incomingHeader.trim();
        }
        return newRequestId();
    }
}
