package com.example.worker;

import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Operational roles that any interchangeable worker can assume dynamically.
 */
public enum WorkerRole {
    INGEST("ingest"),
    INDEX("index"),
    SEARCH("search");

    private final String wireName;

    WorkerRole(String wireName) {
        this.wireName = wireName;
    }

    public String wireName() {
        return wireName;
    }

    public static WorkerRole fromString(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Worker role cannot be blank");
        }
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        return switch (normalized) {
            case "ingest", "crawl" -> INGEST;
            case "index" -> INDEX;
            case "search" -> SEARCH;
            default -> throw new IllegalArgumentException("Unknown worker role: " + value);
        };
    }

    public static Set<WorkerRole> parseSet(String csv) {
        EnumSet<WorkerRole> roles = EnumSet.noneOf(WorkerRole.class);
        if (csv == null || csv.isBlank()) {
            return roles;
        }
        for (String token : csv.split(",")) {
            if (!token.isBlank()) {
                roles.add(fromString(token));
            }
        }
        return roles;
    }

    public static List<String> toWireNames(Set<WorkerRole> roles) {
        return roles.stream().map(WorkerRole::wireName).sorted().toList();
    }
}
