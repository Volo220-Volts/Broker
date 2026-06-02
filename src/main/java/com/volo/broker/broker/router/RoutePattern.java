package com.volo.broker.broker.router;

import java.util.Arrays;
import java.util.List;

public final class RoutePattern {
    private static final int MAX_LENGTH = 255;

    private final List<String> segments;

    private RoutePattern(List<String> segments) {
        this.segments = List.copyOf(segments);
    }

    public static RoutePattern of(String value) {
        String normalized = normalize(value);

        if (normalized.isBlank()) {
            throw new IllegalArgumentException("routing key is required");
        }

        if (normalized.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("routing key is too long");
        }

        List<String> segments = Arrays.stream(normalized.split("\\."))
                .map(String::trim)
                .filter(segment -> !segment.isBlank())
                .toList();

        if (segments.isEmpty()) {
            throw new IllegalArgumentException("routing key must contain at least one segment");
        }

        for (String segment : segments) {
            if (segment.isBlank()) {
                throw new IllegalArgumentException("routing key contains invalid segment");
            }
            if (segment.contains("*") || segment.contains("#")) {
                if (!segment.equals("*") && !segment.equals("#")) {
                    throw new IllegalArgumentException("wildcards must be standalone segments");
                }
            }
        }

        return new RoutePattern(segments);
    }

    public static boolean containsWildcard(String routingKey) {
        return routingKey != null && (routingKey.contains("*") || routingKey.contains("#"));
    }

    public List<String> segments() {
        return segments;
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }
}
