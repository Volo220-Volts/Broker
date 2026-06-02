package com.volo.broker.broker.router;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public final class QueuePath {
    private static final int MAX_LENGTH = 255;

    private final String value;
    private final String routePath;
    private final String queueName;
    private final List<String> segments;

    private QueuePath(
            String value,
            String routePath,
            String queueName,
            List<String> segments
    ) {
        this.value = value;
        this.routePath = routePath;
        this.queueName = queueName;
        this.segments = List.copyOf(segments);
    }

    public static QueuePath of(String value) {
        String normalized = normalize(value);

        if (normalized.isBlank()) {
            throw new IllegalArgumentException("queue path is required");
        }

        if (normalized.length() > MAX_LENGTH) {
            throw new IllegalArgumentException("queue path is too long");
        }

        if (normalized.contains("*") || normalized.contains("#")) {
            throw new IllegalArgumentException("queue path must not contain wildcards");
        }

        List<String> segments = Arrays.stream(normalized.split("\\."))
                .map(String::trim)
                .filter(segment -> !segment.isBlank())
                .toList();

        if (segments.isEmpty()) {
            throw new IllegalArgumentException("queue path must contain at least one segment");
        }

        String queueName = segments.get(segments.size() - 1);
        String routePath = segments.size() == 1
                ? ""
                : String.join(".", segments.subList(0, segments.size() - 1));

        return new QueuePath(normalized, routePath, queueName, segments);
    }

    public String value() {
        return value;
    }

    public String routePath() {
        return routePath;
    }

    public String queueName() {
        return queueName;
    }

    public List<String> segments() {
        return segments;
    }

    public boolean hasRoutePath() {
        return !routePath.isBlank();
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof QueuePath that)) return false;
        return value.equals(that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(value);
    }

    @Override
    public String toString() {
        return value;
    }
}
