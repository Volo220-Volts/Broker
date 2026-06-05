package com.volo.broker.broker.router;

import java.util.List;

public record QueuePath(
        String value,
        String routePath,
        String queueName,
        List<String> segments
) {
    public QueuePath {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("queue path is required");
        }

        if (queueName == null || queueName.isBlank()) {
            throw new IllegalArgumentException("queue name is required");
        }

        routePath = routePath == null ? "" : routePath;
        segments = List.copyOf(segments);
    }

    public static QueuePath of(String rawValue) {
        RouteSegments.ParsedRoute parsed = RouteSegments.parseQueuePath(rawValue);

        List<String> segments = parsed.segments();

        String queueName = segments.get(segments.size() - 1);

        String routePath = segments.size() == 1
                ? ""
                : String.join(".", segments.subList(0, segments.size() - 1));

        return new QueuePath(
                parsed.value(),
                routePath,
                queueName,
                segments
        );
    }

    public boolean hasRoutePath() {
        return !routePath.isBlank();
    }

    @Override
    public String toString() {
        return value;
    }
}