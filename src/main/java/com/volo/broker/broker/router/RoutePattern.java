package com.volo.broker.broker.router;

import java.util.List;

public record RoutePattern(
        String value,
        List<String> segments
) {
    private static final String SINGLE_SEGMENT_WILDCARD = "*";
    private static final String MULTI_SEGMENT_WILDCARD = "#";

    public RoutePattern {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("routing key is required");
        }

        segments = List.copyOf(segments);
    }

    public static RoutePattern of(String rawValue) {
        RouteSegments.ParsedRoute parsed = RouteSegments.parseRoutePattern(rawValue);

        return new RoutePattern(
                parsed.value(),
                parsed.segments()
        );
    }

    public boolean hasWildcard() {
        return segments.stream()
                .anyMatch(RoutePattern::isWildcardSegment);
    }

    public static boolean containsWildcard(String routingKey) {
        return routingKey != null
                && (routingKey.contains(SINGLE_SEGMENT_WILDCARD)
                || routingKey.contains(MULTI_SEGMENT_WILDCARD));
    }

    private static boolean isWildcardSegment(String segment) {
        return SINGLE_SEGMENT_WILDCARD.equals(segment)
                || MULTI_SEGMENT_WILDCARD.equals(segment);
    }

    @Override
    public String toString() {
        return value;
    }
}