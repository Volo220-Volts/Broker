package com.volo.broker.broker.router;

import java.util.List;
import java.util.regex.Pattern;

final class RouteSegments {
    private static final int MAX_LENGTH = 255;
    private static final String SEGMENT_SEPARATOR = "\\.";
    private static final String SINGLE_SEGMENT_WILDCARD = "*";
    private static final String MULTI_SEGMENT_WILDCARD = "#";

    private static final Pattern NORMAL_SEGMENT_PATTERN =
            Pattern.compile("^[a-zA-Z0-9_-]+$");

    private RouteSegments() {
    }

    static ParsedRoute parseQueuePath(String rawValue) {
        String normalized = normalize(rawValue);

        validateRequired(normalized, "queue path");
        validateMaxLength(normalized, "queue path");

        String[] segments = split(normalized);

        validateQueuePathSegments(segments);

        return new ParsedRoute(normalized, List.of(segments));
    }

    static ParsedRoute parseRoutePattern(String rawValue) {
        String normalized = normalize(rawValue);

        validateRequired(normalized, "routing key");
        validateMaxLength(normalized, "routing key");

        String[] segments = split(normalized);

        validateRoutePatternSegments(segments);

        return new ParsedRoute(normalized, List.of(segments));
    }

    private static String normalize(String value) {
        return value == null ? "" : value.trim();
    }

    private static void validateRequired(String value, String fieldName) {
        if (value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " is required");
        }
    }

    private static void validateMaxLength(String value, String fieldName) {
        if (value.length() > MAX_LENGTH) {
            throw new IllegalArgumentException(fieldName + " is too long");
        }
    }

    private static String[] split(String value) {
        return value.split(SEGMENT_SEPARATOR, -1);
    }

    private static void validateQueuePathSegments(String[] segments) {
        validateNotEmpty(segments, "queue path");

        for (String segment : segments) {
            validateNotBlankSegment(segment, "queue path");

            if (segment.contains(SINGLE_SEGMENT_WILDCARD)
                    || segment.contains(MULTI_SEGMENT_WILDCARD)) {
                throw new IllegalArgumentException("queue path must not contain wildcards");
            }

            validateNormalSegment(segment, "queue path");
        }
    }

    private static void validateRoutePatternSegments(String[] segments) {
        validateNotEmpty(segments, "routing key");

        for (int i = 0; i < segments.length; i++) {
            String segment = segments[i];

            validateNotBlankSegment(segment, "routing key");

            if (SINGLE_SEGMENT_WILDCARD.equals(segment)) {
                continue;
            }

            if (MULTI_SEGMENT_WILDCARD.equals(segment)) {
                validateHashWildcardPosition(segments, i);
                continue;
            }

            if (segment.contains(SINGLE_SEGMENT_WILDCARD)
                    || segment.contains(MULTI_SEGMENT_WILDCARD)) {
                throw new IllegalArgumentException("wildcards must be standalone segments");
            }

            validateNormalSegment(segment, "routing key");
        }
    }

    private static void validateNotEmpty(String[] segments, String fieldName) {
        if (segments.length == 0) {
            throw new IllegalArgumentException(fieldName + " must contain at least one segment");
        }
    }

    private static void validateNotBlankSegment(String segment, String fieldName) {
        if (segment == null || segment.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not contain empty segments");
        }
    }

    private static void validateNormalSegment(String segment, String fieldName) {
        if (!NORMAL_SEGMENT_PATTERN.matcher(segment).matches()) {
            throw new IllegalArgumentException(
                    fieldName + " segment contains invalid characters: " + segment
            );
        }
    }

    private static void validateHashWildcardPosition(String[] segments, int index) {
        if (index != segments.length - 1) {
            throw new IllegalArgumentException("'#' wildcard is allowed only as the last segment");
        }
    }

    record ParsedRoute(String value, List<String> segments) {
        ParsedRoute {
            segments = List.copyOf(segments);
        }
    }
}