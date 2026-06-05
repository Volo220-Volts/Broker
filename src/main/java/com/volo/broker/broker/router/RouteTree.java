package com.volo.broker.broker.router;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RouteTree {
    private final RouteNode root = new RouteNode("");

    public void attachQueue(QueuePath queuePath) {
        RouteNode current = root;

        for (String segment : queuePath.segments()) {
            current = current.childOrCreate(segment);
        }

        current.addQueue(queuePath.value());
    }

    public void detachQueue(QueuePath queuePath) {
        detachQueue(root, queuePath.value());
    }

    public Set<String> resolve(String routingKey) {
        RoutePattern pattern = RoutePattern.of(routingKey);
        Set<String> result = new LinkedHashSet<>();

        collectMatches(root, pattern.segments(), 0, result);

        return Set.copyOf(result);
    }

    private void detachQueue(RouteNode node, String queuePath) {
        node.removeQueue(queuePath);

        for (RouteNode child : node.children()) {
            detachQueue(child, queuePath);
        }
    }

    private void collectMatches(RouteNode node, List<String> patternSegments, int index, Set<String> result) {
        if (index == patternSegments.size()) {
            result.addAll(node.queueNames());
            return;
        }

        String segment = patternSegments.get(index);

        if ("#".equals(segment)) {
            collectMatches(node, patternSegments, index + 1, result);
            for (RouteNode child : node.children()) {
                collectMatches(child, patternSegments, index, result);
            }
            return;
        }

        if ("*".equals(segment)) {
            for (RouteNode child : node.children()) {
                collectMatches(child, patternSegments, index + 1, result);
            }
            return;
        }

        RouteNode child = node.child(segment);
        if (child != null) {
            collectMatches(child, patternSegments, index + 1, result);
        }
    }

    private static final class RouteNode {
        private final Map<String, RouteNode> children = new TreeMap<>();
        private final Set<String> queueNames = new LinkedHashSet<>();

        private RouteNode(String segment) {
        }

        private RouteNode child(String segment) {
            return children.get(segment);
        }

        private RouteNode childOrCreate(String segment) {
            return children.computeIfAbsent(segment, RouteNode::new);
        }

        private Set<RouteNode> children() {
            return Set.copyOf(children.values());
        }

        private void addQueue(String queuePath) {
            queueNames.add(queuePath);
        }

        private void removeQueue(String queuePath) {
            queueNames.remove(queuePath);
        }

        private Set<String> queueNames() {
            return Set.copyOf(queueNames);
        }
    }
}
