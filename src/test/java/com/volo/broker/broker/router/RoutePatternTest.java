package com.volo.broker.broker.router;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class RoutePatternTest {

    @Test
    void of_shouldParseExactPattern() {
        RoutePattern pattern = RoutePattern.of("orders.created");

        // assertThat(pattern.value()).isEqualTo("orders.created");
        assertThat(pattern.segments()).containsExactly("orders", "created");
    }

    @Test
    void of_shouldAllowSingleSegmentWildcard() {
        RoutePattern pattern = RoutePattern.of("orders.*");

        assertThat(pattern.segments()).containsExactly("orders", "*");
    }

    @Test
    void of_shouldAllowRecursiveWildcard() {
        RoutePattern pattern = RoutePattern.of("orders.#");

        assertThat(pattern.segments()).containsExactly("orders", "#");
    }

    @Test
    void of_shouldRejectBlankPattern() {
        assertThatThrownBy(() -> RoutePattern.of(""))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void of_shouldRejectEmptySegments() {
        assertThatThrownBy(() -> RoutePattern.of("orders..created"))
                .isInstanceOf(IllegalArgumentException.class);
    }
}