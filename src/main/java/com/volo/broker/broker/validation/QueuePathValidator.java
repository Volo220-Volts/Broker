package com.volo.broker.broker.validation;

import org.springframework.stereotype.Component;

import com.volo.broker.broker.router.QueuePath;

import java.util.regex.Pattern;

@Component
public class QueuePathValidator {
    private static final Pattern ALLOWED_PATTERN =
            Pattern.compile("^[a-zA-Z0-9._-]+$");

    public QueuePath validateAndParse(String queuePath) {
        QueuePath parsed = QueuePath.of(queuePath);

        if (!ALLOWED_PATTERN.matcher(parsed.value()).matches()) {
            throw new IllegalArgumentException(
                    "queue path may contain only letters, digits, dots, underscores and hyphens"
            );
        }

        return parsed;
    }
}
