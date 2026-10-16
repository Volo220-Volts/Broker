package com.volo.broker.persistence;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.QueueSettings;
import com.volo.broker.persistence.aol.io.AtomicFileRenamer;
import com.volo.broker.persistence.dto.StoredQueueDto;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

/**
 * File-based implementation of {@link QueueMetadataStore}.
 *
 * <p>Persists each queue's metadata into {@code <baseDir>/queues/<encodedQueueName>/queue.json}
 * using atomic file write-and-rename semantics to prevent corruption during unexpected shutdowns.
 */
public class FileQueueMetadataStore implements QueueMetadataStore {

    private static final Logger log = LoggerFactory.getLogger(FileQueueMetadataStore.class);
    private static final String QUEUE_META_FILE = "queue.json";
    private static final String QUEUE_META_TMP_FILE = "queue.json.tmp";

    public record QueueMetadataRecord(
            String queuePath,
            long createdAt,
            QueueSettings settings
    ) {}

    private final Path queuesDir;
    private final ObjectMapper objectMapper;

    public FileQueueMetadataStore(Path baseDir) {
        this(baseDir, defaultObjectMapper());
    }

    public FileQueueMetadataStore(Path baseDir, ObjectMapper objectMapper) {
        this.queuesDir = baseDir.resolve("queues");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    @Override
    public void save(BrokerQueue queue) {
        Objects.requireNonNull(queue, "queue must not be null");
        save(queue.queuePath(), queue.settings());
    }

    @Override
    public void save(String queuePath, QueueSettings settings) {
        validateQueuePath(queuePath);
        Objects.requireNonNull(settings, "settings must not be null");

        Path queueDirectory = queueDir(queuePath);
        Path targetFile = queueDirectory.resolve(QUEUE_META_FILE);
        Path tmpFile = queueDirectory.resolve(QUEUE_META_TMP_FILE);

        try {
            Files.createDirectories(queueDirectory);

            QueueMetadataRecord record = new QueueMetadataRecord(
                    queuePath,
                    System.currentTimeMillis(),
                    settings
            );

            byte[] jsonBytes = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(record);

            Files.write(tmpFile, jsonBytes,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.SYNC);

            AtomicFileRenamer.rename(tmpFile, targetFile);

            log.info("Persisted metadata for queue '{}' to '{}'", queuePath, targetFile);
        } catch (IOException e) {
            try {
                Files.deleteIfExists(tmpFile);
            } catch (IOException ignored) {}
            throw new UncheckedIOException("Failed to persist queue metadata for '" + queuePath + "'", e);
        }
    }

    @Override
    public void delete(String queuePath) {
        validateQueuePath(queuePath);
        Path queueDirectory = queueDir(queuePath);
        Path metaFile = queueDirectory.resolve(QUEUE_META_FILE);

        try {
            boolean deleted = Files.deleteIfExists(metaFile);
            if (deleted) {
                log.info("Deleted metadata file for queue '{}'", queuePath);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to delete queue metadata for '" + queuePath + "'", e);
        }
    }

    @Override
    public List<StoredQueueDto> loadAll() {
        if (!Files.exists(queuesDir)) {
            return List.of();
        }

        List<StoredQueueDto> result = new ArrayList<>();
        try (Stream<Path> stream = Files.list(queuesDir)) {
            stream.filter(Files::isDirectory).forEach(dir -> {
                Path metaFile = dir.resolve(QUEUE_META_FILE);
                if (Files.isRegularFile(metaFile)) {
                    try {
                        QueueMetadataRecord record = objectMapper.readValue(metaFile.toFile(), QueueMetadataRecord.class);
                        result.add(new StoredQueueDto(record.queuePath(), record.settings()));
                    } catch (IOException e) {
                        log.error("Failed to load queue metadata from '{}': {}", metaFile, e.getMessage(), e);
                    }
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to list queues directory: " + queuesDir, e);
        }

        log.info("Loaded {} queue definition(s) from '{}'", result.size(), queuesDir);
        return result;
    }

    @Override
    public Optional<QueueSettings> find(String queuePath) {
        validateQueuePath(queuePath);
        Path metaFile = queueDir(queuePath).resolve(QUEUE_META_FILE);
        if (!Files.isRegularFile(metaFile)) {
            return Optional.empty();
        }

        try {
            QueueMetadataRecord record = objectMapper.readValue(metaFile.toFile(), QueueMetadataRecord.class);
            return Optional.ofNullable(record.settings());
        } catch (IOException e) {
            log.error("Failed to read queue metadata from '{}': {}", metaFile, e.getMessage(), e);
            return Optional.empty();
        }
    }

    private Path queueDir(String queuePath) {
        return queuesDir.resolve(encodeQueueName(queuePath));
    }

    public static String encodeQueueName(String queuePath) {
        return queuePath
                .replace('/', '_')
                .replace('\\', '_')
                .replace('\0', '_');
    }

    private static void validateQueuePath(String queuePath) {
        if (queuePath == null || queuePath.isBlank()) {
            throw new IllegalArgumentException("queuePath must not be null or blank");
        }
    }

    private static ObjectMapper defaultObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        mapper.setDefaultPropertyInclusion(JsonInclude.Include.NON_NULL);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);
        return mapper;
    }
}

