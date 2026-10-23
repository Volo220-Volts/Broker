package com.volo.broker.persistence;

import com.volo.broker.broker.queue.BrokerQueue;
import com.volo.broker.broker.queue.settings.*;
import com.volo.broker.broker.router.QueuePath;
import com.volo.broker.persistence.dto.StoredQueueDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class FileQueueMetadataStoreTest {

    @TempDir
    Path tempDir;

    private FileQueueMetadataStore store;

    @BeforeEach
    void setUp() {
        store = new FileQueueMetadataStore(tempDir);
    }

    @Test
    @DisplayName("loadAll on empty directory returns empty list")
    void loadAll_emptyDirectory_returnsEmptyList() {
        List<StoredQueueDto> result = store.loadAll();
        assertThat(result).isEmpty();
    }

    @Test
    @DisplayName("save and load queue with full custom settings")
    void saveAndLoad_customSettings_preservesAllFields() {
        QueueSettings customSettings = new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 7, true, false),
                new MessageRetentionPolicy(true, 60_000L),
                new QueueLifecyclePolicy(false, true, 30_000L, true),
                new QueueCapacityPolicy(true, 5000, 10_000_000L, OverflowPolicy.REJECT_PUBLISH),
                new DispatchPolicy(150, ConsumerSelectionStrategy.ROUND_ROBIN)
        );

        BrokerQueue queue = BrokerQueue.create(QueuePath.of("orders.created"), customSettings);
        store.save(queue);

        List<StoredQueueDto> loaded = store.loadAll();
        assertThat(loaded).hasSize(1);

        StoredQueueDto dto = loaded.get(0);
        assertThat(dto.queuePath()).isEqualTo("orders.created");
        assertThat(dto.settings()).isEqualTo(customSettings);

        Optional<QueueSettings> found = store.find("orders.created");
        assertThat(found).isPresent().contains(customSettings);
    }

    @Test
    @DisplayName("save supports slash and special characters in queue name")
    void save_specialCharactersInQueuePath_encodedCorrectly() {
        QueueSettings settings = QueueSettings.defaults();
        store.save("finance/payments/eu", settings);

        Optional<QueueSettings> found = store.find("finance/payments/eu");
        assertThat(found).isPresent();

        List<StoredQueueDto> all = store.loadAll();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).queuePath()).isEqualTo("finance/payments/eu");
    }

    @Test
    @DisplayName("overwrite updates existing queue metadata atomically")
    void save_existingQueue_overwritesSettings() {
        store.save("my.queue", QueueSettings.defaults());

        QueueSettings updated = new QueueSettings(
                PersistenceMode.PERSISTENT,
                PublishAckMode.ENABLED,
                ConsumerAckMode.ENABLED,
                new RedeliveryPolicy(true, 10, false, false),
                MessageRetentionPolicy.disabled(),
                QueueLifecyclePolicy.defaults(),
                QueueCapacityPolicy.unlimited(),
                new DispatchPolicy(50, ConsumerSelectionStrategy.FIRST_AVAILABLE)
        );

        store.save("my.queue", updated);

        Optional<QueueSettings> found = store.find("my.queue");
        assertThat(found).isPresent().contains(updated);
        assertThat(store.loadAll()).hasSize(1);
    }

    @Test
    @DisplayName("delete removes queue metadata file")
    void delete_removesMetadata() {
        store.save("temp.queue", QueueSettings.defaults());
        assertThat(store.find("temp.queue")).isPresent();

        store.delete("temp.queue");

        assertThat(store.find("temp.queue")).isEmpty();
        assertThat(store.loadAll()).isEmpty();
    }

    @Test
    @DisplayName("delete non-existing queue does not fail")
    void delete_nonExisting_doesNotThrow() {
        store.delete("does.not.exist");
    }

    @Test
    @DisplayName("corrupt metadata file is skipped gracefully during loadAll")
    void loadAll_corruptedFile_isSkippedGracefully() throws IOException {
        store.save("valid.queue", QueueSettings.defaults());

        Path corruptDir = tempDir.resolve("queues").resolve("corrupt_queue");
        Files.createDirectories(corruptDir);
        Files.writeString(corruptDir.resolve("queue.json"), "NOT_VALID_JSON{{{");

        List<StoredQueueDto> all = store.loadAll();
        assertThat(all).hasSize(1);
        assertThat(all.get(0).queuePath()).isEqualTo("valid.queue");
    }

    @Test
    @DisplayName("validate invalid queuePath")
    void invalidQueuePath_throwsIllegalArgumentException() {
        assertThatThrownBy(() -> store.save("", QueueSettings.defaults()))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.find(null))
                .isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> store.delete("   "))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    @DisplayName("legacy queue.json without flowControlPolicy loads with FlowControlPolicy.disabled()")
    void load_legacyJsonWithoutFlowControlPolicy_defaultsToDisabled() throws IOException {
        String legacyJson = """
                {
                  "queuePath" : "legacy.queue",
                  "createdAt" : 1700000000000,
                  "settings" : {
                    "persistenceMode" : "PERSISTENT",
                    "publishAckMode" : "ENABLED",
                    "consumerAckMode" : "ENABLED",
                    "redeliveryPolicy" : {
                      "enabled" : true,
                      "maxAttempts" : 3,
                      "requeueOnDisconnect" : true,
                      "requeueOnSendFailure" : false
                    },
                    "retentionPolicy" : {
                      "ttlEnabled" : false,
                      "ttlMs" : 0
                    },
                    "lifecyclePolicy" : {
                      "autoDelete" : false,
                      "deleteOnIdle" : false,
                      "idleTimeoutMs" : 0,
                      "deleteOnClose" : false
                    },
                    "capacityPolicy" : {
                      "limited" : false,
                      "maxMessages" : 0,
                      "maxBytes" : 0,
                      "overflowPolicy" : "REJECT_PUBLISH"
                    },
                    "dispatchPolicy" : {
                      "prefetchCount" : 100,
                      "consumerSelectionStrategy" : "ROUND_ROBIN"
                    }
                  }
                }
                """;

        Path queueDir = tempDir.resolve("queues").resolve("legacy.queue");
        Files.createDirectories(queueDir);
        Files.writeString(queueDir.resolve("queue.json"), legacyJson);

        Optional<QueueSettings> found = store.find("legacy.queue");
        assertThat(found).isPresent();
        assertThat(found.get().flowControlPolicy()).isNotNull();
        assertThat(found.get().flowControlPolicy().enabled()).isFalse();
    }
}
