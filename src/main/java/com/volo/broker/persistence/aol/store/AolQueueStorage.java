package com.volo.broker.persistence.aol.store;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.QueueStorage;
import com.volo.broker.persistence.aol.api.RecoveryResult;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.codec.DecodedRecord;
import com.volo.broker.persistence.aol.io.AolFileChannel;
import com.volo.broker.persistence.aol.io.SegmentTailValidator;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

public final class AolQueueStorage implements QueueStorage {

    private static final Logger log = Logger.getLogger(AolQueueStorage.class.getName());

    private static final String DATA_FILE_NAME  = "data.log";
    private static final String ACKED_FILE_NAME = "acked.bin";
    private static final int    ACKED_ENTRY_SIZE = Long.BYTES; // 8 bytes per offset
    private static final int    RECORD_LEN_FIELD = Integer.BYTES; // 4 bytes

    private final Path                              baseDir;
    private final DataRecordCodec                   codec;
    private final SegmentTailValidator              tailValidator;
    private final ConcurrentHashMap<String, QueueWriter> writers = new ConcurrentHashMap<>();

    // ------------------------------------------------------------------ construction

    /**
     * Creates a new {@code AolQueueStorage} rooted at {@code baseDir}.
     * The directory (and its parents) is created if it does not exist.
     *
     * @param baseDir root directory for all queue data
     * @throws IOException if the root directory cannot be created
     */
    public AolQueueStorage(Path baseDir) throws IOException {
        this.baseDir       = Objects.requireNonNull(baseDir, "baseDir");
        this.codec         = new DataRecordCodec();
        this.tailValidator = new SegmentTailValidator(codec);
        Files.createDirectories(baseDir);
    }

 
    @Override
    public WriteResult append(String queuePath, QueuedMessage message) throws IOException {
        validateQueuePath(queuePath);
        Objects.requireNonNull(message, "message");
        return writerFor(queuePath).writeMessage(message);
    }


    @Override
    public void acknowledge(String queuePath, long queueOffset) throws IOException {
        validateQueuePath(queuePath);
        writerFor(queuePath).writeAck(queueOffset);
    }


    @Override
    public RecoveryResult recover(String queuePath) throws IOException {
        validateQueuePath(queuePath);

        Path queueDir  = queueDir(queuePath);
        Path dataPath  = queueDir.resolve(DATA_FILE_NAME);
        Path ackedPath = queueDir.resolve(ACKED_FILE_NAME);

        if (!Files.exists(dataPath)) {
            log.info("No data.log for queue '" + queuePath + "' — starting fresh.");
            return RecoveryResult.empty();
        }

        Files.createDirectories(queueDir);

        AolFileChannel dataFile = AolFileChannel.openExisting(dataPath);
        try {
            SegmentTailValidator.ValidationResult tailResult = tailValidator.validate(dataFile);
            if (tailResult.truncationNeeded()) {
                log.warning("Queue '" + queuePath + "': truncating corrupt tail at "
                        + tailResult.lastValidPosition() + " (was " + dataFile.size() + ")");
                dataFile.truncate(tailResult.lastValidPosition());
            }

            Set<Long> ackedOffsets = loadAckedOffsets(ackedPath);

            ScanResult scan = scanDataFile(dataFile, ackedOffsets);

            AolFileChannel ackedFile = Files.exists(ackedPath)
                    ? AolFileChannel.openExisting(ackedPath)
                    : AolFileChannel.createNew(ackedPath);

            QueueWriter writer = new QueueWriter(dataFile, ackedFile, codec, scan.nextOffset());
            QueueWriter previous = writers.put(queuePath, writer);
            if (previous != null) {
                log.warning("Queue '" + queuePath + "' was already recovered — closing old writer.");
                previous.close();
            }

            log.info("Queue '" + queuePath + "' recovered: "
                    + scan.liveMessages().size() + " live messages, nextOffset=" + scan.nextOffset());

            return new RecoveryResult(scan.liveMessages(), scan.nextOffset());

        } catch (Exception e) {
            dataFile.close();
            throw e;
        }
    }

   
    @Override
    public void close() throws IOException {
        IOException firstError = null;
        for (var entry : writers.entrySet()) {
            try {
                entry.getValue().close();
            } catch (IOException e) {
                log.warning("Error closing writer for queue '" + entry.getKey() + "': " + e.getMessage());
                if (firstError == null) firstError = e;
            }
        }
        writers.clear();
        if (firstError != null) throw firstError;
    }

    // ------------------------------------------------------------------ private: writer lifecycle

    /**
     * Returns the existing {@link QueueWriter} for {@code queuePath}, or creates
     * one (including the directory and files) if this is the first access.
     *
     */
    private QueueWriter writerFor(String queuePath) throws IOException {
        QueueWriter existing = writers.get(queuePath);
        if (existing != null) return existing;

        return createWriter(queuePath);
    }

    private synchronized QueueWriter createWriter(String queuePath) throws IOException {
        // Re-check under lock — another thread may have created it while we waited
        QueueWriter existing = writers.get(queuePath);
        if (existing != null) return existing;

        Path queueDir  = queueDir(queuePath);
        Path dataPath  = queueDir.resolve(DATA_FILE_NAME);
        Path ackedPath = queueDir.resolve(ACKED_FILE_NAME);
        Files.createDirectories(queueDir);

        AolFileChannel dataFile = Files.exists(dataPath)
                ? AolFileChannel.openExisting(dataPath)
                : AolFileChannel.createNew(dataPath);

        AolFileChannel ackedFile = Files.exists(ackedPath)
                ? AolFileChannel.openExisting(ackedPath)
                : AolFileChannel.createNew(ackedPath);

        QueueWriter writer = new QueueWriter(dataFile, ackedFile, codec, 0L);
        writers.put(queuePath, writer);
        log.info("Created new QueueWriter for '" + queuePath + "'.");
        return writer;
    }


    private static Set<Long> loadAckedOffsets(Path ackedPath) throws IOException {
        if (!Files.exists(ackedPath)) {
            return Set.of();
        }
        byte[] bytes = Files.readAllBytes(ackedPath);
        int count = bytes.length / ACKED_ENTRY_SIZE;
        Set<Long> offsets = new HashSet<>(count * 2);
        ByteBuffer buf = ByteBuffer.wrap(bytes);
        for (int i = 0; i < count; i++) {
            offsets.add(buf.getLong());
        }
        return offsets;
    }

    /**
     * Sequentially scans {@code dataFile} from position 0, decoding every record.
     * Skips acked messages.
     */
    private ScanResult scanDataFile(AolFileChannel dataFile,
                                    Set<Long> ackedOffsets) throws IOException {
        List<QueuedMessage> live      = new ArrayList<>();
        long                maxOffset = -1;
        long                pos       = 0;
        long                fileSize  = dataFile.size();

        while (pos + RECORD_LEN_FIELD <= fileSize) {

            ByteBuffer lenBuf      = dataFile.read(pos, RECORD_LEN_FIELD);
            int        recordLength = lenBuf.getInt();

            int  minBody   = DataRecordCodec.FIXED_BODY_SIZE + RECORD_LEN_FIELD; // 43
            long recordEnd = pos + RECORD_LEN_FIELD + recordLength;

            if (recordLength < minBody || recordEnd > fileSize) {
                break;
            }

            ByteBuffer  recordBuf = dataFile.read(pos, RECORD_LEN_FIELD + recordLength);
            DecodedRecord decoded = codec.decode(recordBuf);

            maxOffset = Math.max(maxOffset, decoded.queueOffset());

            if (!ackedOffsets.contains(decoded.queueOffset())) {
                live.add(decoded.message().asRedelivered());
            }

            pos = recordEnd;
        }

        long nextOffset = maxOffset < 0 ? 0L : maxOffset + 1;
        return new ScanResult(live, nextOffset);
    }


    private Path queueDir(String queuePath) {
        return baseDir.resolve("queues").resolve(encodeQueueName(queuePath));
    }

    /**
     * Produces a filesystem-safe directory name from a logical queue path.
     * Replaces path separators and other problematic characters.
     *
     * <p>Examples:
     * <pre>
     * "orders.created"        → "orders.created"
     * "finance/payments.fail" → "finance__payments.fail"
     * </pre>
     */
    private static String encodeQueueName(String queuePath) {
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


    private record ScanResult(List<QueuedMessage> liveMessages, long nextOffset) {}
}
