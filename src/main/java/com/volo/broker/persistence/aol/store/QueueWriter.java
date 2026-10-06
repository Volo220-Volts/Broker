package com.volo.broker.persistence.aol.store;

import com.volo.broker.broker.message.QueuedMessage;
import com.volo.broker.persistence.aol.api.WriteResult;
import com.volo.broker.persistence.aol.codec.DataRecordCodec;
import com.volo.broker.persistence.aol.io.AolFileChannel;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Holds all mutable, per-queue I/O state: the data segment file, the acked-offsets
 * file, and the monotonic offset counter. All write operations are {@code synchronized}
 * on this instance to serialise concurrent publisher threads for the same queue.
 */
final class QueueWriter implements Closeable {

    private static final int ACK_RECORD_SIZE = Long.BYTES; // 8 bytes per acked offset

    private final AolFileChannel dataFile;
    private final AolFileChannel ackedFile;
    private final DataRecordCodec codec;

    private long nextOffset;

    QueueWriter(AolFileChannel dataFile,
                AolFileChannel ackedFile,
                DataRecordCodec        codec,
                long                   nextOffset) {
        this.dataFile   = dataFile;
        this.ackedFile  = ackedFile;
        this.codec      = codec;
        this.nextOffset = nextOffset;
    }


    synchronized WriteResult writeMessage(QueuedMessage message) throws IOException {
        long offset  = nextOffset++;
        ByteBuffer encoded = codec.encode(message, offset);
        long physPos = dataFile.write(encoded);
        dataFile.force();
        return new WriteResult(offset, physPos);
    }


    synchronized void writeAck(long queueOffset) throws IOException {
        ByteBuffer buf = ByteBuffer.allocate(ACK_RECORD_SIZE);
        buf.putLong(queueOffset);
        buf.flip();
        ackedFile.write(buf);
        ackedFile.force();
    }


    synchronized long currentNextOffset() {
        return nextOffset;
    }


    @Override
    public synchronized void close() throws IOException {
        IOException first = null;
        try {
            dataFile.close();
        } catch (IOException e) {
            first = e;
        }
        try {
            ackedFile.close();
        } catch (IOException e) {
            if (first == null) first = e;
        }
        if (first != null) throw first;
    }
}
