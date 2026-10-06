package com.volo.broker.persistence.aol.io;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;


public final class AolFileChannel implements AolFile {

    private final Path filePath;
    private final FileChannel channel;

    private long writePosition;


    /**
     * Creates and opens a brand-new, empty segment file.
     * Fails if the file already exists.
     *
     * @param path destination path; parent directory must exist
     * @return an open, empty {@code FileChannelSegmentFile}
     * @throws IOException if the file already exists or cannot be created
     */
    public static AolFileChannel createNew(Path path) throws IOException {
        return new AolFileChannel(path,
                StandardOpenOption.CREATE_NEW,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }

    /**
     * Opens an existing segment file for both reading and appending.
     * The internal write position is initialized to the current file size,
     * so subsequent writes will append after existing content.
     *
     * @param path path to an existing segment file
     * @return an open {@code FileChannelSegmentFile} positioned at end-of-file
     * @throws IOException if the file does not exist or cannot be opened
     */
    public static AolFileChannel openExisting(Path path) throws IOException {
        return new AolFileChannel(path,
                StandardOpenOption.READ,
                StandardOpenOption.WRITE);
    }


    private AolFileChannel(Path path, OpenOption... options) throws IOException {
        this.filePath = path;
        this.channel = FileChannel.open(path, options);
        this.writePosition = channel.size(); // 0 for new files; EOF for existing ones
    }


    @Override
    public long write(ByteBuffer data) throws IOException {
        if (!data.hasRemaining()) {
            throw new IllegalArgumentException("data buffer has no remaining bytes");
        }

        long startPosition = writePosition;
        long currentPos    = writePosition;

        while (data.hasRemaining()) {
            int written = channel.write(data, currentPos);
            currentPos += written;
        }

        writePosition = currentPos;
        return startPosition;
    }

   
    @Override
    public ByteBuffer read(long position, int length) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException("position must be >= 0, got: " + position);
        }
        if (length <= 0) {
            throw new IllegalArgumentException("length must be > 0, got: " + length);
        }

        ByteBuffer buf = ByteBuffer.allocate(length);
        long readPos   = position;

        while (buf.hasRemaining()) {
            int read = channel.read(buf, readPos);
            if (read == -1) {
                throw new EOFException(
                        "Unexpected end of file in %s at position %d (requested %d bytes, got %d)"
                                .formatted(filePath, position, length, length - buf.remaining()));
            }
            readPos += read;
        }

        buf.flip();
        return buf;
    }

 
    @Override
    public long size() {
        return writePosition;
    }

  
    @Override
    public void force() throws IOException {
        channel.force(false);
    }

    
    @Override
    public void truncate(long newSize) throws IOException {
        if (newSize < 0) {
            throw new IllegalArgumentException("newSize must be >= 0, got: " + newSize);
        }
        channel.truncate(newSize);
        writePosition = newSize;
    }

    @Override
    public Path path() {
        return filePath;
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }


    @Override
    public String toString() {
        return "FileChannelSegmentFile{path=%s, writePosition=%d}".formatted(filePath, writePosition);
    }
}
