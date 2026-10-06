package com.volo.broker.persistence.aol.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;

/**
 * Abstraction over a single append-only log file.
 */
public interface AolFile extends Closeable {

    /**
     * Appends all bytes from {@code data} to the end of the file.
     * The buffer's position is advanced to its limit after the call.
     *
     * @param data the bytes to write; must have at least one remaining byte
     * @return the file offset where the written bytes begin
     * @throws IOException if the write fails
     */
    long write(ByteBuffer data) throws IOException;

    /**
     * Reads exactly {@code length} bytes starting at {@code position}.
     *
     * @param position absolute byte offset in the file
     * @param length   number of bytes to read
     * @return a new heap-backed {@link ByteBuffer} ready for reading
     * @throws IOException                   if the read fails
     * @throws java.io.EOFException          if fewer than {@code length} bytes exist at {@code position}
     * @throws IllegalArgumentException      if {@code position < 0} or {@code length ≤ 0}
     */
    ByteBuffer read(long position, int length) throws IOException;

    /**
     * Returns the current file size in bytes.
     */
    long size();

    /**
     * Forces all pending writes to the underlying storage device.
     */
    void force() throws IOException;

    /**
     * Truncates the file to {@code newSize} bytes, discarding any data beyond
     * that point.
     *
     * @param newSize the desired file size in bytes (must be ≥ 0 and ≤ current size)
     * @throws IOException              if the truncation fails
     * @throws IllegalArgumentException if {@code newSize} is negative
     */
    void truncate(long newSize) throws IOException;

    /**
     * Absolute path to the file on the filesystem.
     */
    Path path();

    @Override
    void close() throws IOException;
}
