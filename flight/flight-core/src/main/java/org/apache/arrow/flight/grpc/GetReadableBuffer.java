/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.arrow.flight.grpc;

import com.google.common.io.ByteStreams;
import io.grpc.HasByteBuffer;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.arrow.memory.ArrowBuf;

/**
 * Copy data from a gRPC-provided InputStream into a target ArrowBuf.
 *
 * <p>When the stream is backed by gRPC's own buffers (i.e. it implements {@link HasByteBuffer}),
 * the payload is copied directly out of gRPC's {@link ByteBuffer}s, avoiding an intermediate heap
 * {@code byte[]} allocation and the extra copy that goes with it. Otherwise, we fall back to
 * reading the stream into a heap array.
 *
 * <p>This relies on gRPC's public zero-copy APIs ({@link HasByteBuffer#getByteBuffer()} and
 * {@link InputStream#skip(long)}). No longer access gRPC internals via reflection. See
 * <a href="https://github.com/apache/arrow-java/issues/939">apache/arrow-java#939</a>.
 */
public final class GetReadableBuffer {

  private GetReadableBuffer() {}

  /**
   * Helper method to read a gRPC-provided InputStream into an ArrowBuf.
   *
   * @param stream The stream to read from.
   * @param buf The buffer to read into.
   * @param size The number of bytes to read.
   * @param fastPath Whether to enable the fast path (i.e. copy directly from the stream's backing
   *     {@link ByteBuffer}s when the stream supports it).
   * @throws IOException if there is an error reading from the stream
   */
  public static void readIntoBuffer(
      final InputStream stream, final ArrowBuf buf, final int size, final boolean fastPath)
      throws IOException {
    if (fastPath
        && stream instanceof HasByteBuffer
        && ((HasByteBuffer) stream).byteBufferSupported()) {
      readFromByteBuffers((HasByteBuffer) stream, stream, buf, size);
    } else {
      final byte[] heapBytes = new byte[size];
      ByteStreams.readFully(stream, heapBytes);
      buf.writeBytes(heapBytes);
    }
    buf.writerIndex(size);
  }

  /**
   * Copy {@code size} bytes directly out of the stream's backing {@link ByteBuffer}s into {@code
   * buf}.
   *
   * <p>{@link HasByteBuffer#getByteBuffer()} exposes the next chunk of readable bytes without
   * advancing the stream, so we copy the chunk and then {@link InputStream#skip(long)} past (and
   * release) the bytes we consumed before asking for the next chunk.
   */
  private static void readFromByteBuffers(
      final HasByteBuffer hasByteBuffer,
      final InputStream stream,
      final ArrowBuf buf,
      final int size)
      throws IOException {
    long writeIndex = 0;
    int remaining = size;
    while (remaining > 0) {
      final ByteBuffer chunk = hasByteBuffer.getByteBuffer();
      // getByteBuffer() may expose more than we need (e.g. bytes belonging to the following
      // field), so only copy up to the number of bytes still outstanding. We are allowed to change
      // the returned buffer's position/limit without affecting the stream.
      final int toRead = chunk == null ? 0 : Math.min(remaining, chunk.remaining());
      if (toRead == 0) {
        throw new IOException(
            String.format(
                "Unexpected end of stream: %d of %d bytes remaining to be read", remaining, size));
      }
      chunk.limit(chunk.position() + toRead);
      buf.setBytes(writeIndex, chunk);
      // getByteBuffer() does not advance the stream; skip() consumes (and frees) the bytes.
      long skipped = 0;
      while (skipped < toRead) {
        final long n = stream.skip(toRead - skipped);
        if (n <= 0) {
          throw new IOException("Failed to skip past consumed bytes in the gRPC stream");
        }
        skipped += n;
      }
      writeIndex += toRead;
      remaining -= toRead;
    }
  }
}
