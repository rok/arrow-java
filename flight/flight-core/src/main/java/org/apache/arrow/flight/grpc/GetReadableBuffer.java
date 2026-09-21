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
 * Reads a gRPC message body into an {@link ArrowBuf}.
 *
 * <p>When the stream gRPC hands us exposes its backing buffers through {@link HasByteBuffer}, the
 * bytes are copied straight from those buffers into the target, skipping the intermediate heap
 * array. Otherwise the stream is read into a heap array first.
 */
public class GetReadableBuffer {

  private GetReadableBuffer() {}

  /**
   * Read exactly {@code size} bytes from {@code stream} into {@code buf}.
   *
   * @param stream The stream to read from.
   * @param buf The buffer to read into. Its writer index is set to {@code size} on success.
   * @param size The number of bytes to read.
   * @param fastPath Whether to copy directly from the stream's backing buffers when it exposes
   *     them.
   * @throws IOException if the stream ends early or cannot be read.
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
      buf.setBytes(0, heapBytes);
    }
    buf.writerIndex(size);
  }

  private static void readFromByteBuffers(
      final HasByteBuffer source, final InputStream stream, final ArrowBuf buf, final int size)
      throws IOException {
    int copied = 0;
    while (copied < size) {
      final ByteBuffer chunk = source.getByteBuffer();
      if (chunk == null || !chunk.hasRemaining()) {
        throw new IOException(
            "Unexpected end of gRPC stream: expected " + size + " bytes, got " + copied);
      }
      final int toRead = Math.min(size - copied, chunk.remaining());
      buf.setBytes(copied, chunk, chunk.position(), toRead);
      // getByteBuffer() does not advance the stream; skip() consumes the bytes we just copied.
      consume(stream, toRead);
      copied += toRead;
    }
  }

  private static void consume(final InputStream stream, final int count) throws IOException {
    int skipped = 0;
    while (skipped < count) {
      final long n = stream.skip(count - skipped);
      if (n > 0) {
        skipped += (int) n;
        continue;
      }
      // InputStream.skip is allowed to return 0 before the end of the stream. Force progress with
      // a single-byte read, which does distinguish end of stream.
      if (stream.read() == -1) {
        throw new IOException("Unexpected end of gRPC stream while skipping copied bytes");
      }
      skipped++;
    }
  }
}
