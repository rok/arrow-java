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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.HasByteBuffer;
import io.grpc.KnownLength;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for reading gRPC message bodies into an {@link ArrowBuf} through gRPC's public API. */
public class TestGetReadableBuffer {

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    assertEquals(0, allocator.getAllocatedMemory());
    allocator.close();
  }

  @Test
  public void fastPathReadsThroughByteBuffersNotThroughHeapReads() throws IOException {
    final byte[] payload = payload(64);
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);

      assertArrayEquals(payload, contents(buf));
      assertEquals(0, stream.available());
      assertEquals(0, stream.heapReads, "fast path must not go through InputStream.read");
      assertTrue(stream.byteBufferPeeks > 0, "fast path must use HasByteBuffer.getByteBuffer");
    }
  }

  @Test
  public void fastPathToleratesSkipReturningZero() throws IOException {
    final byte[] payload = payload(48);
    try (ChunkedStream stream = new ChunkedStream(true, payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);

      assertArrayEquals(payload, contents(buf));
      assertEquals(0, stream.available());
    }
  }

  @Test
  public void fastPathCopiesAcrossChunkBoundaries() throws IOException {
    final byte[] payload = payload(70);
    try (ChunkedStream stream = new ChunkedStream(split(payload, 10, 1, 32, 27));
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);

      assertArrayEquals(payload, contents(buf));
      assertEquals(0, stream.available());
      assertEquals(0, stream.heapReads);
    }
  }

  @Test
  public void fastPathReadsOnlyTheRequestedBytes() throws IOException {
    final byte[] payload = payload(40);
    try (ChunkedStream stream = new ChunkedStream(split(payload, 16, 24));
        ArrowBuf buf = allocator.buffer(20)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, 20, true);

      assertArrayEquals(Arrays.copyOf(payload, 20), contents(buf));
      assertEquals(20, stream.available());
    }
  }

  @Test
  public void fallsBackToHeapReadWhenByteBuffersAreNotSupported() throws IOException {
    final byte[] payload = payload(24);
    try (ChunkedStream stream = ChunkedStream.withoutByteBufferSupport(split(payload, 9, 15));
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);

      assertArrayEquals(payload, contents(buf));
      assertEquals(0, stream.available());
      assertEquals(0, stream.byteBufferPeeks, "must not call getByteBuffer when unsupported");
      assertTrue(stream.heapReads > 0);
    }
  }

  @Test
  public void fastPathDisabledUsesHeapRead() throws IOException {
    final byte[] payload = payload(24);
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, false);

      assertArrayEquals(payload, contents(buf));
      assertEquals(0, stream.byteBufferPeeks, "fastPath=false must not touch HasByteBuffer");
      assertTrue(stream.heapReads > 0);
    }
  }

  @Test
  public void fastPathThrowsWhenStreamEndsEarly() throws IOException {
    final byte[] payload = payload(10);
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(16)) {
      assertThrows(
          IOException.class, () -> GetReadableBuffer.readIntoBuffer(stream, buf, 16, true));
    }
  }

  private static byte[][] split(byte[] payload, int... sizes) {
    final byte[][] pieces = new byte[sizes.length][];
    int offset = 0;
    for (int i = 0; i < sizes.length; i++) {
      pieces[i] = Arrays.copyOfRange(payload, offset, offset + sizes[i]);
      offset += sizes[i];
    }
    if (offset != payload.length) {
      throw new IllegalArgumentException("sizes must sum to payload length");
    }
    return pieces;
  }

  private static byte[] payload(int size) {
    final byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i * 7 + 3);
    }
    return bytes;
  }

  private static byte[] contents(ArrowBuf buf) {
    final byte[] out = new byte[(int) buf.writerIndex()];
    buf.getBytes(0, out);
    return out;
  }

  /**
   * A stand-in for gRPC's message stream: a chain of direct buffers exposed through {@link
   * HasByteBuffer} and {@link KnownLength}, with counters for how it was read.
   */
  static final class ChunkedStream extends InputStream implements HasByteBuffer, KnownLength {
    private final Deque<ByteBuffer> chunks = new ArrayDeque<>();
    private final boolean skipReturnsZeroEveryOtherCall;
    private final boolean byteBufferSupported;
    private boolean returnZeroFromNextSkip;
    int heapReads;
    int byteBufferPeeks;

    ChunkedStream(byte[]... pieces) {
      this(false, true, pieces);
    }

    ChunkedStream(boolean skipReturnsZeroEveryOtherCall, byte[]... pieces) {
      this(skipReturnsZeroEveryOtherCall, true, pieces);
    }

    static ChunkedStream withoutByteBufferSupport(byte[]... pieces) {
      return new ChunkedStream(false, false, pieces);
    }

    private ChunkedStream(
        boolean skipReturnsZeroEveryOtherCall, boolean byteBufferSupported, byte[]... pieces) {
      this.skipReturnsZeroEveryOtherCall = skipReturnsZeroEveryOtherCall;
      this.byteBufferSupported = byteBufferSupported;
      this.returnZeroFromNextSkip = skipReturnsZeroEveryOtherCall;
      for (byte[] piece : pieces) {
        final ByteBuffer chunk = ByteBuffer.allocateDirect(piece.length);
        chunk.put(piece).flip();
        chunks.add(chunk);
      }
    }

    private ByteBuffer current() {
      while (!chunks.isEmpty() && !chunks.peek().hasRemaining()) {
        chunks.poll();
      }
      return chunks.peek();
    }

    @Override
    public int read() {
      final ByteBuffer chunk = current();
      return chunk == null ? -1 : chunk.get() & 0xFF;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      heapReads++;
      final ByteBuffer chunk = current();
      if (chunk == null) {
        return -1;
      }
      final int n = Math.min(len, chunk.remaining());
      chunk.get(b, off, n);
      return n;
    }

    @Override
    public long skip(long n) {
      if (skipReturnsZeroEveryOtherCall) {
        returnZeroFromNextSkip = !returnZeroFromNextSkip;
        if (!returnZeroFromNextSkip) {
          // InputStream.skip may legitimately return 0 before end of stream.
          return 0;
        }
      }
      final ByteBuffer chunk = current();
      if (chunk == null) {
        return 0;
      }
      // Like gRPC, skip at most one chunk per call.
      final int skipped = (int) Math.min(n, chunk.remaining());
      chunk.position(chunk.position() + skipped);
      return skipped;
    }

    @Override
    public int available() {
      int total = 0;
      for (ByteBuffer chunk : chunks) {
        total += chunk.remaining();
      }
      return total;
    }

    @Override
    public boolean byteBufferSupported() {
      return byteBufferSupported;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      byteBufferPeeks++;
      final ByteBuffer chunk = current();
      return chunk == null ? null : chunk.duplicate();
    }
  }
}
