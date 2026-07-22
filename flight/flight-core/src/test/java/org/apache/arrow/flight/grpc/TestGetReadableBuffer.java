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

import io.grpc.HasByteBuffer;
import java.io.ByteArrayInputStream;
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

/** Tests for reading a gRPC-provided {@link InputStream} into an {@link ArrowBuf}. */
public class TestGetReadableBuffer {

  private BufferAllocator allocator;

  @BeforeEach
  public void setUp() {
    allocator = new RootAllocator(Long.MAX_VALUE);
  }

  @AfterEach
  public void tearDown() {
    allocator.close();
  }

  @Test
  public void testFastPathSingleChunk() throws IOException {
    final byte[] payload = payload(64);
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);
      assertEquals(payload.length, buf.writerIndex());
      assertArrayEquals(payload, toBytes(buf, payload.length));
    }
  }

  /** Copy loop stitches several backing buffers of the requested size. */
  @Test
  public void testFastPathAcrossChunks() throws IOException {
    final byte[] payload = payload(70);
    try (ChunkedStream stream = new ChunkedStream(slice(payload, 10, 1, 32, 27));
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);
      assertArrayEquals(payload, toBytes(buf, payload.length));
    }
  }

  /**
   * A chunk may hold more than the caller asked for (like next field bytes). Only {@code
   * size} bytes may be consumed, the rest must remain readable.
   */
  @Test
  public void testFastPathDoesNotOverConsume() throws IOException {
    final byte[] payload = payload(48);
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(20)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, 20, true);
      assertArrayEquals(Arrays.copyOf(payload, 20), toBytes(buf, 20));

      final byte[] rest = new byte[payload.length - 20];
      assertEquals(rest.length, stream.read(rest));
      assertArrayEquals(Arrays.copyOfRange(payload, 20, payload.length), rest);
    }
  }

  /** The copy loop must progress when a valid InputStream returns zero from skip(). */
  @Test
  public void testFastPathSkipReturnsZero() throws IOException {
    final byte[] payload = payload(32);
    try (ChunkedStream stream = new ChunkedStream(true, payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, true);
      assertArrayEquals(payload, toBytes(buf, payload.length));
    }
  }

  /** A truncated stream must fail loudly rather than leave the buffer partially filled. */
  @Test
  public void testFastPathTruncatedStream() throws IOException {
    try (ChunkedStream stream = new ChunkedStream(payload(10));
        ArrowBuf buf = allocator.buffer(32)) {
      assertThrows(IOException.class, () -> GetReadableBuffer.readIntoBuffer(stream, buf, 32, true));
    }
  }

  /** Both take the heap-array path: streams without zero-copy support, and fastPath=false. */
  @Test
  public void testSlowPath() throws IOException {
    final byte[] payload = payload(33);
    try (ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(
          new ByteArrayInputStream(payload), buf, payload.length, true);
      assertArrayEquals(payload, toBytes(buf, payload.length));
    }
    try (ChunkedStream stream = new ChunkedStream(payload);
        ArrowBuf buf = allocator.buffer(payload.length)) {
      GetReadableBuffer.readIntoBuffer(stream, buf, payload.length, false);
      assertArrayEquals(payload, toBytes(buf, payload.length));
    }
  }

  private static byte[] payload(int size) {
    final byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }

  private static byte[] toBytes(ArrowBuf buf, int size) {
    final byte[] bytes = new byte[size];
    buf.getBytes(0, bytes);
    return bytes;
  }

  private static byte[][] slice(byte[] payload, int... sizes) {
    final byte[][] chunks = new byte[sizes.length][];
    int offset = 0;
    for (int i = 0; i < sizes.length; i++) {
      chunks[i] = Arrays.copyOfRange(payload, offset, offset + sizes[i]);
      offset += sizes[i];
    }
    return chunks;
  }

  /**
   * For gRPC's buffer-backed streams: {@link #getByteBuffer()} exposes the next chunk
   * and {@link #skip(long)} advances.
   */
  private static final class ChunkedStream extends InputStream implements HasByteBuffer {
    private final Deque<ByteBuffer> chunks = new ArrayDeque<>();
    private boolean skipReturnsZero;

    ChunkedStream(byte[]... chunks) {
      this(false, chunks);
    }

    ChunkedStream(boolean skipReturnsZero, byte[]... chunks) {
      this.skipReturnsZero = skipReturnsZero;
      for (byte[] chunk : chunks) {
        this.chunks.add(ByteBuffer.wrap(chunk));
      }
    }

    @Override
    public boolean byteBufferSupported() {
      return true;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      final ByteBuffer head = chunks.peek();
      // Like gRPC, hand out an independent view so the caller may adjust position/limit freely.
      return head == null ? null : head.duplicate();
    }

    @Override
    public long skip(long n) {
      if (skipReturnsZero) {
        skipReturnsZero = false;
        return 0;
      }
      final ByteBuffer head = chunks.peek();
      if (head == null) {
        return 0;
      }
      final int skipped = (int) Math.min(n, head.remaining());
      head.position(head.position() + skipped);
      if (!head.hasRemaining()) {
        chunks.poll();
      }
      return skipped;
    }

    @Override
    public int read() {
      final byte[] one = new byte[1];
      return read(one, 0, 1) == 1 ? one[0] & 0xFF : -1;
    }

    @Override
    public int read(byte[] dst, int off, int len) {
      final ByteBuffer head = chunks.peek();
      if (head == null) {
        return -1;
      }
      final int read = Math.min(len, head.remaining());
      head.get(dst, off, read);
      if (!head.hasRemaining()) {
        chunks.poll();
      }
      return read;
    }
  }
}
