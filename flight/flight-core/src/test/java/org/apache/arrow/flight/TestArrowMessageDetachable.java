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
package org.apache.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.KnownLength;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for parsing FlightData without copying, by taking ownership of gRPC's buffer. */
public class TestArrowMessageDetachable {

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
  public void contiguousDirectBufferIsDetachedAndWrappedWithoutCopy() throws Exception {
    final byte[] metadata = payload(16);
    final byte[] body = payload(64);
    final byte[] serialized = flightData(metadata, body);
    final MockGrpcInputStream stream = MockGrpcInputStream.direct(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      // gRPC closes the stream it handed us as soon as parse() returns.
      stream.close();

      assertEquals(1, stream.state.detachCount, "must take ownership through detach()");
      assertEquals(
          serialized.length,
          allocator.getAllocatedMemory(),
          "the whole gRPC frame is wrapped, not a copy of the body");
      assertArrayEquals(metadata, contents(message.getApplicationMetadata()));
      assertArrayEquals(body, contents(message.getBufs().iterator().next()));
      assertEquals(
          0, stream.state.detachedCloseCount, "gRPC memory must stay alive with the message");
    }
    assertEquals(
        1, stream.state.detachedCloseCount, "closing the message releases gRPC's buffer once");
  }

  @Test
  public void heapBufferFallsBackToCopyingWithoutDetaching() throws Exception {
    final byte[] metadata = payload(16);
    final byte[] body = payload(64);
    final MockGrpcInputStream stream = MockGrpcInputStream.heap(flightData(metadata, body));

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      stream.close();

      assertEquals(0, stream.state.detachCount, "on-heap buffers cannot be wrapped");
      assertEquals(
          metadata.length + body.length,
          allocator.getAllocatedMemory(),
          "copying path allocates exactly the two fields");
      assertArrayEquals(metadata, contents(message.getApplicationMetadata()));
      assertArrayEquals(body, contents(message.getBufs().iterator().next()));
    }
    assertEquals(0, stream.state.detachedCloseCount);
  }

  @Test
  public void messageSplitAcrossBuffersFallsBackToCopying() throws Exception {
    final byte[] metadata = payload(16);
    final byte[] body = payload(64);
    final byte[] serialized = flightData(metadata, body);
    final MockGrpcInputStream stream = MockGrpcInputStream.directFragmented(serialized, 20);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      stream.close();

      assertEquals(0, stream.state.detachCount, "only a single contiguous buffer is wrapped");
      assertEquals(metadata.length + body.length, allocator.getAllocatedMemory());
      assertArrayEquals(metadata, contents(message.getApplicationMetadata()));
      assertArrayEquals(body, contents(message.getBufs().iterator().next()));
    }
  }

  @Test
  public void parseFailureReleasesTheDetachedBuffer() throws Exception {
    // A complete body, then a descriptor whose declared length runs past the end of the frame.
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(bytes);
    out.writeBytes(FlightData.DATA_BODY_FIELD_NUMBER, ByteString.copyFrom(payload(8)));
    out.writeTag(FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    out.writeUInt32NoTag(10);
    out.writeRawByte(1);
    out.flush();
    final MockGrpcInputStream stream = MockGrpcInputStream.direct(bytes.toByteArray());

    assertThrows(
        RuntimeException.class, () -> ArrowMessage.createMarshaller(allocator).parse(stream));
    stream.close();

    assertEquals(1, stream.state.detachCount);
    assertEquals(1, stream.state.detachedCloseCount, "the detached buffer must not leak");
    assertEquals(0, allocator.getAllocatedMemory());
  }

  @Test
  public void lastOfDuplicateBodyFieldsWinsWithoutLeaking() throws Exception {
    final byte[] first = payload(8);
    final byte[] second = payload(24);
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    final CodedOutputStream out = CodedOutputStream.newInstance(bytes);
    out.writeBytes(FlightData.DATA_BODY_FIELD_NUMBER, ByteString.copyFrom(first));
    out.writeBytes(FlightData.DATA_BODY_FIELD_NUMBER, ByteString.copyFrom(second));
    out.flush();
    final byte[] serialized = bytes.toByteArray();
    final MockGrpcInputStream stream = MockGrpcInputStream.direct(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      stream.close();
      assertEquals(1, stream.state.detachCount);
      assertArrayEquals(second, contents(message.getBufs().iterator().next()));
      assertEquals(serialized.length, allocator.getAllocatedMemory());
    }
    assertEquals(1, stream.state.detachedCloseCount);
  }

  private static byte[] flightData(byte[] metadata, byte[] body) {
    return FlightData.newBuilder()
        .setAppMetadata(ByteString.copyFrom(metadata))
        .setDataBody(ByteString.copyFrom(body))
        .build()
        .toByteArray();
  }

  private static byte[] payload(int size) {
    final byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) (i * 13 + 7);
    }
    return bytes;
  }

  private static byte[] contents(ArrowBuf buf) {
    final byte[] out = new byte[(int) buf.writerIndex()];
    buf.getBytes(0, out);
    return out;
  }

  /**
   * A stand-in for the stream gRPC's Netty transport hands to a marshaller: one buffer, exposed
   * through the three public capabilities, with counters for detach and close.
   */
  static final class MockGrpcInputStream extends InputStream
      implements Detachable, HasByteBuffer, KnownLength {

    static final class State {
      int detachCount;
      int detachedCloseCount;
    }

    final State state;
    private ByteBuffer buffer;
    private final boolean detached;

    /** How many bytes getByteBuffer() exposes at once; gRPC may hold a message in pieces. */
    private final int exposeLimit;

    static MockGrpcInputStream direct(byte[] bytes) {
      return directFragmented(bytes, Integer.MAX_VALUE);
    }

    static MockGrpcInputStream directFragmented(byte[] bytes, int exposeLimit) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes).flip();
      return new MockGrpcInputStream(buffer, new State(), false, exposeLimit);
    }

    static MockGrpcInputStream heap(byte[] bytes) {
      return new MockGrpcInputStream(ByteBuffer.wrap(bytes), new State(), false, Integer.MAX_VALUE);
    }

    private MockGrpcInputStream(ByteBuffer buffer, State state, boolean detached, int exposeLimit) {
      this.buffer = buffer;
      this.state = state;
      this.detached = detached;
      this.exposeLimit = exposeLimit;
    }

    @Override
    public int read() {
      return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (!buffer.hasRemaining()) {
        return -1;
      }
      final int n = Math.min(len, buffer.remaining());
      buffer.get(b, off, n);
      return n;
    }

    @Override
    public long skip(long n) {
      final int skipped = (int) Math.min(n, buffer.remaining());
      buffer.position(buffer.position() + skipped);
      return skipped;
    }

    @Override
    public int available() {
      return buffer.remaining();
    }

    @Override
    public boolean byteBufferSupported() {
      return true;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      if (!buffer.hasRemaining()) {
        return null;
      }
      final ByteBuffer view = buffer.duplicate();
      if (exposeLimit < view.remaining()) {
        view.limit(view.position() + exposeLimit);
      }
      return view;
    }

    @Override
    public InputStream detach() {
      state.detachCount++;
      final MockGrpcInputStream owner = new MockGrpcInputStream(buffer, state, true, exposeLimit);
      buffer = ByteBuffer.allocate(0);
      return owner;
    }

    @Override
    public void close() {
      if (detached) {
        state.detachedCloseCount++;
      }
      buffer = ByteBuffer.allocate(0);
    }
  }
}
