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

import com.google.common.collect.Iterables;
import com.google.protobuf.ByteString;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.WireFormat;
import io.grpc.Detachable;
import io.grpc.HasByteBuffer;
import io.grpc.KnownLength;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import org.apache.arrow.flight.impl.Flight.FlightData;
import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests the detachable ArrowMessage read path. */
public class TestArrowMessageDetachable {
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
  public void testContiguousDirectBufferIsDetached() throws Exception {
    final byte[] body = payload(64);
    final byte[] serialized = serializeBody(body);
    final MockGrpcInputStream stream = MockGrpcInputStream.direct(serialized);

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertEquals(1, stream.state.detachCount);
      assertEquals(serialized.length, allocator.getAllocatedMemory());
      assertBodyEquals(message, body);
      assertEquals(0, stream.state.closeCount);
    }
    assertEquals(1, stream.state.closeCount);
    assertEquals(0, allocator.getAllocatedMemory());
    stream.close();
  }

  @Test
  public void testHeapBufferFallsBackWithoutDetaching() throws Exception {
    final byte[] body = payload(32);
    final MockGrpcInputStream stream = MockGrpcInputStream.heap(serializeBody(body));

    try (ArrowMessage message = ArrowMessage.createMarshaller(allocator).parse(stream)) {
      assertEquals(0, stream.state.detachCount);
      assertBodyEquals(message, body);
    }
    assertEquals(0, allocator.getAllocatedMemory());
    stream.close();
    assertEquals(1, stream.state.closeCount);
  }

  @Test
  public void testDetachedBufferIsClosedOnParseFailure() throws Exception {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final CodedOutputStream coded = CodedOutputStream.newInstance(output);
    coded.writeBytes(FlightData.DATA_BODY_FIELD_NUMBER, ByteString.copyFrom(payload(8)));
    coded.writeTag(
        FlightData.FLIGHT_DESCRIPTOR_FIELD_NUMBER, WireFormat.WIRETYPE_LENGTH_DELIMITED);
    coded.writeUInt32NoTag(10);
    coded.writeRawByte(1);
    coded.flush();
    final MockGrpcInputStream stream = MockGrpcInputStream.direct(output.toByteArray());

    assertThrows(
        RuntimeException.class, () -> ArrowMessage.createMarshaller(allocator).parse(stream));
    assertEquals(1, stream.state.detachCount);
    assertEquals(1, stream.state.closeCount);
    assertEquals(0, allocator.getAllocatedMemory());
    stream.close();
  }

  private static byte[] serializeBody(byte[] body) {
    return FlightData.newBuilder().setDataBody(ByteString.copyFrom(body)).build().toByteArray();
  }

  private static byte[] payload(int size) {
    final byte[] bytes = new byte[size];
    for (int i = 0; i < size; i++) {
      bytes[i] = (byte) i;
    }
    return bytes;
  }

  private static void assertBodyEquals(ArrowMessage message, byte[] expected) {
    final ArrowBuf body = Iterables.getOnlyElement(message.getBufs());
    final byte[] actual = new byte[expected.length];
    body.getBytes(0, actual);
    assertArrayEquals(expected, actual);
  }

  private static final class MockGrpcInputStream extends InputStream
      implements Detachable, HasByteBuffer, KnownLength {
    private final State state;
    private ByteBuffer buffer;
    private boolean ownsBuffer = true;
    private boolean closed;

    private MockGrpcInputStream(ByteBuffer buffer, State state) {
      this.buffer = buffer;
      this.state = state;
    }

    private static MockGrpcInputStream direct(byte[] bytes) {
      final ByteBuffer buffer = ByteBuffer.allocateDirect(bytes.length);
      buffer.put(bytes).flip();
      return new MockGrpcInputStream(buffer, new State());
    }

    private static MockGrpcInputStream heap(byte[] bytes) {
      return new MockGrpcInputStream(ByteBuffer.wrap(bytes), new State());
    }

    @Override
    public boolean byteBufferSupported() {
      return true;
    }

    @Override
    public ByteBuffer getByteBuffer() {
      return buffer.hasRemaining() ? buffer.duplicate() : null;
    }

    @Override
    public InputStream detach() {
      state.detachCount++;
      final ByteBuffer detached = buffer;
      buffer = ByteBuffer.allocate(0);
      ownsBuffer = false;
      return new MockGrpcInputStream(detached, state);
    }

    @Override
    public int available() {
      return buffer.remaining();
    }

    @Override
    public int read() {
      return buffer.hasRemaining() ? buffer.get() & 0xFF : -1;
    }

    @Override
    public int read(byte[] bytes, int offset, int size) {
      if (!buffer.hasRemaining()) {
        return -1;
      }
      final int read = Math.min(size, buffer.remaining());
      buffer.get(bytes, offset, read);
      return read;
    }

    @Override
    public long skip(long size) {
      final int skipped = (int) Math.min(Math.max(size, 0), buffer.remaining());
      buffer.position(buffer.position() + skipped);
      return skipped;
    }

    @Override
    public void close() throws IOException {
      if (!closed) {
        closed = true;
        buffer = ByteBuffer.allocate(0);
        if (ownsBuffer) {
          ownsBuffer = false;
          state.closeCount++;
        }
      }
    }
  }

  private static final class State {
    private int detachCount;
    private int closeCount;
  }
}
