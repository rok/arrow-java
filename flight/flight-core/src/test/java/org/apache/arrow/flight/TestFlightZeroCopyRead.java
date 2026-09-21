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

import static org.apache.arrow.flight.FlightTestUtil.LOCALHOST;
import static org.apache.arrow.flight.Location.forGrpcInsecure;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.common.io.ByteStreams;
import io.grpc.Drainable;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.ipc.message.IpcOption;
import org.junit.jupiter.api.Test;

/**
 * Proves the zero-copy read path is taken over a real gRPC transport, not only against a mock.
 *
 * <p>The oracle is allocator accounting. When the client takes ownership of gRPC's buffer, it is
 * charged for the whole wire frame (header, tags and body). When it copies, it is charged for a
 * buffer of the body's size only. The two are never equal, so the number distinguishes the paths.
 */
public class TestFlightZeroCopyRead {

  private static final int ROWS = 1000;

  @Test
  public void recordBatchIsReadWithoutCopyingOverRealTransport() throws Exception {
    assertTrue(ArrowMessage.ENABLE_ZERO_COPY_READ, "zero-copy reads must be enabled for this test");
    try (BufferAllocator serverAllocator = new RootAllocator(Long.MAX_VALUE);
        BufferAllocator clientAllocator = new RootAllocator(Long.MAX_VALUE);
        FlightServer server =
            FlightServer.builder(
                    serverAllocator,
                    forGrpcInsecure(LOCALHOST, 0),
                    new OneBatchProducer(serverAllocator))
                .build()
                .start();
        FlightClient client = FlightClient.builder(clientAllocator, server.getLocation()).build()) {
      final long wireSize = wireSizeOfTheBatch(clientAllocator);
      assertEquals(0, clientAllocator.getAllocatedMemory());

      try (FlightStream stream = client.getStream(new Ticket(new byte[0]))) {
        assertTrue(stream.next());
        final VectorSchemaRoot root = stream.getRoot();
        final IntVector values = (IntVector) root.getVector("c1");
        assertEquals(ROWS, root.getRowCount());
        for (int i = 0; i < ROWS; i++) {
          assertEquals(i * 3, values.get(i));
        }
        assertEquals(
            wireSize,
            clientAllocator.getAllocatedMemory(),
            "the client should be holding gRPC's whole frame, not a copy of the body");
        assertFalse(stream.next());
      }
      assertEquals(0, clientAllocator.getAllocatedMemory(), "gRPC's buffer must be released");
    }
  }

  /** The exact number of bytes the server puts on the wire for the batch the producer sends. */
  private static long wireSizeOfTheBatch(BufferAllocator allocator) throws Exception {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    // Same ownership convention as the server: the batch's buffer references belong to the
    // message, which is closed once the wire stream is done with them.
    try (VectorSchemaRoot root = theBatch(allocator);
        ArrowMessage message =
            new ArrowMessage(
                new VectorUnloader(root).getRecordBatch(), null, false, IpcOption.DEFAULT);
        InputStream wire = ArrowMessage.createMarshaller(allocator).stream(message)) {
      if (wire instanceof Drainable) {
        ((Drainable) wire).drainTo(bytes);
      } else {
        ByteStreams.copy(wire, bytes);
      }
    }
    return bytes.size();
  }

  private static VectorSchemaRoot theBatch(BufferAllocator allocator) {
    final IntVector values = new IntVector("c1", allocator);
    final VectorSchemaRoot root = VectorSchemaRoot.of(values);
    root.allocateNew();
    for (int i = 0; i < ROWS; i++) {
      values.set(i, i * 3);
    }
    values.setValueCount(ROWS);
    root.setRowCount(ROWS);
    return root;
  }

  private static final class OneBatchProducer extends NoOpFlightProducer {
    private final BufferAllocator allocator;

    OneBatchProducer(BufferAllocator allocator) {
      this.allocator = allocator;
    }

    @Override
    public void getStream(CallContext context, Ticket ticket, ServerStreamListener listener) {
      try (VectorSchemaRoot root = theBatch(allocator)) {
        listener.start(root);
        listener.putNext();
        listener.completed();
      }
    }
  }
}
