/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.engine.architecture.pythonworker

import com.twitter.util.{Await, Duration, Promise}
import org.apache.arrow.flight.{Action, FlightClient, FlightDescriptor, Location, SyncPutListener}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema => ArrowSchema}
import org.apache.arrow.vector.{VarBinaryVector, VectorSchemaRoot}
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.messaginglayer.NetworkOutputGateway
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  EmbeddedControlMessage,
  EmbeddedControlMessageType,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  DirectControlMessagePayloadV2,
  PythonControlMessage,
  PythonDataHeader,
  StateFrame,
  WorkflowFIFOMessage
}
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import scala.jdk.CollectionConverters._

/**
  * Exercises the JVM-side Flight server of the Python worker bridge without a
  * Python process: the test plays the Python worker's part with a plain Arrow
  * FlightClient and asserts on what the server forwards to its
  * NetworkOutputGateway. This is the JVM half of the protocol implemented by
  * `network_sender.py` / `network_receiver.py`.
  */
class PythonProxyServerSpec extends AnyFlatSpec {

  private val jvmWorkerId = ActorVirtualIdentity("jvm-proxy-server")
  private val pythonWorkerId = ActorVirtualIdentity("python-worker-1")
  private val controllerId = ActorVirtualIdentity("CONTROLLER")

  // ---------------------------------------------------------------------------
  // Test harness — a running PythonProxyServer plus a FlightClient connected to
  // it, with every message the server emits captured from the output gateway.
  // ---------------------------------------------------------------------------

  private class ServerFixture {
    val sentMessages = new ConcurrentLinkedQueue[WorkflowFIFOMessage]()
    val gateway = new NetworkOutputGateway(jvmWorkerId, msg => { sentMessages.add(msg); () })
    val portPromise: Promise[Int] = Promise[Int]()
    val server = new PythonProxyServer(gateway, jvmWorkerId, portPromise)
    server.run() // FlightServer.start() binds and returns; gRPC threads serve
    val clientAllocator = new RootAllocator()
    val flightClient: FlightClient = FlightClient
      .builder(
        clientAllocator,
        Location.forGrpcInsecure("localhost", server.getPortNumber.get())
      )
      .build()

    def messages: List[WorkflowFIFOMessage] = sentMessages.asScala.toList

    def close(): Unit = {
      quietly(flightClient.close())
      quietly(server.close())
      quietly(clientAllocator.close())
    }
  }

  private def quietly(f: => Unit): Unit =
    try f
    catch { case _: Exception => () }

  private def withServer(test: ServerFixture => Unit): Unit = {
    val fixture = new ServerFixture
    try test(fixture)
    finally fixture.close()
  }

  private def awaitTrue(timeoutMs: Long = 10000)(cond: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!cond && System.currentTimeMillis() < deadline) Thread.sleep(20)
    assert(cond, s"condition not met within ${timeoutMs}ms")
  }

  /** Mirror of PythonProxyClient.writeArrowStream: put one batch of rows. */
  private def putBatch(
      fixture: ServerFixture,
      header: PythonDataHeader,
      arrowSchema: ArrowSchema,
      fillRoot: VectorSchemaRoot => Unit
  ): Long = {
    val descriptor = FlightDescriptor.command(header.toByteArray)
    val listener = new SyncPutListener
    val root = VectorSchemaRoot.create(arrowSchema, fixture.clientAllocator)
    try {
      val writer = fixture.flightClient.startPut(descriptor, root, listener)
      root.allocateNew()
      fillRoot(root)
      writer.putNext()
      root.clear()
      writer.completed()
      val ack = listener.poll(5, TimeUnit.SECONDS)
      assert(ack != null, "no credit ack received for the put within 5s")
      val ackBuf = ack.getApplicationMetadata
      val credit = ackBuf.getLong(0)
      ackBuf.close()
      listener.close()
      credit
    } finally {
      root.close()
    }
  }

  // ---------------------------------------------------------------------------
  // Port + handshake
  // ---------------------------------------------------------------------------

  "PythonProxyServer" should "bind a free local port exposed via getPortNumber" in {
    withServer { fixture =>
      assert(fixture.server.getPortNumber.get() > 0)
    }
  }

  it should "complete the port-number promise and reply ok on a handshake action" in {
    withServer { fixture =>
      val result = fixture.flightClient
        .doAction(new Action("handshake", "45678".getBytes(StandardCharsets.UTF_8)))
        .next()
      assert(new String(result.getBody, StandardCharsets.UTF_8) == "ok")
      assert(Await.result(fixture.portPromise, Duration.fromSeconds(5)) == 45678)
    }
  }

  // ---------------------------------------------------------------------------
  // control actions — routed to the gateway on the control channel, acked with
  // a little-endian credit value
  // ---------------------------------------------------------------------------

  it should "route a control action carrying a ControlInvocation to the tag's receiver" in {
    withServer { fixture =>
      val tag = ChannelIdentity(pythonWorkerId, controllerId, isControl = true)
      val invocation = ControlInvocation(
        "StartWorker",
        EmptyRequest(),
        AsyncRPCContext(pythonWorkerId, controllerId),
        3L
      )
      val controlMessage = PythonControlMessage(
        tag,
        DirectControlMessagePayloadV2.defaultInstance.withControlInvocation(invocation)
      )

      val results = fixture.flightClient.doAction(new Action("control", controlMessage.toByteArray))
      val credit = ByteBuffer.wrap(results.next().getBody).order(ByteOrder.LITTLE_ENDIAN).getLong
      assert(credit == 30L, "the action ack must carry the credit value little-endian")
      assert(!results.hasNext, "a control action must produce exactly one result")

      val out = fixture.messages
      assert(out.size == 1)
      // The gateway sends on ITS control channel to the tag's receiver.
      assert(out.head.channelId == ChannelIdentity(jvmWorkerId, controllerId, isControl = true))
      assert(out.head.payload == invocation)
    }
  }

  it should "route a control action carrying a ReturnInvocation to the tag's receiver" in {
    withServer { fixture =>
      val tag = ChannelIdentity(pythonWorkerId, controllerId, isControl = true)
      val reply = ReturnInvocation(42L, EmptyReturn())
      val controlMessage = PythonControlMessage(
        tag,
        DirectControlMessagePayloadV2.defaultInstance.withReturnInvocation(reply)
      )

      val results = fixture.flightClient.doAction(new Action("control", controlMessage.toByteArray))
      results.next()

      val out = fixture.messages
      assert(out.size == 1)
      assert(out.head.channelId == ChannelIdentity(jvmWorkerId, controllerId, isControl = true))
      assert(out.head.payload == reply)
    }
  }

  // ---------------------------------------------------------------------------
  // puts — data / state / ECM payload types, acked with a credit value
  // ---------------------------------------------------------------------------

  it should "deliver a Data put as a DataFrame on the header's channel and ack with credits" in {
    withServer { fixture =>
      val to =
        ChannelIdentity(pythonWorkerId, ActorVirtualIdentity("downstream"), isControl = false)
      val schema = Schema()
        .add(new Attribute("v", AttributeType.INTEGER))
        .add(new Attribute("s", AttributeType.STRING))
      def tuple(v: Int, s: String): Tuple =
        Tuple.builder(schema).addSequentially(Array(Int.box(v), s)).build()
      val tuples = Array(tuple(1, "a"), tuple(2, "b"))

      val credit = putBatch(
        fixture,
        PythonDataHeader(to, "Data"),
        ArrowUtils.fromTexeraSchema(schema),
        root => tuples.foreach(t => ArrowUtils.appendTexeraTuple(t, root))
      )
      assert(credit == 31L, "the put ack must carry the credit value")

      awaitTrue()(fixture.messages.nonEmpty)
      val out = fixture.messages
      assert(out.size == 1)
      assert(out.head.channelId == to)
      assert(out.head.payload == DataFrame(tuples))
    }
  }

  it should "reassemble a single-row State put into a StateFrame carrying the loop envelope" in {
    withServer { fixture =>
      val to =
        ChannelIdentity(pythonWorkerId, ActorVirtualIdentity("downstream"), isControl = false)
      val state = State(Map("count" -> "5"))
      val row = state.toTuple(loopCounter = 3L, loopStartId = "loop-start-1")

      putBatch(
        fixture,
        PythonDataHeader(to, "State"),
        ArrowUtils.fromTexeraSchema(State.schema),
        root => ArrowUtils.appendTexeraTuple(row, root)
      )

      awaitTrue()(fixture.messages.nonEmpty)
      val out = fixture.messages
      assert(out.size == 1)
      assert(out.head.channelId == to)
      assert(out.head.payload == StateFrame(state, 3L, "loop-start-1"))
    }
  }

  it should "parse an ECM put back into the original EmbeddedControlMessage" in {
    withServer { fixture =>
      val to =
        ChannelIdentity(pythonWorkerId, ActorVirtualIdentity("downstream"), isControl = false)
      val ecm = EmbeddedControlMessage(
        EmbeddedControlMessageIdentity("ecm-1"),
        EmbeddedControlMessageType.NO_ALIGNMENT,
        Seq.empty,
        Map.empty
      )
      // Mirror of PythonProxyClient.sendECM: one VarBinary "payload" column,
      // one row holding the serialized ECM.
      val field = new Field("payload", FieldType.nullable(new ArrowType.Binary), null)
      val arrowSchema = new ArrowSchema(List(field).asJava)

      putBatch(
        fixture,
        PythonDataHeader(to, "ECM"),
        arrowSchema,
        root => {
          val vector = root.getVector("payload").asInstanceOf[VarBinaryVector]
          vector.setSafe(0, ecm.toByteArray)
          vector.setValueCount(1)
          root.setRowCount(1)
        }
      )

      awaitTrue()(fixture.messages.nonEmpty)
      val out = fixture.messages
      assert(out.size == 1)
      assert(out.head.channelId == to)
      assert(out.head.payload == ecm)
    }
  }
}
