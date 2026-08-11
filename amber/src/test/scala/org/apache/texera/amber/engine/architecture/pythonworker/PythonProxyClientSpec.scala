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

import com.twitter.util.Promise
import org.apache.arrow.flight.{
  Action,
  FlightProducer,
  FlightServer,
  FlightStream,
  Location,
  NoOpFlightProducer,
  PutResult,
  Result
}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VarBinaryVector
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.state.State
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.pythonworker.WorkerBatchInternalQueue.{
  DataElement,
  EmbeddedControlMessageElement
}
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
import org.apache.texera.amber.engine.common.actormessage.{CreditUpdate, PythonActorMessage}
import org.apache.texera.amber.engine.common.ambermessage.{
  DataFrame,
  DirectControlMessagePayloadV2,
  PythonControlMessage,
  PythonDataHeader,
  StateFrame
}
import org.apache.texera.amber.util.ArrowUtils
import org.scalatest.flatspec.AnyFlatSpec

import java.net.ServerSocket
import java.nio.charset.StandardCharsets
import java.util.concurrent.ConcurrentLinkedQueue
import scala.jdk.CollectionConverters._

/**
  * Exercises PythonProxyClient without a Python process: a Scala
  * NoOpFlightProducer stands in for the Python worker's Flight server
  * (`network_receiver.py`), speaking the same action/put protocol — heartbeat
  * acks, per-action queue-size replies, and per-put credit metadata.
  */
class PythonProxyClientSpec extends AnyFlatSpec {

  private val clientWorkerId = ActorVirtualIdentity("python-proxy-client")
  private val controllerId = ActorVirtualIdentity("CONTROLLER")
  private val upstreamChannel =
    ChannelIdentity(controllerId, clientWorkerId, isControl = true)

  private def freePort: Int = {
    val socket = new ServerSocket(0)
    try socket.getLocalPort
    finally socket.close()
  }

  private def quietly(f: => Unit): Unit =
    try f
    catch { case _: Exception => () }

  private def awaitTrue(timeoutMs: Long = 10000)(cond: => Boolean): Unit = {
    val deadline = System.currentTimeMillis() + timeoutMs
    while (!cond && System.currentTimeMillis() < deadline) Thread.sleep(20)
    assert(cond, s"condition not met within ${timeoutMs}ms")
  }

  // ---------------------------------------------------------------------------
  // Stand-in for the Python worker's Flight server: records every action and
  // put it receives and acks them the way network_receiver.py does.
  // ---------------------------------------------------------------------------

  private case class RecordedPut(
      header: PythonDataHeader,
      tuples: Vector[Tuple],
      ecmBytes: Option[Array[Byte]]
  )

  private class FakePythonWorkerServer(heartbeatBody: String) extends NoOpFlightProducer {
    val actionsReceived = new ConcurrentLinkedQueue[(String, Array[Byte])]()
    val putsReceived = new ConcurrentLinkedQueue[RecordedPut]()
    @volatile var reportedQueueSize: Long = 0L

    override def doAction(
        context: FlightProducer.CallContext,
        action: Action,
        listener: FlightProducer.StreamListener[Result]
    ): Unit = {
      actionsReceived.add((action.getType, action.getBody))
      action.getType match {
        case "heartbeat" =>
          listener.onNext(new Result(heartbeatBody.getBytes(StandardCharsets.UTF_8)))
          listener.onCompleted()
        case "control" | "actor" =>
          // The ack body is the Python worker's queue size, sent as a decimal
          // string; the client parses it for flow control.
          listener.onNext(
            new Result(reportedQueueSize.toString.getBytes(StandardCharsets.UTF_8))
          )
          listener.onCompleted()
        case _ => // "shutdown": the client does not consume a reply
          listener.onCompleted()
      }
    }

    override def acceptPut(
        context: FlightProducer.CallContext,
        flightStream: FlightStream,
        ackStream: FlightProducer.StreamListener[PutResult]
    ): Runnable = { () =>
      {
        val header = PythonDataHeader.parseFrom(flightStream.getDescriptor.getCommand)
        val tuples = Vector.newBuilder[Tuple]
        var ecmBytes: Option[Array[Byte]] = None
        while (flightStream.next()) {
          val root = flightStream.getRoot
          if (header.payloadType == "ECM") {
            ecmBytes = Some(root.getVector("payload").asInstanceOf[VarBinaryVector].get(0))
          } else {
            (0 until root.getRowCount).foreach(i => tuples += ArrowUtils.getTexeraTuple(i, root))
          }
        }
        putsReceived.add(RecordedPut(header, tuples.result(), ecmBytes))
        // Ack with the queue size as put metadata, the way the Python side does.
        val ackAllocator = new RootAllocator(1024)
        try {
          val buf = ackAllocator.buffer(java.lang.Long.BYTES)
          buf.writeLong(reportedQueueSize)
          ackStream.onNext(PutResult.metadata(buf))
          buf.close()
        } finally {
          ackAllocator.close()
        }
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Test harness — fake server + a PythonProxyClient main loop on its own thread
  // ---------------------------------------------------------------------------

  private class ClientFixture(heartbeatBody: String = "ack") {
    val producer = new FakePythonWorkerServer(heartbeatBody)
    val serverAllocator = new RootAllocator()
    val port: Int = freePort
    val server: FlightServer = FlightServer
      .builder(serverAllocator, Location.forGrpcInsecure("localhost", port), producer)
      .build()
    server.start()
    val portPromise: Promise[Int] = Promise[Int]()
    portPromise.setValue(port)
    val client = new PythonProxyClient(portPromise, clientWorkerId)
    private val loopThread = new Thread(() => client.run(), "python-proxy-client-test-loop")
    loopThread.setDaemon(true)
    // The tear-down interrupt below surfaces as an InterruptedException out of
    // the blocking queue take; keep it off stderr.
    loopThread.setUncaughtExceptionHandler((_, _) => ())

    def start(): Unit = loopThread.start()

    def close(): Unit = {
      quietly(client.close())
      loopThread.interrupt() // unblock the main loop's queue take
      loopThread.join(5000)
      quietly(server.close())
      quietly(serverAllocator.close())
    }
  }

  private def withClient(heartbeatBody: String = "ack")(test: ClientFixture => Unit): Unit = {
    val fixture = new ClientFixture(heartbeatBody)
    try test(fixture)
    finally fixture.close()
  }

  // ---------------------------------------------------------------------------
  // connection + control path
  // ---------------------------------------------------------------------------

  "PythonProxyClient" should
    "connect via heartbeat and forward an enqueued ControlInvocation as a control action" in {
    withClient() { fixture =>
      fixture.start()
      val invocation = ControlInvocation(
        "OpenExecutor",
        EmptyRequest(),
        AsyncRPCContext(controllerId, clientWorkerId),
        5L
      )
      fixture.client.enqueueCommand(invocation, upstreamChannel)

      awaitTrue()(fixture.producer.actionsReceived.asScala.exists(_._1 == "control"))
      assert(
        fixture.producer.actionsReceived.asScala.head._1 == "heartbeat",
        "the client must handshake before draining its queue"
      )
      val body = fixture.producer.actionsReceived.asScala.find(_._1 == "control").get._2
      val parsed = PythonControlMessage.parseFrom(body)
      assert(parsed.tag == upstreamChannel)
      assert(
        parsed.payload ==
          DirectControlMessagePayloadV2.defaultInstance.withControlInvocation(invocation)
      )
    }
  }

  it should "forward an enqueued ReturnInvocation as a control action" in {
    withClient() { fixture =>
      fixture.start()
      val reply = ReturnInvocation(42L, EmptyReturn())
      fixture.client.enqueueCommand(reply, upstreamChannel)

      awaitTrue()(fixture.producer.actionsReceived.asScala.exists(_._1 == "control"))
      val body = fixture.producer.actionsReceived.asScala.find(_._1 == "control").get._2
      val parsed = PythonControlMessage.parseFrom(body)
      assert(parsed.tag == upstreamChannel)
      assert(
        parsed.payload ==
          DirectControlMessagePayloadV2.defaultInstance.withReturnInvocation(reply)
      )
    }
  }

  it should "forward an enqueued actor command as an actor action" in {
    withClient() { fixture =>
      fixture.start()
      fixture.client.enqueueActorCommand(CreditUpdate())

      awaitTrue()(fixture.producer.actionsReceived.asScala.exists(_._1 == "actor"))
      val body = fixture.producer.actionsReceived.asScala.find(_._1 == "actor").get._2
      assert(PythonActorMessage.parseFrom(body) == PythonActorMessage(CreditUpdate()))
    }
  }

  it should "update its Python-queue credit from the action ack" in {
    withClient() { fixture =>
      fixture.producer.reportedQueueSize = 7L
      fixture.start()
      assert(fixture.client.getQueuedCredit == 0L)

      fixture.client.enqueueActorCommand(CreditUpdate())
      awaitTrue()(fixture.client.getQueuedCredit == 7L)
    }
  }

  // ---------------------------------------------------------------------------
  // data path — Data / State / ECM puts
  // ---------------------------------------------------------------------------

  it should "stream an enqueued DataFrame to the server under a Data header" in {
    withClient() { fixture =>
      fixture.start()
      val schema = Schema()
        .add(new Attribute("v", AttributeType.INTEGER))
        .add(new Attribute("s", AttributeType.STRING))
      def tuple(v: Int, s: String): Tuple =
        Tuple.builder(schema).addSequentially(Array(Int.box(v), s)).build()
      val tuples = Array(tuple(1, "a"), tuple(2, "b"))
      val dataChannel = ChannelIdentity(clientWorkerId, controllerId, isControl = false)

      fixture.client.enqueueData(DataElement(DataFrame(tuples), dataChannel))

      awaitTrue()(!fixture.producer.putsReceived.isEmpty)
      val put = fixture.producer.putsReceived.peek()
      assert(put.header == PythonDataHeader(dataChannel, "Data"))
      assert(put.tuples == tuples.toVector)
    }
  }

  it should "stream an enqueued StateFrame as a single State row carrying the loop envelope" in {
    withClient() { fixture =>
      fixture.start()
      val state = State(Map("count" -> "5"))
      val dataChannel = ChannelIdentity(clientWorkerId, controllerId, isControl = false)

      fixture.client.enqueueData(
        DataElement(StateFrame(state, loopCounter = 3L, loopStartId = "loop-start-1"), dataChannel)
      )

      awaitTrue()(!fixture.producer.putsReceived.isEmpty)
      val put = fixture.producer.putsReceived.peek()
      assert(put.header == PythonDataHeader(dataChannel, "State"))
      assert(put.tuples.size == 1)
      val row = put.tuples.head
      assert(State.fromTuple(row) == state)
      assert(State.loopCounterFrom(row) == 3L)
      assert(State.loopStartIdFrom(row) == "loop-start-1")
    }
  }

  it should "stream an enqueued EmbeddedControlMessage as a serialized ECM put" in {
    withClient() { fixture =>
      fixture.start()
      val ecm = EmbeddedControlMessage(
        EmbeddedControlMessageIdentity("ecm-1"),
        EmbeddedControlMessageType.NO_ALIGNMENT,
        Seq.empty,
        Map.empty
      )
      val dataChannel = ChannelIdentity(clientWorkerId, controllerId, isControl = false)

      fixture.client.enqueueData(EmbeddedControlMessageElement(ecm, dataChannel))

      awaitTrue()(!fixture.producer.putsReceived.isEmpty)
      val put = fixture.producer.putsReceived.peek()
      assert(put.header == PythonDataHeader(dataChannel, "ECM"))
      assert(put.ecmBytes.isDefined)
      assert(EmbeddedControlMessage.parseFrom(put.ecmBytes.get) == ecm)
    }
  }

  // ---------------------------------------------------------------------------
  // failure paths
  // ---------------------------------------------------------------------------

  it should "abort with WorkflowRuntimeException once connection retries are exhausted" in {
    // A free port with nothing listening: every connection attempt is refused.
    val portPromise = Promise[Int]()
    portPromise.setValue(freePort)
    val client = new PythonProxyClient(portPromise, ActorVirtualIdentity("no-server"))
    assertThrows[WorkflowRuntimeException] {
      client.run()
    }
  }

  it should "abort when the server heartbeat does not reply ack" in {
    withClient(heartbeatBody = "nak") { fixture =>
      // run() on the test thread: it must give up after the retry budget
      // because the heartbeat body check fails on every attempt.
      assertThrows[WorkflowRuntimeException] {
        fixture.client.run()
      }
    }
  }

  it should "tolerate close() before any connection is established" in {
    val portPromise = Promise[Int]()
    portPromise.setValue(freePort)
    val client = new PythonProxyClient(portPromise, ActorVirtualIdentity("never-connected"))
    // Must not throw: the internal null flight client is handled.
    client.close()
    succeed
  }
}
