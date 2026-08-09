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

package org.apache.texera.web.service

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Future => TwitterFuture}
import io.reactivex.rxjava3.disposables.Disposable
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.workflow.{PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.CoordinatorConfig
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType,
  DebugCommandRequest => AmberDebugCommandRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceFs2Grpc
import org.apache.texera.amber.engine.common.client.AmberClient
import org.apache.texera.amber.engine.common.executionruntimestate.ExecutionConsoleStore
import org.apache.texera.web.WebsocketInput
import org.apache.texera.web.model.websocket.event.TexeraWebSocketEvent
import org.apache.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import org.apache.texera.web.model.websocket.request.python.DebugCommandRequest
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * The `ConsoleMessageProcessor` object is covered by the first half of this suite. The service
  * class around it was entirely uncovered: it owns the console diff that decides what the frontend
  * is told, the worker-to-operator keying that decides where a message lands, and the websocket
  * handlers behind the debugger.
  *
  * Everything here runs on an empty-plan AmberClient with a mocked coordinator, so no engine,
  * database or Iceberg storage is involved. The Iceberg-backed writer path (`getOrCreateWriter` and
  * the commit loop on execution state) is `private` and storage-bound, and is left uncovered rather
  * than padded with a no-throw test.
  */
class ExecutionConsoleServiceSpec
    extends TestKit(ActorSystem("ExecutionConsoleServiceSpec"))
    with AnyFlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    try TestKit.shutdownActorSystem(system)
    finally super.afterAll()
  }

  // Constants for testing
  val standardBufferSize: Int = 100
  val smallBufferSize: Int = 2
  val messageDisplayLength: Int = 100

  "processConsoleMessage" should "truncate message title when it exceeds display length" in {
    // Create a long message title that exceeds display length
    val longTitle = "a" * (messageDisplayLength + 10)
    val expectedTruncatedTitle = "a" * (messageDisplayLength - 3) + "..."

    // Create a console message with a long title
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      longTitle,
      "message content"
    )

    // Call the method under test
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Verify the title was truncated
    processedMessage.title shouldBe expectedTruncatedTitle
  }

  it should "not truncate message title when it does not exceed display length" in {
    // Create a short message title that doesn't exceed display length
    val shortTitle = "Short Title"

    // Create a console message with a short title
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      shortTitle,
      "message content"
    )

    // Call the method under test
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Verify the title was not truncated
    processedMessage.title shouldBe shortTitle
  }

  "addMessageToOperatorConsole" should "add message to buffer when buffer is not full" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create console messages
    val message1 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 1",
      "content 1"
    )

    val message2 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 2",
      "content 2"
    )

    // Add first message
    val storeWithMessage1 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        consoleStore,
        opId,
        message1,
        standardBufferSize
      )

    // Add second message
    val storeWithMessage2 = ConsoleMessageProcessor.addMessageToOperatorConsole(
      storeWithMessage1,
      opId,
      message2,
      standardBufferSize
    )

    // Verify both messages are in the buffer
    val opInfo = storeWithMessage2.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 1"
    opInfo.consoleMessages(1).title shouldBe "Message 2"
  }

  it should "remove oldest message when buffer is full" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create console messages
    val message1 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 1",
      "content 1"
    )

    val message2 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 2",
      "content 2"
    )

    val message3 = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      "Message 3",
      "content 3"
    )

    // Fill the buffer
    val storeWithMessage1 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        consoleStore,
        opId,
        message1,
        smallBufferSize
      )
    val storeWithMessage2 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        storeWithMessage1,
        opId,
        message2,
        smallBufferSize
      )

    // Add one more message which should remove the oldest
    val storeWithMessage3 =
      ConsoleMessageProcessor.addMessageToOperatorConsole(
        storeWithMessage2,
        opId,
        message3,
        smallBufferSize
      )

    // Verify the first message was removed and only the second and third remain
    val opInfo = storeWithMessage3.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 2"
    opInfo.consoleMessages(1).title shouldBe "Message 3"
  }

  "the complete message processing flow" should "handle messages correctly" in {
    // Create a test console store
    val consoleStore = new ExecutionConsoleStore()
    val opId = "op1"

    // Create a message with a title that needs truncation
    val longTitle = "a" * (messageDisplayLength + 10)
    val consoleMessage = new ConsoleMessage(
      "worker1",
      Timestamp(Instant.now),
      ConsoleMessageType.PRINT,
      "test",
      longTitle,
      "message content"
    )

    // Process the message first
    val processedMessage =
      ConsoleMessageProcessor.processConsoleMessage(consoleMessage, messageDisplayLength)

    // Then update the store
    val updatedStore = ConsoleMessageProcessor.addMessageToOperatorConsole(
      consoleStore,
      opId,
      processedMessage,
      standardBufferSize
    )

    // Verify correct processing
    val opInfo = updatedStore.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 1

    // Check that title was truncated
    val expectedTruncatedTitle = "a" * (messageDisplayLength - 3) + "..."
    opInfo.consoleMessages.head.title shouldBe expectedTruncatedTitle
  }
  // ---------------------------------------------------------------- instance

  /** Empty-plan client that captures the ConsoleMessage callback the service registers. */
  private final class TestAmberClient(
      override val coordinatorInterface: CoordinatorServiceFs2Grpc[TwitterFuture, Unit]
  ) extends AmberClient(
        system,
        new WorkflowContext(),
        PhysicalPlan(Set.empty, Set.empty),
        CoordinatorConfig(None, None, None, None),
        _ => ()
      ) {
    var consoleCallback: ConsoleMessage => Unit = _

    override def registerCallback[T](callback: T => Unit)(implicit ct: ClassTag[T]): Disposable = {
      if (ct.runtimeClass == classOf[ConsoleMessage]) {
        consoleCallback = callback.asInstanceOf[ConsoleMessage => Unit]
      }
      Disposable.empty()
    }

    def dispose(): Unit = super.shutdown()
  }

  private final class Fixture(
      val client: TestAmberClient,
      val coordinator: CoordinatorServiceFs2Grpc[TwitterFuture, Unit],
      val stateStore: ExecutionStateStore,
      val wsInput: WebsocketInput,
      val service: ExecutionConsoleService
  ) {
    def close(): Unit = {
      service.unsubscribeAll()
      client.dispose()
    }
  }

  private def fixture(): Fixture = {
    val coordinator = mock[CoordinatorServiceFs2Grpc[TwitterFuture, Unit]]
    val client = new TestAmberClient(coordinator)
    val stateStore = new ExecutionStateStore
    val wsInput = new WebsocketInput(ListBuffer.empty[Throwable] += _)
    val service = new ExecutionConsoleService(client, stateStore, wsInput, new WorkflowContext())
    new Fixture(client, coordinator, stateStore, wsInput, service)
  }

  private def message(
      workerId: String = "Worker:WF1-udf1-main-0",
      title: String = "hello",
      msgType: ConsoleMessageType = ConsoleMessageType.PRINT
  ): ConsoleMessage =
    new ConsoleMessage(workerId, Timestamp(Instant.now), msgType, "src", title, "content")

  private def withFixture(body: Fixture => Unit): Unit = {
    val f = fixture()
    try body(f)
    finally f.close()
  }

  "processConsoleMessage" should "leave a debugger message untouched however long it is" in {
    // The debugger's output is the payload the user asked to see; truncating it would cut off the
    // frame or variable they are inspecting.
    withFixture { f =>
      val long = "a" * (f.service.consoleMessageDisplayLength + 50)

      val processed = f.service.processConsoleMessage(
        message(title = long, msgType = ConsoleMessageType.DEBUGGER)
      )

      processed.title shouldBe long
    }
  }

  it should "still truncate an ordinary message to the configured length" in {
    withFixture { f =>
      val long = "a" * (f.service.consoleMessageDisplayLength + 50)

      val processed = f.service.processConsoleMessage(message(title = long))

      processed.title.length shouldBe f.service.consoleMessageDisplayLength
      processed.title should endWith("...")
    }
  }

  "the console diff handler" should "report only the messages added since the last state" in {
    // The frontend appends what it is sent. Emitting the whole buffer instead of the delta would
    // duplicate every earlier line on each update.
    withFixture { f =>
      // One batch is published per state update. Subscribing up front and reading the batch for the
      // SECOND message is what shows the delta: the first message must not appear in it again.
      val batches = ListBuffer.empty[Iterable[TexeraWebSocketEvent]]
      val sub = f.stateStore.consoleStore.getWebsocketEventObservable
        .subscribe((batch: Iterable[TexeraWebSocketEvent]) => batches += batch)

      try {
        f.client.consoleCallback(message(title = "first"))
        f.client.consoleCallback(message(title = "second"))
      } finally sub.dispose()

      val titles =
        batches.last.collect { case e: ConsoleUpdateEvent => e.messages.map(_.title) }.flatten
      titles shouldBe Seq("second")
    }
  }

  "the console message callback" should "file a message under the logical operator id" in {
    // The worker id carries the physical layer and worker index; the frontend console is keyed by
    // the logical operator, so anything else silently strands the output.
    withFixture { f =>
      f.client.consoleCallback(message(workerId = "Worker:WF1-udf1-main-0"))

      f.stateStore.consoleStore.getState.operatorConsole.keys should contain("udf1")
    }
  }

  it should "store the truncated form, not the original" in {
    withFixture { f =>
      val long = "b" * (f.service.consoleMessageDisplayLength + 50)

      f.client.consoleCallback(message(title = long))

      val stored = f.stateStore.consoleStore.getState.operatorConsole("udf1").consoleMessages
      stored.map(_.title.length) shouldBe Seq(f.service.consoleMessageDisplayLength)
    }
  }

  "a debug command" should "be attributed to the user that issued it" in {
    withFixture { f =>
      (f.coordinator.debugCommand _)
        .expects(AmberDebugCommandRequest("Worker:WF1-udf1-main-0", "break 12"), ())
        .returning(TwitterFuture.value(EmptyReturn()))

      f.wsInput.onNext(
        DebugCommandRequest("udf1", "Worker:WF1-udf1-main-0", "break 12"),
        Some(7)
      )

      val stored = f.stateStore.consoleStore.getState.operatorConsole("udf1").consoleMessages
      stored.map(_.source) shouldBe Seq("USER-7")
      stored.map(_.title) shouldBe Seq("break 12")
    }
  }

  it should "fall back to UNKNOWN when there is no session user" in {
    withFixture { f =>
      (f.coordinator.debugCommand _)
        .expects(*, *)
        .returning(TwitterFuture.value(EmptyReturn()))

      f.wsInput.onNext(DebugCommandRequest("udf1", "Worker:WF1-udf1-main-0", "cont"), None)

      f.stateStore.consoleStore.getState
        .operatorConsole("udf1")
        .consoleMessages
        .map(_.source) shouldBe Seq("USER-UNKNOWN")
    }
  }

  it should "file the command under the operator, not the worker" in {
    // req carries both; keying by workerId would scatter the command across per-worker consoles.
    withFixture { f =>
      (f.coordinator.debugCommand _).expects(*, *).returning(TwitterFuture.value(EmptyReturn()))

      f.wsInput.onNext(DebugCommandRequest("udf1", "Worker:WF1-udf1-main-0", "cont"), Some(1))

      val keys = f.stateStore.consoleStore.getState.operatorConsole.keys
      keys should contain("udf1")
      keys should not contain "Worker:WF1-udf1-main-0"
    }
  }
}
