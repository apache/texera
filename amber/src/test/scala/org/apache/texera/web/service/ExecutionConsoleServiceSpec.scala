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
import org.apache.pekko.actor.ActorSystem
import org.apache.pekko.testkit.TestKit
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  ConsoleMessage,
  ConsoleMessageType,
  DebugCommandRequest => AmberDebugCommandRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  EvaluatedValue,
  TypedValue
}
import org.apache.texera.amber.engine.architecture.rpc.coordinatorservice.CoordinatorServiceFs2Grpc
import org.apache.texera.amber.engine.common.executionruntimestate.{
  EvaluatedValueList,
  ExecutionConsoleStore,
  OperatorConsole
}
import org.apache.texera.web.WebsocketInput
import org.apache.texera.web.model.websocket.event.python.ConsoleUpdateEvent
import org.apache.texera.web.model.websocket.request.python.DebugCommandRequest
import org.apache.texera.web.model.websocket.response.python.PythonExpressionEvaluateResponse
import org.apache.texera.web.service.RecordingExecutionClient.recordBatches
import org.apache.texera.web.storage.ExecutionStateStore
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.concurrent.PatienceConfiguration.{Interval, Timeout}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Millis, Span}

import java.time.Instant
import java.util.concurrent.ExecutorService
import scala.collection.mutable.ListBuffer

/**
  * The `ConsoleMessageProcessor` object is covered by the first half of this suite. The service
  * class around it was entirely uncovered: it owns the console diff that decides what the frontend
  * is told, the worker-to-operator keying that decides where a message lands, and the websocket
  * handlers behind the debugger.
  *
  * Everything here runs on an empty-plan AmberClient with a mocked coordinator, so no engine is
  * involved. Note that ExecutionConsoleService still schedules asynchronous console persistence
  * (writer creation + operator-executions insertion); this suite focuses on console routing/diffing
  * and does not assert on the persistence side effects.
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

  private def message(
      workerId: String = "Worker:WF1-udf1-main-0",
      title: String = "hello",
      msgType: ConsoleMessageType = ConsoleMessageType.PRINT,
      source: String = "src",
      content: String = "content"
  ): ConsoleMessage =
    new ConsoleMessage(workerId, Timestamp(Instant.now), msgType, source, title, content)

  private def addMessages(bufferSize: Int, titles: String*): ExecutionConsoleStore =
    titles.zipWithIndex.foldLeft(new ExecutionConsoleStore()) {
      case (store, (title, idx)) =>
        ConsoleMessageProcessor.addMessageToOperatorConsole(
          store,
          "op1",
          message(
            workerId = "worker1",
            source = "test",
            title = title,
            content = s"content ${idx + 1}"
          ),
          bufferSize
        )
    }

  "processConsoleMessage" should "truncate message title when it exceeds display length" in {
    val longTitle = "a" * (messageDisplayLength + 10)
    val expectedTruncatedTitle = "a" * (messageDisplayLength - 3) + "..."
    val processedMessage = ConsoleMessageProcessor.processConsoleMessage(
      message(
        workerId = "worker1",
        source = "test",
        title = longTitle,
        content = "message content"
      ),
      messageDisplayLength
    )
    processedMessage.title shouldBe expectedTruncatedTitle
  }

  it should "not truncate message title when it does not exceed display length" in {
    val shortTitle = "Short Title"
    val processedMessage = ConsoleMessageProcessor.processConsoleMessage(
      message(
        workerId = "worker1",
        source = "test",
        title = shortTitle,
        content = "message content"
      ),
      messageDisplayLength
    )
    processedMessage.title shouldBe shortTitle
  }

  "addMessageToOperatorConsole" should "add message to buffer when buffer is not full" in {
    val opInfo = addMessages(standardBufferSize, "Message 1", "Message 2").operatorConsole("op1")
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 1"
    opInfo.consoleMessages(1).title shouldBe "Message 2"
  }

  it should "remove oldest message when buffer is full" in {
    val opInfo =
      addMessages(smallBufferSize, "Message 1", "Message 2", "Message 3").operatorConsole("op1")
    opInfo.consoleMessages.size shouldBe 2
    opInfo.consoleMessages.head.title shouldBe "Message 2"
    opInfo.consoleMessages(1).title shouldBe "Message 3"
  }

  "the complete message processing flow" should "handle messages correctly" in {
    val opId = "op1"
    val longTitle = "a" * (messageDisplayLength + 10)
    val processedMessage = ConsoleMessageProcessor.processConsoleMessage(
      message(
        workerId = "worker1",
        source = "test",
        title = longTitle,
        content = "message content"
      ),
      messageDisplayLength
    )
    val updatedStore = ConsoleMessageProcessor.addMessageToOperatorConsole(
      new ExecutionConsoleStore(),
      opId,
      processedMessage,
      standardBufferSize
    )
    val opInfo = updatedStore.operatorConsole(opId)
    opInfo.consoleMessages.size shouldBe 1
    opInfo.consoleMessages.head.title shouldBe ("a" * (messageDisplayLength - 3) + "...")
  }
  // ---------------------------------------------------------------- instance

  private final class Fixture(
      val client: RecordingExecutionClient,
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

  private def withFixture(body: Fixture => Unit): Unit = {
    val coordinator = mock[CoordinatorServiceFs2Grpc[TwitterFuture, Unit]]
    val client = new RecordingExecutionClient(system, Some(coordinator))
    val stateStore = new ExecutionStateStore
    val wsInput = new WebsocketInput(ListBuffer.empty[Throwable] += _)
    val service = new ExecutionConsoleService(client, stateStore, wsInput, new WorkflowContext())
    val f = new Fixture(client, coordinator, stateStore, wsInput, service)
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
      val batches = recordBatches(f.stateStore.consoleStore) {
        f.client.fire(message(title = "first"))
        f.client.fire(message(title = "second"))
      }

      val titles =
        batches.last.collect { case e: ConsoleUpdateEvent => e.messages.map(_.title) }.flatten
      titles shouldBe Seq("second")
    }
  }

  it should "emit PythonExpressionEvaluateResponse only for newly added evaluation keys" in {
    withFixture { f =>
      val val1 =
        EvaluatedValue(Some(TypedValue("x", "r1", "1", "int", expandable = false)), Seq.empty)
      val val2 =
        EvaluatedValue(Some(TypedValue("y", "r2", "2", "int", expandable = false)), Seq.empty)
      val val3 =
        EvaluatedValue(Some(TypedValue("z", "r3", "3", "int", expandable = false)), Seq.empty)

      val batches = recordBatches(f.stateStore.consoleStore) {
        f.stateStore.consoleStore.updateState { store =>
          store.addOperatorConsole(
            "udf1" -> OperatorConsole(evaluateExprResults =
              Map("x" -> EvaluatedValueList(Seq(val1)), "y" -> EvaluatedValueList(Seq(val2)))
            )
          )
        }
        f.stateStore.consoleStore.updateState { store =>
          val current = store.operatorConsole("udf1")
          store.addOperatorConsole(
            "udf1" -> current.addEvaluateExprResults("z" -> EvaluatedValueList(Seq(val3)))
          )
        }
      }

      val firstEval = batches.head.collect {
        case e: PythonExpressionEvaluateResponse => e.expression -> e.values
      }.toMap
      firstEval shouldBe Map("x" -> Seq(val1), "y" -> Seq(val2))

      val secondEval = batches.last.collect {
        case e: PythonExpressionEvaluateResponse => e.expression -> e.values
      }.toMap
      secondEval shouldBe Map("z" -> Seq(val3))
    }
  }

  "the console message callback" should "file a message under the logical operator id" in {
    // The worker id carries the physical layer and worker index; the frontend console is keyed by
    // the logical operator, so anything else silently strands the output.
    withFixture { f =>
      f.client.fire(message(workerId = "Worker:WF1-udf1-main-0"))

      f.stateStore.consoleStore.getState.operatorConsole.keys should contain("udf1")
    }
  }

  it should "store the truncated form, not the original" in {
    withFixture { f =>
      val long = "b" * (f.service.consoleMessageDisplayLength + 50)

      f.client.fire(message(title = long))

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

  "unsubscribeAll" should "shutdown consoleWriterThread" in {
    withFixture { f =>
      f.client.fire(message(title = "test"))

      val threadField = classOf[ExecutionConsoleService].getDeclaredField("consoleWriterThread")
      threadField.setAccessible(true)
      val executor = threadField.get(f.service).asInstanceOf[ExecutorService]

      // Verify it is initially active
      executor.isShutdown shouldBe false

      // Trigger the teardown
      f.service.unsubscribeAll()

      // Use Eventually to wait for async termination without blocking arbitrarily
      eventually(Timeout(Span(2000, Millis)), Interval(Span(50, Millis))) {
        executor.isShutdown shouldBe true
        executor.isTerminated shouldBe true
      }
    }
  }
}
