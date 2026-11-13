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

package org.apache.texera.amber.engine.architecture.coordinator.promisehandlers

import com.google.protobuf.timestamp.Timestamp
import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ConsoleMessage,
  ConsoleMessageTriggeredRequest,
  ConsoleMessageType
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.flatspec.AnyFlatSpec

import java.time.Instant
import scala.collection.mutable.ArrayBuffer

/**
  * `consoleMessageTriggered` is the coordinator's half of the console: a worker reports a print, an
  * error or a debugger line (`DataProcessor.handleExecutorException`, and the Python runtime's
  * equivalent), and the coordinator is the only hop between that worker and the browser.
  *
  * It is a pure relay, and everything that could break it is invisible from the handler itself:
  * the message has to leave on the *client* channel (nothing else is subscribed to it, so a message
  * sent anywhere else is simply lost), it has to arrive unmodified (the console panel groups rows
  * by `workerId`/`msgType` and renders `title`/`message` verbatim), and it must not be duplicated
  * onto a worker as well.
  *
  * `ConsoleMessage` is itself the `ClientEvent`, so this spec reads what the coordinator's output
  * handler emits and asserts on the wire message the client actor would receive. The harness is the
  * one from `EvaluatePythonExpressionHandlerSpec`; no ActorSystem is involved.
  */
class ConsoleMessageHandlerSpec extends AnyFlatSpec {

  private val awaitTimeout = Duration.fromSeconds(1)

  /** The worker that reported the message; also the RPC sender, as in production. */
  private val reportingWorkerId = ActorVirtualIdentity("Worker:WF1-udf-main-2")
  private val rpcContext = AsyncRPCContext(reportingWorkerId, COORDINATOR)

  /** The channel the client actor listens on. */
  private val clientChannel = ChannelIdentity(COORDINATOR, CLIENT, isControl = true)

  /**
    * Every text field holds a different value, so a relay that shuffled two of them (say `source`
    * into `title`) cannot pass by accident.
    */
  private val consoleMessage = ConsoleMessage(
    workerId = reportingWorkerId.name,
    timestamp = Timestamp(Instant.parse("2020-01-02T03:04:05Z")),
    msgType = ConsoleMessageType.ERROR,
    source = "(udf.py:31)",
    title = "ZeroDivisionError: division by zero",
    message = "Traceback (most recent call last)"
  )

  private def newFixture()
      : (CoordinatorAsyncRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(m) => sent += m
      case _        => ()
    }
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      outputHandler
    )
    (new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  behavior of "ConsoleMessageHandler"

  it should "put the message on the client channel and nowhere else" in {
    val (init, sent) = newFixture()

    init.consoleMessageTriggered(ConsoleMessageTriggeredRequest(consoleMessage), rpcContext)

    // Exactly one message: the console is a browser-facing feed, so relaying a copy back to a
    // worker would be both useless and, for a paused worker, extra queued work.
    assert(sent.size == 1)
    assert(sent.head.channelId == clientChannel)
  }

  it should "hand the client the reported message unchanged" in {
    val (init, sent) = newFixture()

    init.consoleMessageTriggered(ConsoleMessageTriggeredRequest(consoleMessage), rpcContext)

    // The coordinator knows nothing about console formatting; the browser renders these fields
    // directly, so the relayed payload has to be the reported one, field for field.
    assert(sent.head.payload == consoleMessage)
  }

  it should "acknowledge the reporting worker" in {
    val (init, _) = newFixture()

    val response =
      init.consoleMessageTriggered(ConsoleMessageTriggeredRequest(consoleMessage), rpcContext)

    // The worker treats this as a plain RPC and its promise stays unfulfilled until the reply
    // comes back; the console is best-effort, so the reply cannot depend on the client.
    assert(Await.result(response, awaitTimeout) == EmptyReturn())
  }
}
