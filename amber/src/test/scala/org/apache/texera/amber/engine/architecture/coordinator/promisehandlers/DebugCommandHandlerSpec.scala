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

import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.core.workflow.WorkflowContext
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  DebugCommandRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `debugCommand` is how a debugger command typed in the UI reaches one Python worker: the web
  * layer calls it on the coordinator (`ExecutionConsoleService`), and the coordinator relays it to
  * the single worker the request names.
  *
  * The routing is the whole handler, and it is easy to get wrong in a way nothing else notices: the
  * target worker is carried in the request body, *not* in the RPC context, so a relay that reused
  * `ctx` would send the command back to whoever asked (the client) instead of to the worker, and a
  * breakpoint would silently never be set.
  *
  * The relay is also deliberately fire-and-forget: the reply is produced without waiting for the
  * worker, because a worker stopped at a breakpoint answers only when it resumes and the UI must
  * not block until then.
  *
  * The harness mirrors `EvaluatePythonExpressionHandlerSpec`: a real `CoordinatorProcessor` whose
  * output handler collects the dispatched control messages, so what is asserted is the wire
  * message a worker would receive. No ActorSystem and no workers are needed.
  */
class DebugCommandHandlerSpec extends AnyFlatSpec {

  private val awaitTimeout = Duration.fromSeconds(1)

  /** The worker named *in the request* — the one the command must reach. */
  private val targetWorkerId = ActorVirtualIdentity("Worker:WF1-udf-main-3")

  /** The command's sender, distinct from the target so a relay that reuses `ctx` is visible. */
  private val rpcContext = AsyncRPCContext(CLIENT, COORDINATOR)

  private val request = DebugCommandRequest(targetWorkerId.name, "break 12")

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

  /** The debug-command invocations the coordinator put on the wire. */
  private def dispatched(sent: ArrayBuffer[WorkflowFIFOMessage]): Seq[ControlInvocation] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(_, _, invocation: ControlInvocation)
          if invocation.methodName == "debugCommand" =>
        invocation
    }

  behavior of "DebugCommandHandler"

  it should "address the command to the worker the request names, not to the sender" in {
    val (init, sent) = newFixture()

    init.debugCommand(request, rpcContext)

    val invocations = dispatched(sent)
    assert(invocations.size == 1)
    assert(invocations.head.context.receiver == targetWorkerId)
    // The coordinator is the one asking the worker, so the relayed call is its own, not a
    // forwarded copy of the client's context.
    assert(invocations.head.context.sender == COORDINATOR)
  }

  it should "relay the request body verbatim" in {
    val (init, sent) = newFixture()

    init.debugCommand(request, rpcContext)

    // The worker-side handler parses `cmd` itself, so nothing may be rewritten, reordered or
    // dropped on the way out.
    assert(dispatched(sent).map(_.command) == Seq(request))
  }

  it should "answer without waiting for the worker to run the command" in {
    val (init, sent) = newFixture()

    val response = init.debugCommand(request, rpcContext)

    // The worker's own reply is still outstanding — a worker sitting at a breakpoint answers only
    // once it resumes — yet the coordinator has already answered its caller.
    assert(dispatched(sent).size == 1)
    assert(Await.result(response, awaitTimeout) == EmptyReturn())
  }
}
