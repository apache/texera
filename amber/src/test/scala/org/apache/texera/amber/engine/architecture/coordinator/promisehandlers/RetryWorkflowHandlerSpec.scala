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
  RetryWorkflowRequest
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.{CLIENT, COORDINATOR}
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `retryWorkflow` is the coordinator-side entry point behind "retry" on a paused, failed
  * execution. It is a two-step fan-out: tell each worker the request names to discard and re-run
  * the tuple it died on (`retryCurrentTuple`), then resume the whole workflow by calling
  * `resumeWorkflow` on itself.
  *
  * The order is the contract. A resume that went out first would let an untouched worker step past
  * the failing tuple before it was told to retry it, so the per-worker preparation is dispatched
  * before the resume even though it is not awaited: the retries are fire-and-forget
  * (`Future.collect(...).unit` is discarded), and the reply the caller sees is the resume's.
  *
  * There is no call site for `retryWorkflow` inside the engine — it is reached only from a client
  * over the coordinator RPC surface — so these tests characterize a declared RPC contract rather
  * than an internally exercised path.
  *
  * The harness mirrors `DebugCommandHandlerSpec`: a real `CoordinatorProcessor` whose output
  * handler collects the dispatched control messages, so what is asserted is the wire message each
  * worker would receive. No ActorSystem and no live workers are needed — which is also why the
  * returned `Future` is never awaited, only inspected for pendency: `resumeWorkflow` only
  * completes once a coordinator loop that does not exist here replies to it, and that pendency is
  * itself what proves the caller was handed the resume's own reply rather than a fabricated one.
  */
class RetryWorkflowHandlerSpec extends AnyFlatSpec {

  /** Two distinct workers, so "reached every worker" is distinguishable from "reached one". */
  private val firstWorkerId = ActorVirtualIdentity("Worker:WF1-udf-main-0")
  private val secondWorkerId = ActorVirtualIdentity("Worker:WF1-udf-main-1")

  /** The retry comes from the client, so a handler that reused `ctx` would be visible. */
  private val rpcContext = AsyncRPCContext(CLIENT, COORDINATOR)

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

  private def dispatched(sent: ArrayBuffer[WorkflowFIFOMessage]): Seq[ControlInvocation] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(_, _, invocation: ControlInvocation) => invocation
    }

  behavior of "RetryWorkflowHandler"

  it should "send a retry to every worker the request names" in {
    val (init, sent) = newFixture()

    init.retryWorkflow(
      RetryWorkflowRequest(Seq(firstWorkerId, secondWorkerId)),
      rpcContext
    )

    val retries = dispatched(sent).filter(_.methodName == "retryCurrentTuple")
    assert(retries.size == 2)
    // Addressed to the workers named in the request body, not to the client that asked.
    assert(retries.map(_.context.receiver) == Seq(firstWorkerId, secondWorkerId))
    assert(retries.forall(_.context.sender == COORDINATOR))
    // Deliberately no assertion on `command`: the generated stub types it as `EmptyRequest`, a
    // protobuf message with no fields, so every instance equals every other and no edit to the
    // handler could make such an assertion fail without first failing to compile.
  }

  it should "resume the workflow through the coordinator itself" in {
    val (init, sent) = newFixture()

    val response = init.retryWorkflow(RetryWorkflowRequest(Seq(firstWorkerId)), rpcContext)

    val resumes = dispatched(sent).filter(_.methodName == "resumeWorkflow")
    assert(resumes.size == 1)
    // The coordinator resumes the workflow by calling its own handler, so both ends of the
    // invocation are the coordinator; the client is not in the loop.
    assert(resumes.head.context == AsyncRPCContext(COORDINATOR, COORDINATOR))
    // And the reply the caller gets IS the resume's: `resumeWorkflow` hands back a promise that
    // completes only when the coordinator loop answers, and no such loop runs here, so the
    // response must still be pending. A handler that dispatched the resume and then returned its
    // own `Future.value(EmptyReturn())` would tell the client the workflow had resumed before it
    // had.
    assert(!response.isDefined)
  }

  it should "prepare every worker before resuming the workflow" in {
    val (init, sent) = newFixture()

    init.retryWorkflow(
      RetryWorkflowRequest(Seq(firstWorkerId, secondWorkerId)),
      rpcContext
    )

    // A resume dispatched ahead of a retry would let a worker run past the tuple it failed on
    // before it was told to re-run it.
    assert(
      dispatched(sent).map(_.methodName) ==
        Seq("retryCurrentTuple", "retryCurrentTuple", "resumeWorkflow")
    )
  }

  it should "dispatch only the resume when the request names no workers" in {
    val (init, sent) = newFixture()

    init.retryWorkflow(RetryWorkflowRequest(Seq.empty), rpcContext)

    // The worker list drives the fan-out; an empty one still resumes, because "retry" on an
    // execution whose workers all completed is just a resume.
    assert(dispatched(sent).map(_.methodName) == Seq("resumeWorkflow"))
  }
}
