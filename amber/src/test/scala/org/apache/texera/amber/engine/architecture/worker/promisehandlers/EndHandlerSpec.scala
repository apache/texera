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

package org.apache.texera.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.{Await, Duration, Future}
import org.apache.texera.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  ReturnInvocation
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  ActorCommandElement,
  DPInputQueueElement,
  FIFOMessageElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer
}
import org.apache.texera.amber.engine.common.actormessage.Backpressure
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue

/**
  * `endWorker` is the coordinator's acknowledgement point before it sends actor-level `gracefulStop`.
  *
  * A successful reply means the worker has drained every queued workflow message. If the queue still contains work,
  * the handler must fail so the region execution manager can retry the kill instead of stopping the actor too early.
  */
class EndHandlerSpec extends AnyFlatSpec {
  private val workerId = ActorVirtualIdentity("Worker:WF1-test-op-main-0")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(1)

  private def createEndHandlerForQueue(
      queue: LinkedBlockingQueue[DPInputQueueElement]
  ): DataProcessorRPCHandlerInitializer = {
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = _ => ()
    val dp = new DataProcessor(workerId, outputHandler, queue)
    new DataProcessorRPCHandlerInitializer(dp)
  }

  private def await[T](future: Future[T]): T = Await.result(future, awaitTimeout)

  private def assertEndWorkerFails(handler: DataProcessorRPCHandlerInitializer): Unit = {
    val exception = intercept[IllegalStateException] {
      await(handler.endWorker(EmptyRequest(), rpcContext))
    }
    assert(exception.getMessage == "worker still has unprocessed messages")
  }

  private def queueWithFifoControlMessage(): LinkedBlockingQueue[DPInputQueueElement] = {
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    queue.put(
      FIFOMessageElement(
        WorkflowFIFOMessage(
          ChannelIdentity(COORDINATOR, workerId, isControl = true),
          0,
          ControlInvocation(METHOD_QUERY_STATISTICS, EmptyRequest(), rpcContext, 1)
        )
      )
    )
    queue
  }

  private def queueWithActorCommand(): LinkedBlockingQueue[DPInputQueueElement] = {
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    queue.put(ActorCommandElement(Backpressure(enableBackpressure = true)))
    queue
  }

  private def ackElement(sequenceNumber: Long, commandId: Long): DPInputQueueElement =
    FIFOMessageElement(
      WorkflowFIFOMessage(
        ChannelIdentity(COORDINATOR, workerId, isControl = true),
        sequenceNumber,
        ReturnInvocation(commandId, EmptyReturn())
      )
    )

  "EndHandler" should "reply successfully when there are no unprocessed messages" in {
    val handler = createEndHandlerForQueue(new LinkedBlockingQueue[DPInputQueueElement]())

    assert(await(handler.endWorker(EmptyRequest(), rpcContext)) == EmptyReturn())
  }

  it should "fail when a FIFO control message is still queued" in {
    val handler = createEndHandlerForQueue(queueWithFifoControlMessage())

    assertEndWorkerFails(handler)
  }

  it should "fail when an actor command is still queued" in {
    val handler = createEndHandlerForQueue(queueWithActorCommand())

    assertEndWorkerFails(handler)
  }

  it should "reply successfully when only RPC acks (ReturnInvocations) are queued" in {
    // Race at region termination: the coordinator's ReturnInvocation ack for
    // this worker's own fire-and-forget RPC (e.g. workerExecutionCompleted)
    // can still be in flight when EndWorker arrives. Such acks carry no work
    // -- the worker never awaits them -- so an ack-only backlog must not fail
    // the kill (it previously did, making region termination retry and loop
    // e2e tests flaky).
    val queue = new LinkedBlockingQueue[DPInputQueueElement]()
    queue.put(ackElement(sequenceNumber = 0, commandId = 1))
    queue.put(ackElement(sequenceNumber = 1, commandId = 2))
    val handler = createEndHandlerForQueue(queue)

    assert(await(handler.endWorker(EmptyRequest(), rpcContext)) == EmptyReturn())
  }

  it should "fail when an RPC ack is queued together with real work" in {
    // Leniency is strictly ack-only: any non-ack message (here a control
    // invocation) still fails the kill so the retry lets the worker drain it.
    val queue = queueWithFifoControlMessage()
    queue.put(ackElement(sequenceNumber = 1, commandId = 2))
    val handler = createEndHandlerForQueue(queue)

    assertEndWorkerFails(handler)
  }
}
