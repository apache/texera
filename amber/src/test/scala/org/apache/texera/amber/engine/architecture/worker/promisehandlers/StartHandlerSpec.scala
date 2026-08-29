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

import com.twitter.util.{Await, Duration}
import org.apache.texera.amber.core.WorkflowRuntimeException
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.statistics.WorkerState.UNINITIALIZED
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable.ArrayBuffer

/**
  * `startWorker` is sent only to the workers that begin a region: a source operator, which produces
  * its own input, and an operator whose input ports are fed from materialized storage rather than
  * from an upstream worker. Every other worker starts because data arrives on one of its channels,
  * and receiving `StartWorker` means the scheduler picked the wrong worker set.
  *
  * That third case is the one pinned here. It is a hard failure on purpose: a worker that quietly
  * accepted the message would transition to RUNNING with no input to process and no channel that
  * will ever finish, and the region would hang instead of reporting a scheduling bug. The two
  * starting cases are covered where the workers really run; what has no coverage anywhere is the
  * refusal, and a refusal that stopped firing would be invisible until a workflow hung.
  *
  * The harness is the bare `DataProcessor` of the other worker handler specs — no ActorSystem, and
  * no operator ever runs, because the handler must reject before doing any work.
  */
class StartHandlerSpec extends AnyFlatSpec {

  import StartHandlerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-filter-main-2")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(5)

  /**
    * A worker of a mid-workflow operator: not a source, and with no input port bound to
    * materialized storage, so it has no way to start itself.
    */
  private def newHandler()
      : (DataProcessorRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = {
      case Right(m) => sent += m
      case _        => ()
    }
    val dp = new DataProcessor(
      workerId,
      outputHandler,
      new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.executor = new NonSourceExecutor
    (new DataProcessorRPCHandlerInitializer(dp), sent)
  }

  behavior of "StartHandler"

  it should "refuse a worker that is neither a source nor a reader of materialized input" in {
    val (handler, _) = newHandler()

    val failure = intercept[WorkflowRuntimeException] {
      Await.result(handler.startWorker(EmptyRequest(), rpcContext), awaitTimeout)
    }

    // The message has to name the worker: the coordinator logs it as a control-message failure, and
    // the worker set it came from is the only clue to which operator the scheduler mis-selected.
    assert(failure.getMessage.contains(workerId.name))
    // Distinguishes this refusal from `WorkerStateManager`'s own `InvalidStateException`, which is
    // also a `WorkflowRuntimeException` and is what an accepted-but-not-READY worker would raise.
    assert(failure.getMessage.contains("unexpected StartWorker"))
  }

  it should "refuse before touching the worker" in {
    val (handler, sent) = newHandler()

    intercept[WorkflowRuntimeException] {
      Await.result(handler.startWorker(EmptyRequest(), rpcContext), awaitTimeout)
    }

    // A refusal that had already half-started the worker would leave it advertising a state it
    // cannot back out of, and would have told the downstream region its channel had opened.
    assert(handler.dp.stateManager.getCurrentState == UNINITIALIZED)
    assert(handler.dp.inputManager.getAllPorts.isEmpty)
    assert(sent.isEmpty)
  }
}

object StartHandlerSpec {

  /** Not a `SourceOperatorExecutor`, so the handler's source branch cannot apply. */
  class NonSourceExecutor extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }
}
