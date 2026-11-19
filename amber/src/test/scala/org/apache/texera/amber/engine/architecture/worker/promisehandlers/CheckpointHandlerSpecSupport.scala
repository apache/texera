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
import org.apache.pekko.actor.{ActorSystem, Props}
import org.apache.pekko.testkit.{TestActorRef, TestKit}
import org.apache.texera.amber.clustering.SingleNodeListener
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage,
  WorkerReplayInitialization
}
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer,
  WorkflowWorker
}
import org.apache.texera.amber.engine.common.AmberRuntime
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.util.concurrent.LinkedBlockingQueue

abstract class CheckpointHandlerSpecSupport(
    actorSystemName: String,
    workerName: String,
    checkpointName: String,
    timeoutSeconds: Int
) extends TestKit(ActorSystem(actorSystemName, AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  protected val workerId: ActorVirtualIdentity = ActorVirtualIdentity(workerName)
  protected val rpcContext: AsyncRPCContext = AsyncRPCContext(COORDINATOR, workerId)
  protected val checkpointId: EmbeddedControlMessageIdentity =
    EmbeddedControlMessageIdentity(checkpointName)
  private val awaitTimeout = Duration.fromSeconds(timeoutSeconds)

  override def beforeAll(): Unit = {
    // WorkflowActor's actor service resolves node addresses through /user/cluster-info.
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = TestKit.shutdownActorSystem(system)

  protected def await[T](future: Future[T]): T = Await.result(future, awaitTimeout)

  protected def newDpHandler(
      executor: OperatorExecutor,
      outputHandler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit = _ => (),
      inputQueue: LinkedBlockingQueue[DPInputQueueElement] =
        new LinkedBlockingQueue[DPInputQueueElement]()
  ): DataProcessorRPCHandlerInitializer = {
    val dp = new DataProcessor(workerId, outputHandler, inputQueue)
    dp.executor = executor
    new DataProcessorRPCHandlerInitializer(dp)
  }

  /**
    * A live worker, so the main-thread hand-off is delivered as in production
    * (`logManager.sendCommitted` -> `self !` -> `handleTriggerClosure`).
    */
  protected def liveWorker(): TestActorRef[WorkflowWorker] = {
    val worker = TestActorRef(
      new WorkflowWorker(WorkerConfig(workerId), WorkerReplayInitialization())
    )
    // The DP thread would otherwise race the assertions on the worker's queue and recorded inputs.
    worker.underlyingActor.dpThread.stop()
    worker
  }

  protected def queryStatisticsMessage(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(
      ChannelIdentity(COORDINATOR, workerId, isControl = true),
      seq,
      ControlInvocation(METHOD_QUERY_STATISTICS, EmptyRequest(), rpcContext, seq)
    )
}

object CheckpointHandlerSpecSupport {

  /** Not `CheckpointSupport`, so estimate and serializeState take their skip branches. */
  class PlainExecutor extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }
}
