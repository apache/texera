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
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmptyRequest,
  FinalizeCheckpointRequest
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.scheduling.config.WorkerConfig
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  WorkerReplayInitialization
}
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer,
  WorkflowWorker
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.common.{
  AmberRuntime,
  CheckpointState,
  CheckpointSupport,
  SerializedState
}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpecLike

import java.net.URI
import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/**
  * `finalizeCheckpoint` is the second half of a worker's checkpoint. It has two disjoint jobs,
  * selected by whether `prepareCheckpoint` registered a checkpoint under this id:
  *   - a checkpoint exists: fold the input messages recorded since `prepareCheckpoint` into it,
  *     stop that recording, and write the checkpoint out.
  *   - no checkpoint exists (an estimate-only request): report the operator's estimated cost and
  *     touch nothing.
  *
  * The breakages this spec catches:
  *   - taking the estimate branch when a checkpoint *is* registered, which would report a made-up
  *     size and drop the recorded in-flight messages.
  *   - creating the destination folder on an estimate-only request.
  *   - reporting a non-zero size for an operator that cannot checkpoint, or ignoring the estimate of
  *     one that can.
  *   - saving another checkpoint's recorded messages, or leaving this checkpoint's recording in
  *     place so the worker keeps buffering input forever.
  *
  * The write itself is not asserted: `SequentialRecordWriter` serializes through
  * `AmberRuntime.serde`, and `CheckpointState` is not `java.io.Serializable`, so the shipped Pekko
  * config (kryo bound to `java.io.Serializable`, java serialization off) has no binding for it. The
  * write-branch case below therefore asserts only the state the handler mutates before writing, and
  * does not depend on whether the call as a whole succeeds.
  *
  * The write branch hands a closure to the worker's main thread and blocks until it runs, so that
  * case uses a real `WorkflowWorker` behind a `TestActorRef` (synchronous dispatch, as in
  * `WorkflowWorkerSpec`) and the worker's own `DataProcessor`. The estimate branch never reaches the
  * main thread and uses a bare `DataProcessor`.
  */
class FinalizeCheckpointHandlerSpec
    extends TestKit(ActorSystem("FinalizeCheckpointHandlerSpec", AmberRuntime.pekkoConfig))
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  import FinalizeCheckpointHandlerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-finalize-main-0")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(10)
  private val checkpointId = EmbeddedControlMessageIdentity("finalize-checkpoint-1")
  private val unrelatedCheckpointId = EmbeddedControlMessageIdentity("some-other-checkpoint")

  override def beforeAll(): Unit = {
    // WorkflowActor's actor service resolves node addresses through /user/cluster-info.
    system.actorOf(Props[SingleNodeListener](), "cluster-info")
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  private def await[T](future: Future[T]): T = Await.result(future, awaitTimeout)

  /** A handler over a bare DataProcessor: enough for the estimate branch. */
  private def estimateHandler(executor: OperatorExecutor): DataProcessorRPCHandlerInitializer = {
    val dp = new DataProcessor(
      workerId,
      _ => (),
      new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.executor = executor
    new DataProcessorRPCHandlerInitializer(dp)
  }

  /**
    * A live worker, so the main-thread hand-off in the write branch is delivered exactly as in
    * production (`logManager.sendCommitted` -> `self !` -> `handleTriggerClosure`).
    */
  private def liveWorker(): TestActorRef[WorkflowWorker] = {
    val worker = TestActorRef(
      new WorkflowWorker(WorkerConfig(workerId), WorkerReplayInitialization())
    )
    // The DP thread would otherwise race the assertions on the worker's recorded inputs.
    worker.underlyingActor.dpThread.stop()
    worker
  }

  private def recordedMessage(seq: Long): WorkflowFIFOMessage =
    WorkflowFIFOMessage(
      ChannelIdentity(COORDINATOR, workerId, isControl = true),
      seq,
      ControlInvocation(METHOD_QUERY_STATISTICS, EmptyRequest(), rpcContext, seq)
    )

  private def storageAt(uri: String): SequentialRecordStorage[CheckpointState] =
    SequentialRecordStorage.getStorage[CheckpointState](Some(new URI(uri)))

  behavior of "FinalizeCheckpointHandler"

  it should "report zero for an operator that cannot checkpoint" in {
    val handler = estimateHandler(new PlainExecutor)

    val response = await(
      handler.finalizeCheckpoint(
        FinalizeCheckpointRequest(checkpointId, "ram:///finalize-plain-estimate/"),
        rpcContext
      )
    )

    assert(response.size == 0L)
  }

  it should "report the operator's own estimated cost when it can checkpoint" in {
    val handler = estimateHandler(new EstimatingExecutor(estimatedCost = 4242L))

    val response = await(
      handler.finalizeCheckpoint(
        FinalizeCheckpointRequest(checkpointId, "ram:///finalize-supported-estimate/"),
        rpcContext
      )
    )

    assert(response.size == 4242L)
  }

  it should "not create the destination folder for an estimate-only request" in {
    val handler = estimateHandler(new EstimatingExecutor(estimatedCost = 1L))
    val parent = "ram:///finalize-estimate-no-write/"

    await(
      handler.finalizeCheckpoint(
        FinalizeCheckpointRequest(checkpointId, parent + "checkpoint-folder/"),
        rpcContext
      )
    )

    // Opening a writer creates the folder, so its absence is what shows nothing was written.
    assert(!storageAt(parent).containsFolder("checkpoint-folder"))
  }

  it should "fold this checkpoint's recorded messages in and stop only its recording" in {
    val worker = liveWorker()
    val dp = worker.underlyingActor.dp
    // A distinctive estimate: a handler that fell through to the estimate branch would neither
    // touch the recorded inputs nor populate the checkpoint below.
    dp.executor = new EstimatingExecutor(estimatedCost = 4242L)
    val checkpoint = new CheckpointState()
    dp.ecmManager.checkpoints(checkpointId) = checkpoint
    val recorded = ArrayBuffer(recordedMessage(1), recordedMessage(2))
    worker.underlyingActor.recordedInputs(checkpointId) = recorded
    worker.underlyingActor.recordedInputs(unrelatedCheckpointId) = ArrayBuffer(recordedMessage(99))
    val handler = new DataProcessorRPCHandlerInitializer(dp)

    // The outcome is intentionally ignored: everything asserted below happens on the worker's main
    // thread, before the storage write this call ends with (see the note in the class comment).
    Try(
      handler.finalizeCheckpoint(
        FinalizeCheckpointRequest(checkpointId, "ram:///finalize-fold/"),
        rpcContext
      )
    )

    assert(
      checkpoint.load[mutable.ArrayBuffer[WorkflowFIFOMessage]](
        SerializedState.IN_FLIGHT_MSG_KEY
      ) == recorded
    )
    // Recording is per-checkpoint: this one is done, the other must keep going.
    assert(!worker.underlyingActor.recordedInputs.contains(checkpointId))
    assert(worker.underlyingActor.recordedInputs.contains(unrelatedCheckpointId))
  }

  it should "record an empty in-flight buffer when nothing was recorded for the checkpoint" in {
    val worker = liveWorker()
    val dp = worker.underlyingActor.dp
    val checkpoint = new CheckpointState()
    dp.ecmManager.checkpoints(checkpointId) = checkpoint
    val handler = new DataProcessorRPCHandlerInitializer(dp)

    // Outcome ignored for the same reason as the case above.
    Try(
      handler.finalizeCheckpoint(
        FinalizeCheckpointRequest(checkpointId, "ram:///finalize-fold-empty/"),
        rpcContext
      )
    )

    // `WorkflowWorker.loadFromCheckpoint` reads this key unconditionally, so it has to be present
    // even when no input arrived between prepare and finalize.
    assert(
      checkpoint
        .load[mutable.ArrayBuffer[WorkflowFIFOMessage]](SerializedState.IN_FLIGHT_MSG_KEY)
        .isEmpty
    )
  }
}

object FinalizeCheckpointHandlerSpec {

  /** Not `CheckpointSupport`: the estimate branch must fall through to 0. */
  class PlainExecutor extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

  class EstimatingExecutor(estimatedCost: Long) extends OperatorExecutor with CheckpointSupport {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

    override def serializeState(
        currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = currentIteratorState

    override def deserializeState(
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator.empty

    override def getEstimatedCheckpointCost: Long = estimatedCost
  }
}
