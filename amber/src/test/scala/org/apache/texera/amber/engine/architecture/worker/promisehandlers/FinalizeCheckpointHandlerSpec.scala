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

import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.EmbeddedControlMessageIdentity
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.FinalizeCheckpointRequest
import org.apache.texera.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import org.apache.texera.amber.engine.architecture.worker.promisehandlers.CheckpointHandlerSpecSupport.PlainExecutor
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.{CheckpointState, CheckpointSupport, SerializedState}

import java.net.URI
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
  * The write branch hands a closure to the worker's main thread and blocks until it runs, so that
  * case uses a real `WorkflowWorker` behind a `TestActorRef` (synchronous dispatch, as in
  * `WorkflowWorkerSpec`) and the worker's own `DataProcessor`. The estimate branch never reaches the
  * main thread and uses a bare `DataProcessor`.
  */
class FinalizeCheckpointHandlerSpec
    extends CheckpointHandlerSpecSupport(
      "FinalizeCheckpointHandlerSpec",
      "Worker:WF1-finalize-main-0",
      "finalize-checkpoint-1",
      10
    ) {

  import FinalizeCheckpointHandlerSpec._

  private val unrelatedCheckpointId = EmbeddedControlMessageIdentity("some-other-checkpoint")

  private def finalizeSize(handler: DataProcessorRPCHandlerInitializer, writeTo: String): Long =
    await(
      handler.finalizeCheckpoint(FinalizeCheckpointRequest(checkpointId, writeTo), rpcContext)
    ).size

  private def storageAt(uri: String): SequentialRecordStorage[CheckpointState] =
    SequentialRecordStorage.getStorage[CheckpointState](Some(new URI(uri)))

  behavior of "FinalizeCheckpointHandler"

  it should "report zero for an operator that cannot checkpoint" in {
    assert(finalizeSize(newDpHandler(new PlainExecutor), "ram:///finalize-plain-estimate/") == 0L)
  }

  it should "report the operator's own estimated cost when it can checkpoint" in {
    assert(
      finalizeSize(
        newDpHandler(new EstimatingExecutor(estimatedCost = 4242L)),
        "ram:///finalize-supported-estimate/"
      ) == 4242L
    )
  }

  it should "not create the destination folder for an estimate-only request" in {
    val parent = "ram:///finalize-estimate-no-write/"
    finalizeSize(
      newDpHandler(new EstimatingExecutor(estimatedCost = 1L)),
      parent + "checkpoint-folder/"
    )

    // Opening a writer creates the folder, so its absence is what shows nothing was written.
    assert(!storageAt(parent).containsFolder("checkpoint-folder"))
  }

  it should "persist and restore a non-empty worker checkpoint" in {
    val destination = "ram:///finalize-persist/"
    val worker = liveWorker()
    val dp = worker.underlyingActor.dp
    val checkpoint = new CheckpointState()
    checkpoint.save("unicode-payload", "checkpoint-δ")
    dp.ecmManager.checkpoints(checkpointId) = checkpoint

    val size = finalizeSize(new DataProcessorRPCHandlerInitializer(dp), destination)

    val restored = SequentialRecordStorage
      .fetchAllRecords(storageAt(destination), workerId.name.replace("Worker:", ""))
      .toList
    assert(size > 0L)
    assert(restored.size == 1)
    assert(restored.head.load[String]("unicode-payload") == "checkpoint-δ")
  }

  it should "fold this checkpoint's recorded messages in and stop only its recording" in {
    val worker = liveWorker()
    val dp = worker.underlyingActor.dp
    // A distinctive estimate: a handler that fell through to the estimate branch would neither
    // touch the recorded inputs nor populate the checkpoint below.
    dp.executor = new EstimatingExecutor(estimatedCost = 4242L)
    val checkpoint = new CheckpointState()
    dp.ecmManager.checkpoints(checkpointId) = checkpoint
    val recorded = ArrayBuffer(queryStatisticsMessage(1), queryStatisticsMessage(2))
    worker.underlyingActor.recordedInputs(checkpointId) = recorded
    worker.underlyingActor.recordedInputs(unrelatedCheckpointId) =
      ArrayBuffer(queryStatisticsMessage(99))

    finalizeSize(new DataProcessorRPCHandlerInitializer(dp), "ram:///finalize-fold/")

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

    finalizeSize(new DataProcessorRPCHandlerInitializer(dp), "ram:///finalize-fold-empty/")

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
