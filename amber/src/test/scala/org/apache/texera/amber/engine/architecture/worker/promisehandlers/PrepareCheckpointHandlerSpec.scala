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

import org.apache.pekko.testkit.TestActorRef
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmptyRequest,
  PrepareCheckpointRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_QUERY_STATISTICS
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  FIFOMessageElement,
  MainThreadDelegateMessage,
  TimerBasedControlElement
}
import org.apache.texera.amber.engine.architecture.worker.promisehandlers.CheckpointHandlerSpecSupport.PlainExecutor
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessorRPCHandlerInitializer,
  WorkflowWorker
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.texera.amber.engine.common.{CheckpointState, CheckpointSupport, SerializedState}

import scala.collection.mutable.ArrayBuffer

/**
  * `prepareCheckpoint` is the first half of a worker's checkpoint. It must not serialize anything
  * itself: the DP thread may be in the middle of a tuple when the request arrives, so the handler
  * only *registers* a serialization callback that the DP loop later fires at a safe point
  * (`SerializationManager.applySerialization`).
  *
  * The breakages this spec catches:
  *   - inverting the estimate-only test, i.e. registering a serialization for a request that only
  *     asked for a size estimate (a real checkpoint would then be taken, and written, unasked), or
  *     skipping registration for a real checkpoint.
  *   - serializing eagerly inside the handler instead of at the DP's serialization point.
  *   - registering the checkpoint under the wrong id, which would make `finalizeCheckpoint` fall
  *     through to its estimate-only branch and silently write nothing.
  *   - not handing the operator the DP's pending output state, or dropping the replacement
  *     iterator the operator returns — the un-emitted tuples would be lost on restore.
  *
  * The registered callback's last step hands a closure to the worker's main thread and blocks on
  * it. Most of the cases below stop there: they install an output handler that throws
  * [[PrepareCheckpointHandlerSpec.MainThreadHandOffReached]] at the hand-off, so everything the
  * callback does before it (the part that runs on the DP thread) is observable in isolation.
  *
  * The last case instead lets the hand-off through to a real worker actor, which is what covers the
  * other half of the checkpoint: the main-thread closure drains the input queue into
  * `DP_QUEUED_MSG_KEY`, snapshots un-acked output into `OUTPUT_MSG_KEY`, and starts input recording
  * by seeding `worker.recordedInputs(checkpointId)` -- the buffer `finalizeCheckpoint` later folds
  * in. `FinalizeCheckpointHandlerSpec` hand-installs that buffer, so this is the only place the
  * prepare-to-finalize handshake is verified end to end.
  */
class PrepareCheckpointHandlerSpec
    extends CheckpointHandlerSpecSupport(
      "PrepareCheckpointHandlerSpec",
      "Worker:WF1-prepare-main-0",
      "prepare-checkpoint-1",
      5
    ) {

  import PrepareCheckpointHandlerSpec._

  /** Records main-thread hand-offs, then aborts so the blocking wait is never reached. */
  private class OutputRecorder {
    val delegates: ArrayBuffer[MainThreadDelegateMessage] = ArrayBuffer()

    def handle(msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]): Unit =
      msg match {
        case Left(delegate) =>
          delegates += delegate
          throw new MainThreadHandOffReached
        case Right(_) => ()
      }
  }

  private def newHandler(
      executor: OperatorExecutor
  ): (DataProcessorRPCHandlerInitializer, OutputRecorder) = {
    val recorder = new OutputRecorder
    (newDpHandler(executor, recorder.handle), recorder)
  }

  /**
    * A handler whose DP shares the worker's input queue and routes main-thread hand-offs to it,
    * which is the wiring `WorkflowWorker` builds for its own DP
    * (`WorkflowActor.sendMessageFromLogWriterToActor`).
    */
  private def newHandlerOn(
      worker: TestActorRef[WorkflowWorker],
      executor: OperatorExecutor
  ): DataProcessorRPCHandlerInitializer =
    newDpHandler(
      executor,
      {
        case Left(delegate) => worker ! delegate
        case Right(_)       => ()
      },
      worker.underlyingActor.inputQueue
    )

  private def prepare(handler: DataProcessorRPCHandlerInitializer, estimateOnly: Boolean): Unit =
    assert(
      await(
        handler.prepareCheckpoint(PrepareCheckpointRequest(checkpointId, estimateOnly), rpcContext)
      ) == EmptyReturn()
    )

  behavior of "PrepareCheckpointHandler"

  it should "register no serialization for an estimate-only request" in {
    val (handler, recorder) = newHandler(new PlainExecutor)

    prepare(handler, estimateOnly = true)

    // Nothing was registered, so the DP's serialization point is a no-op and no checkpoint state
    // exists for this id — the coordinator will get a size estimate from `finalizeCheckpoint`
    // instead.
    handler.dp.serializationManager.applySerialization()
    assert(recorder.delegates.isEmpty)
    assert(handler.dp.ecmManager.checkpoints.isEmpty)
  }

  it should "reply before any state is serialized for a real checkpoint" in {
    val (handler, recorder) = newHandler(new PlainExecutor)

    prepare(handler, estimateOnly = false)

    // The reply is what releases the embedded control message; the serialization itself must still
    // be pending, waiting for the DP loop to reach a safe point.
    assert(handler.dp.ecmManager.checkpoints.isEmpty)
    assert(recorder.delegates.isEmpty)
  }

  it should "serialize the DP state under the requested checkpoint id when the DP fires it" in {
    val (handler, recorder) = newHandler(new PlainExecutor)
    // Pre-install a distinctive pending output. Production reaches
    // `outputIterator.setTupleOutput(...)` ONLY inside the `case support: CheckpointSupport`
    // arm, so for a plain executor this iterator must come back untouched -- that is the
    // observable consequence of taking the skip branch. (Asserting the *absence* of the
    // fixture's operator-state key would prove nothing: only the fixture writes that key, and
    // this executor has no serializeState at all, so it could never appear.)
    val pendingOutput: (TupleLike, Option[PortIdentity]) = (emptyTuple, Some(PortIdentity(3)))
    handler.dp.outputManager.outputIterator.setTupleOutput(Iterator(pendingOutput))
    prepare(handler, estimateOnly = false)

    intercept[MainThreadHandOffReached] {
      handler.dp.serializationManager.applySerialization()
    }

    assert(handler.dp.ecmManager.checkpoints.keySet == Set(checkpointId))
    assert(handler.dp.ecmManager.checkpoints(checkpointId).has(SerializedState.DP_STATE_KEY))
    assert(handler.dp.outputManager.outputIterator.outputIter.toList == List(pendingOutput))
    assert(recorder.delegates.size == 1)
  }

  it should "hand the operator the DP's pending output and install the iterator it returns" in {
    val executor = new CheckpointAwareExecutor
    val (handler, _) = newHandler(executor)
    val pendingOutput: (TupleLike, Option[PortIdentity]) = (emptyTuple, Some(PortIdentity(3)))
    handler.dp.outputManager.outputIterator.setTupleOutput(Iterator(pendingOutput))
    prepare(handler, estimateOnly = false)

    intercept[MainThreadHandOffReached] {
      handler.dp.serializationManager.applySerialization()
    }

    // Output the executor produced but the DP has not emitted yet is part of the operator's state;
    // it is handed over so the operator can persist it.
    assert(executor.receivedOutputState == List(pendingOutput))
    // ...and whatever the operator returns becomes the DP's output from now on. The replacement
    // uses a different port than `pendingOutput`, so a handler that reinstalled the original (or
    // left the iterator untouched) is visible here.
    assert(
      handler.dp.outputManager.outputIterator.outputIter.toList ==
        List((emptyTuple, Some(RestoredPortId)))
    )
    assert(handler.dp.ecmManager.checkpoints(checkpointId).has(OperatorStateKey))
  }

  it should "save the worker's queued input and start recording what arrives next" in {
    val worker = liveWorker()
    val handler = newHandlerOn(worker, new PlainExecutor)
    val queued = queryStatisticsMessage(7L)
    worker.underlyingActor.inputQueue.put(FIFOMessageElement(queued))
    // Not a message from anywhere: the timer re-issues it after a restore, so checkpointing it
    // would replay a statistics query the coordinator never sent.
    worker.underlyingActor.inputQueue.put(
      TimerBasedControlElement(
        ControlInvocation(METHOD_QUERY_STATISTICS, EmptyRequest(), rpcContext, 8L)
      )
    )

    prepare(handler, estimateOnly = false)
    handler.dp.serializationManager.applySerialization()

    val checkpoint = handler.dp.ecmManager.checkpoints(checkpointId)
    // Messages that arrived but have not been processed are part of the worker's state: on restore
    // they go back into the queue, so anything dropped here is a lost control message or batch.
    assert(
      checkpoint
        .load[ArrayBuffer[WorkflowFIFOMessage]](SerializedState.DP_QUEUED_MSG_KEY)
        .toList == List(queued)
    )
    // Nothing was sent, so nothing is awaiting an ack -- but the key still has to exist, because
    // `WorkflowWorker.loadFromCheckpoint` reads it unconditionally.
    assert(checkpoint.load[Array[WorkflowFIFOMessage]](SerializedState.OUTPUT_MSG_KEY).isEmpty)
    // The hand-off ends by opening this buffer, and `finalizeCheckpoint` folds it in: without it,
    // every message arriving between the two halves of the checkpoint would be lost on restore.
    assert(worker.underlyingActor.recordedInputs.keySet == Set(checkpointId))
  }
}

object PrepareCheckpointHandlerSpec {

  /** Key the CheckpointSupport fixture writes, used to tell the two executor branches apart. */
  val OperatorStateKey = "spec-operator-state"

  /** Port carried by the iterator the fixture returns from `serializeState`. */
  val RestoredPortId: PortIdentity = PortIdentity(9)

  val emptyTuple: Tuple = Tuple.builder(new Schema()).build()

  class MainThreadHandOffReached
      extends RuntimeException("output handler reached the main-thread hand-off")

  class CheckpointAwareExecutor extends OperatorExecutor with CheckpointSupport {
    var receivedOutputState: List[(TupleLike, Option[PortIdentity])] = Nil

    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

    override def serializeState(
        currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])],
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = {
      receivedOutputState = currentIteratorState.toList
      checkpoint.save(OperatorStateKey, "written-by-operator")
      Iterator((emptyTuple, Some(RestoredPortId)))
    }

    override def deserializeState(
        checkpoint: CheckpointState
    ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator.empty

    override def getEstimatedCheckpointCost: Long = 0L
  }
}
