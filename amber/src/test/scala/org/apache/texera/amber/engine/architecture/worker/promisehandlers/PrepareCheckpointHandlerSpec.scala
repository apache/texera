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
import org.apache.texera.amber.core.executor.OperatorExecutor
import org.apache.texera.amber.core.tuple.{Schema, Tuple, TupleLike}
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  EmbeddedControlMessageIdentity
}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  PrepareCheckpointRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.texera.amber.engine.architecture.worker.{
  DataProcessor,
  DataProcessorRPCHandlerInitializer
}
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.engine.common.{CheckpointState, CheckpointSupport, SerializedState}
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue
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
  * it, which needs a live worker actor. These tests therefore install an output handler that throws
  * [[PrepareCheckpointHandlerSpec.MainThreadHandOffReached]] at that hand-off: everything the
  * callback does before it (the part that runs on the DP thread) is observable, and the assertions
  * below stop there.
  *
  * Not covered here, and not covered anywhere else either: this handler's own main-thread closure,
  * which drains the input queue into `DP_QUEUED_MSG_KEY`, snapshots un-acked output into
  * `OUTPUT_MSG_KEY`, and starts input recording by seeding `worker.recordedInputs(checkpointId)`.
  * `FinalizeCheckpointHandlerSpec` exercises a *different* closure and hand-installs
  * `recordedInputs` itself, so the prepare-to-finalize handshake is currently unverified. Closing
  * that gap needs a live worker actor (the `TestActorRef` recipe in `FinalizeCheckpointHandlerSpec`
  * would do it) and is left as follow-up.
  */
class PrepareCheckpointHandlerSpec extends AnyFlatSpec {

  import PrepareCheckpointHandlerSpec._

  private val workerId = ActorVirtualIdentity("Worker:WF1-prepare-main-0")
  private val rpcContext = AsyncRPCContext(COORDINATOR, workerId)
  private val awaitTimeout = Duration.fromSeconds(5)
  private val checkpointId = EmbeddedControlMessageIdentity("prepare-checkpoint-1")

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
    val dp = new DataProcessor(
      workerId,
      recorder.handle,
      new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.executor = executor
    (new DataProcessorRPCHandlerInitializer(dp), recorder)
  }

  private def await[T](future: Future[T]): T = Await.result(future, awaitTimeout)

  behavior of "PrepareCheckpointHandler"

  it should "register no serialization for an estimate-only request" in {
    val (handler, recorder) = newHandler(new PlainExecutor)

    assert(
      await(
        handler.prepareCheckpoint(PrepareCheckpointRequest(checkpointId, true), rpcContext)
      ) == EmptyReturn()
    )

    // Nothing was registered, so the DP's serialization point is a no-op and no checkpoint state
    // exists for this id — the coordinator will get a size estimate from `finalizeCheckpoint`
    // instead.
    handler.dp.serializationManager.applySerialization()
    assert(recorder.delegates.isEmpty)
    assert(handler.dp.ecmManager.checkpoints.isEmpty)
  }

  it should "reply before any state is serialized for a real checkpoint" in {
    val (handler, recorder) = newHandler(new PlainExecutor)

    assert(
      await(
        handler.prepareCheckpoint(PrepareCheckpointRequest(checkpointId, false), rpcContext)
      ) == EmptyReturn()
    )

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
    await(handler.prepareCheckpoint(PrepareCheckpointRequest(checkpointId, false), rpcContext))

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
    await(handler.prepareCheckpoint(PrepareCheckpointRequest(checkpointId, false), rpcContext))

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
}

object PrepareCheckpointHandlerSpec {

  /** Key the CheckpointSupport fixture writes, used to tell the two executor branches apart. */
  val OperatorStateKey = "spec-operator-state"

  /** Port carried by the iterator the fixture returns from `serializeState`. */
  val RestoredPortId: PortIdentity = PortIdentity(9)

  val emptyTuple: Tuple = Tuple.builder(new Schema()).build()

  class MainThreadHandOffReached
      extends RuntimeException("output handler reached the main-thread hand-off")

  /** Not `CheckpointSupport`, so `serializeWorkerState` must take its skip branch. */
  class PlainExecutor extends OperatorExecutor {
    override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty
  }

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
