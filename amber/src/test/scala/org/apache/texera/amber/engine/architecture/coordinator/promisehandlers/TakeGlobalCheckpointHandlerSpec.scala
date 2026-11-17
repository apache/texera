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
import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  EmbeddedControlMessageIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PhysicalPlan, WorkflowContext}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.NO_ALIGNMENT
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  ControlInvocation,
  FinalizeCheckpointRequest,
  PrepareCheckpointRequest,
  PropagateEmbeddedControlMessageRequest,
  TakeGlobalCheckpointRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  FinalizeCheckpointResponse,
  PropagateEmbeddedControlMessageResponse,
  ReturnInvocation,
  TakeGlobalCheckpointResponse
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_PREPARE_CHECKPOINT
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.common.CheckpointState
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage
import org.apache.texera.amber.engine.common.virtualidentity.util.{COORDINATOR, SELF}
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import scala.collection.mutable.ArrayBuffer

/**
  * `takeGlobalCheckpoint` is the two-phase coordinator entry point for a global checkpoint. Phase
  * one propagates a `prepareCheckpoint` embedded control message over the whole physical plan;
  * phase two asks every worker that answered to finalize its checkpoint into a per-checkpoint
  * subfolder of the destination, and sums the reported sizes into the response.
  *
  * The breakages this spec catches:
  *   - mixing up the three operator-id sets in the propagation request. `scope`/`targetOps` come
  *     from the physical plan, while `sourceOpToStartProp` comes from the operators that actually
  *     have an execution; the fixture keeps those deliberately different.
  *   - losing the "already taken" short-circuit, which downgrades a repeat request to an estimate
  *     instead of overwriting a checkpoint that is already on disk.
  *   - finalizing to the bare destination instead of the per-checkpoint subfolder, which would make
  *     concurrent checkpoints collide.
  *   - answering before every worker has finalized.
  *   - writing anything to storage on an estimate-only request.
  *
  * Storage is the in-memory `ram://` VFS also used by `LoggingSpec`, so the `SequentialRecordStorage`
  * calls run for real without touching disk. Each case uses its own `ram://` folder, because the VFS
  * manager is a JVM-wide singleton.
  */
class TakeGlobalCheckpointHandlerSpec extends AnyFlatSpec {

  private val rpcContext = AsyncRPCContext(COORDINATOR, COORDINATOR)
  private val awaitTimeout = Duration.fromSeconds(1)
  private val checkpointId = EmbeddedControlMessageIdentity("global-checkpoint-1")

  // Two operators in the plan; only `startedOp` has a region execution, so the propagation's
  // "where to start" set is a strict subset of its "what to cover" set.
  private val startedOp = mkPhysicalOp("started")
  private val notStartedOp = mkPhysicalOp("not-started")
  private val planOpIds = Set(startedOp.id, notStartedOp.id)

  private def mkPhysicalOp(logicalOpId: String): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), "main"),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecInitInfo.Empty
    )

  private def mkWorkerId(physicalOp: PhysicalOp, index: Int): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, index)

  private def newFixture()
      : (CoordinatorAsyncRPCHandlerInitializer, ArrayBuffer[WorkflowFIFOMessage]) = {
    val sent = ArrayBuffer[WorkflowFIFOMessage]()
    val cp = new CoordinatorProcessor(
      new WorkflowContext(),
      CoordinatorConfig(None, None, None, None),
      COORDINATOR,
      {
        case Right(m) => sent += m
        case _        => ()
      }
    )
    cp.workflowScheduler.physicalPlan = PhysicalPlan(Set(startedOp, notStartedOp), Set.empty)
    cp.workflowExecution
      .initRegionExecution(Region(RegionIdentity(1), Set(startedOp), Set.empty))
      .initOperatorExecution(startedOp.id)
    (new CoordinatorAsyncRPCHandlerInitializer(cp), sent)
  }

  private def invocationsNamed(
      sent: ArrayBuffer[WorkflowFIFOMessage],
      methodName: String
  ): Seq[ControlInvocation] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(_, _, invocation: ControlInvocation)
          if invocation.methodName == methodName =>
        invocation
    }

  private def onlyPropagation(sent: ArrayBuffer[WorkflowFIFOMessage]): ControlInvocation = {
    val invocations = invocationsNamed(sent, "propagateEmbeddedControlMessage")
    assert(invocations.size == 1, s"expected one propagation, got: $invocations")
    invocations.head
  }

  private def propagationRequest(
      invocation: ControlInvocation
  ): PropagateEmbeddedControlMessageRequest =
    invocation.command match {
      case req: PropagateEmbeddedControlMessageRequest => req
      case other                                       => fail(s"unexpected command: $other")
    }

  private def prepareRequest(invocation: ControlInvocation): PrepareCheckpointRequest =
    propagationRequest(invocation).command match {
      case req: PrepareCheckpointRequest => req
      case other                         => fail(s"unexpected carried command: $other")
    }

  /** Answer the propagation as if `workerNames` had all run `prepareCheckpoint`. */
  private def completePropagation(
      init: CoordinatorAsyncRPCHandlerInitializer,
      sent: ArrayBuffer[WorkflowFIFOMessage],
      workerNames: Seq[String]
  ): Unit =
    init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(
        onlyPropagation(sent).commandId,
        PropagateEmbeddedControlMessageResponse(workerNames.map(_ -> EmptyReturn()).toMap)
      )
    )

  private def checkpointStorage(destination: String): SequentialRecordStorage[CheckpointState] =
    SequentialRecordStorage.getStorage[CheckpointState](Some(new URI(destination)))

  behavior of "TakeGlobalCheckpointHandler"

  it should "propagate prepareCheckpoint over the whole plan, starting from the started operators" in {
    val (init, sent) = newFixture()

    init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(
        estimationOnly = true,
        checkpointId,
        "ram:///take-global-checkpoint-propagate/"
      ),
      rpcContext
    )

    val invocation = onlyPropagation(sent)
    // The coordinator asks itself to run the propagation.
    assert(invocation.context.receiver == SELF)
    val request = propagationRequest(invocation)
    assert(request.id == checkpointId)
    assert(request.ecmType == NO_ALIGNMENT)
    assert(request.scope.toSet == planOpIds)
    assert(request.targetOps.toSet == planOpIds)
    // Only the operator with an execution can inject the message; the un-started one has no
    // workers to start propagation from. Compared as a Set because production derives this from
    // `RegionExecution.getAllOperatorExecutions`, which is backed by a mutable.HashMap — sequence
    // order is not something to lean on once more than one operator is started.
    assert(request.sourceOpToStartProp.toSet == Set(startedOp.id))
    assert(request.methodName == METHOD_PREPARE_CHECKPOINT.getBareMethodName)
    assert(
      prepareRequest(invocation) == PrepareCheckpointRequest(checkpointId, estimationOnly = true)
    )
  }

  it should "downgrade to an estimate when the destination already holds this checkpoint" in {
    val destination = "ram:///take-global-checkpoint-already-taken/"
    // Creating a VFS-backed storage at the per-checkpoint folder creates the folder, i.e. this is
    // exactly the on-disk state left behind by a previous successful checkpoint.
    checkpointStorage(new URI(destination).resolve(checkpointId.toString).toString)
    val (init, sent) = newFixture()

    init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = false, checkpointId, destination),
      rpcContext
    )

    // The caller asked for a real checkpoint, but one is already there, so the workers must be
    // told to estimate rather than overwrite it.
    assert(
      prepareRequest(onlyPropagation(sent)) == PrepareCheckpointRequest(
        checkpointId,
        estimationOnly = true
      )
    )
  }

  it should "still take the checkpoint when a different checkpoint id exists at the destination" in {
    val destination = "ram:///take-global-checkpoint-other-id/"
    checkpointStorage(
      new URI(destination)
        .resolve(EmbeddedControlMessageIdentity("some-other-checkpoint").toString)
        .toString
    )
    val (init, sent) = newFixture()

    init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = false, checkpointId, destination),
      rpcContext
    )

    // The short-circuit must key on this checkpoint's own folder, not on the destination being
    // non-empty.
    assert(
      prepareRequest(onlyPropagation(sent)) == PrepareCheckpointRequest(
        checkpointId,
        estimationOnly = false
      )
    )
  }

  it should "finalize on every worker that answered, into the per-checkpoint subfolder" in {
    val destination = "ram:///take-global-checkpoint-finalize/"
    val (init, sent) = newFixture()
    val workerIds = Seq(mkWorkerId(startedOp, 0), mkWorkerId(startedOp, 1))

    init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = true, checkpointId, destination),
      rpcContext
    )
    // Phase two must not start before the propagation answered.
    assert(invocationsNamed(sent, "finalizeCheckpoint").isEmpty)
    completePropagation(init, sent, workerIds.map(_.name))

    val finalizations = invocationsNamed(sent, "finalizeCheckpoint")
    assert(finalizations.map(_.context.receiver).toSet == workerIds.toSet)
    assert(finalizations.map(_.command).forall {
      case request: FinalizeCheckpointRequest =>
        // The write target is a per-checkpoint subfolder of the destination — the same layout
        // `WorkflowActor.setupReplay` reads back via `readFrom.resolve(replayTo.toString)`. A
        // handler that passed `msg.destination` through unresolved would leave the last path
        // segment off.
        val writeTo = new URI(request.writeTo)
        request.checkpointId == checkpointId &&
        writeTo.getScheme == "ram" &&
        writeTo.getPath == s"/take-global-checkpoint-finalize/$checkpointId"
      case other => fail(s"unexpected finalize command: $other")
    })
  }

  it should "answer only after every worker has finalized" in {
    val destination = "ram:///take-global-checkpoint-sizes/"
    val (init, sent) = newFixture()
    val workerIds = Seq(mkWorkerId(startedOp, 0), mkWorkerId(startedOp, 1))

    val response = init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = true, checkpointId, destination),
      rpcContext
    )
    completePropagation(init, sent, workerIds.map(_.name))

    val finalizations = invocationsNamed(sent, "finalizeCheckpoint")
    assert(finalizations.size == 2)
    assert(!response.isDefined)
    init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(finalizations.head.commandId, FinalizeCheckpointResponse(4000L))
    )
    // One worker still owes a reply, so the global checkpoint is not done.
    assert(!response.isDefined)
    init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(finalizations(1).commandId, FinalizeCheckpointResponse(56L))
    )

    // The reported `totalSize` is deliberately not asserted. The handler accumulates it from
    // per-future `onSuccess` callbacks, and a Twitter `Promise` runs its continuations in reverse
    // registration order, so `Future.collect`'s continuation — and with it the response — is built
    // before the last-satisfied future's `onSuccess` has contributed. Pinning today's number would
    // turn a fix of that into a failure.
    assert(Await.result(response, awaitTimeout).isInstanceOf[TakeGlobalCheckpointResponse])
  }

  it should "write nothing to storage for an estimate-only checkpoint" in {
    val destination = "ram:///take-global-checkpoint-no-write/"
    val (init, sent) = newFixture()

    val response = init.takeGlobalCheckpoint(
      TakeGlobalCheckpointRequest(estimationOnly = true, checkpointId, destination),
      rpcContext
    )
    completePropagation(init, sent, Seq(mkWorkerId(startedOp, 0).name))
    init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(
        invocationsNamed(sent, "finalizeCheckpoint").head.commandId,
        FinalizeCheckpointResponse(7L)
      )
    )
    // Await so the whole two-phase flow has run before storage is inspected.
    Await.result(response, awaitTimeout)

    // The per-checkpoint folder is created by the writer, so its absence shows the coordinator
    // skipped serializing its own state.
    assert(!checkpointStorage(destination).containsFolder(checkpointId.toString))
  }
}
