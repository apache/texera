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
  ChannelIdentity,
  EmbeddedControlMessageIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import org.apache.texera.amber.core.workflow.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PortIdentity,
  WorkflowContext
}
import org.apache.texera.amber.engine.architecture.coordinator.{
  CoordinatorAsyncRPCHandlerInitializer,
  CoordinatorConfig,
  CoordinatorProcessor
}
import org.apache.texera.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.EmbeddedControlMessageType.{
  NO_ALIGNMENT,
  PORT_ALIGNMENT
}
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  EmbeddedControlMessage,
  EmbeddedControlMessageType,
  EmptyRequest,
  PropagateEmbeddedControlMessageRequest
}
import org.apache.texera.amber.engine.architecture.rpc.controlreturns.{
  PropagateEmbeddedControlMessageResponse,
  ReturnInvocation,
  StringResponse
}
import org.apache.texera.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.METHOD_RETRIEVE_STATE
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}
import org.apache.texera.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import org.apache.texera.amber.engine.common.ambermessage.WorkflowFIFOMessage
import org.apache.texera.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter
import org.apache.texera.amber.engine.common.virtualidentity.util.COORDINATOR
import org.apache.texera.amber.util.VirtualIdentityUtils
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable.ArrayBuffer

/**
  * `propagateEmbeddedControlMessage` turns a "run this command on these operators" request into a
  * single embedded control message (ECM) that rides the data channels: it packs one
  * `ControlInvocation` per target worker into the ECM's command mapping, computes the set of
  * channels the ECM must traverse, and injects it on the control channel of every source operator's
  * workers.
  *
  * The breakages this spec catches:
  *   - collapsing `targetOps` and `scope` into one another. They are deliberately different sets:
  *     `targetOps` decides who runs the command, `scope` decides how far the ECM travels.
  *   - admitting a data channel whose endpoints are not both in `scope`, which would make the ECM
  *     wait for alignment on a channel that never receives it.
  *   - dropping either direction of the source workers' control channels from the scope.
  *   - answering (or marking the replay destination) before every target worker has replied — the
  *     mark is what a later replay uses as its stop point, so it must not be written while the
  *     propagation is still in flight.
  *   - dispatching to workers of a region execution that has already completed.
  */
class EmbeddedControlMessageHandlerSpec extends AnyFlatSpec {

  private val awaitTimeout = Duration.fromSeconds(1)
  private val ecmId = EmbeddedControlMessageIdentity("ecm-under-test")
  private val retrieveState = METHOD_RETRIEVE_STATE.getBareMethodName

  private val sourceOp = mkPhysicalOp("source", "main")
  private val middleOp = mkPhysicalOp("middle", "main")
  private val sinkOp = mkPhysicalOp("sink", "main")
  private val sourceWorker = mkWorkerId(sourceOp, 0)
  private val middleWorker = mkWorkerId(middleOp, 0)
  private val sinkWorker = mkWorkerId(sinkOp, 0)
  private val sourceToMiddle =
    PhysicalLink(sourceOp.id, PortIdentity(), middleOp.id, PortIdentity())
  private val middleToSink = PhysicalLink(middleOp.id, PortIdentity(), sinkOp.id, PortIdentity())

  private def mkPhysicalOp(logicalOpId: String, layerName: String): PhysicalOp =
    PhysicalOp(
      PhysicalOpIdentity(OperatorIdentity(logicalOpId), layerName),
      DEFAULT_WORKFLOW_ID,
      DEFAULT_EXECUTION_ID,
      OpExecInitInfo.Empty
    )

  private def mkWorkerId(physicalOp: PhysicalOp, index: Int): ActorVirtualIdentity =
    VirtualIdentityUtils.createWorkerIdentity(DEFAULT_WORKFLOW_ID, physicalOp.id, index)

  /** Records the ECM ids the handler marks, so completion ordering can be asserted. */
  private class RecordingLogManager extends ReplayLogManager {
    val marked: ArrayBuffer[EmbeddedControlMessageIdentity] = ArrayBuffer()

    override def setupWriter(logWriter: SequentialRecordWriter[ReplayLogRecord]): Unit = ()

    override def sendCommitted(
        msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]
    ): Unit = ()

    override def terminate(): Unit = ()

    override def markAsReplayDestination(id: EmbeddedControlMessageIdentity): Unit = marked += id
  }

  private case class Fixture(
      init: CoordinatorAsyncRPCHandlerInitializer,
      sent: ArrayBuffer[WorkflowFIFOMessage],
      logManager: RecordingLogManager
  )

  private def newFixture(): Fixture = {
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
    val logManager = new RecordingLogManager()
    cp.setupLogManager(logManager)
    Fixture(new CoordinatorAsyncRPCHandlerInitializer(cp), sent, logManager)
  }

  /**
    * Seed one running region execution spanning source -> middle -> sink, with one worker each and
    * a channel execution per data link. The region's only port is the sink's input port and it is
    * left incomplete, which is what keeps the region execution in the running set.
    */
  private def seedRunningRegion(init: CoordinatorAsyncRPCHandlerInitializer): Unit = {
    val region = Region(
      RegionIdentity(1),
      physicalOps = Set(sourceOp, middleOp, sinkOp),
      physicalLinks = Set(sourceToMiddle, middleToSink),
      ports = Set(GlobalPortIdentity(sinkOp.id, PortIdentity(), input = true))
    )
    val regionExecution = init.cp.workflowExecution.initRegionExecution(region)
    regionExecution.initOperatorExecution(sourceOp.id).initWorkerExecution(sourceWorker)
    regionExecution.initOperatorExecution(middleOp.id).initWorkerExecution(middleWorker)
    regionExecution.initOperatorExecution(sinkOp.id).initWorkerExecution(sinkWorker)
    regionExecution
      .initLinkExecution(sourceToMiddle)
      .initChannelExecution(ChannelIdentity(sourceWorker, middleWorker, isControl = false))
    regionExecution
      .initLinkExecution(middleToSink)
      .initChannelExecution(ChannelIdentity(middleWorker, sinkWorker, isControl = false))
  }

  /**
    * The request used by most cases: `scope` deliberately stops before the sink operator and
    * `targetOps` is narrower still, so a handler that confused the two sets would be caught.
    */
  private def request(
      ecmType: EmbeddedControlMessageType = PORT_ALIGNMENT
  ): PropagateEmbeddedControlMessageRequest =
    PropagateEmbeddedControlMessageRequest(
      sourceOpToStartProp = Seq(sourceOp.id),
      id = ecmId,
      ecmType = ecmType,
      scope = Seq(sourceOp.id, middleOp.id),
      targetOps = Seq(middleOp.id),
      command = EmptyRequest(),
      methodName = retrieveState
    )

  private def embeddedMessages(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): Seq[(ChannelIdentity, EmbeddedControlMessage)] =
    sent.toSeq.collect {
      case WorkflowFIFOMessage(channelId, _, ecm: EmbeddedControlMessage) => (channelId, ecm)
    }

  private def onlyEmbeddedMessage(
      sent: ArrayBuffer[WorkflowFIFOMessage]
  ): (ChannelIdentity, EmbeddedControlMessage) = {
    val messages = embeddedMessages(sent)
    assert(messages.size == 1, s"expected exactly one ECM, got: $messages")
    messages.head
  }

  behavior of "EmbeddedControlMessageHandler"

  it should "inject the ECM on the control channel of every source operator's workers" in {
    val fixture = newFixture()
    seedRunningRegion(fixture.init)

    fixture.init.propagateEmbeddedControlMessage(request(), fixture.init.mkContext(COORDINATOR))

    val (channelId, ecm) = onlyEmbeddedMessage(fixture.sent)
    assert(channelId == ChannelIdentity(COORDINATOR, sourceWorker, isControl = true))
    assert(ecm.id == ecmId)
    // A hard-coded alignment mode would show up here: PORT_ALIGNMENT is not the value any of the
    // in-tree callers of this handler pass.
    assert(ecm.ecmType == PORT_ALIGNMENT)
  }

  it should "pack one invocation per target worker, not per in-scope worker" in {
    val fixture = newFixture()
    seedRunningRegion(fixture.init)

    fixture.init.propagateEmbeddedControlMessage(request(), fixture.init.mkContext(COORDINATOR))

    val (_, ecm) = onlyEmbeddedMessage(fixture.sent)
    // `targetOps` is only the middle operator while `scope` also covers the source, so a handler
    // that keyed the mapping off `scope` would carry an extra entry here.
    assert(ecm.commandMapping.keySet == Set(middleWorker.name))
    val invocation = ecm.commandMapping(middleWorker.name)
    assert(invocation.methodName == retrieveState)
    assert(invocation.command == EmptyRequest())
    assert(invocation.context.receiver == middleWorker)
  }

  it should "scope the ECM to in-scope data channels plus both source control directions" in {
    val fixture = newFixture()
    seedRunningRegion(fixture.init)

    fixture.init.propagateEmbeddedControlMessage(request(), fixture.init.mkContext(COORDINATOR))

    val (_, ecm) = onlyEmbeddedMessage(fixture.sent)
    assert(
      ecm.scope.toSet == Set(
        ChannelIdentity(sourceWorker, middleWorker, isControl = false),
        ChannelIdentity(COORDINATOR, sourceWorker, isControl = true),
        ChannelIdentity(sourceWorker, COORDINATOR, isControl = true)
      )
    )
    // middle -> sink crosses out of `scope`, so alignment must not wait on it.
    assert(!ecm.scope.contains(ChannelIdentity(middleWorker, sinkWorker, isControl = false)))
  }

  it should "answer only after every target worker replied, and report the replies by worker" in {
    val fixture = newFixture()
    seedRunningRegion(fixture.init)

    val response = fixture.init.propagateEmbeddedControlMessage(
      request(),
      fixture.init.mkContext(COORDINATOR)
    )

    assert(!response.isDefined)
    // The replay destination is the stop point of a future replay; writing it while the
    // propagation is still outstanding would truncate that replay early.
    assert(fixture.logManager.marked.isEmpty)

    val (_, ecm) = onlyEmbeddedMessage(fixture.sent)
    val commandId = ecm.commandMapping(middleWorker.name).commandId
    fixture.init.cp.asyncRPCClient.fulfillPromise(
      ReturnInvocation(commandId, StringResponse("middle-state"))
    )

    assert(
      Await.result(response, awaitTimeout) == PropagateEmbeddedControlMessageResponse(
        Map(middleWorker.name -> StringResponse("middle-state"))
      )
    )
    assert(fixture.logManager.marked.toSeq == Seq(ecmId))
  }

  it should "ignore the workers of a region execution that already completed" in {
    val fixture = newFixture()
    seedRunningRegion(fixture.init)
    // A second, finished execution of the middle operator with a different worker. Its output port
    // is marked completed, which is what makes the region execution report COMPLETED.
    val retiredWorker = mkWorkerId(middleOp, 7)
    val completedRegion = Region(
      RegionIdentity(2),
      physicalOps = Set(middleOp),
      physicalLinks = Set.empty,
      ports = Set(GlobalPortIdentity(middleOp.id, PortIdentity(), input = false))
    )
    val completedExecution = fixture.init.cp.workflowExecution
      .initRegionExecution(completedRegion)
      .initOperatorExecution(middleOp.id)
    completedExecution
      .initWorkerExecution(retiredWorker)
      .getOutputPortExecution(PortIdentity())
      .setCompleted()

    fixture.init.propagateEmbeddedControlMessage(request(), fixture.init.mkContext(COORDINATOR))

    val (_, ecm) = onlyEmbeddedMessage(fixture.sent)
    assert(ecm.commandMapping.keySet == Set(middleWorker.name))
    assert(!ecm.commandMapping.contains(retiredWorker.name))
  }

  it should "complete immediately and still mark the destination when there is nothing to target" in {
    val fixture = newFixture()

    val response = fixture.init.propagateEmbeddedControlMessage(
      PropagateEmbeddedControlMessageRequest(
        sourceOpToStartProp = Seq.empty,
        id = ecmId,
        ecmType = NO_ALIGNMENT,
        scope = Seq.empty,
        targetOps = Seq.empty,
        command = EmptyRequest(),
        methodName = retrieveState
      ),
      fixture.init.mkContext(COORDINATOR)
    )

    // No source workers to inject into and no targets to wait for: the propagation is vacuously
    // done, and the replay destination is still recorded so a replay can stop here.
    assert(embeddedMessages(fixture.sent).isEmpty)
    assert(
      Await.result(response, awaitTimeout) == PropagateEmbeddedControlMessageResponse(Map.empty)
    )
    assert(fixture.logManager.marked.toSeq == Seq(ecmId))
  }
}
