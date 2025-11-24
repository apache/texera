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

package org.apache.amber.engine.architecture.worker

import org.apache.amber.core.executor.OperatorExecutor
import org.apache.amber.core.tuple.{AttributeType, Schema, Tuple, TupleLike}
import org.apache.amber.core.virtualidentity._
import org.apache.amber.core.workflow.PortIdentity
import org.apache.amber.core.workflow.WorkflowContext.DEFAULT_WORKFLOW_ID
import org.apache.amber.engine.architecture.logreplay.{ReplayLogManager, ReplayLogRecord}
import org.apache.amber.engine.architecture.messaginglayer.WorkerTimerService
import org.apache.amber.engine.architecture.rpc.controlcommands.{
  AsyncRPCContext,
  EmbeddedControlMessage,
  EmbeddedControlMessageType,
  EmptyRequest
}
import org.apache.amber.engine.architecture.rpc.workerservice.WorkerServiceGrpc.{
  METHOD_END_CHANNEL,
  METHOD_FLUSH_NETWORK_BUFFER,
  METHOD_OPEN_EXECUTOR
}
import org.apache.amber.engine.architecture.worker.WorkflowWorker.{
  DPInputQueueElement,
  MainThreadDelegateMessage
}
import org.apache.amber.engine.architecture.worker.statistics.WorkerState.READY
import org.apache.amber.engine.common.ambermessage.{DataFrame, WorkflowFIFOMessage}
import org.apache.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import org.apache.amber.engine.common.storage.SequentialRecordStorage
import org.apache.amber.engine.common.virtualidentity.util.CONTROLLER
import org.apache.amber.util.VirtualIdentityUtils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import java.util.concurrent.LinkedBlockingQueue

class DataProcessorSpec extends AnyFlatSpec with MockFactory with BeforeAndAfterEach {
  private val testOpId = PhysicalOpIdentity(OperatorIdentity("testop"), "main")
  private val upstreamOpId = PhysicalOpIdentity(OperatorIdentity("sender"), "main")
  private val testWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    testOpId,
    0
  )
  private val senderWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    upstreamOpId,
    0
  )

  private val executor = mock[OperatorExecutor]
  private val inputPortId = PortIdentity()
  private val outputPortId = PortIdentity()
  private val outputHandler = mock[Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit]
  private val adaptiveBatchingMonitor = mock[WorkerTimerService]
  private val schema: Schema = Schema().add("field1", AttributeType.INTEGER)
  private val tuples: Array[Tuple] = (0 until 400)
    .map(i => TupleLike(i).enforceSchema(schema))
    .toArray
  private val logStorage = SequentialRecordStorage.getStorage[ReplayLogRecord](None)
  private val logManager: ReplayLogManager =
    ReplayLogManager.createLogManager(logStorage, "none", x => {})
  private val endChannelPayload = EmbeddedControlMessage(
    EmbeddedControlMessageIdentity("EndChannel"),
    EmbeddedControlMessageType.PORT_ALIGNMENT,
    Seq(),
    Map(
      testWorkerId.name ->
        ControlInvocation(
          METHOD_END_CHANNEL.getBareMethodName,
          EmptyRequest(),
          AsyncRPCContext(ActorVirtualIdentity(""), ActorVirtualIdentity("")),
          -1
        )
    )
  )

  def mkDataProcessor: DataProcessor = {
    val dp: DataProcessor = new DataProcessor(
      testWorkerId,
      outputHandler,
      inputMessageQueue = new LinkedBlockingQueue[DPInputQueueElement]()
    )
    dp.initTimerService(adaptiveBatchingMonitor)
    dp
  }

  "data processor" should "process data messages" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).once()
    (executor.open _).expects().once()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    (
        (
          input: Int
        ) => executor.produceStateOnFinish(input)
    )
      .expects(0)
      .returning(None)
    (
        (
          input: Int
        ) => executor.onFinishMultiPort(input)
    )
      .expects(
        0
      )
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (executor.close _).expects().once()
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.processDCM(
      ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(CONTROLLER, testWorkerId),
        0
      )
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processECM(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      endChannelPayload,
      logManager
    )

    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

  "data processor" should "process control messages during data processing" in {
    val dp = mkDataProcessor
    dp.executor = executor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    (executor.open _).expects().once()
    tuples.foreach { x =>
      (
          (
              tuple: Tuple,
              input: Int
          ) => executor.processTupleMultiPort(tuple, input)
      )
        .expects(x, 0)
    }
    (
        (
          input: Int
        ) => executor.produceStateOnFinish(input)
    )
      .expects(0)
      .returning(None)
    (
        (
          input: Int
        ) => executor.onFinishMultiPort(input)
    )
      .expects(0)
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    dp.inputManager.addPort(inputPortId, schema, List.empty, List.empty)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.outputManager.addPort(outputPortId, schema, None)
    dp.processDCM(
      ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(
        METHOD_OPEN_EXECUTOR,
        EmptyRequest(),
        AsyncRPCContext(CONTROLLER, testWorkerId),
        0
      )
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.processDCM(
        ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
        ControlInvocation(
          METHOD_FLUSH_NETWORK_BUFFER,
          EmptyRequest(),
          AsyncRPCContext(CONTROLLER, testWorkerId),
          1
        )
      )
      dp.continueDataProcessing()
    }
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (executor.close _).expects().once()
    dp.processECM(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      endChannelPayload,
      logManager
    )
    while (dp.inputManager.hasUnfinishedInput || dp.outputManager.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

}
