package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.messaginglayer.{OutputManager, WorkerTimerService}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.{InputExhausted, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.ambermessage.{
  ChannelID,
  DataFrame,
  EndOfUpstream,
  WorkflowFIFOMessage
}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.texera.workflow.common.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class DataProcessorSpec extends AnyFlatSpec with MockFactory with BeforeAndAfterEach {
  private val testOpId = PhysicalOpIdentity(OperatorIdentity("testop"), "main")
  private val upstreamOpId = PhysicalOpIdentity(OperatorIdentity("sender"), "main")
  private val testWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    DEFAULT_EXECUTION_ID,
    testOpId,
    0
  )
  private val senderWorkerId: ActorVirtualIdentity = VirtualIdentityUtils.createWorkerIdentity(
    DEFAULT_WORKFLOW_ID,
    DEFAULT_EXECUTION_ID,
    upstreamOpId,
    0
  )

  private val operator = mock[OperatorExecutor]
  private val upstreamOp =
    PhysicalOp(id = upstreamOpId, DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, opExecInitInfo = null)
  private val testOp =
    PhysicalOp(id = testOpId, DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, opExecInitInfo = null)
  private val link = PhysicalLink(upstreamOp, 0, testOp, 0)
  private val physicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        DEFAULT_WORKFLOW_ID,
        DEFAULT_EXECUTION_ID,
        testOpId.logicalOpId,
        OpExecInitInfo(_ => operator)
      )
      .addInput(link.fromOp, 0, 0)
  private val outputHandler = mock[WorkflowFIFOMessage => Unit]
  private val adaptiveBatchingMonitor = mock[WorkerTimerService]
  private val tuples: Array[ITuple] = (0 until 400).map(ITuple(_)).toArray

  def mkDataProcessor: DataProcessor = {
    val dp: DataProcessor =
      new DataProcessor(testWorkerId, outputHandler) {
        override val outputManager: OutputManager = mock[OutputManager]
        override val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
      }
    dp.initOperator(0, physicalOp, Iterator.empty)
    dp.initTimerService(adaptiveBatchingMonitor)
    dp
  }

  case class DummyControl() extends ControlCommand[Unit]

  "data processor" should "process data messages" in {
    val dp = mkDataProcessor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).once()
    (operator.open _).expects().once()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    (operator.processTuple _).expects(
      Right(InputExhausted()),
      0,
      dp.pauseManager,
      dp.asyncRPCClient
    )
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (dp.asyncRPCClient.send[Unit] _).expects(*, *).anyNumberOfTimes()
    (dp.outputManager.emitEndOfUpstream _).expects().once()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (operator.close _).expects().once()
    dp.registerInput(senderWorkerId, link.id)
    dp.processControlPayload(
      ChannelID(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(
      ChannelID(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processDataPayload(
      ChannelID(senderWorkerId, testWorkerId, isControl = false),
      EndOfUpstream()
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

  "data processor" should "process control messages during data processing" in {
    val dp = mkDataProcessor
    dp.stateManager.transitTo(READY)
    (outputHandler.apply _).expects(*).anyNumberOfTimes()
    (operator.open _).expects().once()
    tuples.foreach { x =>
      (operator.processTuple _).expects(Left(x), 0, dp.pauseManager, dp.asyncRPCClient)
    }
    (operator.processTuple _).expects(
      Right(InputExhausted()),
      0,
      dp.pauseManager,
      dp.asyncRPCClient
    )
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (dp.asyncRPCClient.send[Unit] _).expects(*, *).anyNumberOfTimes()
    dp.registerInput(senderWorkerId, link.id)
    dp.processControlPayload(
      ChannelID(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(
      ChannelID(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      (dp.outputManager.flushAll _).expects().once()
      dp.processControlPayload(
        ChannelID(CONTROLLER, testWorkerId, isControl = true),
        ControlInvocation(0, FlushNetworkBuffer())
      )
      dp.continueDataProcessing()
    }
    (dp.outputManager.emitEndOfUpstream _).expects().once()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (operator.close _).expects().once()
    dp.processDataPayload(
      ChannelID(senderWorkerId, testWorkerId, isControl = false),
      EndOfUpstream()
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

}
