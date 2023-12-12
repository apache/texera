package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecInitInfo}
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.messaginglayer.{OutputManager, WorkerTimerService}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.InputExhausted
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
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

class DataProcessorSpec extends AnyFlatSpec with MockFactory with BeforeAndAfterEach {
  private val identifier: ActorVirtualIdentity = ActorVirtualIdentity("DP mock")
  private val senderID: ActorVirtualIdentity = ActorVirtualIdentity("mock sender")
  private val operatorIdentity: OperatorIdentity = OperatorIdentity("testOperator")
  private val operator = mock[OperatorExecutor]
  private val linkID: LinkIdentity =
    LinkIdentity(
      LayerIdentity("testUpstream", "main"),
      0,
      LayerIdentity("testOperator", "main"),
      0
    )
  private val opExecConfig =
    OpExecConfig
      .oneToOneLayer(1, operatorIdentity, OpExecInitInfo(_ => operator))
      .addInput(linkID.from, 0, 0)
  private val outputHandler = mock[WorkflowFIFOMessage => Unit]
  private val adaptiveBatchingMonitor = mock[WorkerTimerService]
  private val tuples: Array[ITuple] = (0 until 400).map(ITuple(_)).toArray

  def mkDataProcessor: DataProcessor = {
    val dp: DataProcessor =
      new DataProcessor(identifier, outputHandler) {
        override val outputManager: OutputManager = mock[OutputManager]
        override val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
      }
    dp.initOperator(0, opExecConfig, Iterator.empty)
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
    dp.registerInput(senderID, linkID)
    dp.processControlPayload(
      ChannelID(CONTROLLER, identifier, true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(ChannelID(senderID, identifier, false), DataFrame(tuples))
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processDataPayload(ChannelID(senderID, identifier, false), EndOfUpstream())
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
    dp.registerInput(senderID, linkID)
    dp.processControlPayload(
      ChannelID(CONTROLLER, identifier, true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(ChannelID(senderID, identifier, false), DataFrame(tuples))
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      (dp.outputManager.flushAll _).expects().once()
      dp.processControlPayload(
        ChannelID(CONTROLLER, identifier, true),
        ControlInvocation(0, FlushNetworkBuffer())
      )
      dp.continueDataProcessing()
    }
    (dp.outputManager.emitEndOfUpstream _).expects().once()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (operator.close _).expects().once()
    dp.processDataPayload(ChannelID(senderID, identifier, false), EndOfUpstream())
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

}
