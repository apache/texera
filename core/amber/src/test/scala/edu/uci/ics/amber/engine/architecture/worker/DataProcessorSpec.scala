package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.messaginglayer.OutputManager.FlushNetworkBuffer
import edu.uci.ics.amber.engine.architecture.messaginglayer.{OutputManager, WorkerTimerService}
import edu.uci.ics.amber.engine.architecture.scheduling.config.{OperatorConfig, WorkerConfig}
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.{InputExhausted, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.ambermessage.{DataFrame, EndOfUpstream, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.ControlInvocation
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  ChannelIdentity,
  OperatorIdentity,
  PhysicalOpIdentity
}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext.{
  DEFAULT_EXECUTION_ID,
  DEFAULT_WORKFLOW_ID
}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec

import scala.collection.mutable

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

  private val operator = mock[OperatorExecutor]
  private val upstreamOp =
    PhysicalOp(id = upstreamOpId, DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, opExecInitInfo = null)
  private val testOp =
    PhysicalOp(id = testOpId, DEFAULT_WORKFLOW_ID, DEFAULT_EXECUTION_ID, opExecInitInfo = null)
  private val inputPortId = PortIdentity()
  private val link = PhysicalLink(upstreamOp.id, PortIdentity(), testOp.id, inputPortId)
  private val physicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        DEFAULT_WORKFLOW_ID,
        DEFAULT_EXECUTION_ID,
        testOpId.logicalOpId,
        OpExecInitInfo((_, _, _) => operator)
      )
      .withInputPorts(List(InputPort()), mutable.Map(PortIdentity() -> null))
      .withOutputPorts(List(OutputPort()), mutable.Map(PortIdentity() -> null))
      .addInputLink(link)
  private val outputHandler = mock[Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit]
  private val adaptiveBatchingMonitor = mock[WorkerTimerService]
  private val tuples: Array[Tuple] = (0 until 400)
    .map(i =>
      TupleLike(i).enforceSchema(Schema.builder().add("field1", AttributeType.INTEGER).build())
    )
    .toArray

  def mkDataProcessor: DataProcessor = {
    val dp: DataProcessor =
      new DataProcessor(testWorkerId, outputHandler) {
        override val outputManager: OutputManager = mock[OutputManager]
        override val asyncRPCClient: AsyncRPCClient = mock[AsyncRPCClient]
      }
    dp.initOperator(0, physicalOp, OperatorConfig(List(WorkerConfig(testWorkerId))), Iterator.empty)
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
      (
          (
              tuple: Either[Tuple, InputExhausted],
              input: Int
          ) => operator.processTupleMultiPort(tuple, input)
      )
        .expects(Left(x), 0)
    }
    (
        (
            tuple: Either[Tuple, InputExhausted],
            input: Int
        ) => operator.processTupleMultiPort(tuple, input)
    )
      .expects(
        Right(InputExhausted()),
        0
      )
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (dp.asyncRPCClient.send[Unit] _).expects(*, *).anyNumberOfTimes()
    (dp.outputManager.emitEndOfUpstream _).expects().once()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (operator.close _).expects().once()
    dp.inputGateway.addPort(inputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.processControlPayload(
      ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
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
      (
          (
              tuple: Either[Tuple, InputExhausted],
              input: Int
          ) => operator.processTupleMultiPort(tuple, input)
      )
        .expects(Left(x), 0)
    }
    (
        (
            tuple: Either[Tuple, InputExhausted],
            input: Int
        ) => operator.processTupleMultiPort(tuple, input)
    )
      .expects(Right(InputExhausted()), 0)
    (adaptiveBatchingMonitor.startAdaptiveBatching _).expects().anyNumberOfTimes()
    (dp.asyncRPCClient.send[Unit] _).expects(*, *).anyNumberOfTimes()
    dp.inputGateway.addPort(inputPortId)
    dp.inputGateway
      .getChannel(ChannelIdentity(senderWorkerId, testWorkerId, isControl = false))
      .setPortId(inputPortId)
    dp.processControlPayload(
      ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
      ControlInvocation(0, OpenOperator())
    )
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      DataFrame(tuples)
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      (dp.outputManager.flush _).expects(None).once()
      dp.processControlPayload(
        ChannelIdentity(CONTROLLER, testWorkerId, isControl = true),
        ControlInvocation(0, FlushNetworkBuffer())
      )
      dp.continueDataProcessing()
    }
    (dp.outputManager.emitEndOfUpstream _).expects().once()
    (adaptiveBatchingMonitor.stopAdaptiveBatching _).expects().once()
    (operator.close _).expects().once()
    dp.processDataPayload(
      ChannelIdentity(senderWorkerId, testWorkerId, isControl = false),
      EndOfUpstream()
    )
    while (dp.hasUnfinishedInput || dp.hasUnfinishedOutput) {
      dp.continueDataProcessing()
    }
  }

}
