package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.{ActorRef, Props}
import akka.util.Timeout
import com.softwaremill.macwire.wire
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.common.WorkflowActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlInputPort.WorkflowControlMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.{
  NetworkAck,
  NetworkMessage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  DataInputPort,
  DataOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.{
  EndMarker,
  EndOfAllMarker,
  InputTuple
}
import edu.uci.ics.amber.engine.architecture.worker.neo._
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler.WorkerPause
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage._
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient.{ControlInvocation, ReturnPayload}
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCHandlerInitializer, AsyncRPCServer}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager._
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{
  IOperatorExecutor,
  ISourceOperatorExecutor,
  ITupleSinkOperatorExecutor
}
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.annotation.elidable
import scala.annotation.elidable.INFO
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object WorkflowWorker {
  def props(
      id: ActorVirtualIdentity,
      op: IOperatorExecutor,
      parentNetworkCommunicationActorRef: ActorRef
  ): Props =
    Props(new WorkflowWorker(id, op, parentNetworkCommunicationActorRef))
}

class WorkflowWorker(
    identifier: ActorVirtualIdentity,
    operator: IOperatorExecutor,
    parentNetworkCommunicationActorRef: ActorRef
) extends WorkflowActor(identifier, parentNetworkCommunicationActorRef) {
  implicit val ec: ExecutionContext = context.dispatcher
  implicit val timeout: Timeout = 5.seconds

  lazy val pauseManager: PauseManager = wire[PauseManager]
  lazy val dataProcessor: DataProcessor = wire[DataProcessor]
  lazy val dataInputPort: DataInputPort = wire[DataInputPort]
  lazy val dataOutputPort: DataOutputPort = wire[DataOutputPort]
  lazy val batchProducer: TupleToBatchConverter = wire[TupleToBatchConverter]
  lazy val tupleProducer: BatchToTupleConverter = wire[BatchToTupleConverter]
  lazy val workerStateManager: WorkerStateManager = wire[WorkerStateManager]

  val rpcHandlerInitializer: AsyncRPCHandlerInitializer =
    wire[WorkerAsyncRPCHandlerInitializer]

  val receivedFaultedTupleIds: mutable.HashSet[Long] = new mutable.HashSet[Long]()
  var isCompleted = false
  @elidable(INFO) var startTime = 0L

  def onSkipTuple(faultedTuple: FaultedTuple): Unit = {
    if (faultedTuple.isInput) {
      dataProcessor.setCurrentTuple(null)
    } else {
      // if it's output tuple, it will be ignored
    }
  }

  def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      batchProducer.passTupleToDownstream(faultedTuple.tuple)
    } else {
      dataProcessor.prependElement(InputTuple(faultedTuple.tuple))
    }
  }

  def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      batchProducer.passTupleToDownstream(faultedTuple.tuple)
    } else {
      dataProcessor.prependElement(InputTuple(faultedTuple.tuple))
    }
  }

  def getInputRowCount(): Long = {
    val (inputCount, _) = dataProcessor.collectStatistics()
    inputCount
  }

  def getOutputRowCount(): Long = {
    val (_, outputCount) = dataProcessor.collectStatistics()
    outputCount
  }

  def getResultTuples(): mutable.MutableList[ITuple] = {
    this.operator match {
      case processor: ITupleSinkOperatorExecutor =>
        mutable.MutableList(processor.getResultTuples(): _*)
      case _ =>
        mutable.MutableList()
    }
  }

  override def receive: Receive = receiveAndProcessMessages

  def receiveAndProcessMessages: Receive = {
    disallowActorRefRelatedMessages orElse
      processControlMessages orElse
      oldControlMessageHandler orElse
      receiveDataMessages orElse {
      case other =>
        logger.logError(
          WorkflowRuntimeError(s"unhandled message: $other", identifier.toString, Map.empty)
        )
    }
  }

  def oldControlMessageHandlingLogic: Receive = {
    case ExecutionCompleted() =>
      workerStateManager.confirmState(Running)
      isCompleted = true
      workerStateManager.transitTo(Completed)
      reportState()
    case ReturnPayload(_, v: ExecutionPaused) =>
      workerStateManager.confirmState(Pausing)
      workerStateManager.transitTo(Paused)
      reportState()
    case UpdateInputLinking(identifier, inputNum) =>
      workerStateManager.confirmState(Ready)
      logger.logInfo(s"received register input for ${this.identifier}")
      sender ! Ack
      tupleProducer.registerInput(identifier, inputNum)
    case LocalBreakpointTriggered() =>
      workerStateManager.confirmState(Running)
      dataProcessor.breakpoints.foreach { brk =>
        if (brk.isTriggered)
          dataProcessor.unhandledFaultedTuples(brk.triggeredTupleId) =
            new FaultedTuple(brk.triggeredTuple, brk.triggeredTupleId, brk.isInput)
      }
      context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
    case other =>
  }

  def getOldWorkerState: WorkerState.Value = {
    workerStateManager.getCurrentState match {
      case UnInitialized =>
        WorkerState.Uninitialized
      case Ready =>
        WorkerState.Ready
      case Running =>
        WorkerState.Running
      case Pausing =>
        WorkerState.Pausing
      case Paused =>
        WorkerState.Paused
      case Completed =>
        WorkerState.Completed
      case Recovering =>
        WorkerState.Running
    }
  }

  def reportState(): Unit = context.parent ! ReportState(getOldWorkerState)

  def oldControlMessageHandler: Receive = {
    case Start =>
      sender ! Ack
      workerStateManager.confirmState(Ready)
      if (operator.isInstanceOf[ISourceOperatorExecutor]) {
        dataProcessor.appendElement(EndMarker())
        dataProcessor.appendElement(EndOfAllMarker())
        workerStateManager.transitTo(Running)
        reportState()
      } else {
        logger.logError(
          WorkflowRuntimeError(
            "unexpected Start message for non-source operator!",
            identifier.toString,
            Map.empty
          )
        )
      }
    case Pause =>
      if (workerStateManager.getCurrentState != Completed) {
        workerStateManager.confirmState(Running, Ready)
        asyncRPCClient.send(WorkerPause(), identifier) //send to myself
        workerStateManager.transitTo(Pausing)
      }
      reportState()
    case Resume =>
      if (workerStateManager.getCurrentState != Completed) {
        pauseManager.resume()
        workerStateManager.transitTo(Running)
      }
      reportState()
    case AckedWorkerInitialization(recoveryInformation) =>
      workerStateManager.confirmState(UnInitialized)
      operator.open()
      workerStateManager.transitTo(Ready)
      reportState()
    case QueryState =>
      reportState()
    case QueryStatistics =>
      sender ! ReportStatistics(
        WorkerStatistics(getOldWorkerState, getInputRowCount(), getOutputRowCount())
      )
    case QueryTriggeredBreakpoints =>
      val toReport = dataProcessor.breakpoints.filter(_.isTriggered)
      if (toReport.nonEmpty) {
        toReport.foreach(_.isReported = true)
        sender ! ReportedTriggeredBreakpoints(toReport)
      } else {
        throw new WorkflowRuntimeException(
          WorkflowRuntimeError(
            "no triggered local breakpoints but worker in triggered breakpoint state",
            "WorkerBase:allowQueryTriggeredBreakpoints",
            Map()
          )
        )
      }
    case QueryBreakpoint(id) =>
      val toReport = dataProcessor.breakpoints.find(_.id == id)
      if (toReport.isDefined) {
        toReport.get.isReported = true
        context.parent ! ReportedQueriedBreakpoint(toReport.get)
        context.parent ! ReportState(WorkerState.LocalBreakpointTriggered)
      }
    case CollectSinkResults =>
      sender ! WorkerMessage.ReportOutputResult(this.getResultTuples().toList)
    case AssignBreakpoint(bp) =>
      sender ! Ack
      dataProcessor.registerBreakpoint(bp)
//      if (!dataProcessor.breakpoints.exists(_.isDirty)) {
//        onPaused() //back to paused
//        context.unbecome()
//        unstashAll()
//      }
    case RemoveBreakpoint(id) =>
      sender ! Ack
      dataProcessor.removeBreakpoint(id)
//      if (!dataProcessor.breakpoints.exists(_.isDirty)) {
//        onPaused() //back to paused
//        context.unbecome()
//        unstashAll()
//      }
    case SkipTuple(f) =>
      workerStateManager.confirmState(Paused)
      sender ! Ack
      if (!receivedFaultedTupleIds.contains(f.id)) {
        receivedFaultedTupleIds.add(f.id)
        dataProcessor.unhandledFaultedTuples.remove(f.id)
        onSkipTuple(f)
      }
    case ModifyTuple(f) =>
      workerStateManager.confirmState(Paused)
      sender ! Ack
      if (!receivedFaultedTupleIds.contains(f.id)) {
        receivedFaultedTupleIds.add(f.id)
        dataProcessor.unhandledFaultedTuples.remove(f.id)
        onModifyTuple(f)
      }
    case ResumeTuple(f) =>
      workerStateManager.confirmState(Paused)
      sender ! Ack
      if (!receivedFaultedTupleIds.contains(f.id)) {
        receivedFaultedTupleIds.add(f.id)
        dataProcessor.unhandledFaultedTuples.remove(f.id)
        onResumeTuple(f)
      }
    case UpdateOutputLinking(policy, tag, receivers) =>
      sender ! Ack
      batchProducer.addPolicy(policy, tag, receivers)
  }

  final def receiveDataMessages: Receive = {
    case msg @ NetworkMessage(id, data: WorkflowDataMessage) =>
      if (workerStateManager.getCurrentState == Ready) {
        workerStateManager.transitTo(Running)
        reportState()
      }
      sender ! NetworkAck(id)
      dataInputPort.handleDataMessage(data)
  }

  def processControlMessages: Receive = {
    case msg @ NetworkMessage(id, cmd: WorkflowControlMessage) =>
      logger.logInfo(s"received ${msg.internalMessage}")
      sender ! NetworkAck(id)
      // use control input port to pass control messages
      controlInputPort.handleControlMessage(cmd)
      // for compatibility, call the old control message handling logic
      oldControlMessageHandlingLogic(cmd.payload)
  }

}
