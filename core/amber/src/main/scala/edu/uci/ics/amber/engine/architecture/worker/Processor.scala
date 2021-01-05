package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.messaginglayer.DataInputPort.WorkflowDataMessage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkOutputGate.NetworkMessage
import edu.uci.ics.amber.engine.architecture.worker.neo.WorkerInternalQueue.InputTuple
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{QueryState, _}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.{
  ActorVirtualIdentity,
  WorkerActorVirtualIdentity
}
import edu.uci.ics.amber.engine.common.ambertag.{LayerTag, WorkerTag}
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.{
  AdvancedMessageSending,
  ElidableStatement,
  IOperatorExecutor,
  ITupleSinkOperatorExecutor
}
import edu.uci.ics.amber.engine.faulttolerance.recovery.RecoveryPacket
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.amber.error.WorkflowRuntimeError

import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.mutable

object Processor {
  def props(processor: IOperatorExecutor, tag: WorkerTag): Props =
    Props(new Processor(processor, tag))
}

class Processor(var operator: IOperatorExecutor, val tag: WorkerTag)
    extends WorkerBase(WorkerActorVirtualIdentity(tag.getGlobalIdentity)) {
  var savedModifyLogic: mutable.Queue[(Long, Long, OpExecConfig)] =
    new mutable.Queue[(Long, Long, OpExecConfig)]()

  @elidable(INFO) var processTime = 0L
  @elidable(INFO) var processStart = 0L

  override def onReset(value: Any, recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onReset(value, recoveryInformation)
    operator = value.asInstanceOf[IOperatorExecutor]
    operator.open()
    while (
      savedModifyLogic.nonEmpty && savedModifyLogic.head._1 == 0 && savedModifyLogic.head._2 == 0
    ) {
      savedModifyLogic.dequeue()
    }
    dataProcessor.resetBreakpoints()
    batchProducer.resetPolicies()
    context.become(ready)
    if (receivedRecoveryInformation.contains((0, 0))) {
      self ! Pause
    }
  }

  override def onResuming(): Unit = {
    super.onResuming()
    pauseManager.resume()
  }

  override def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      batchProducer.passTupleToDownstream(faultedTuple.tuple)
    } else {
      dataProcessor.prependElement(InputTuple(faultedTuple.tuple))
    }
  }

  override def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      batchProducer.passTupleToDownstream(faultedTuple.tuple)
    } else {
      dataProcessor.prependElement(InputTuple(faultedTuple.tuple))
    }
  }

  override def onCompleted(): Unit = {
    super.onCompleted()
    ElidableStatement.info {
      log.info(
        "completed its job. total: {} ms, processing: {} ms",
        (System.nanoTime() - startTime) / 1000000,
        processTime / 1000000
      )
    }
  }

  override def getResultTuples(): mutable.MutableList[ITuple] = {
    this.operator match {
      case processor: ITupleSinkOperatorExecutor =>
        mutable.MutableList(processor.getResultTuples(): _*)
      case _ =>
        mutable.MutableList()
    }
  }

  private[this] def waitProcessing: Receive = {
    case ExecutionPaused =>
      context.become(paused)
      onPaused()
      unstashAll()
    case ReportFailure(e) =>
      throw e
    case ExecutionCompleted =>
      onCompleted()
      context.become(completed)
      unstashAll()

    case LocalBreakpointTriggered =>
      onBreakpointTriggered()
      context.become(paused)
      context.become(breakpointTriggered, discardOld = false)
      unstashAll()
    case QueryState => sender ! ReportState(WorkerState.Pausing)
    case msg        => stash()
  }

  override def onPaused(): Unit = {
    val (inputCount, outputCount) = dataProcessor.collectStatistics()
    log.info(s"${tag.getGlobalIdentity} paused at $inputCount , $outputCount")
    context.parent ! ReportCurrentProcessingTuple(self.path, dataProcessor.getCurrentInputTuple)
    context.parent ! RecoveryPacket(tag, inputCount, outputCount)
    context.parent ! ReportState(WorkerState.Paused)
  }


  override def onInitialization(recoveryInformation: Seq[(Long, Long)]): Unit = {
    super.onInitialization(recoveryInformation)
    operator.open()
  }

  override def getInputRowCount(): Long = {
    val (inputCount, _) = dataProcessor.collectStatistics()
    inputCount
  }

  override def getOutputRowCount(): Long = {
    val (_, outputCount) = dataProcessor.collectStatistics()
    outputCount
  }

  final def activateWhenReceiveDataMessages: Receive = {
    case msg @ NetworkMessage(_, data: WorkflowDataMessage) =>
      stash()
      onStart()
      context.become(running)
      unstashAll()
  }

  final def disallowDataMessages: Receive = {
    case msg @ NetworkMessage(_, data: WorkflowDataMessage) =>
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          "not supposed to receive data messages at this time",
          "Principal:disallowDataMessages",
          Map()
        )
      )
  }

  final def receiveDataMessages: Receive = {
    case msg @ NetworkMessage(_, data: WorkflowDataMessage) =>
      dataInputPort.handleDataMessage(data)
  }

  final def allowUpdateInputLinking: Receive = {
    case UpdateInputLinking(identifier, inputNum) =>
      sender ! Ack
      tupleProducer.registerInput(identifier, inputNum)
  }

  final def disallowUpdateInputLinking: Receive = {
    case UpdateInputLinking(edgeID, inputNum) =>
      sender ! Ack
      throw new WorkflowRuntimeException(
        WorkflowRuntimeError(
          s"update input linking of $edgeID is not allowed at this time",
          "Principal:disallowUpdateInputLinking",
          Map()
        )
      )
  }

  final def reactOnUpstreamExhausted: Receive = {
    case ReportUpstreamExhausted(from) =>
      AdvancedMessageSending.nonBlockingAskWithRetry(
        context.parent,
        ReportWorkerPartialCompleted(tag, from),
        10,
        0
      )
  }

  final def allowOperatorLogicUpdate: Receive = {
    case ModifyLogic(newMetadata) =>
      sender ! Ack
      //val json: JsValue = Json.parse(newLogic)
      // val operatorType = json("operatorID").as[String]
      val (inputCount, outputCount) = dataProcessor.collectStatistics()
      savedModifyLogic.enqueue((inputCount, outputCount, newMetadata))
      log.info("modify logic received by worker " + this.self.path.name + ", updating logic")
//      newMetadata match {
//        case filterOpMetadata: FilterOpExecConfig =>
//          val dp = dataProcessor.asInstanceOf[FilterOpExec]
//          dp.filterFunc = filterOpMetadata.filterOpExec().filterFunc
//        case t => throw new NotImplementedError("Unknown operator type: " + t)
//      }
      log.info(
        "modify logic received by worker " + this.self.path.name + ", updating logic completed"
      )
      throw new UnsupportedOperationException("this functionality is temporarily disabled")
  }

  override def postStop(): Unit = {}

  override def ready: Receive =
    activateWhenReceiveDataMessages orElse allowUpdateInputLinking orElse super.ready

  override def pausedBeforeStart: Receive =
    receiveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.pausedBeforeStart

  override def running: Receive =
    receiveDataMessages orElse disallowUpdateInputLinking orElse reactOnUpstreamExhausted orElse super.running

  override def paused: Receive =
    receiveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.paused

  override def breakpointTriggered: Receive =
    receiveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.breakpointTriggered

  override def completed: Receive =
    disallowDataMessages orElse disallowUpdateInputLinking orElse super.completed

  override def newControlMessageHandler: Receive =
    allowUpdateInputLinking orElse super.newControlMessageHandler

}
