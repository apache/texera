package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.Props
import edu.uci.ics.amber.engine.architecture.breakpoint.FaultedTuple
import edu.uci.ics.amber.engine.architecture.receivesemantics.FIFOAccessPort
import edu.uci.ics.amber.engine.architecture.worker.neo.PauseManager
import edu.uci.ics.amber.engine.common.amberexception.AmberException
import edu.uci.ics.amber.engine.common.ambermessage.ControlMessage.{QueryState, _}
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage._
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

import scala.annotation.elidable
import scala.annotation.elidable._
import scala.collection.mutable

object Processor {
  def props(processor: IOperatorExecutor, tag: WorkerTag): Props =
    Props(new Processor(processor, tag))
}

class Processor(var operator: IOperatorExecutor, val tag: WorkerTag) extends WorkerBase {

  val input = new FIFOAccessPort()
  val aliveUpstreams = new mutable.HashSet[LayerTag]
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
    input.reset()
    dataProcessor.resetBreakpoints()
    tupleOutput.resetOutput()
    context.become(ready)
    if (receivedRecoveryInformation.contains((0, 0))) {
      self ! Pause
    }
  }

  override def onResuming(): Unit = {
    super.onResuming()
    pauseManager.resume()
  }

  override def onSkipTuple(faultedTuple: FaultedTuple): Unit = {
    super.onSkipTuple(faultedTuple)
    if (faultedTuple.isInput) {
      tupleInput.getNextInputTuple
    } else {
      //if it's output tuple, it will be ignored
    }
  }

  override def onResumeTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      var i = 0
      while (i < tupleOutput.output.length) {
        tupleOutput.output(i).accept(faultedTuple.tuple)
        i += 1
      }
    } else {
      //if its input tuple, the same breakpoint will be triggered again
    }
  }

  override def onModifyTuple(faultedTuple: FaultedTuple): Unit = {
    if (!faultedTuple.isInput) {
      userFixedTuple = faultedTuple.tuple
    } else {
      throw new NotImplementedError("modify output tuple in processor")
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

  def onSaveDataMessage(seq: Long, payload: Array[ITuple]): Unit = {
    input.preCheck(seq, payload, sender) match {
      case Some(batches) =>
        val currentEdge = input.actorToEdge(sender)
        for (i <- batches)
          workerInternalQueue.addBatch((currentEdge, i))
      case None =>
    }
  }

  def onSaveEndSending(seq: Long): Unit = {
    if (input.registerEnd(sender, seq)) {
      val currentEdge: LayerTag = input.actorToEdge(sender)
      workerInternalQueue.addBatch((currentEdge, null))
    }
  }

  def onReceiveEndSending(seq: Long): Unit = {
    onSaveEndSending(seq)
  }

  def onReceiveDataMessage(seq: Long, payload: Array[ITuple]): Unit = {
    input.preCheck(seq, payload, sender) match {
      case Some(batches) =>
        val currentEdge = input.actorToEdge(sender)
        for (i <- batches)
          workerInternalQueue.addBatch((currentEdge, i))
      case None =>
    }
  }

  override def onPaused(): Unit = {
    val (inputCount, outputCount) = dataProcessor.collectStatistics()
    log.info(s"paused at $inputCount , $outputCount")
    context.parent ! ReportCurrentProcessingTuple(self.path, dataProcessor.getCurrentInputTuple)
    context.parent ! RecoveryPacket(tag, inputCount, outputCount)
    context.parent ! ReportState(WorkerState.Paused)
  }

  override def onPausing(): Unit = {
    super.onPausing()
    pauseManager.pause()
    // if dp thread is blocking on waiting for input tuples:
    if (workerInternalQueue.blockingDeque.isEmpty && tupleInput.isCurrentBatchExhausted) {
      // insert dummy batch to unblock dp thread
      workerInternalQueue.addBatch(null)
    }
    pauseManager.waitForDPThread()
    onPaused()
    context.become(paused)
    unstashAll()
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
    case EndSending(_) | DataMessage(_, _) | RequireAck(_: EndSending) | RequireAck(
          _: DataMessage
        ) =>
      stash()
      onStart()
      context.become(running)
      unstashAll()
  }

  final def disallowDataMessages: Receive = {
    case EndSending(_) | DataMessage(_, _) | RequireAck(_: EndSending) | RequireAck(
          _: DataMessage
        ) =>
      throw new AmberException("not supposed to receive data messages at this time")
  }

  final def saveDataMessages: Receive = {
    case DataMessage(seq, payload) =>
      onSaveDataMessage(seq, payload)
    case RequireAck(msg: DataMessage) =>
      sender ! AckWithSequenceNumber(msg.sequenceNumber)
      onSaveDataMessage(msg.sequenceNumber, msg.payload)
    case EndSending(seq) =>
      onSaveEndSending(seq)
    case RequireAck(msg: EndSending) =>
      sender ! AckOfEndSending
      onSaveEndSending(msg.sequenceNumber)
  }

  final def receiveDataMessages: Receive = {
    case EndSending(seq) =>
      onReceiveEndSending(seq)
    case DataMessage(seq, payload) =>
      onReceiveDataMessage(seq, payload)
    case RequireAck(msg: EndSending) =>
      sender ! AckOfEndSending
      onReceiveEndSending(msg.sequenceNumber)
    case RequireAck(msg: DataMessage) =>
      sender ! AckWithSequenceNumber(msg.sequenceNumber)
      onReceiveDataMessage(msg.sequenceNumber, msg.payload)
  }

  final def allowUpdateInputLinking: Receive = {
    case UpdateInputLinking(inputActor, edgeID, inputNum) =>
      sender ! Ack
      workerInternalQueue.inputMap(edgeID) = inputNum
      aliveUpstreams.add(edgeID)
      input.addSender(inputActor, edgeID)
  }

  final def disallowUpdateInputLinking: Receive = {
    case UpdateInputLinking(inputActor, edgeID, inputNum) =>
      sender ! Ack
      throw new AmberException(s"update input linking of $edgeID is not allowed at this time")
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

  override def postStop(): Unit = {
    input.endToBeReceived.clear()
    input.actorToEdge.clear()
    input.seqNumMap.clear()
    input.endMap.clear()
    aliveUpstreams.clear()
  }

  override def ready: Receive =
    activateWhenReceiveDataMessages orElse allowUpdateInputLinking orElse super.ready

  override def pausedBeforeStart: Receive =
    saveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.pausedBeforeStart

  override def running: Receive =
    receiveDataMessages orElse disallowUpdateInputLinking orElse reactOnUpstreamExhausted orElse super.running

  override def paused: Receive =
    saveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.paused

  override def breakpointTriggered: Receive =
    saveDataMessages orElse allowUpdateInputLinking orElse allowOperatorLogicUpdate orElse super.breakpointTriggered

  override def completed: Receive =
    disallowDataMessages orElse disallowUpdateInputLinking orElse super.completed

}
