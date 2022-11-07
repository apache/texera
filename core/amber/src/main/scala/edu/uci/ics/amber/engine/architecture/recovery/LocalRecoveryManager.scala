package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.{
  InMemDeterminant,
  LinkChange,
  ProcessControlMessage,
  SenderActorChange,
  StepDelta,
  TimeStamp
}
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogReader
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue
import edu.uci.ics.amber.engine.architecture.worker.WorkerInternalQueue.{
  ControlElement,
  EndMarker,
  EndOfAllMarker,
  InputTuple,
  InternalQueueElement,
  SenderChangeMarker
}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import lbmq.LinkedBlockingMultiQueue

import java.util.concurrent.LinkedBlockingQueue
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LocalRecoveryManager(logReader: DeterminantLogReader) {

  private val records = new RecordIterator(logReader)
  private val inputMapping = mutable
    .HashMap[ActorVirtualIdentity, LinkedBlockingQueue[InternalQueueElement]]()
    .withDefaultValue(new LinkedBlockingQueue[InternalQueueElement]())
  private val controlMessages = mutable
    .HashMap[ActorVirtualIdentity, mutable.Queue[ControlElement]]()
    .withDefaultValue(new mutable.Queue[ControlElement]())
  private var step = 0L
  private var targetVId: ActorVirtualIdentity = _
  private var currentInputSender: ActorVirtualIdentity = _
  private var cleaned = false

  def add(elem: InternalQueueElement): Unit = {
    elem match {
      case tuple: InputTuple =>
        currentInputSender = tuple.from
        inputMapping(tuple.from).put(tuple)
      case SenderChangeMarker(newUpstreamLink) =>
      //ignore, we use log to enforce original order
      case control: ControlElement =>
        controlMessages(control.from).enqueue(control)
      case WorkerInternalQueue.EndMarker =>
        inputMapping(currentInputSender).put(EndMarker)
      case WorkerInternalQueue.EndOfAllMarker =>
        inputMapping(currentInputSender).put(EndOfAllMarker)
    }
  }

  def replayCompleted(): Boolean = records.isEmpty

  def drainAllStashedElements(
      dataQueue: LinkedBlockingMultiQueue[Int, InternalQueueElement]#SubQueue,
      controlQueue: LinkedBlockingMultiQueue[Int, InternalQueueElement]#SubQueue
  ): Unit = {
    if (!cleaned) {
      getAllStashedInputs.foreach(dataQueue.add)
      getAllStashedControls.foreach(controlQueue.add)
      cleaned = true
    }
  }

  private def getAllStashedInputs: Iterable[InternalQueueElement] = {
    val res = new ArrayBuffer[InternalQueueElement]
    inputMapping.values.foreach { x =>
      while (!x.isEmpty) {
        res.append(x.take())
      }
    }
    res
  }

  private def getAllStashedControls: Iterable[ControlElement] = {
    val res = new ArrayBuffer[ControlElement]
    controlMessages.foreach { x =>
      while (x._2.nonEmpty) {
        res.append(x._2.dequeue())
      }
    }
    res
  }

  def stepDecrement(): Unit = {
    if (step > 0) {
      step -= 1
    }
  }

  def isReadyToEmitNextControl: Boolean = {
    step == 0
  }

  def popDeterminant(): InMemDeterminant = {
    val determinant = records.peek()
    records.readNext()
    determinant
  }

  def readNextAndAssignStepDelta(): Unit = {
    records.readNext()
    records.peek() match {
      case StepDelta(steps) =>
        step = steps
      case other => //skip
    }
  }

  def get(): InternalQueueElement = {
    records.peek() match {
      case SenderActorChange(actorVirtualIdentity) =>
        readNextAndAssignStepDelta()
        targetVId = actorVirtualIdentity
        get()
      case LinkChange(linkIdentity) =>
        readNextAndAssignStepDelta()
        SenderChangeMarker(linkIdentity)
      case StepDelta(steps) =>
        if (step == 0) {
          readNextAndAssignStepDelta()
          get()
        } else {
          //wait until input[targetVId] available
          inputMapping(targetVId).take()
        }
      case ProcessControlMessage(controlPayload, from) =>
        readNextAndAssignStepDelta()
        ControlElement(controlPayload, from)
      case TimeStamp(value) => ???
    }
  }
}
