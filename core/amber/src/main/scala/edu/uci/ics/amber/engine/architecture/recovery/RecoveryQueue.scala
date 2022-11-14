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

class RecoveryQueue(logReader: DeterminantLogReader) {
  private val records = logReader.mkLogRecordIterator()
  private val inputMapping = mutable
    .HashMap[ActorVirtualIdentity, LinkedBlockingQueue[InternalQueueElement]]()
  private val controlMessages = mutable
    .HashMap[ActorVirtualIdentity, mutable.Queue[ControlElement]]()
  private var step = 0L
  private var targetVId: ActorVirtualIdentity = _
  private var currentInputSender: ActorVirtualIdentity = _
  private var cleaned = false
  private val callbacksOnEnd = new ArrayBuffer[() => Unit]()
  private var endCallbackTriggered = false

  def registerOnEnd(callback: () => Unit): Unit = {
    callbacksOnEnd.append(callback)
  }

  def isReplayCompleted: Boolean = {
    val res = !records.hasNext
    if (res && !endCallbackTriggered) {
      endCallbackTriggered = true
      callbacksOnEnd.foreach(callback => callback())
    }
    res
  }

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

  def add(elem: InternalQueueElement): Unit = {
    elem match {
      case tuple: InputTuple =>
        currentInputSender = tuple.from
        inputMapping
          .getOrElseUpdate(tuple.from, new LinkedBlockingQueue[InternalQueueElement]())
          .put(tuple)
      case SenderChangeMarker(newUpstreamLink) =>
      //ignore, we use log to enforce original order
      case control: ControlElement =>
        controlMessages
          .getOrElseUpdate(control.from, new mutable.Queue[ControlElement]())
          .enqueue(control)
      case WorkerInternalQueue.EndMarker =>
        inputMapping
          .getOrElseUpdate(currentInputSender, new LinkedBlockingQueue[InternalQueueElement]())
          .put(EndMarker)
      case WorkerInternalQueue.EndOfAllMarker =>
        inputMapping
          .getOrElseUpdate(currentInputSender, new LinkedBlockingQueue[InternalQueueElement]())
          .put(EndOfAllMarker)
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

  def isReadyToEmitNextControl: Boolean = {
    step -= 1
    step == 0
  }

  def popDeterminant(): InMemDeterminant = {
    records.next()
  }

  def processInternalEvents(): InMemDeterminant = {
    var res: InMemDeterminant = null
    while (res == null) {
      records.next() match {
        case StepDelta(steps) =>
          step += steps
        case SenderActorChange(actorVirtualIdentity) =>
          targetVId = actorVirtualIdentity
        case other =>
          res = other
      }
    }
    res
  }

  def get(): InternalQueueElement = {
    if (step > 0) {
      //wait until input[targetVId] available
      val res = inputMapping
        .getOrElseUpdate(targetVId, new LinkedBlockingQueue[InternalQueueElement]())
        .take()
      res
    } else {
      processInternalEvents() match {
        case SenderActorChange(actorVirtualIdentity) =>
          throw new RuntimeException("cannot handle sender actor change here!")
        case LinkChange(linkIdentity) =>
          SenderChangeMarker(linkIdentity)
        case StepDelta(steps) =>
          throw new RuntimeException("cannot handle step delta here!")
        case ProcessControlMessage(controlPayload, from) =>
          ControlElement(controlPayload, from)
        case TimeStamp(value) => ???
      }
    }
  }
}
