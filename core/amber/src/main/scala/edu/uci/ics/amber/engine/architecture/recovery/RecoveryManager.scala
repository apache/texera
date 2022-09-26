package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.{
  InMemDeterminant,
  LinkChange,
  ProcessControlMessage,
  ProcessEnd,
  ProcessEndOfAll,
  SenderActorChange,
  StepDelta,
  TimeStamp
}
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogReader
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

class RecoveryManager(logReader: DeterminantLogReader) {

  private val records = new RecordIterator(logReader)
  private val inputMapping = mutable
    .HashMap[ActorVirtualIdentity, LinkedBlockingQueue[InputTuple]]()
    .withDefaultValue(new LinkedBlockingQueue[InputTuple]())
  private val controlMessages = mutable
    .HashMap[ActorVirtualIdentity, mutable.Queue[ControlElement]]()
    .withDefaultValue(new mutable.Queue[ControlElement]())
  private val controlCounter = mutable.HashMap[ActorVirtualIdentity, Int]().withDefaultValue(0)
  private var step = 0L
  private var targetVId: ActorVirtualIdentity = _
  private var cleaned = false

  def acceptInput(tuple: InputTuple): Unit = {
    print("received "+tuple)
    inputMapping(tuple.from).put(tuple)
  }

  def acceptControl(control: ControlElement): Unit = {
    controlMessages(control.from).enqueue(control)
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

  private def getAllStashedInputs: Iterable[InputTuple] = {
    val res = new ArrayBuffer[InputTuple]
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
      x._2.drop(controlCounter(x._1))
      while (x._2.nonEmpty) {
        res.append(x._2.dequeue())
      }
    }
    res
  }

  def stepDecrement(): Unit ={
    if (step > 0) {
      step -= 1
    }
  }

  def isReadyToEmitNextControl: Boolean = {
    step == 0
  }

  def getDeterminant(): InMemDeterminant = {
    val determinant = records.peek()
    records.readNext()
    determinant
  }

  def readNextAndAssignStepDelta(): Unit ={
    records.readNext()
    records.peek() match{
      case StepDelta(steps) =>
        step = steps
      case other => //skip
    }
  }

  def get(): InternalQueueElement = {
    records.peek() match {
      case ProcessEnd =>
        readNextAndAssignStepDelta()
        EndMarker
      case ProcessEndOfAll =>
        readNextAndAssignStepDelta()
        EndOfAllMarker
      case SenderActorChange(actorVirtualIdentity) =>
        readNextAndAssignStepDelta()
        targetVId = actorVirtualIdentity
        get()
      case LinkChange(linkIdentity) =>
        readNextAndAssignStepDelta()
        SenderChangeMarker(linkIdentity)
      case StepDelta(steps) =>
        if(step == 0){
          readNextAndAssignStepDelta()
          get()
        }else if(targetVId != null){
          //wait until input[targetVId] available
          inputMapping(targetVId).take()
        }else{
          throw new RuntimeException("cannot take from vid = null step = "+step)
        }
      case ProcessControlMessage(controlPayload, from) =>
        readNextAndAssignStepDelta()
        controlCounter(from) += 1
        ControlElement(controlPayload, from)
      case TimeStamp(value) => ???
    }
  }
}
