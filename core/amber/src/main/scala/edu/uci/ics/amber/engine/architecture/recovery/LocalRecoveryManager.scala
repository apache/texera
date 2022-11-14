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

class LocalRecoveryManager(recoveryQueue: RecoveryQueue) {

  private val callbacksOnStart = new ArrayBuffer[() => Unit]()

  def registerOnStart(callback: () => Unit): Unit = {
    callbacksOnStart.append(callback)
  }

  def registerOnEnd(callback: () => Unit): Unit = {
    recoveryQueue.registerOnEnd(callback)
  }

  def Start(): Unit = {
    callbacksOnStart.foreach(callback => callback())
  }

  def getFIFOState(reader: DeterminantLogReader): Map[ActorVirtualIdentity, Long] = {
    val iter = reader.mkLogRecordIterator()
    val fifoState = new mutable.AnyRefMap[ActorVirtualIdentity, Long]()
    while (iter.hasNext) {
      iter.next() match {
        case ProcessControlMessage(controlPayload, from) =>
          if (fifoState.contains(from)) {
            fifoState(from) += 1
          } else {
            fifoState(from) = 1
          }
        case other => //skip
      }
    }
    fifoState.toMap
  }

}
