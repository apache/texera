package edu.uci.ics.amber.engine.architecture.recovery

import edu.uci.ics.amber.engine.architecture.logging.{InMemDeterminant, ProcessControlMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class LocalRecoveryManager {

  private val callbacksOnStart = new ArrayBuffer[() => Unit]()
  private val callbacksOnEnd = new ArrayBuffer[() => Unit]()

  def registerOnStart(callback: () => Unit): Unit = {
    callbacksOnStart.append(callback)
  }

  def registerOnEnd(callback: () => Unit): Unit = {
    callbacksOnEnd.append(callback)
  }

  def Start(): Unit = {
    callbacksOnStart.foreach(callback => callback())
  }

  def End(): Unit = {
    callbacksOnEnd.foreach(callback => callback())
  }

  def getFIFOState(iter: Iterator[InMemDeterminant]): Map[ActorVirtualIdentity, Long] = {
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
