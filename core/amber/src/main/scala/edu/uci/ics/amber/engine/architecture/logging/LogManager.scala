package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.storage.{
  DeterminantLogStorage,
  LocalFSLogStorage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

//In-mem formats:
sealed trait InMemDeterminant
case class SenderChangeTo(actorVirtualIdentity: ActorVirtualIdentity) extends InMemDeterminant
case object ProcessDataTuple extends InMemDeterminant
case class ProcessControlMessage(controlPayload:ControlPayload, from:ActorVirtualIdentity) extends InMemDeterminant

class LogManager(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    actorId: ActorVirtualIdentity
) {

  val enabledLogging: Boolean =
    AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")

  private val logStorage: DeterminantLogStorage = if (enabledLogging) {
    new LocalFSLogStorage(actorId.name.replace("Worker:", ""))
  } else {
    null
  }

  private val writer = if (enabledLogging) {
    val res = new AsyncLogWriter(networkCommunicationActor, logStorage)
    res.start()
    res
  } else {
    null
  }

  def logInMemDeterminant(determinant: InMemDeterminant): Unit = {
    if (!enabledLogging) {
      return
    }
    writer.putDeterminant(determinant)
  }

  def sendDirectlyOrCommitted(sendRequest: SendRequest): Unit = {
    if (!enabledLogging) {
      networkCommunicationActor ! sendRequest
    } else {
      writer.putOutput(sendRequest)
    }
  }

  def terminate(): Unit = {
    if (enabledLogging) {
      writer.terminate()
    }
  }

}
