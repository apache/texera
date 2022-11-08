package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.logging.storage.{
  DeterminantLogStorage,
  LocalFSLogStorage
}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

//In-mem formats:
sealed trait InMemDeterminant
case class LinkChange(linkIdentity: LinkIdentity) extends InMemDeterminant
case class SenderActorChange(actorVirtualIdentity: ActorVirtualIdentity) extends InMemDeterminant
case class StepDelta(steps: Long) extends InMemDeterminant
case class ProcessControlMessage(controlPayload: ControlPayload, from: ActorVirtualIdentity)
    extends InMemDeterminant
case class TimeStamp(value: Long) extends InMemDeterminant
case object TerminateSignal extends InMemDeterminant

object LogManager {
  def getLogManager(
      enabledLogging: Boolean,
      networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef
  ): LogManager = {
    if (enabledLogging) {
      new LogManagerImpl(networkCommunicationActor)
    } else {
      new EmptyLogManagerImpl(networkCommunicationActor)
    }
  }
}

abstract class LogManager {
  def setupWriter(logWriter: DeterminantLogWriter): Unit

  def getDeterminantLogger: DeterminantLogger

  def sendCommitted(sendRequest: SendRequest): Unit

  def terminate(): Unit

}
