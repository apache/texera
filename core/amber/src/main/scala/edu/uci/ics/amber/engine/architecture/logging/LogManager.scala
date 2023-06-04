package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

//In-mem formats:
sealed trait InMemDeterminant
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

trait LogManager {
  def setupWriter(logWriter: DeterminantLogWriter): Unit

  def getDeterminantLogger: DeterminantLogger

  def sendCommitted(sendRequest: SendRequest): Unit

  def terminate(): Unit

}

class EmptyLogManagerImpl(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef
) extends LogManager {
  override def setupWriter(logWriter: DeterminantLogStorage.DeterminantLogWriter): Unit = {}

  override def getDeterminantLogger: DeterminantLogger = new EmptyDeterminantLogger()

  override def sendCommitted(
      sendRequest: NetworkCommunicationActor.SendRequest
  ): Unit = {
    networkCommunicationActor ! sendRequest
  }

  override def terminate(): Unit = {}
}

class LogManagerImpl(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef
) extends LogManager {

  private val determinantLogger = new DeterminantLoggerImpl()

  private var writer: AsyncLogWriter = _

  def setupWriter(logWriter: DeterminantLogWriter): Unit = {
    writer = new AsyncLogWriter(networkCommunicationActor, logWriter)
    writer.start()
  }

  def getDeterminantLogger: DeterminantLogger = determinantLogger

  def sendCommitted(sendRequest: SendRequest): Unit = {
    writer.putDeterminants(determinantLogger.drainCurrentLogRecords())
    writer.putOutput(sendRequest)
  }

  def terminate(): Unit = {
    writer.terminate()
  }

}
