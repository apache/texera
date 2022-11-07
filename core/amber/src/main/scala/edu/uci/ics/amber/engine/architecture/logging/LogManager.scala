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
case class SenderChange(actorVirtualIdentity: ActorVirtualIdentity) extends InMemDeterminant
case class StepDelta(steps: Long) extends InMemDeterminant
case class ProcessControlMessage(controlPayload: ControlPayload, from: ActorVirtualIdentity)
    extends InMemDeterminant
case class TimeStamp(value: Long) extends InMemDeterminant

class LogManager(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    logName: String
) {

  val enabledLogging: Boolean =
    AmberUtils.amberConfig.getBoolean("fault-tolerance.enable-determinant-logging")

  private val determinantLogger = if (enabledLogging) {
    new DeterminantLogger()
  } else {
    null
  }

  private val logStorage: DeterminantLogStorage = if (enabledLogging) {
    new LocalFSLogStorage(logName)
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

  def getDeterminantLogger: DeterminantLogger = determinantLogger

  def sendDirectlyOrCommitted(sendRequest: SendRequest): Unit = {
    if (!enabledLogging) {
      networkCommunicationActor ! sendRequest
    } else {
      writer.putDeterminants(determinantLogger.drainCurrentLogRecords())
      writer.putOutput(sendRequest)
    }
  }

  def terminate(): Unit = {
    if (enabledLogging) {
      writer.terminate()
    }
  }

}
