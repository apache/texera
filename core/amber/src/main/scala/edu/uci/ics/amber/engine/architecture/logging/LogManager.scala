package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.logging.storage.{DeterminantLogStorage, LocalFSLogStorage}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayload
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}

//In-mem formats:
sealed trait InMemDeterminant
case object ProcessEnd extends InMemDeterminant
case object ProcessEndOfAll extends InMemDeterminant
case class LinkChange(linkIdentity: LinkIdentity) extends InMemDeterminant
case class SenderActorChange(actorVirtualIdentity: ActorVirtualIdentity) extends InMemDeterminant
case class StepDelta(steps: Long) extends InMemDeterminant
case class ProcessControlMessage(controlPayload: ControlPayload, from: ActorVirtualIdentity)
    extends InMemDeterminant
case class TimeStamp(value: Long) extends InMemDeterminant

class LogManager(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    logWriter: DeterminantLogWriter
) {


  private val enabledLogging = logWriter == null

  private val determinantLogger = if (enabledLogging) {
    new DeterminantLogger()
  } else {
    null
  }

  private val writer = if (enabledLogging) {
    val res = new AsyncLogWriter(networkCommunicationActor, logWriter)
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
