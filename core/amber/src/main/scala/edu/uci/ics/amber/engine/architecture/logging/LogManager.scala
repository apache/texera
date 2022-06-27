package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.determinants.{ControlInput, DataInputOrder}
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.ambermessage.ControlPayloadV2
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity

class LogManager(writer: AsyncLogWriter) {

  private var cursor: Long = 0L
  private var prevCount: Long = 0L
  private var currentInput: ActorVirtualIdentity = _

  def logDataInputOrder(actorVirtualIdentity: ActorVirtualIdentity): Unit = {
    if (currentInput == null || currentInput != actorVirtualIdentity) {
      writer.putDeterminant(DataInputOrder(prevCount, actorVirtualIdentity))
      prevCount = 0
      currentInput = actorVirtualIdentity
    }
    cursor += 1
    prevCount += 1
  }

  def logControlInput(command: ControlPayloadV2, from: ActorVirtualIdentity): Unit = {
    writer.putDeterminant(ControlInput(cursor, command, from))
    cursor += 1
  }

  def sendAfterCommit(sendRequest: SendRequest): Unit = {
    writer.putOutput(sendRequest, cursor)
  }

}
