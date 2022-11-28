package edu.uci.ics.amber.engine.architecture.logging
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor

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
