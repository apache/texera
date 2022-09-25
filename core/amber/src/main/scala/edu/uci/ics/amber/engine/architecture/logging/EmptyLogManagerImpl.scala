package edu.uci.ics.amber.engine.architecture.logging
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor

class EmptyLogManagerImpl extends LogManager {
  override def setupWriter(logWriter: DeterminantLogStorage.DeterminantLogWriter): Unit = {}

  override def getDeterminantLogger: DeterminantLogger = new EmptyDeterminantLogger()

  override def sendDirectlyOrCommitted(
      sendRequest: NetworkCommunicationActor.SendRequest
  ): Unit = {}

  override def terminate(): Unit = {}
}
