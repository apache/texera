package edu.uci.ics.amber.engine.architecture.logging

import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest

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
