package edu.uci.ics.amber.engine.architecture.logging

import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.architecture.logging.determinants.{
  CursorUpdate,
  Determinant,
  DeterminantMessage
}
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest

import java.util

class AsyncLogWriter(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    logStorage: DeterminantLogStorage
) extends Thread {

  case class SendRequestWithCursor(request: SendRequest, cursor: Long)

  private val drained = new util.ArrayList[Either[Determinant, SendRequestWithCursor]]()
  private val writerQueue =
    Queues.newLinkedBlockingQueue[Either[Determinant, SendRequestWithCursor]]()
  @volatile private var stopped = false
  private val logInterval = 500
  private val logWriter = logStorage.getWriter

  def putDeterminant(determinant: Determinant): Unit = {
    writerQueue.put(Left(determinant))
  }

  def putOutput(output: SendRequest, cursor: Long): Unit = {
    writerQueue.put(Right(SendRequestWithCursor(output, cursor)))
  }

  def terminate(): Unit = {
    stopped = true
  }

  override def run(): Unit = {
    while (!stopped) {
      if (logInterval > 0) {
        Thread.sleep(logInterval)
      }
      if (writerQueue.drainTo(drained) == 0) {
        drained.add(writerQueue.take())
      }
      var cursor = 0L
      drained.forEach {
        case Left(value)  => logWriter.writeLogRecord(value.asMessage.toByteArray)
        case Right(value) => cursor = value.cursor
      }
      logWriter.writeLogRecord(CursorUpdate(cursor).toByteArray)
      logWriter.flush()
      drained.forEach {
        case Left(value)  => // skip
        case Right(value) => networkCommunicationActor ! value.request
      }
      drained.clear()
    }
    logWriter.close()
  }

}
