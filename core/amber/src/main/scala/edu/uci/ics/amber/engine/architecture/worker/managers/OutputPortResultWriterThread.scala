package edu.uci.ics.amber.engine.architecture.worker.managers

import com.google.common.collect.Queues
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.tuple.Tuple

import java.util.concurrent.LinkedBlockingQueue

sealed trait TerminateSignal
case object PortStorageWriterTerminateSignal extends TerminateSignal

class OutputPortResultWriterThread(
    bufferedItemWriter: BufferedItemWriter[Tuple]
) extends Thread {

  val queue: LinkedBlockingQueue[Either[Tuple, TerminateSignal]] =
    Queues.newLinkedBlockingQueue[Either[Tuple, TerminateSignal]]()

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      val queueContent = queue.take()
      queueContent match {
        case Left(tuple) => bufferedItemWriter.putOne(tuple)
        case Right(_)    => internalStop = true
      }
    }
    bufferedItemWriter.close()
  }
}
