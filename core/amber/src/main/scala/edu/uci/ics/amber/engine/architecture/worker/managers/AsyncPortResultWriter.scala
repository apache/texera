package edu.uci.ics.amber.engine.architecture.worker.managers

import com.google.common.collect.Queues
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

sealed trait TerminateSignal
case object TerminateSignalInst extends TerminateSignal

class AsyncPortResultWriter(writer: BufferedItemWriter[Tuple]) extends Thread {

  private val queue = Queues
    .newLinkedBlockingQueue[Either[Tuple, TerminateSignal]]() // (Location, Tuple)
  private var stopped = false
  private val gracefullyStopped = new CompletableFuture[Unit]()

  def putTuple(tuple: Tuple): Unit = {
    assert(!stopped)
    queue.put(Left(tuple)) // Non-blocking enqueue
  }

  def terminate(): Unit = {
    stopped = true
    queue.put(Right(TerminateSignalInst))
    gracefullyStopped.get() // blocks the calling thread until the writer(s) are closed
  }

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      val queueContent = queue.take()
      queueContent match {
        case Left(tuple) => writer.putOne(tuple)
        case Right(_)    => internalStop = true
      }
    }
    writer.close() // Close all writers on termination
    gracefullyStopped.complete(())
  }
}
