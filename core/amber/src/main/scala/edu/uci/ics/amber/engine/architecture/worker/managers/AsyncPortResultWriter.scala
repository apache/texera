package edu.uci.ics.amber.engine.architecture.worker.managers

import com.google.common.collect.Queues
import edu.uci.ics.amber.core.storage.model.BufferedItemWriter
import edu.uci.ics.amber.core.tuple.Tuple
import edu.uci.ics.amber.core.workflow.PortIdentity

import java.util.concurrent.CompletableFuture
import scala.collection.mutable

sealed trait TerminateSignal
case object TerminateSignalInst extends TerminateSignal

class AsyncPortResultWriter extends Thread {

  private val writerMap: mutable.Map[PortIdentity, BufferedItemWriter[Tuple]] =
    mutable.HashMap[PortIdentity, BufferedItemWriter[Tuple]]()
  private val queue = Queues
    .newLinkedBlockingQueue[Either[(PortIdentity, Tuple), TerminateSignal]]() // (Location, Tuple)
  private var stopped = false
  private val gracefullyStopped = new CompletableFuture[Unit]()

  def addWriter(location: PortIdentity, writer: BufferedItemWriter[Tuple]): Unit = {
    writerMap.put(location, writer)
  }

  def putTuple(location: PortIdentity, tuple: Tuple): Unit = {
    assert(!stopped)
    queue.put(Left((location, tuple))) // Non-blocking enqueue
  }

  def terminate(): Unit = {
    stopped = true
    queue.put(Right(TerminateSignalInst))
    gracefullyStopped.get()
  }

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      val queueContent = queue.take()
      queueContent match {
        case Left((location, tuple)) => writerMap.get(location).foreach(_.putOne(tuple))
        case Right(_)                => internalStop = true
      }
    }
    writerMap.values.foreach(_.close()) // Close all writers on termination
    gracefullyStopped.complete(())
  }
}
