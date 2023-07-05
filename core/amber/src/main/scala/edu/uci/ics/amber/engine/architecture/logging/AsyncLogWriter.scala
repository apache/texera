package edu.uci.ics.amber.engine.architecture.logging

import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.architecture.logging.storage.DeterminantLogStorage.DeterminantLogWriter
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor
import edu.uci.ics.amber.engine.architecture.messaginglayer.NetworkCommunicationActor.SendRequest
import edu.uci.ics.amber.engine.common.AmberUtils

import java.util
import java.util.concurrent.CompletableFuture
import scala.collection.JavaConverters._

class AsyncLogWriter(
    networkCommunicationActor: NetworkCommunicationActor.NetworkSenderActorRef,
    writer: DeterminantLogWriter
) extends Thread {
  private val drained = new util.ArrayList[Either[InMemDeterminant, SendRequest]]()
  private val writerQueue =
    Queues.newLinkedBlockingQueue[Either[InMemDeterminant, SendRequest]]()
  private var stopped = false
  private val logInterval =
    AmberUtils.amberConfig.getLong("fault-tolerance.log-flush-interval-ms")
  private val gracefullyStopped = new CompletableFuture[Unit]()

  def putDeterminants(determinants: Array[InMemDeterminant]): Unit = {
    assert(!stopped)
    determinants.foreach(x => {
      writerQueue.put(Left(x))
    })
  }

  def putOutput(output: SendRequest): Unit = {
    assert(!stopped)
    writerQueue.put(Right(output))
  }

  def terminate(): Unit = {
    stopped = true
    writerQueue.put(Left(TerminateSignal))
    gracefullyStopped.get()
  }

  override def run(): Unit = {
    var internalStop = false
    while (!internalStop) {
      if (logInterval > 0) {
        Thread.sleep(logInterval)
      }
      internalStop = drainWriterQueueAndProcess()
    }
    writer.close()
    gracefullyStopped.complete(())
  }

  def drainWriterQueueAndProcess(): Boolean = {
    var stop = false
    if (writerQueue.drainTo(drained) == 0) {
      drained.add(writerQueue.take())
    }
    var drainedScala = drained.asScala
    if (drainedScala.last == Left(TerminateSignal)) {
      drainedScala = drainedScala.dropRight(1)
      stop = true
    }
    drainedScala
      .filter(_.isLeft)
      .map(_.left.get)
      .foreach(x => writer.writeLogRecord(x))
    writer.flush()
    drainedScala.filter(_.isRight).foreach(x => networkCommunicationActor ! x.right.get)
    drained.clear()
    stop
  }

}
