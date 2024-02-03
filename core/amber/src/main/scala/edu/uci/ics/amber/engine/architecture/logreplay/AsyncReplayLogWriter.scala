package edu.uci.ics.amber.engine.architecture.logreplay

import com.google.common.collect.Queues
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter

import java.util
import java.util.concurrent.CompletableFuture
import scala.jdk.CollectionConverters.ListHasAsScala

class AsyncReplayLogWriter(
    handler: WorkflowFIFOMessage => Unit,
    writer: SequentialRecordWriter[ReplayLogRecord]
) extends Thread {
  private val drained = new util.ArrayList[Either[ReplayLogRecord, WorkflowFIFOMessage]]()
  private val writerQueue =
    Queues.newLinkedBlockingQueue[Either[ReplayLogRecord, WorkflowFIFOMessage]]()
  private var stopped = false
  private val logInterval =
    AmberConfig.faultToleranceLogFlushIntervalInMs
  private val gracefullyStopped = new CompletableFuture[Unit]()

  def putLogRecords(records: Array[ReplayLogRecord]): Unit = {
    assert(!stopped)
    records.foreach(x => {
      writerQueue.put(Left(x))
    })
  }

  def putOutput(output: WorkflowFIFOMessage): Unit = {
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
      .foreach(x => writer.writeRecord(x))
    writer.flush()
    drainedScala.filter(_.isRight).foreach(x => handler(x.right.get))
    drained.clear()
    stop
  }

}
