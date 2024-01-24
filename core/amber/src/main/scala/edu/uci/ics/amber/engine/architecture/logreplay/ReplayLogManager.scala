package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter
import edu.uci.ics.amber.engine.common.storage.{EmptyRecordStorage, SequentialRecordStorage}
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, ChannelMarkerIdentity}

//In-mem formats:
sealed trait ReplayLogRecord

case class MessageContent(message: WorkflowFIFOMessage) extends ReplayLogRecord
case class ProcessingStep(channelId: ChannelIdentity, step: Long) extends ReplayLogRecord
case class ReplayDestination(id: ChannelMarkerIdentity) extends ReplayLogRecord
case object TerminateSignal extends ReplayLogRecord

object ReplayLogManager {
  def createLogManager(
      logStorage: SequentialRecordStorage[ReplayLogRecord],
      logFileName: String,
      handler: WorkflowFIFOMessage => Unit
  ): ReplayLogManager = {
    logStorage match {
      case _: EmptyRecordStorage[ReplayLogRecord] =>
        new EmptyReplayLogManagerImpl(handler)
      case other =>
        val manager = new ReplayLogManagerImpl(handler)
        manager.setupWriter(other.getWriter(logFileName))
        manager
    }
  }
}

trait ReplayLogManager {

  protected val cursor = new ProcessingStepCursor()

  def setupWriter(logWriter: SequentialRecordWriter[ReplayLogRecord]): Unit

  def sendCommitted(msg: WorkflowFIFOMessage): Unit

  def terminate(): Unit

  def getStep: Long = cursor.getStep

  def markAsReplayDestination(id: ChannelMarkerIdentity): Unit

  def withFaultTolerant(
      channelId: ChannelIdentity,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit = {
    cursor.setCurrentChannel(channelId)
    try {
      code
    } catch {
      case t: Throwable => throw t
    } finally {
      cursor.stepIncrement()
    }
  }

}

class EmptyReplayLogManagerImpl(handler: WorkflowFIFOMessage => Unit) extends ReplayLogManager {
  override def setupWriter(
      logWriter: SequentialRecordStorage.SequentialRecordWriter[ReplayLogRecord]
  ): Unit = {}

  override def sendCommitted(msg: WorkflowFIFOMessage): Unit = {
    handler(msg)
  }

  override def terminate(): Unit = {}

  override def markAsReplayDestination(id: ChannelMarkerIdentity): Unit = {}
}

class ReplayLogManagerImpl(handler: WorkflowFIFOMessage => Unit) extends ReplayLogManager {

  private val replayLogger = new ReplayLoggerImpl()

  private var writer: AsyncReplayLogWriter = _

  override def withFaultTolerant(
      channelId: ChannelIdentity,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit = {
    replayLogger.logCurrentStepWithMessage(cursor.getStep, channelId, message)
    super.withFaultTolerant(channelId, message)(code)
  }

  override def markAsReplayDestination(id: ChannelMarkerIdentity): Unit = {
    replayLogger.markAsReplayDestination(id)
  }

  override def setupWriter(logWriter: SequentialRecordWriter[ReplayLogRecord]): Unit = {
    writer = new AsyncReplayLogWriter(handler, logWriter)
    writer.start()
  }

  override def sendCommitted(msg: WorkflowFIFOMessage): Unit = {
    writer.putLogRecords(replayLogger.drainCurrentLogRecords(cursor.getStep))
    writer.putOutput(msg)
  }

  override def terminate(): Unit = {
    writer.terminate()
  }

}
