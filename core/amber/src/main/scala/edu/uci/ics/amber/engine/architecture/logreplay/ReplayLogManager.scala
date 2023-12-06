package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.ReplayLogWriter
import edu.uci.ics.amber.engine.architecture.logreplay.storage.{ReplayLogStorage, EmptyLogStorage}
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}

//In-mem formats:
sealed trait ReplayLogRecord

case class MessageContent(message: WorkflowFIFOMessage) extends ReplayLogRecord
case class ProcessingStep(channelID: ChannelID, steps: Long) extends ReplayLogRecord
case object TerminateSignal extends ReplayLogRecord

object ReplayLogManager {
  def createLogManager(
      logStorage: ReplayLogStorage,
      handler: WorkflowFIFOMessage => Unit
  ): ReplayLogManager = {
    logStorage match {
      case _: EmptyLogStorage =>
        new EmptyReplayLogManagerImpl(handler)
      case other =>
        val manager = new ReplayLogManagerImpl(handler)
        manager.setupWriter(other.getWriter)
        manager
    }
  }
}

trait ReplayLogManager {
  def setupWriter(logWriter: ReplayLogWriter): Unit

  def sendCommitted(msg: WorkflowFIFOMessage): Unit

  def terminate(): Unit

  def getStep: Long

  def withFaultTolerant(
      channel: ChannelID,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit

}

class EmptyReplayLogManagerImpl(handler: WorkflowFIFOMessage => Unit) extends ReplayLogManager {
  override def setupWriter(logWriter: ReplayLogStorage.ReplayLogWriter): Unit = {}

  override def sendCommitted(msg: WorkflowFIFOMessage): Unit = {
    handler(msg)
  }

  override def getStep: Long = 0L

  override def terminate(): Unit = {}

  override def withFaultTolerant(channel: ChannelID, message: Option[WorkflowFIFOMessage])(
      code: => Unit
  ): Unit = code
}

class ReplayLogManagerImpl(handler: WorkflowFIFOMessage => Unit) extends ReplayLogManager {

  private val replayLogger = new ReplayLoggerImpl()

  private var writer: AsyncReplayLogWriter = _

  private val cursor = new ProcessingStepCursor()

  def withFaultTolerant(
      channel: ChannelID,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit = {
    replayLogger.logCurrentStepWithMessage(cursor.getStep, channel, message)
    cursor.setCurrentChannel(channel)
    try {
      code
    } catch {
      case t: Throwable => throw t
    } finally {
      cursor.stepIncrement()
    }
  }

  def getStep: Long = cursor.getStep

  def setupWriter(logWriter: ReplayLogWriter): Unit = {
    writer = new AsyncReplayLogWriter(handler, logWriter)
    writer.start()
  }

  def sendCommitted(msg: WorkflowFIFOMessage): Unit = {
    writer.putLogRecords(replayLogger.drainCurrentLogRecords(cursor.getStep))
    writer.putOutput(msg)
  }

  def terminate(): Unit = {
    writer.terminate()
  }

}
