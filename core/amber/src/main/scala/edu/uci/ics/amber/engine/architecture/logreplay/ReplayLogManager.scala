package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor
import edu.uci.ics.amber.engine.architecture.logreplay.storage.ReplayLogStorage.ReplayLogWriter
import edu.uci.ics.amber.engine.architecture.logreplay.storage.{ReplayLogStorage, EmptyLogStorage}
import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}

//In-mem formats:
sealed trait ReplayLogRecord

case class MessageContent(message: WorkflowFIFOMessage) extends ReplayLogRecord
case class ProcessingStep(channelID: ChannelID, step: Long) extends ReplayLogRecord
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

  protected val cursor = new ProcessingStepCursor()

  def setupWriter(logWriter: ReplayLogWriter): Unit

  def sendCommitted(msg: WorkflowFIFOMessage): Unit

  def terminate(): Unit

  def getStep: Long = cursor.getStep

  def withFaultTolerant(
      channel: ChannelID,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit = {
    cursor.setCurrentChannel(channel)
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
  override def setupWriter(logWriter: ReplayLogStorage.ReplayLogWriter): Unit = {}

  override def sendCommitted(msg: WorkflowFIFOMessage): Unit = {
    handler(msg)
  }

  override def terminate(): Unit = {}
}

class ReplayLogManagerImpl(handler: WorkflowFIFOMessage => Unit) extends ReplayLogManager {

  private val replayLogger = new ReplayLoggerImpl()

  private var writer: AsyncReplayLogWriter = _

  override def withFaultTolerant(
      channel: ChannelID,
      message: Option[WorkflowFIFOMessage]
  )(code: => Unit): Unit = {
    replayLogger.logCurrentStepWithMessage(cursor.getStep, channel, message)
    super.withFaultTolerant(channel, message)(code)
  }

  override def setupWriter(logWriter: ReplayLogWriter): Unit = {
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
