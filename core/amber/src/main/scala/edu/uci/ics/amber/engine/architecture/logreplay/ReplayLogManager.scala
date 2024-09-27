package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.architecture.common.ProcessingStepCursor
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.MainThreadDelegateMessage
import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.storage.SequentialRecordStorage.SequentialRecordWriter
import edu.uci.ics.amber.engine.common.storage.{EmptyRecordStorage, SequentialRecordStorage}
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, ChannelMarkerIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

//In-mem formats:
sealed trait ReplayLogRecord

case class MessageContent(message: WorkflowFIFOMessage) extends ReplayLogRecord
case class OutputRecord(portIdOpt: Option[PortIdentity]) extends ReplayLogRecord
case class ProcessingStep(channelId: ChannelIdentity, step: Long) extends ReplayLogRecord
case class ReplayDestination(id: ChannelMarkerIdentity) extends ReplayLogRecord
case object TerminateSignal extends ReplayLogRecord

object ReplayLogManager {
  def createLogManager(
      logStorage: SequentialRecordStorage[ReplayLogRecord],
      logFileName: String,
      handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
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

  def sendCommitted(msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]): Unit

  def terminate(): Unit

  def getStep: Long = cursor.getStep

  def getLogger:ReplayLogger

  def markAsReplayDestination(id: ChannelMarkerIdentity): Unit

  def withFaultTolerant(
      channelId: ChannelIdentity,
      message: Option[WorkflowFIFOMessage],
      disableFT:Boolean
  )(code: => Unit): Unit = {
    if(disableFT){
      code
    }else{
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

}

class EmptyReplayLogManagerImpl(
    handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit
) extends ReplayLogManager {
  override def setupWriter(
      logWriter: SequentialRecordStorage.SequentialRecordWriter[ReplayLogRecord]
  ): Unit = {}

  override def sendCommitted(msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]): Unit = {
    handler(msg)
  }

  override def terminate(): Unit = {}

  override def markAsReplayDestination(id: ChannelMarkerIdentity): Unit = {}

  def getLogger:ReplayLogger = new EmptyReplayLogger()
}

class ReplayLogManagerImpl(handler: Either[MainThreadDelegateMessage, WorkflowFIFOMessage] => Unit)
    extends ReplayLogManager {

  private val replayLogger = new ReplayLoggerImpl()

  private var writer: AsyncReplayLogWriter = _

  def getLogger:ReplayLogger = replayLogger

  override def withFaultTolerant(
      channelId: ChannelIdentity,
      message: Option[WorkflowFIFOMessage],
      disableFT:Boolean
  )(code: => Unit): Unit = {
    if(!disableFT){
      replayLogger.logCurrentStepWithMessage(cursor.getStep, channelId, message)
    }
    super.withFaultTolerant(channelId, message, disableFT)(code)
  }

  override def markAsReplayDestination(id: ChannelMarkerIdentity): Unit = {
    replayLogger.markAsReplayDestination(id)
  }

  override def setupWriter(logWriter: SequentialRecordWriter[ReplayLogRecord]): Unit = {
    writer = new AsyncReplayLogWriter(handler, logWriter)
    writer.start()
  }

  override def sendCommitted(msg: Either[MainThreadDelegateMessage, WorkflowFIFOMessage]): Unit = {
    writer.putLogRecords(replayLogger.drainCurrentLogRecords(cursor.getStep))
    writer.putOutput(msg)
  }

  override def terminate(): Unit = {
    writer.terminate()
  }

}
