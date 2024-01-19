package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.{ChannelID, WorkflowFIFOMessage}
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelMarkerIdentity

abstract class ReplayLogger {

  def logCurrentStepWithMessage(
      step: Long,
      channel: ChannelID,
      msg: Option[WorkflowFIFOMessage]
  ): Unit

  def markAsReplayDestination(id: ChannelMarkerIdentity): Unit

  def drainCurrentLogRecords(step: Long): Array[ReplayLogRecord]

}
