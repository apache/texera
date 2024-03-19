package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, ChannelMarkerIdentity}

abstract class ReplayLogger {

  def logCurrentStepWithMessage(
      step: Long,
      channelId: ChannelIdentity,
      msg: Option[WorkflowFIFOMessage]
  ): Unit

  def markAsReplayDestination(id: ChannelMarkerIdentity): Unit

  def drainCurrentLogRecords(step: Long): Array[ReplayLogRecord]

}
