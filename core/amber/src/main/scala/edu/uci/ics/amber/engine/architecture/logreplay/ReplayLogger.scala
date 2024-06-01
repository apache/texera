package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.WorkflowFIFOMessage
import edu.uci.ics.amber.engine.common.virtualidentity.{ChannelIdentity, ChannelMarkerIdentity}
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

abstract class ReplayLogger {

  def writeOutputLog(portIdOpt: Option[PortIdentity]):Unit
  def logCurrentStepWithMessage(
      step: Long,
      channelId: ChannelIdentity,
      msg: Option[WorkflowFIFOMessage]
  ): Unit

  def markAsReplayDestination(id: ChannelMarkerIdentity): Unit

  def drainCurrentLogRecords(step: Long): Array[ReplayLogRecord]

}
