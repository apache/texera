package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity

trait OrderEnforcer {
  def isCompleted: Boolean
  def canProceed(channelId: ChannelIdentity): Boolean
}
