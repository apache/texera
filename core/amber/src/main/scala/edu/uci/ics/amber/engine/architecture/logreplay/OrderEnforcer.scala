package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity

trait OrderEnforcer {
  def isCompleted: Boolean
  def canProceed(channelId: ChannelIdentity): Boolean
}
