package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.core.virtualidentity.ChannelIdentity

trait OrderEnforcer {
  var isCompleted: Boolean

  def canProceed(channelId: ChannelIdentity): Boolean
}
