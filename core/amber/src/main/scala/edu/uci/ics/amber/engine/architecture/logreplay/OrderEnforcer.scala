package edu.uci.ics.amber.engine.architecture.logreplay

import edu.uci.ics.amber.engine.common.ambermessage.ChannelID

trait OrderEnforcer {
  var isCompleted: Boolean
  def canProceed(channelID: ChannelID): Boolean
}
