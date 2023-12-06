package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.common.ambermessage.ChannelID

trait InputGateway {
  def tryPickControlChannel: Option[AmberFIFOChannel]

  def tryPickChannel: Option[AmberFIFOChannel]

  def getAllDataChannels: Iterable[AmberFIFOChannel]

  def getChannel(channelId: ChannelID): AmberFIFOChannel

  def getAllControlChannels: Iterable[AmberFIFOChannel]

  def addEnforcer(enforcer: OrderEnforcer): Unit
}
