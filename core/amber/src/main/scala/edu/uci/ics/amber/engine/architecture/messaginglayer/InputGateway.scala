package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity

trait InputGateway {
  def tryPickControlChannel: Option[AmberFIFOChannel]

  def tryPickChannel: Option[AmberFIFOChannel]

  def getAllChannels: Iterable[AmberFIFOChannel]

  def getAllDataChannels: Iterable[AmberFIFOChannel]

  def getChannel(channelId: ChannelIdentity): AmberFIFOChannel

  def getAllControlChannels: Iterable[AmberFIFOChannel]

  def addEnforcer(enforcer: OrderEnforcer): Unit
}
