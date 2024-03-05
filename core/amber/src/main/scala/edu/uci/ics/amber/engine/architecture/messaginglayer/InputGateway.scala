package edu.uci.ics.amber.engine.architecture.messaginglayer

import edu.uci.ics.amber.engine.architecture.logreplay.OrderEnforcer
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

trait InputGateway {

  def tryPickControlChannel: Option[AmberFIFOChannel]

  def tryPickChannel: Option[AmberFIFOChannel]

  def getAllChannels: Iterable[AmberFIFOChannel]

  def getAllDataChannels: Iterable[AmberFIFOChannel]

  def getChannel(channelId: ChannelIdentity): AmberFIFOChannel

  def getAllControlChannels: Iterable[AmberFIFOChannel]

  def addEnforcer(enforcer: OrderEnforcer): Unit

}
