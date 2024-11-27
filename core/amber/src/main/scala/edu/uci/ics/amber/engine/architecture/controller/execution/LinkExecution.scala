package edu.uci.ics.amber.engine.architecture.controller.execution

import edu.uci.ics.amber.virtualidentity.ChannelIdentity

import scala.collection.mutable

case class LinkExecution() {
  private val channelExecutions: mutable.Map[ChannelIdentity, ChannelExecution] = mutable.HashMap()

  def initChannelExecution(channelId: ChannelIdentity): Unit = {
    assert(!channelExecutions.contains(channelId))
    channelExecutions(channelId) = ChannelExecution()
  }

  def getAllChannelExecutions: Iterable[(ChannelIdentity, ChannelExecution)] = channelExecutions
}
