package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.{AmberLogging, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}

import scala.collection.IterableOnce.iterableOnceExtensionMethods
import scala.collection.mutable

class UpstreamLinkStatus(val actorId: ActorVirtualIdentity) extends AmberLogging {
  def getPortId(channelId: ChannelIdentity): PortIdentity = {
    channelIdToInputPortMapping(channelId)
  }

  /**
    * The scheduler may not schedule the entire workflow at once. Consider a 2-phase hash join where the first
    * region to be scheduled is the build part of the workflow and the join operator. The hash join workers will
    * only receive the workers from the upstream operator on the build side in `upstreamMap` through
    * `UpdateInputLinkingHandler`. Thus, the hash join worker may wrongly deduce that all inputs are done when
    * the build part completes. Therefore, we have a `inputDataChannels` to track the number of actual upstream
    * links that a worker receives data from.
    */
  private val upstreamMapReverse =
    new mutable.HashMap[ActorVirtualIdentity, ChannelIdentity]
  private val completedChannels = new mutable.HashSet[ChannelIdentity]()
  private val inputDataChannels
      : mutable.HashMap[PortIdentity, mutable.ListBuffer[ChannelIdentity]] =
    mutable.HashMap()
  private val channelIdToInputPortMapping: mutable.HashMap[ChannelIdentity, PortIdentity] =
    mutable.HashMap()

  def registerInput(
      fromWorkerId: ActorVirtualIdentity,
      inputChannelId: ChannelIdentity,
      portId: PortIdentity
  ): Unit = {
    val channelIds = inputDataChannels.getOrElseUpdate(portId, new mutable.ListBuffer())
    channelIds.addOne(inputChannelId)
    upstreamMapReverse.update(fromWorkerId, inputChannelId)
    channelIdToInputPortMapping(inputChannelId) = portId
  }

  def markChannelCompleted(channelId: ChannelIdentity): Unit = {
    completedChannels.add(channelId)
  }

  def isPortCompleted(portId: PortIdentity): Boolean = {
    if (portId == null) {
      return true // special case for source operator
    }
    val channelIds = inputDataChannels(portId)
    channelIds.toSet.subsetOf(completedChannels)
  }

  def isAllEOF: Boolean = completedChannels.equals(inputDataChannels)
}
