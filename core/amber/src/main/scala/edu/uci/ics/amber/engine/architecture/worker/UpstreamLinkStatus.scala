package edu.uci.ics.amber.engine.architecture.worker

import edu.uci.ics.amber.engine.common.{AmberLogging, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

import scala.collection.mutable

class UpstreamLinkStatus(val actorId: ActorVirtualIdentity) extends AmberLogging {

  /**
    * The scheduler may not schedule the entire workflow at once. Consider a 2-phase hash join where the first
    * region to be scheduled is the build part of the workflow and the join operator. The hash join workers will
    * only receive the workers from the upstream operator on the build side in `upstreamMap` through
    * `UpdateInputLinkingHandler`. Thus, the hash join worker may wrongly deduce that all inputs are done when
    * the build part completes. Therefore, we have a `allUpstreamLinks` to track the number of actual upstream
    * links that a worker receives data from.
    */
  private val upstreamMap =
    new mutable.HashMap[PhysicalLink, Set[ActorVirtualIdentity]].withDefaultValue(Set())
  private val upstreamMapReverse =
    new mutable.HashMap[ActorVirtualIdentity, PhysicalLink]
  private val endReceivedFromWorkers = new mutable.HashSet[ActorVirtualIdentity]
  private val completedLinks = new mutable.HashSet[PhysicalLink]()
  private var allUpstreamLinks: Set[PhysicalLink] = Set.empty

  def setAllUpstreamLinks(newSet: Set[PhysicalLink]): Unit = {
    this.allUpstreamLinks = newSet
  }

  def registerInput(identifier: ActorVirtualIdentity, input: PhysicalLink): Unit = {
    assert(
      allUpstreamLinks.contains(input),
      "unexpected input link " + input + " for operator " + VirtualIdentityUtils.getPhysicalOpId(
        actorId
      )
    )
    upstreamMap.update(input, upstreamMap(input) + identifier)
    upstreamMapReverse.update(identifier, input)
  }

  def getInputLink(workerId: ActorVirtualIdentity): PhysicalLink =
    upstreamMapReverse(workerId)

  def markWorkerEOF(identifier: ActorVirtualIdentity): Unit = {
    if (identifier != null) {
      endReceivedFromWorkers.add(identifier)
      val link = upstreamMapReverse(identifier)
      if (upstreamMap(link).subsetOf(endReceivedFromWorkers)) {
        completedLinks.add(link)
      }
    }
  }

  def allUncompletedSenders: Set[ActorVirtualIdentity] = {
    upstreamMap.filterKeys(k => !completedLinks.contains(k)).values.flatten.toSet
  }

  def isLinkEOF(link: PhysicalLink): Boolean = {
    if (link == null) {
      return true // special case for source operator
    }
    completedLinks.contains(link)
  }

  def isAllEOF: Boolean = completedLinks.equals(allUpstreamLinks)
}
