package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{
  PhysicalLinkIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

case class RegionLink(fromRegion: Region, toRegion: Region)

case class RegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A pipelined region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class Region(
    id: RegionIdentity,
    physicalOpIds: List[PhysicalOpIdentity],
    physicalLinkIds: List[PhysicalLinkIdentity],
    // These are the operators that receive blocking inputs from this region,
    // we mark output links from this region as blocking.
    blockingLinkIds: List[PhysicalLinkIdentity] = List.empty
) {

  /**
    * Return all PhysicalOpIds that this region may affect.
    * This includes:
    *   1) operators in this region;
    *   2) operators not in this region but blocked by this region.
    */
  def getEffectiveOperators: List[PhysicalOpIdentity] = {
    physicalOpIds ++ blockingLinkIds.map(linkId => linkId.to)
  }

}
