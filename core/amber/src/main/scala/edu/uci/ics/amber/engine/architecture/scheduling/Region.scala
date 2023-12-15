package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{
  PhysicalLinkIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

case class RegionLink(fromRegion: Region, toRegion: Region)

case class RegionIdentity(workflowId: WorkflowIdentity, pipelineId: String)

// A (pipelined) region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class Region(
    id: RegionIdentity,
    physicalOpIds: List[PhysicalOpIdentity],
    physicalLinkIds: List[PhysicalLinkIdentity],
    // links where this region receives blocking output (of the upstream region).
    upstreamLinkIds: List[PhysicalLinkIdentity] = List.empty,
    // links where this region generates blocking output.
    downstreamLinkIds: List[PhysicalLinkIdentity] = List.empty
) {

  /**
    * Return all PhysicalOpIds that this region may affect.
    * This includes:
    *   1) operators in this region;
    *   2) operators not in this region but blocked by this region (connected by the downstream links).
    */
  def getEffectiveOperators: List[PhysicalOpIdentity] = {
    physicalOpIds ++ downstreamLinkIds.map(linkId => linkId.to)
  }

}
