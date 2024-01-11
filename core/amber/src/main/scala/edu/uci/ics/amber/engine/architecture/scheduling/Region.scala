package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalLinkIdentity, PhysicalOpIdentity}

case class RegionLink(fromRegion: Region, toRegion: Region)

case class RegionIdentity(id: String)

case class WorkerConfig()

case class RegionConfig(
    workerConfigs: Map[PhysicalOpIdentity, List[WorkerConfig]]
)

// A (pipelined) region can have a single source. A source is an operator with
// only blocking inputs or no inputs at all.
case class Region(
    id: RegionIdentity,
    physicalOpIds: Set[PhysicalOpIdentity],
    physicalLinkIds: Set[PhysicalLinkIdentity],
    config: Option[RegionConfig] = None,
    // operators whose all inputs are from upstream region.
    sourcePhysicalOpIds: Set[PhysicalOpIdentity] = Set.empty,
    // links to downstream regions, where this region generates blocking output.
    downstreamLinkIds: Set[PhysicalLinkIdentity] = Set.empty
) {

  /**
    * Return all PhysicalOpIds that this region may affect.
    * This includes:
    *   1) operators in this region;
    *   2) operators not in this region but blocked by this region (connected by the downstream links).
    */
  def getEffectiveOperators: Set[PhysicalOpIdentity] = {
    physicalOpIds ++ downstreamLinkIds.map(linkId => linkId.to)
  }

}
