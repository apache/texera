package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

case class RegionPlan(
    // regions in topological order of the regionDAG
    regions: List[Region],
    regionLinks: Set[RegionLink]
) {

  def getUpstreamRegions(region: Region): Set[Region] = {
    regionLinks.filter(link => link.toRegion == region).map(_.fromRegion)
  }

  def getRegionOfPhysicalLink(link: PhysicalLink): Region = {
    regions.find(region => region.getEffectiveLinks.contains(link)).get
  }

}
