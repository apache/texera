package edu.uci.ics.amber.engine.architecture.scheduling
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

case class RegionPlan(
    // regions in topological order of the regionDAG
    regions: List[Region],
    regionLinks: Set[RegionLink]
) {
  private val regionMapping: Map[RegionIdentity, Region] =
    regions.map(region => region.id -> region).toMap

  def getUpstreamRegions(region: Region): Set[Region] = {
    regionLinks
      .filter(link => link.toRegionId == region.id)
      .map(_.fromRegionId)
      .map(regionId => regionMapping(regionId))
  }

  def getRegionOfPhysicalLink(link: PhysicalLink): Region = {
    regions.find(region => region.getAllLinks.contains(link)).get
  }

}
