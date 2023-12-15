package edu.uci.ics.amber.engine.architecture.scheduling

case class RegionPlan(
    regions: List[Region],
    regionLinks: List[RegionLink]
) {

  def getUpstreamRegions(region: Region): Set[Region] = {
    regionLinks.filter(link => link.toRegion == region).map(_.fromRegion).toSet
  }

}
