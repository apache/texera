package edu.uci.ics.amber.engine.architecture.scheduling

class ExecutionPlan(
    val regionsToSchedule: List[PipelinedRegion] = List.empty,
    val regionAncestorMapping: Map[PipelinedRegion, Set[PipelinedRegion]] = Map.empty
) {

  def getAllRegions: List[PipelinedRegion] = regionsToSchedule
}
