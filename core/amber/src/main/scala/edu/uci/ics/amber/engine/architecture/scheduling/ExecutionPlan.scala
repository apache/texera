package edu.uci.ics.amber.engine.architecture.scheduling

class ExecutionPlan(
    val regionsToSchedule: List[Region] = List.empty,
    val regionAncestorMapping: Map[Region, Set[Region]] = Map.empty
) {

  def getAllRegions: List[Region] = regionsToSchedule
}
