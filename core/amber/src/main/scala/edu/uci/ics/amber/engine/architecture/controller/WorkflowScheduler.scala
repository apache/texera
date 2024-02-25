package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.scheduling.{
  ExpansionGreedyRegionPlanGenerator,
  Region,
  RegionIdentity,
  RegionPlan
}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkflowScheduler(workflowContext: WorkflowContext, opResultStorage: OpResultStorage)
    extends java.io.Serializable {
  var physicalPlan: PhysicalPlan = _
  var regionPlan: RegionPlan = _
  private var regionExecutionOrder: Iterator[Set[Region]] = _

  /**
    * Update the total order of the regions to be executed, based on the current physicalPlan.
    */
  def updateSchedule(physicalPlan: PhysicalPlan): Unit = {
    // generate an RegionPlan with regions.
    //  currently, ExpansionGreedyRegionPlanGenerator is the only RegionPlan generator.
    val (regionPlan, updatedPhysicalPlan) = new ExpansionGreedyRegionPlanGenerator(
      workflowContext,
      physicalPlan,
      opResultStorage
    ).generate()
    this.regionPlan = regionPlan
    this.physicalPlan = updatedPhysicalPlan

    // generate the total order of Regions to be executed.
    this.regionExecutionOrder = {
      val levels = mutable.Map.empty[RegionIdentity, Int]
      val levelSets = mutable.Map.empty[Int, mutable.Set[RegionIdentity]]

      regionPlan.topologicalIterator().foreach { currentVertex =>
        val currentLevel = regionPlan.dag.incomingEdgesOf(currentVertex).asScala.foldLeft(0) {
          (maxLevel, incomingEdge) =>
            val sourceVertex = regionPlan.dag.getEdgeSource(incomingEdge)
            math.max(maxLevel, levels.getOrElse(sourceVertex, 0) + 1)
        }

        levels(currentVertex) = currentLevel
        levelSets.getOrElseUpdate(currentLevel, mutable.Set.empty).add(currentVertex)
      }

      val maxLevel = levels.values.maxOption.getOrElse(0)
      (0 to maxLevel).iterator.map(level => levelSets.getOrElse(level, mutable.Set.empty).toSet)
    }.map(regionIds => regionIds.map(regionId => regionPlan.getRegion(regionId)))
  }

  def getNextRegions: Set[Region] =
    if (regionExecutionOrder.hasNext) regionExecutionOrder.next()
    else Set.empty

}
