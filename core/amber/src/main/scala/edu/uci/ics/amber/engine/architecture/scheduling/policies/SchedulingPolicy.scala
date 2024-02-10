package edu.uci.ics.amber.engine.architecture.scheduling.policies

import edu.uci.ics.amber.engine.architecture.controller.{ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.scheduling.{
  GlobalPortIdentity,
  Region,
  RegionIdentity,
  RegionPlan
}

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class SchedulingPolicy(
    protected val regionPlan: RegionPlan
) {

  protected val completedRegions = new mutable.HashSet[Region]()
  // regions currently running
  private val runningRegions = new mutable.HashSet[Region]()

  private val completedPortIdsOfRegion
      : mutable.HashMap[RegionIdentity, mutable.HashSet[GlobalPortIdentity]] = mutable.HashMap()
  private def isRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Boolean = {
    region.getPorts.subsetOf(completedPortIdsOfRegion.getOrElse(region.id, mutable.HashSet()))
  }

  private def getRegion(portId: GlobalPortIdentity): Option[Region] = {
    runningRegions.find(r => r.getPorts.contains(portId))
  }
  private def getNextRegions: Set[Region] = {

    def getRegionsOrder(regionPlan: RegionPlan): List[Set[RegionIdentity]] = {
      val levels = mutable.Map.empty[RegionIdentity, Int]
      val levelSets = mutable.Map.empty[Int, mutable.Set[RegionIdentity]]
      val iterator = regionPlan.topologicalIterator()

      iterator.foreach { currentVertex =>
        val currentLevel = regionPlan.dag.incomingEdgesOf(currentVertex).asScala.foldLeft(0) {
          (maxLevel, incomingEdge) =>
            val sourceVertex = regionPlan.dag.getEdgeSource(incomingEdge)
            val sourceLevel = levels.getOrElse(sourceVertex, 0)
            math.max(maxLevel, sourceLevel + 1)
        }
        levels.update(currentVertex, currentLevel)
        val verticesAtCurrentLevel =
          levelSets.getOrElseUpdate(currentLevel, mutable.Set.empty[RegionIdentity])
        verticesAtCurrentLevel.add(currentVertex)
      }

      val maxLevel = levels.values.maxOption.getOrElse(0)
      (0 to maxLevel).toList.map(level => levelSets.getOrElse(level, mutable.Set.empty).toSet)
    }

    getRegionsOrder(regionPlan)
      .map(regionIds => regionIds.diff(completedRegions.map(_.id))).find(_.nonEmpty) match {
      case Some(regionIds) => regionIds.map(regionId => regionPlan.getRegion(regionId))
      case None => Set()
    }

  }

  def startWorkflow(): Set[Region] = {
    getNextRegions
  }

  def onPortCompletion(
      executionState: ExecutionState,
      portId: GlobalPortIdentity
  ): Set[Region] = {
    getRegion(portId) match {
      case Some(region) =>
        val portIds =
          completedPortIdsOfRegion.getOrElseUpdate(
            region.id,
            new mutable.HashSet[GlobalPortIdentity]()
          )
        portIds.add(portId)
        if (isRegionCompleted(executionState, region)) {
          runningRegions.remove(region)
          completedRegions.add(region)
          getNextRegions
        }else{
          Set()
        }
      case None =>
        Set() // currently, the virtual input ports of source operators do not belong to any region
    }
  }

  def addToRunningRegions(regions: Set[Region]): Unit = {
    runningRegions ++= regions
  }

  def getCompletedRegions: Set[Region] = completedRegions.toSet

}
