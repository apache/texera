package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkflowExecutor(
    regionPlan: RegionPlan,
    executionState: ExecutionState,
    actorService: AkkaActorService,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {

  private val regionExecutors: mutable.HashMap[RegionIdentity, RegionExecutor] = mutable.HashMap()

  def executeNextRegions(): Future[Unit] = {
    Future
      .collect(
        getNextRegions.toSeq
          .map(region => {
            regionExecutors(region.id) = new RegionExecutor(
              region,
              executionState,
              asyncRPCClient,
              actorService,
              controllerConfig
            )
            regionExecutors(region.id)
          })
          .map(regionExecutor => regionExecutor.execute)
      )
      .unit
  }

  def updateRegionExecutionState(portId: GlobalPortIdentity): Unit = {
    regionPlan.regions
      .filter(region => region.getPorts.contains(portId))
      .filter(region => RegionExecution.isRegionCompleted(executionState, region))
      .foreach { region =>
        regionExecutors(region.id).regionExecution.running = false
        regionExecutors(region.id).regionExecution.completed = true
      }
  }

  private def getNextRegions: Set[Region] = {
    if (
      regionExecutors.values
        .map(regionExecutor => regionExecutor.getRegionExecution)
        .exists(regionExecution => regionExecution.running)
    ) {
      return Set.empty
    }
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
      .map(regionIds =>
        regionIds.diff(
          regionExecutors
            .filter {
              case (_, regionExecutor) => regionExecutor.regionExecution.completed
            }
            .keys
            .toSet
        )
      )
      .find(_.nonEmpty) match {
      case Some(regionIds) => regionIds.map(regionId => regionPlan.getRegion(regionId))
      case None            => Set()
    }

  }

}
