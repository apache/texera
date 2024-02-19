package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkflowExecutionController(
    regionPlan: RegionPlan,
    executionState: ExecutionState,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {

  private val regionExecutors: mutable.HashMap[RegionIdentity, RegionExecutionController] =
    mutable.HashMap()

  private val regionExecutionOrder: List[Set[RegionIdentity]] = {
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
    (0 to maxLevel).toList.map(level => levelSets.getOrElse(level, mutable.Set.empty).toSet)
  }

  /**
    * The entry function for WorkflowExecutor.
    * Each invocation will execute the next batch of Regions that are ready to be executed, if there are any.
    */
  def executeNextRegions(actorService: AkkaActorService): Future[Unit] = {
    Future
      .collect(
        getNextRegions
          .map(region => {
            regionExecutors(region.id) = new RegionExecutionController(
              region,
              executionState,
              asyncRPCClient,
              controllerConfig
            )
            regionExecutors(region.id)
          })
          .map(regionExecutor => regionExecutor.execute(actorService))
          .toSeq
      )
      .unit
  }

  def markRegionCompletion(portId: GlobalPortIdentity): Unit = {
    regionPlan.getRegionOfPortId(portId) match {
      case Some(region) =>
        if (RegionExecution.isRegionCompleted(executionState, region)) {
          regionExecutors(region.id).regionExecution.running = false
          regionExecutors(region.id).regionExecution.completed = true
        }
      case None => // do nothing. currently the source input ports and sink output ports are not captured.
    }

  }

  /**
    * get the next batch of Regions to execute.
    */
  private def getNextRegions: Set[Region] = {
    if (regionExecutors.values.map(_.getRegionExecution).exists(_.running)) {
      return Set.empty
    }

    val completedRegions: Set[RegionIdentity] = regionExecutors.collect {
      case (regionId, executor) if executor.regionExecution.completed => regionId
    }.toSet

    regionExecutionOrder
      .map(_ -- completedRegions)
      .find(_.nonEmpty)
      .getOrElse(Set.empty)
      .map(regionPlan.getRegion)

  }

}
