package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.controller.execution.WorkflowExecution
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class WorkflowExecutionController(
    getNextRegions: () => Set[Region],
    workflowExecution: WorkflowExecution,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {

  private val regionExecutionControllers
      : mutable.HashMap[RegionIdentity, RegionExecutionController] =
    mutable.HashMap()

  /**
    * The entry function for WorkflowExecutor.
    * Each invocation will execute the next batch of Regions that are ready to be executed, if there are any.
    */
  def executeNextRegions(actorService: AkkaActorService): Future[Unit] = {
    Future
      .collect(
        getNextRegions
          .map(region => {
            workflowExecution.initRegionExecution(region)
            regionExecutionControllers(region.id) = new RegionExecutionController(
              region,
              workflowExecution,
              asyncRPCClient,
              controllerConfig
            )
            regionExecutionControllers(region.id)
          })
          .map(regionExecutionController => regionExecutionController.execute(actorService))
          .toSeq
      )
      .unit
  }

  /**
    * get the next batch of Regions to execute.
    */
  private def getNextRegions: Set[Region] = {
    if (workflowExecution.getRunningRegionExecutions.nonEmpty) {
      return Set.empty
    }

    Try(getNextRegions()) match {
      case Success(regions)   => regions
      case Failure(exception) => Set.empty // surpass exception
    }
  }

}
