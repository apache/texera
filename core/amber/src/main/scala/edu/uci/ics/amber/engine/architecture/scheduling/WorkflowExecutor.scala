package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatusUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.config.{OperatorConfig, ResourceConfig}
import edu.uci.ics.amber.engine.architecture.scheduling.policies.RegionExecutionState
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class WorkflowExecutor(
    regionPlan: RegionPlan,
    executionState: ExecutionState,
    actorService: AkkaActorService,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {
  val regionExecutionState: RegionExecutionState = new RegionExecutionState()

  private val startedRegions = new mutable.HashSet[RegionIdentity]()

  def startWorkflow(): Future[Seq[Unit]] = {
    executeRegions(getNextRegions)
  }

  def onPortCompletion(portId: GlobalPortIdentity): Unit = {
    regionExecutionState
      .getRegion(portId)
      .filter(region => regionExecutionState.isRegionCompleted(executionState, region))
      .map { region =>
        regionExecutionState.runningRegions.remove(region)
        regionExecutionState.completedRegions.add(region)
      }
  }

  private def executeRegions(regions: Set[Region]): Future[Seq[Unit]] = {
    Future.collect(regions.toSeq.map(region => scheduleRegion(region)))
  }

  private def buildOperators(
      operators: Iterator[PhysicalOp],
      resourceConfig: ResourceConfig
  ): Unit = {
    operators.foreach(physicalOp =>
      buildOperator(
        physicalOp,
        resourceConfig.operatorConfigs(physicalOp.id)
      )
    )

  }

  private def buildOperator(
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig
  ): Unit = {
    val opExecution = executionState.initOperatorState(physicalOp.id, operatorConfig)
    physicalOp.build(
      actorService,
      opExecution,
      operatorConfig,
      controllerConfig.workerRestoreConfMapping,
      controllerConfig.workerLoggingConfMapping
    )
  }
  private def initExecutors(operators: Set[PhysicalOp]): Future[Seq[Unit]] = {
    Future
      .collect(
        // initialize executors in Python
        operators
          .filter(op => op.isPythonOperator)
          .flatMap(op => {
            val workerIds = executionState.getOperatorExecution(op.id).getWorkerExecutions.keys
            workerIds.map(workerId => (workerId, op))
          })
          .map {
            case (workerId, pythonUDFPhysicalOp) =>
              asyncRPCClient
                .send(
                  InitializeOperatorLogic(
                    pythonUDFPhysicalOp.getPythonCode,
                    pythonUDFPhysicalOp.isSourceOperator,
                    pythonUDFPhysicalOp.outputPorts.values.head._3
                  ),
                  workerId
                )
          }
          .toSeq
      )
  }

  /**
    * assign ports to all operators in this region
    */
  private def assignPorts(region: Region): Future[Seq[Unit]] = {
    val resourceConfig = region.resourceConfig.get
    Future.collect(
      region.getOperators
        .flatMap { physicalOp: PhysicalOp =>
          physicalOp.inputPorts.keys
            .map(inputPortId => GlobalPortIdentity(physicalOp.id, inputPortId, input = true))
            .concat(
              physicalOp.outputPorts.keys
                .map(outputPortId => GlobalPortIdentity(physicalOp.id, outputPortId, input = false))
            )
        }
        .flatMap { globalPortId =>
          {
            resourceConfig.operatorConfigs(globalPortId.opId).workerConfigs.map(_.workerId).map {
              workerId =>
                asyncRPCClient.send(AssignPort(globalPortId.portId, globalPortId.input), workerId)
            }
          }
        }
        .toSeq
    )
  }

  private def connectChannels(links: Set[PhysicalLink]): Future[Seq[Unit]] = {
    Future.collect(
      links.map { link: PhysicalLink => asyncRPCClient.send(LinkWorkers(link), CONTROLLER) }.toSeq
    )
  }

  private def openOperators(operators: Set[PhysicalOp]): Future[Seq[Unit]] = {
    Future
      .collect(
        operators
          .map(_.id)
          .map(opId => executionState.getOperatorExecution(opId))
          .flatMap(operatorExecution => operatorExecution.getWorkerExecutions.keys)
          .map { workerId =>
            asyncRPCClient.send(OpenOperator(), workerId)
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))
    val sourceOpIds = region.getSourceOperators.map(_.id)
    Future.collect(sourceOpIds.flatMap { opId =>
      executionState
        .getOperatorExecution(opId)
        .getWorkerExecutions
        .map {
          case (workerId, workerExecution) =>
            asyncRPCClient
              .send(StartWorker(), workerId)
              .map(ret =>
                // update worker state
                workerExecution.state = ret
              )
        }
    }.toSeq)
  }

  private def scheduleRegion(region: Region): Future[Unit] = {
    val operatorsToBuild = region
      .topologicalIterator()
      .filter(opId => { !executionState.hasOperatorExecution(opId) })
      .map(opId => region.getOperator(opId))
    val linksToInit = region.getLinks
    val resourceConfig = region.resourceConfig.get
    regionExecutionState.addToRunningRegions(Set(region))

    buildOperators(operatorsToBuild, resourceConfig)
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        region.getOperators
          .map(_.id)
          .map(physicalOpId => {
            physicalOpId.logicalOpId.id -> executionState
              .getOperatorExecution(physicalOpId)
              .getBuiltWorkerIds
              .map(_.name)
              .toList
          })
          .toMap
      )
    )

    val operatorsToInit = region.getOperators.filter(op =>
      executionState.getAllOperatorExecutions
        .filter(a => a._2.getState == WorkflowAggregatedState.UNINITIALIZED)
        .map(_._1)
        .toSet
        .contains(op.id)
    )

    Future(())
      .flatMap(_ => initExecutors(operatorsToInit))
      .flatMap(_ => assignPorts(region))
      .flatMap(_ => connectChannels(linksToInit))
      .flatMap(_ => openOperators(operatorsToInit))
      .flatMap(_ => sendStarts(region))
      .map(_ => {

        startedRegions.add(region.id)
      })
      .rescue {
        case err: Throwable =>
          // this call may come from client or worker(by execution completed)
          // thus we need to force it to send error to client.
          asyncRPCClient.sendToClient(FatalError(err, None))
          Future.Unit
      }
    Future.Unit
  }

  private def getNextRegions: Set[Region] = {
    if (regionExecutionState.runningRegions.nonEmpty) {
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
      .map(regionIds => regionIds.diff(regionExecutionState.completedRegions.map(_.id)))
      .find(_.nonEmpty) match {
      case Some(regionIds) => regionIds.map(regionId => regionPlan.getRegion(regionId))
      case None            => Set()
    }

  }

}
