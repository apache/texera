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
import edu.uci.ics.amber.engine.architecture.controller.{ControllerConfig, ExecutionState, Workflow}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.scheduling.policies.SchedulingPolicy
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable

class WorkflowScheduler(
    regionsToSchedule: mutable.Buffer[Region],
    executionState: ExecutionState,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {
  val schedulingPolicy: SchedulingPolicy = new SchedulingPolicy(regionsToSchedule)

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we do not want
  // to build, init them multiple times. Currently, we use "opened" to indicate that an operator is built,
  // execution function is initialized, and ready for input.
  // This will be refactored later.
  private val openedOperators = new mutable.HashSet[PhysicalOpIdentity]()
  private val activatedLink = new mutable.HashSet[PhysicalLink]()
  private val startedRegions = new mutable.HashSet[RegionIdentity]()

  def startWorkflow(
      workflow: Workflow,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.startWorkflow(workflow)
    doSchedulingWork(nextRegionsToSchedule, akkaActorService)
  }

  def onPortCompletion(
      workflow: Workflow,
      akkaActorService: AkkaActorService,
      portId: GlobalPortIdentity
  ): Future[Seq[Unit]] = {
    val nextRegionsToSchedule = schedulingPolicy.onPortCompletion(workflow, executionState, portId)
    doSchedulingWork(nextRegionsToSchedule, akkaActorService)
  }

  private def doSchedulingWork(
      regions: Set[Region],
      actorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    if (regions.nonEmpty) {
      Future.collect(
        regions.toArray.map(region => scheduleRegion(region, actorService))
      )
    } else {
      Future(Seq())
    }
  }

  private def constructRegion(region: Region, akkaActorService: AkkaActorService): Unit = {
    val resourceConfig = region.resourceConfig.get
    region
      .topologicalIterator()
      // TOOTIMIZE: using opened state which indicates an operator is built, init, and opened.
      .filter(physicalOpId => !openedOperators.contains(physicalOpId))
      .foreach { (physicalOpId: PhysicalOpIdentity) =>
        val physicalOp = region.getOperator(physicalOpId)
        buildOperator(
          physicalOp,
          resourceConfig.operatorConfigs(physicalOpId),
          akkaActorService
        )
      }
  }

  private def buildOperator(
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      actorService: AkkaActorService
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
  private def initExecutors(region: Region): Future[Seq[Unit]] = {

    val opIdsToInit = region.getOperators
      .filter(physicalOp => physicalOp.isPythonOperator)
      // TOOTIMIZE: using opened state which indicates an operator is built, init, and opened.
      .map(_.id)
      .diff(openedOperators)

    Future
      .collect(
        // initialize python operator code
        executionState
          .getPythonWorkerToOperatorExec(opIdsToInit)
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

  private def connectChannels(region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getOperators.map(_.id)
    Future.collect(
      region.getLinks
        .filter(link => {
          !activatedLink.contains(link) &&
            allOperatorsInRegion.contains(link.fromOpId) &&
            allOperatorsInRegion.contains(link.toOpId)
        })
        .map { link: PhysicalLink =>
          asyncRPCClient
            .send(LinkWorkers(link), CONTROLLER)
            .onSuccess(_ => activatedLink.add(link))
        }
        .toSeq
    )
  }

  private def openOperators(region: Region): Future[Seq[Unit]] = {
    val allNotOpenedOperators =
      region.getOperators.map(_.id).diff(openedOperators)
    Future
      .collect(
        executionState
          .getAllWorkersForOperators(allNotOpenedOperators)
          .map { workerID =>
            asyncRPCClient.send(OpenOperator(), workerID)
          }
          .toSeq
      )
      .onSuccess(_ => allNotOpenedOperators.foreach(opId => openedOperators.add(opId)))
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {

    region.getOperators
      .map(_.id)
      .filter(opId =>
        executionState.getOperatorExecution(opId).getState == WorkflowAggregatedState.UNINITIALIZED
      )
      .foreach(opId => executionState.getOperatorExecution(opId).setAllWorkerState(READY))
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

  private def executeRegion(region: Region): Future[Unit] = {
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
    Future(())
      .flatMap(_ => initExecutors(region))
      .flatMap(_ => assignPorts(region))
      .flatMap(_ => connectChannels(region))
      .flatMap(_ => openOperators(region))
      .flatMap(_ => sendStarts(region))
      .map(_ => {
        schedulingPolicy.addToRunningRegions(Set(region))
        startedRegions.add(region.id)
      })
  }

  private def scheduleRegion(region: Region, actorService: AkkaActorService): Future[Unit] = {

    constructRegion(region, actorService)
    executeRegion(region).rescue {
      case err: Throwable =>
        // this call may come from client or worker(by execution completed)
        // thus we need to force it to send error to client.
        asyncRPCClient.sendToClient(FatalError(err, None))
        Future.Unit
    }
  }

}
