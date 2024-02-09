package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.common.{AkkaActorRefMappingService, AkkaActorService}
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
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.SchedulerTimeSlotEventHandler.SchedulerTimeSlotEvent
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.READY
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowScheduler(
    regionsToSchedule: mutable.Buffer[Region],
    executionState: ExecutionState,
    controllerConfig: ControllerConfig,
    asyncRPCClient: AsyncRPCClient
) extends LazyLogging {
  val schedulingPolicy: SchedulingPolicy =
    SchedulingPolicy.createPolicy(
      AmberConfig.schedulingPolicyName,
      regionsToSchedule
    )

  // Since one operator/link(i.e. links within an operator) can belong to multiple regions, we do not want
  // to build, init them multiple times. Currently, we use "opened" to indicate that an operator is built,
  // execution function is initialized, and ready for input.
  // This will be refactored later.
  private val openedOperators = new mutable.HashSet[PhysicalOpIdentity]()
  private val activatedLink = new mutable.HashSet[PhysicalLink]()

  private val constructingRegions = new mutable.HashSet[RegionIdentity]()
  private val startedRegions = new mutable.HashSet[RegionIdentity]()

  def startWorkflow(
      workflow: Workflow,
      akkaActorRefMappingService: AkkaActorRefMappingService,
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

  def onTimeSlotExpired(
      workflow: Workflow,
      timeExpiredRegions: Set[Region],
      akkaActorRefMappingService: AkkaActorRefMappingService,
      akkaActorService: AkkaActorService
  ): Future[Seq[Unit]] = {
    val nextRegions = schedulingPolicy.onTimeSlotExpired(workflow)
    var regionsToPause: Set[Region] = Set()
    if (nextRegions.nonEmpty) {
      regionsToPause = timeExpiredRegions
    }

    doSchedulingWork(nextRegions, akkaActorService)
      .flatMap(_ => {
        val pauseFutures = new ArrayBuffer[Future[Unit]]()
        regionsToPause.foreach(stoppingRegion => {
          schedulingPolicy.removeFromRunningRegion(Set(stoppingRegion))
          executionState
            .getAllWorkersOfRegion(stoppingRegion)
            .foreach(wid => {
              pauseFutures.append(
                asyncRPCClient
                  .send(SchedulerTimeSlotEvent(true), wid)
              )
            })
        })
        Future.collect(pauseFutures)
      })
      .map(_ => {
        Seq()
      })
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

  private def constructRegion(
      region: Region,
      akkaActorService: AkkaActorService
  ): Unit = {
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
      controllerActorService: AkkaActorService
  ): Unit = {
    val opExecution = executionState.initOperatorState(physicalOp.id, operatorConfig)
    physicalOp.build(
      controllerActorService,
      opExecution,
      operatorConfig,
      controllerConfig.workerRestoreConfMapping,
      controllerConfig.workerLoggingConfMapping
    )
  }
  private def initializePythonOperators(region: Region): Future[Seq[Unit]] = {

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

  private def activateAllLinks(region: Region): Future[Seq[Unit]] = {
    val allOperatorsInRegion = region.getOperators.map(_.id)
    Future.collect(
      // activate all links
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

  private def openAllOperators(region: Region): Future[Seq[Unit]] = {
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

  private def startRegion(region: Region): Future[Seq[Unit]] = {

    region.getOperators
      .map(_.id)
      .filter(opId =>
        executionState.getOperatorExecution(opId).getState == WorkflowAggregatedState.UNINITIALIZED
      )
      .foreach(opId => executionState.getOperatorExecution(opId).setAllWorkerState(READY))
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))

    val ops = region.getSourceOperators
    if (!schedulingPolicy.getRunningRegions.contains(region)) {
      val futures = ops
        .map(_.id)
        .flatMap { opId =>
          val opExecution = executionState.getOperatorExecution(opId)
          opExecution.getBuiltWorkerIds
            .map(worker =>
              asyncRPCClient
                .send(StartWorker(), worker)
                .map(ret =>
                  // update worker state
                  opExecution.getWorkerExecution(worker).state = ret
                )
            )
        }
        .toSeq
      Future.collect(futures)
    } else {
      throw new WorkflowRuntimeException(
        s"Start region called on an already running region: ${region.id}"
      )
    }
  }

  private def prepareAndStartRegion(
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
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
      .flatMap(_ => initializePythonOperators(region))
      .flatMap(_ => assignPorts(region))
      .flatMap(_ => activateAllLinks(region))
      .flatMap(_ => openAllOperators(region))
      .flatMap(_ => startRegion(region))
      .map(_ => {
        constructingRegions.remove(region.id)
        schedulingPolicy.addToRunningRegions(Set(region), actorService)
        startedRegions.add(region.id)
      })
  }

  private def resumeRegion(
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (!schedulingPolicy.getRunningRegions.contains(region)) {
      Future
        .collect(
          executionState
            .getAllWorkersOfRegion(region)
            .map(worker =>
              asyncRPCClient
                .send(SchedulerTimeSlotEvent(false), worker)
            )
            .toSeq
        )
        .map { _ =>
          schedulingPolicy.addToRunningRegions(Set(region), actorService)
        }
    } else {
      throw new WorkflowRuntimeException(
        s"Resume region called on an already running region: ${region.id}"
      )
    }

  }

  private def scheduleRegion(
      region: Region,
      actorService: AkkaActorService
  ): Future[Unit] = {
    if (constructingRegions.contains(region.id)) {
      return Future(())
    }
    if (!startedRegions.contains(region.id)) {
      constructingRegions.add(region.id)

      constructRegion(region, actorService)
      prepareAndStartRegion(region, actorService).rescue {
        case err: Throwable =>
          // this call may come from client or worker(by execution completed)
          // thus we need to force it to send error to client.
          asyncRPCClient.sendToClient(FatalError(err, None))
          Future.Unit
      }
    } else {
      // region has already been constructed. Just needs to resume
      resumeRegion(region, actorService)
    }

  }

}
