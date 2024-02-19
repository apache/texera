package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
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
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.Seq

case object RegionExecution {
  def isRegionCompleted(
      executionState: ExecutionState,
      region: Region
  ): Boolean = {
    region.getPorts.forall(globalPortId => {
      val operatorExecution = executionState.getOperatorExecution(globalPortId.opId)
      if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
      else operatorExecution.isOutputPortCompleted(globalPortId.portId)
    })
  }
}
case class RegionExecution() {
  var running: Boolean = false
  var completed: Boolean = false

}
class RegionExecutionController(
    region: Region,
    executionState: ExecutionState,
    asyncRPCClient: AsyncRPCClient,
    controllerConfig: ControllerConfig
) {
  // TODO: for now we keep the state with the Executor.
  //   After refactoring the ExecutionState, we can move this into executionState
  val regionExecution: RegionExecution = RegionExecution()

  def getRegionExecution: RegionExecution = {
    regionExecution
  }

  def execute(actorService: AkkaActorService): Future[Unit] = {

    // find out the operators needs to be built.
    // some operators may have already been built in previous regions.
    val operatorsToBuild = region
      .topologicalIterator()
      .filter(opId => { !executionState.hasOperatorExecution(opId) })
      .map(opId => region.getOperator(opId))

    // fetch resource config
    val resourceConfig = region.resourceConfig.get

    // mark the region as running
    regionExecution.running = true

    // build operators, init workers
    operatorsToBuild.foreach(physicalOp =>
      buildOperator(
        actorService,
        physicalOp,
        resourceConfig.operatorConfigs(physicalOp.id)
      )
    )

    // update UI
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

    // initialize the operators that are uninitialized
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
      .flatMap(_ => connectChannels(region.getLinks))
      .flatMap(_ => openOperators(operatorsToInit))
      .flatMap(_ => sendStarts(region))
      .rescue {
        case err: Throwable =>
          // this call may come from client or worker(by execution completed)
          // thus we need to force it to send error to client.
          asyncRPCClient.sendToClient(FatalError(err, None))
          Future.Unit
      }
      .unit
  }
  private def buildOperator(
      actorService: AkkaActorService,
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig
  ): Unit = {
    val opExecution = executionState.initOperatorState(physicalOp.id, operatorConfig)
    physicalOp.build(
      actorService,
      opExecution,
      operatorConfig,
      controllerConfig.stateRestoreConfOpt,
      controllerConfig.faultToleranceConfOpt
    )
  }
  private def initExecutors(operators: Set[PhysicalOp]): Future[Seq[Unit]] = {
    Future
      .collect(
        // initialize executors in Python
        operators
          .filter(op => op.isPythonOperator)
          .flatMap(op => {
            executionState
              .getOperatorExecution(op.id)
              .getBuiltWorkerIds
              .map(workerId => (workerId, op))
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
          .flatMap(opId => executionState.getOperatorExecution(opId).getBuiltWorkerIds)
          .map { workerId =>
            asyncRPCClient.send(OpenOperator(), workerId)
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(WorkflowStatusUpdate(executionState.getWorkflowStatus))
    Future.collect(
      region.getSourceOperators
        .map(_.id)
        .flatMap { opId =>
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
        }
        .toSeq
    )
  }

}
