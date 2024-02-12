package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  WorkflowStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.pythonworker.promisehandlers.InitializeOperatorLogicHandler.InitializeOperatorLogic
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenOperatorHandler.OpenOperator
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState._
import edu.uci.ics.texera.web.workflowruntimestate.{OperatorRuntimeStats, WorkflowAggregatedState}

import scala.collection.{Seq, mutable}

case object RegionExecution {
  def isRegionCompleted(
      regionExecution: RegionExecution,
      region: Region
  ): Boolean = {
    region.getPorts.forall(globalPortId => {
      val operatorExecution = regionExecution.getOperatorExecution(globalPortId.opId)
      if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
      else operatorExecution.isOutputPortCompleted(globalPortId.portId)
    })
  }
}
case class RegionExecution(region: Region) {

  var running: Boolean = false
  var completed: Boolean = false

  private val operatorExecutions: mutable.Map[PhysicalOpIdentity, OperatorExecution] =
    mutable.HashMap()

  def initOperatorExecution(
      physicalOpId: PhysicalOpIdentity
  ): OperatorExecution = {
    assert(!operatorExecutions.contains(physicalOpId))
    operatorExecutions.getOrElseUpdate(physicalOpId, new OperatorExecution())
  }

  def getAllBuiltWorkers: Iterable[ActorVirtualIdentity] =
    operatorExecutions.values.flatMap(operator => operator.getWorkerIds)

  def getOperatorExecution(opId: PhysicalOpIdentity): OperatorExecution = operatorExecutions(opId)

  def hasOperatorExecution(opId: PhysicalOpIdentity): Boolean = operatorExecutions.contains(opId)

  def getAllOperatorExecutions: Iterable[(PhysicalOpIdentity, OperatorExecution)] =
    operatorExecutions

  def getStats: Map[String, OperatorRuntimeStats] = {
    // TODO: fix the aggregation here. The stats should be on port level.
    operatorExecutions.map {
      case (physicalOpId, operatorExecution) =>
        physicalOpId.logicalOpId.id -> operatorExecution.getStats
    }.toMap
  }

  def isCompleted: Boolean =
    region.getPorts.forall(globalPortId => {
      val operatorExecution = this.getOperatorExecution(globalPortId.opId)
      if (globalPortId.input) operatorExecution.isInputPortCompleted(globalPortId.portId)
      else operatorExecution.isOutputPortCompleted(globalPortId.portId)

    })

  def getState: WorkflowAggregatedState = {
    if(isCompleted){
      WorkflowAggregatedState.COMPLETED
    }else{
      WorkflowAggregatedState.RUNNING
    }
  }

}
class RegionExecutionController(
    region: Region,
    regionExecution: RegionExecution,
    asyncRPCClient: AsyncRPCClient,
    actorService: AkkaActorService,
    controllerConfig: ControllerConfig
) {

  def getRegionExecution: RegionExecution = {
    regionExecution
  }

  def execute: Future[Unit] = {

    // find out the operators needs to be built.
    // some operators may have already been built in previous regions.
    val operatorsToBuild = region
      .topologicalIterator()
      .filter(opId => { !regionExecution.hasOperatorExecution(opId) })
      .map(opId => region.getOperator(opId))

    // fetch resource config
    val resourceConfig = region.resourceConfig.get

    // mark the region as running
//    regionExecution.running = true

    // build operators, init workers
    operatorsToBuild.foreach(physicalOp =>
      buildOperator(
        physicalOp,
        resourceConfig.operatorConfigs(physicalOp.id),
        regionExecution
      )
    )

    // update UI
    asyncRPCClient.sendToClient(WorkflowStatsUpdate(regionExecution.getStats))
    asyncRPCClient.sendToClient(
      WorkerAssignmentUpdate(
        region.getOperators
          .map(_.id)
          .map(physicalOpId => {
            physicalOpId.logicalOpId.id -> regionExecution
              .getOperatorExecution(physicalOpId)
              .getWorkerIds
              .map(_.name)
              .toList
          })
          .toMap
      )
    )

    // initialize the operators that are uninitialized
    val operatorsToInit = region.getOperators.filter(op =>
      regionExecution.getAllOperatorExecutions
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
      physicalOp: PhysicalOp,
      operatorConfig: OperatorConfig,
      regionExecution: RegionExecution
  ): Unit = {
    val opExecution = regionExecution.initOperatorExecution(physicalOp.id)
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
            regionExecution
              .getOperatorExecution(op.id)
              .getWorkerIds
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
          .flatMap(opId => regionExecution.getOperatorExecution(opId).getWorkerIds)
          .map { workerId =>
            asyncRPCClient.send(OpenOperator(), workerId)
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(WorkflowStatsUpdate(regionExecution.getStats))
    Future.collect(
      region.getSourceOperators
        .map(_.id)
        .flatMap { opId =>
          regionExecution
            .getOperatorExecution(opId)
            .getWorkerIds
            .map { workerId =>
              asyncRPCClient
                .send(StartWorker(), workerId)
                .map(ret =>
                  // update worker state
                  regionExecution.getOperatorExecution(opId).getWorkerExecution(workerId).state =
                    ret
                )
            }
        }
        .toSeq
    )
  }

}
