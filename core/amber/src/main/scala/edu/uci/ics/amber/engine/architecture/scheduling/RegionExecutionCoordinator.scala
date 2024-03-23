package edu.uci.ics.amber.engine.architecture.scheduling

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.controller.ControllerEvent.{
  WorkerAssignmentUpdate,
  ExecutionStatsUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.execution.{
  OperatorExecution,
  WorkflowExecution
}
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.FatalErrorHandler.FatalError
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.InitializeExecutorHandler.InitializeExecutor
import edu.uci.ics.amber.engine.architecture.scheduling.config.{OperatorConfig, ResourceConfig}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AssignPortHandler.AssignPort
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.OpenExecutorHandler.OpenExecutor
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.web.workflowruntimestate.WorkflowAggregatedState

import scala.collection.Seq
class RegionExecutionCoordinator(
    region: Region,
    workflowExecution: WorkflowExecution,
    asyncRPCClient: AsyncRPCClient,
    controllerConfig: ControllerConfig
) {
  def execute(actorService: AkkaActorService): Future[Unit] = {

    // fetch resource config
    val resourceConfig = region.resourceConfig.get

    val regionExecution = workflowExecution.getRegionExecution(region.id)

    region.getOperators.foreach(physicalOp => {
      // Check for existing execution for this operator
      val existOpExecution =
        workflowExecution.getAllRegionExecutions.exists(_.hasOperatorExecution(physicalOp.id))

      // Initialize operator execution, reusing existing execution if available
      val operatorExecution = regionExecution.initOperatorExecution(
        physicalOp.id,
        if (existOpExecution) Some(workflowExecution.getLatestOperatorExecution(physicalOp.id))
        else None
      )

      // If no existing execution, build the operator with specified config
      if (!existOpExecution) {
        buildOperator(
          actorService,
          physicalOp,
          resourceConfig.operatorConfigs(physicalOp.id),
          operatorExecution
        )
      }
    })

    // update UI
    asyncRPCClient.sendToClient(ExecutionStatsUpdate(regionExecution.getStats))
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
      .flatMap(_ => initExecutors(operatorsToInit, resourceConfig))
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
      operatorConfig: OperatorConfig,
      operatorExecution: OperatorExecution
  ): Unit = {
    physicalOp.build(
      actorService,
      operatorExecution,
      operatorConfig,
      controllerConfig.stateRestoreConfOpt,
      controllerConfig.faultToleranceConfOpt
    )
  }
  private def initExecutors(
      operators: Set[PhysicalOp],
      resourceConfig: ResourceConfig
  ): Future[Seq[Unit]] = {
    Future
      .collect(
        operators
          .flatMap(physicalOp => {
            val workerConfigs = resourceConfig.operatorConfigs(physicalOp.id).workerConfigs
            workerConfigs.map(_.workerId).map { workerId =>
              asyncRPCClient
                .send(
                  InitializeExecutor(
                    workerConfigs.length,
                    physicalOp.opExecInitInfo,
                    physicalOp.isSourceOperator
                  ),
                  workerId
                )
            }
          })
          .toSeq
      )
  }
  private def assignPorts(region: Region): Future[Seq[Unit]] = {
    val resourceConfig = region.resourceConfig.get
    Future.collect(
      region.getOperators
        .flatMap { physicalOp: PhysicalOp =>
          val inputPortMapping = physicalOp.inputPorts
            .flatMap {
              case (inputPortId, (_, _, Right(schema))) =>
                Some(GlobalPortIdentity(physicalOp.id, inputPortId, input = true) -> schema)
              case _ => None
            }
          val outputPortMapping = physicalOp.outputPorts
            .flatMap {
              case (outputPortId, (_, _, Right(schema))) =>
                Some(GlobalPortIdentity(physicalOp.id, outputPortId, input = false) -> schema)
              case _ => None
            }
          inputPortMapping ++ outputPortMapping
        }
        .flatMap {
          case (globalPortId, schema) =>
            resourceConfig.operatorConfigs(globalPortId.opId).workerConfigs.map(_.workerId).map {
              workerId =>
                asyncRPCClient
                  .send(AssignPort(globalPortId.portId, globalPortId.input, schema), workerId)
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
          .flatMap(opId =>
            workflowExecution.getRegionExecution(region.id).getOperatorExecution(opId).getWorkerIds
          )
          .map { workerId =>
            asyncRPCClient.send(OpenExecutor(), workerId)
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(workflowExecution.getRegionExecution(region.id).getStats)
    )
    Future.collect(
      region.getSourceOperators
        .map(_.id)
        .flatMap { opId =>
          workflowExecution
            .getRegionExecution(region.id)
            .getOperatorExecution(opId)
            .getWorkerIds
            .map { workerId =>
              asyncRPCClient
                .send(StartWorker(), workerId)
                .map(state =>
                  // update worker state
                  workflowExecution
                    .getRegionExecution(region.id)
                    .getOperatorExecution(opId)
                    .getWorkerExecution(workerId)
                    .setState(state)
                )
            }
        }
        .toSeq
    )
  }

}
