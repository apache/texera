package edu.uci.ics.amber.engine.architecture.scheduling

import com.google.protobuf.any.Any
import com.google.protobuf.ByteString
import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.common.AkkaActorService
import edu.uci.ics.amber.engine.architecture.controller.{
  ControllerConfig,
  ExecutionStatsUpdate,
  WorkerAssignmentUpdate
}
import edu.uci.ics.amber.engine.architecture.controller.execution.{
  OperatorExecution,
  WorkflowExecution
}
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AssignPortRequest,
  EmptyRequest,
  InitializeExecutorRequest,
  LinkWorkersRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{
  EmptyReturn,
  WorkflowAggregatedState
}
import edu.uci.ics.amber.engine.architecture.scheduling.config.{OperatorConfig, ResourceConfig}
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.amber.engine.common.model.PhysicalOp
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

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
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
    )
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
  ): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .flatMap(physicalOp => {
            val workerConfigs = resourceConfig.operatorConfigs(physicalOp.id).workerConfigs
            workerConfigs.map(_.workerId).map { workerId =>
              val bytes = AmberRuntime.serde.serialize(physicalOp.opExecInitInfo).get
              asyncRPCClient.workerInterface.initializeExecutor(
                InitializeExecutorRequest(
                  workerConfigs.length,
                  Any.of(
                    "edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo",
                    ByteString.copyFrom(bytes)
                  ),
                  physicalOp.isSourceOperator,
                  "scala"
                ),
                asyncRPCClient.mkContext(workerId)
              )
            }
          })
          .toSeq
      )
  }
  private def assignPorts(region: Region): Future[Seq[EmptyReturn]] = {
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
                asyncRPCClient.workerInterface.assignPort(
                  AssignPortRequest(globalPortId.portId, globalPortId.input, schema.toRawSchema),
                  asyncRPCClient.mkContext(workerId)
                )
            }
        }
        .toSeq
    )
  }

  private def connectChannels(links: Set[PhysicalLink]): Future[Seq[EmptyReturn]] = {
    Future.collect(
      links.map { link: PhysicalLink =>
        asyncRPCClient.controllerInterface.linkWorkers(
          LinkWorkersRequest(link),
          asyncRPCClient.mkContext(CONTROLLER)
        )
      }.toSeq
    )
  }

  private def openOperators(operators: Set[PhysicalOp]): Future[Seq[EmptyReturn]] = {
    Future
      .collect(
        operators
          .map(_.id)
          .flatMap(opId =>
            workflowExecution.getRegionExecution(region.id).getOperatorExecution(opId).getWorkerIds
          )
          .map { workerId =>
            asyncRPCClient.workerInterface
              .openExecutor(EmptyRequest(), asyncRPCClient.mkContext(workerId))
          }
          .toSeq
      )
  }

  private def sendStarts(region: Region): Future[Seq[Unit]] = {
    asyncRPCClient.sendToClient(
      ExecutionStatsUpdate(
        workflowExecution.getAllRegionExecutionsStats
      )
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
              asyncRPCClient.workerInterface
                .startWorker(EmptyRequest(), asyncRPCClient.mkContext(workerId))
                .map(resp =>
                  // update worker state
                  workflowExecution
                    .getRegionExecution(region.id)
                    .getOperatorExecution(opId)
                    .getWorkerExecution(workerId)
                    .setState(resp.state)
                )
            }
        }
        .toSeq
    )
  }

}
