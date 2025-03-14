package edu.uci.ics.amber.engine.architecture.common

import akka.actor.{Address, AddressFromURIString, Deploy}
import akka.remote.RemoteScope
import edu.uci.ics.amber.core.workflow.deployment.{DeploymentStrategies, NodeProfiles}
import edu.uci.ics.amber.core.workflow.{
  CustomizedPreference,
  GoToSpecificNode,
  PhysicalOp,
  PreferController,
  RoundRobinPreference
}
import edu.uci.ics.amber.engine.architecture.controller.execution.OperatorExecution
import edu.uci.ics.amber.engine.architecture.deploysemantics.AddressInfo
import edu.uci.ics.amber.engine.architecture.pythonworker.PythonWorkflowWorker
import edu.uci.ics.amber.engine.architecture.scheduling.config.OperatorConfig
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker
import edu.uci.ics.amber.engine.architecture.worker.WorkflowWorker.{
  FaultToleranceConfig,
  StateRestoreConfig,
  WorkerReplayInitialization
}
import edu.uci.ics.amber.util.VirtualIdentityUtils

object ExecutorDeployment {

  def createWorkers(
      deploymentStrategy: String,
      operators: Set[PhysicalOp],
      op: PhysicalOp,
      controllerActorService: AkkaActorService,
      operatorExecution: OperatorExecution,
      operatorConfig: OperatorConfig,
      stateRestoreConfig: Option[StateRestoreConfig],
      replayLoggingConfig: Option[FaultToleranceConfig]
  ): Unit = {

    val addressInfo = AddressInfo(
      controllerActorService.getClusterNodeAddresses,
      controllerActorService.self.path.address
    )

    operatorConfig.workerConfigs.foreach(workerConfig => {
      val workerId = workerConfig.workerId
      val workerIndex = VirtualIdentityUtils.getWorkerIndex(workerId)

      val locationPreference =
        if (deploymentStrategy == null || deploymentStrategy.isEmpty || deploymentStrategy == "rr")
          op.locationPreference.getOrElse(RoundRobinPreference)
        else
          CustomizedPreference

      val preferredAddress: Address = locationPreference match {
        case PreferController =>
          addressInfo.controllerAddress
        case node: GoToSpecificNode =>
          val targetAddress = AddressFromURIString(node.nodeAddr)
          addressInfo.allAddresses.find(addr => addr == targetAddress) match {
            case Some(address) => address
            case None =>
              throw new IllegalStateException(
                s"Designated node address '${node.nodeAddr}' not found among available addresses: " +
                  addressInfo.allAddresses.map(_.host.getOrElse("None")).mkString(", ")
              )
          }
        case RoundRobinPreference =>
          assert(
            addressInfo.allAddresses.nonEmpty,
            "Execution failed to start, no available computation nodes"
          )
          addressInfo.allAddresses(workerIndex % addressInfo.allAddresses.length)
        case CustomizedPreference =>
          deploymentStrategy match {
            case "maxQuality" =>
              AddressFromURIString(
                DeploymentStrategies.maxQuality(
                  addressInfo.allAddresses.map(_.toString),
                  NodeProfiles.getAllProfiles,
                  operators,
                  op
                )
              )
            case "maxSpeed" =>
              AddressFromURIString(
                DeploymentStrategies.maxSpeed(
                  addressInfo.allAddresses.map(_.toString),
                  NodeProfiles.getAllProfiles,
                  operators,
                  op
                )
              )
            case "hybrid" =>
              AddressFromURIString(
                DeploymentStrategies.hybrid(
                  addressInfo.allAddresses.map(_.toString),
                  NodeProfiles.getAllProfiles,
                  operators,
                  op
                )
              )
          }
      }

      val workflowWorker = if (op.isPythonBased) {
        PythonWorkflowWorker.props(workerConfig)
      } else {
        WorkflowWorker.props(
          workerConfig,
          WorkerReplayInitialization(
            stateRestoreConfig,
            replayLoggingConfig
          )
        )
      }
      // Note: At this point, we don't know if the actor is fully initialized.
      // Thus, the ActorRef returned from `controllerActorService.actorOf` is ignored.
      controllerActorService.actorOf(
        workflowWorker.withDeploy(Deploy(scope = RemoteScope(preferredAddress)))
      )
      operatorExecution.initWorkerExecution(workerId)
    })
  }

}
