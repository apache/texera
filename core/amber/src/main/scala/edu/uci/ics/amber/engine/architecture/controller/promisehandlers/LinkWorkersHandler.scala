package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalLinkIdentity

object LinkWorkersHandler {
  final case class LinkWorkers(linkId: PhysicalLinkIdentity) extends ControlCommand[Unit]
}

/** add a data transfer partitioning to the sender workers and update input linking
  * for the receiver workers of a link strategy.
  *
  * possible sender: controller, client
  */
trait LinkWorkersHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  registerHandler { (msg: LinkWorkers, sender) =>
    {
      val channelConfigs = cp.workflow.regionPlan
        .getRegionOfPhysicalLink(msg.linkId)
        .get
        .config
        .get
        .channelConfigs(msg.linkId)

      val senderWorkerIds = cp.executionState.getOperatorExecution(msg.linkId.from).getBuiltWorkerIds
      val futures = senderWorkerIds
        .zip(channelConfigs)
        .flatMap({
          case (senderWorkerId, channelConfig) =>
            Seq(
              send(AddPartitioning(msg.linkId, channelConfig.partitioning), senderWorkerId)
            ) ++ channelConfig.receiverWorkerIds.map(
              send(UpdateInputLinking(senderWorkerId, msg.linkId), _)
            )
        })

      Future.collect(futures).map { _ =>
        // returns when all has completed

      }
    }
  }

}
