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
      val linkConfig = cp.workflow.regionPlan
        .getRegionOfPhysicalLink(msg.linkId)
        .get
        .config
        .get
        .linkConfigs(msg.linkId)

      val futures = linkConfig.channelConfigs
        .flatMap(channelConfig =>
          Seq(
            send(AddPartitioning(msg.linkId, linkConfig.partitioning), channelConfig.fromWorkerId),
            send(
              UpdateInputLinking(channelConfig.fromWorkerId, msg.linkId),
              channelConfig.toWorkerId
            )
          )
        )

      Future.collect(futures).map { _ =>
        // returns when all has completed

      }
    }
  }

}
