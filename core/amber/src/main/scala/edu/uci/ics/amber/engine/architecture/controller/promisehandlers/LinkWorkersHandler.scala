package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

object LinkWorkersHandler {
  final case class LinkWorkers(link: PhysicalLink) extends ControlCommand[Unit]
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
        .getRegionOfPhysicalLink(msg.link)
        .get
        .config
        .get
        .linkConfigs(msg.link)

      val futures = linkConfig.channelConfigs
        .flatMap(channelConfig =>
          Seq(
            send(AddPartitioning(msg.link, linkConfig.partitioning), channelConfig.fromWorkerId),
            send(
              UpdateInputLinking(channelConfig.fromWorkerId, msg.link),
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
