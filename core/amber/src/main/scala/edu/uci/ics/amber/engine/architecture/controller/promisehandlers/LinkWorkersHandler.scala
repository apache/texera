package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.UpdateInputLinkingHandler.UpdateInputLinking
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.CONTROLLER
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

  registerHandler[LinkWorkers, Unit] { (msg, sender) =>
    {
      val resourceConfig = cp.workflow.regionPlan
        .getRegionOfPhysicalLink(msg.link)
        .resourceConfig
        .get
      val linkConfig = resourceConfig.linkConfigs(msg.link)

      val futures = linkConfig.channelConfigs
        .flatMap(channelConfig => {
          cp.executionState.builtChannels
            .add(ChannelIdentity(CONTROLLER, channelConfig.fromWorkerId, isControl = true))
          cp.executionState.builtChannels
            .add(ChannelIdentity(CONTROLLER, channelConfig.toWorkerId, isControl = true))
          cp.executionState.builtChannels
            .add(
              ChannelIdentity(
                channelConfig.fromWorkerId,
                channelConfig.toWorkerId,
                isControl = false
              )
            )
          Seq(
            send(AddPartitioning(msg.link, linkConfig.partitioning), channelConfig.fromWorkerId),
            send(
              UpdateInputLinking(channelConfig.fromWorkerId, msg.link),
              channelConfig.toWorkerId
            )
          )
        })

      Future.collect(futures).map { _ =>
        // returns when all has completed

      }
    }
  }

}
