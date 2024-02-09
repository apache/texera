package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers.LinkWorkersHandler.LinkWorkers
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddPartitioningHandler.AddPartitioning
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.AddInputChannelHandler.AddInputChannel
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
        .map(_.channelId)
        .flatMap(channelId => {
          cp.executionState.builtChannels
            .add(ChannelIdentity(CONTROLLER, channelId.fromWorkerId, isControl = true))
          cp.executionState.builtChannels
            .add(ChannelIdentity(CONTROLLER, channelId.toWorkerId, isControl = true))
          cp.executionState.builtChannels
            .add(channelId)
          Seq(
            send(AddPartitioning(msg.link, linkConfig.partitioning), channelId.fromWorkerId),
            send(
              AddInputChannel(channelId, msg.link.toPortId),
              channelId.toWorkerId
            )
          )
        })

      Future.collect(futures).map { _ =>
        // returns when all has completed

      }
    }
  }

}
