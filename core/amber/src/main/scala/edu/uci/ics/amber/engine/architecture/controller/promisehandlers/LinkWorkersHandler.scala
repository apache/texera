package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, LinkWorkersRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.Empty
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink

/** add a data transfer partitioning to the sender workers and update input linking
  * for the receiver workers of a link strategy.
  *
  * possible sender: controller, client
  */
trait LinkWorkersHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def sendLinkWorkers(msg: LinkWorkersRequest, ctx: AsyncRPCContext): Future[Empty] = {
    val region = cp.workflowExecutionCoordinator.getRegionOfLink(msg.link)
    val resourceConfig = region.resourceConfig.get
    val linkConfig = resourceConfig.linkConfigs(msg.link)
    val linkExecution =
      cp.workflowExecution.getRegionExecution(region.id).initLinkExecution(msg.link)
    val futures = linkConfig.channelConfigs
      .map(_.channelId)
      .flatMap(channelId => {
        linkExecution.initChannelExecution(channelId)
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
