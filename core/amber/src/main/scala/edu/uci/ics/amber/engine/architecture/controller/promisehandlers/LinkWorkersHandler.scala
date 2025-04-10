package edu.uci.ics.amber.engine.architecture.controller.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.ControllerAsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AddInputChannelRequest,
  AddPartitioningRequest,
  AsyncRPCContext,
  LinkWorkersRequest
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn

/** add a data transfer partitioning to the sender workers and update input linking
  * for the receiver workers of a link strategy.
  *
  * possible sender: controller, client
  */
trait LinkWorkersHandler {
  this: ControllerAsyncRPCHandlerInitializer =>

  override def linkWorkers(msg: LinkWorkersRequest, ctx: AsyncRPCContext): Future[EmptyReturn] = {
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
          workerInterface.addPartitioning(
            AddPartitioningRequest(msg.link, linkConfig.partitioning),
            mkContext(channelId.fromWorkerId)
          ),
          workerInterface.addInputChannel(
            AddInputChannelRequest(channelId, msg.link.toPortId),
            mkContext(channelId.toWorkerId)
          )
        )
      })

    Future.collect(futures).map { _ =>
      // returns when all has completed
      EmptyReturn()
    }
  }

}
