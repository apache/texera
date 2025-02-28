package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest, StartInputChannelRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def startWorker(
      request: StartInputChannelRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerStateResponse] = {
    logger.info("Starting the worker.")
    var channelId = request.channelId
    if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      channelId = ChannelIdentity(ActorVirtualIdentity("SOURCE_STARTER"), actorId, isControl = false)
      dp.stateManager.assertState(READY)
      dp.stateManager.transitTo(RUNNING)
      // for source operator: add a virtual input channel just for kicking off the execution
      val dummyInputPortId = PortIdentity()
      dp.inputManager.addPort(dummyInputPortId, null)
      dp.inputGateway.getChannel(channelId).setPortId(dummyInputPortId)
      //dp.processDataPayload(channelId, MarkerFrame(StartOfInputChannel()))
      //dp.processDataPayload(channelId, MarkerFrame(EndOfInputChannel()))
      dp.endOfInputChannel(channelId)
    } else {
      println(
        s"non-source worker $actorId received unexpected StartWorker!")
    }
    WorkerStateResponse(dp.stateManager.getCurrentState)
  }
}
