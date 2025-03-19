package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.core.WorkflowRuntimeException
import edu.uci.ics.amber.core.executor.SourceOperatorExecutor
import edu.uci.ics.amber.core.virtualidentity.{ActorVirtualIdentity, ChannelIdentity}
import edu.uci.ics.amber.core.workflow.PortIdentity
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AsyncRPCContext, EmptyRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def startWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerStateResponse] = {
    logger.info("Starting the worker.")
    if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      val channelId = ChannelIdentity(ActorVirtualIdentity("SOURCE_STARTER"), actorId, isControl = false)
      dp.stateManager.assertState(READY)
      dp.stateManager.transitTo(RUNNING)
      // for source operator: add a virtual input channel just for kicking off the execution
      val dummyInputPortId = PortIdentity()
      dp.inputManager.addPort(dummyInputPortId, null)
      dp.inputGateway.getChannel(channelId).setPortId(dummyInputPortId)
      dp.startOfInputChannel(channelId)
      WorkerStateResponse(dp.stateManager.getCurrentState)
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }
}
