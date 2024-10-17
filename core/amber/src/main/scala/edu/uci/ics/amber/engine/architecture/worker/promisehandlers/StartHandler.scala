package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{EmptyRequest, AsyncRPCContext}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.WorkerStateResponse
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.MarkerFrame
import edu.uci.ics.amber.engine.common.executor.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.model.{EndOfInputChannel, StartOfInputChannel}
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SOURCE_STARTER_ACTOR
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def startWorker(
      request: EmptyRequest,
      ctx: AsyncRPCContext
  ): Future[WorkerStateResponse] = {
    logger.info("Starting the worker.")
    if (dp.executor.isInstanceOf[SourceOperatorExecutor]) {
      dp.stateManager.assertState(READY)
      dp.stateManager.transitTo(RUNNING)
      // for source operator: add a virtual input channel just for kicking off the execution
      val dummyInputPortId = PortIdentity()
      dp.inputManager.addPort(dummyInputPortId, null)
      dp.inputGateway
        .getChannel(ChannelIdentity(SOURCE_STARTER_ACTOR, actorId, isControl = false))
        .setPortId(dummyInputPortId)
      dp.processDataPayload(
        ChannelIdentity(SOURCE_STARTER_ACTOR, dp.actorId, isControl = false),
        MarkerFrame(StartOfInputChannel())
      )
      dp.processDataPayload(
        ChannelIdentity(SOURCE_STARTER_ACTOR, dp.actorId, isControl = false),
        MarkerFrame(EndOfInputChannel())
      )
      WorkerStateResponse(dp.stateManager.getCurrentState)
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }

}
