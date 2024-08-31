package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}
import edu.uci.ics.amber.engine.common.SourceOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.MarkerFrame
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.SOURCE_STARTER_ACTOR
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.EndOfUpstream

object StartHandler {
  final case class StartWorker() extends ControlCommand[WorkerState]
}

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: StartWorker, sender) =>
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
        MarkerFrame(EndOfUpstream())
      )
      dp.stateManager.getCurrentState
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }
}
