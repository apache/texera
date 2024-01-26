package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers.StartHandler.StartWorker
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{READY, RUNNING}
import edu.uci.ics.amber.engine.common.ISourceOperatorExecutor
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.ambermessage.EndOfUpstream
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCServer.ControlCommand
import edu.uci.ics.amber.engine.common.virtualidentity.ChannelIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{
  SOURCE_STARTER_ACTOR,
  SOURCE_STARTER_OP
}
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}

object StartHandler {
  final case class StartWorker() extends ControlCommand[WorkerState]
}

trait StartHandler {
  this: DataProcessorRPCHandlerInitializer =>

  registerHandler { (msg: StartWorker, sender) =>
    logger.info("Starting the worker.")
    if (dp.operator.isInstanceOf[ISourceOperatorExecutor]) {
      dp.stateManager.assertState(READY)
      dp.stateManager.transitTo(RUNNING)
      // for source operator: add a virtual input channel just for kicking off the execution
      dp.registerInput(
        SOURCE_STARTER_ACTOR,
        PhysicalLink(SOURCE_STARTER_OP, PortIdentity(), dp.getOperatorId, PortIdentity())
      )
      dp.processDataPayload(
        ChannelIdentity(SOURCE_STARTER_ACTOR, dp.actorId, isControl = false),
        EndOfUpstream()
      )
      dp.stateManager.getCurrentState
    } else {
      throw new WorkflowRuntimeException(
        s"non-source worker $actorId received unexpected StartWorker!"
      )
    }
  }
}
