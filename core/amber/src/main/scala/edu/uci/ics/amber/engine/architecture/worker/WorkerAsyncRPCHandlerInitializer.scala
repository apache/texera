package edu.uci.ics.amber.engine.architecture.worker

import akka.actor.ActorContext
import edu.uci.ics.amber.engine.architecture.messaginglayer.{
  BatchToTupleConverter,
  ControlOutputPort,
  DataOutputPort,
  TupleToBatchConverter
}
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers._
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCClient,
  AsyncRPCHandlerInitializer,
  AsyncRPCServer
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, WorkflowLogger}

class WorkerAsyncRPCHandlerInitializer(
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val dataOutputPort: DataOutputPort,
    val tupleToBatchConverter: TupleToBatchConverter,
    val batchToTupleConverter: BatchToTupleConverter,
    val pauseManager: PauseManager,
    val dataProcessor: DataProcessor,
    val operator: IOperatorExecutor,
    val breakpointManager: BreakpointManager,
    val stateManager: WorkerStateManager,
    val actorContext: ActorContext,
    source: AsyncRPCClient,
    receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with PauseHandler
    with AddOutputPolicyHandler
    with CollectSinkResultsHandler
    with QueryAndRemoveBreakpointsHandler
    with QueryCurrentInputTupleHandler
    with QueryStatisticsHandler
    with ResumeHandler
    with StartHandler
    with UpdateInputLinkingHandler {
  val logger: WorkflowLogger = WorkflowLogger("WorkerControlHandler")
}
