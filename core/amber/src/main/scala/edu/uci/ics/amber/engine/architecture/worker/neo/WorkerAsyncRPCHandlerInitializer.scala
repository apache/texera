package edu.uci.ics.amber.engine.architecture.worker.neo

import edu.uci.ics.amber.engine.architecture.messaginglayer.{BatchToTupleConverter, ControlOutputPort, DataOutputPort, TupleToBatchConverter}
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.{AddOutputPolicyHandler, CollectSinkResultsHandler, PauseHandler, QueryBreakpointsHandler, QueryCurrentInputTupleHandler, QueryStatisticsHandler, ResumeHandler, StartHandler, UpdateInputLinkingHandler}
import edu.uci.ics.amber.engine.common.{IOperatorExecutor, WorkflowLogger}
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.{AsyncRPCClient, AsyncRPCHandlerInitializer, AsyncRPCServer, WorkflowPromise}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager

class WorkerAsyncRPCHandlerInitializer(
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val dataOutputPort: DataOutputPort,
    val tupleToBatchConverter: TupleToBatchConverter,
    val batchToTupleConverter: BatchToTupleConverter,
    val pauseManager: PauseManager,
    val dataProcessor: DataProcessor,
    val operator:IOperatorExecutor,
    val breakpointManager: BreakpointManager,
    val stateManager: WorkerStateManager,
    source: AsyncRPCClient,
    receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with PauseHandler
with AddOutputPolicyHandler
with CollectSinkResultsHandler
with QueryBreakpointsHandler
with QueryCurrentInputTupleHandler
with QueryStatisticsHandler
with ResumeHandler
with StartHandler
with UpdateInputLinkingHandler
{
  val logger: WorkflowLogger = WorkflowLogger("WorkerControlHandler")
}
