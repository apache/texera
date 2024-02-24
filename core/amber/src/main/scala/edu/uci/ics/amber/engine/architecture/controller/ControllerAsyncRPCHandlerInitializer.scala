package edu.uci.ics.amber.engine.architecture.controller

import edu.uci.ics.amber.engine.architecture.controller.promisehandlers._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.AmberLogging

class ControllerAsyncRPCHandlerInitializer(
    val cp: ControllerProcessor
) extends AsyncRPCHandlerInitializer(cp.asyncRPCClient, cp.asyncRPCServer)
    with AmberLogging
    with LinkWorkersHandler
    with WorkerExecutionCompletedHandler
    with WorkerStateUpdatedHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with PortCompletedHandler
    with FatalErrorHandler
    with ConsoleMessageHandler
    with RetryWorkflowHandler
    with ModifyLogicHandler
    with EvaluatePythonExpressionHandler
    with DebugCommandHandler
    with TakeGlobalCheckpointHandler
    with ChannelMarkerHandler
    with RetrieveWorkflowStateHandler {
  val actorId: ActorVirtualIdentity = cp.actorId

}
