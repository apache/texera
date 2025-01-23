package edu.uci.ics.amber.engine.architecture.controller

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.controller.promisehandlers._
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.AsyncRPCContext
import edu.uci.ics.amber.engine.architecture.rpc.controllerservice.ControllerServiceFs2Grpc
import edu.uci.ics.amber.engine.common.AmberLogging
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.core.virtualidentity.ActorVirtualIdentity

class ControllerAsyncRPCHandlerInitializer(
    val cp: ControllerProcessor
) extends AsyncRPCHandlerInitializer(cp.asyncRPCClient, cp.asyncRPCServer)
    with ControllerServiceFs2Grpc[Future, AsyncRPCContext]
    with AmberLogging
    with LinkWorkersHandler
    with WorkerExecutionCompletedHandler
    with WorkerStateUpdatedHandler
    with PauseHandler
    with QueryWorkerStatisticsHandler
    with ResumeHandler
    with StartWorkflowHandler
    with PortCompletedHandler
    with ConsoleMessageHandler
    with RetryWorkflowHandler
    with EvaluatePythonExpressionHandler
    with DebugCommandHandler
    with TakeGlobalCheckpointHandler
    with ChannelMarkerHandler
    with RetrieveWorkflowStateHandler {
  val actorId: ActorVirtualIdentity = cp.actorId
}
