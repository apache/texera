package edu.uci.ics.amber.engine.architecture.worker

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{AddInputChannelRequest, AddPartitioningRequest, AssignPortRequest, AsyncRPCContext, FinalizeCheckpointRequest, InitializeExecutorRequest, UpdateExecutorRequest}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.{ FinalizeCheckpointResponse, WorkerMetricsResponse, WorkerStateResponse}
import edu.uci.ics.amber.engine.architecture.rpc.workerservice.WorkerServiceFs2Grpc
import edu.uci.ics.amber.engine.architecture.worker.promisehandlers._
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCHandlerInitializer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.AmberLogging

class DataProcessorRPCHandlerInitializer(val dp: DataProcessor)
    extends AsyncRPCHandlerInitializer(dp.asyncRPCClient, dp.asyncRPCServer)
      with WorkerServiceFs2Grpc[Future, AsyncRPCContext]
    with AmberLogging
    with InitializeExecutorHandler
    with OpenExecutorHandler
    with PauseHandler
    with AddPartitioningHandler
    with QueryStatisticsHandler
    with ResumeHandler
    with StartHandler
    with AssignPortHandler
    with AddInputChannelHandler
    with ShutdownDPThreadHandler
    with FlushNetworkBufferHandler
    with UpdateExecutorHandler
    with RetrieveStateHandler
    with PrepareCheckpointHandler
    with FinalizeCheckpointHandler {
  val actorId: ActorVirtualIdentity = dp.actorId
}
