package edu.uci.ics.amber.engine.architecture.worker.neo

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionPaused
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.{
  RPCClient,
  RPCHandlerInitializer,
  RPCServer,
  WorkflowPromise
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager

class WorkerRPCHandlerInitializer(
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val pauseManager: PauseManager,
    val dataProcessor: DataProcessor,
    rpcClient: RPCClient,
    rpcServer: RPCServer
) extends RPCHandlerInitializer(rpcClient, rpcServer)
    with PauseHandler {

  def createPromise[T](): (Promise[T], Long) = {
    rpcClient.createPromise()
  }
}
