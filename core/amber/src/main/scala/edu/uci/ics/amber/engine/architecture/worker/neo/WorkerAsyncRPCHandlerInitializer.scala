package edu.uci.ics.amber.engine.architecture.worker.neo

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler
import edu.uci.ics.amber.engine.common.WorkflowLogger
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionPaused
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.rpc.{
  AsyncRPCHandlerInitializer,
  AsyncRPCServer,
  AsyncRPCClient,
  WorkflowPromise
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager

class WorkerAsyncRPCHandlerInitializer(
                                       val selfID: ActorVirtualIdentity,
                                       val controlOutputPort: ControlOutputPort,
                                       val pauseManager: PauseManager,
                                       val dataProcessor: DataProcessor,
                                       source: AsyncRPCClient,
                                       receiver: AsyncRPCServer
) extends AsyncRPCHandlerInitializer(source, receiver)
    with PauseHandler {
  val logger: WorkflowLogger = WorkflowLogger("WorkerControlHandler")
}
