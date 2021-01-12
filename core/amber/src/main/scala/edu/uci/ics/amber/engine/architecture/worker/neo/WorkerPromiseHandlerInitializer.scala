package edu.uci.ics.amber.engine.architecture.worker.neo

import com.twitter.util.Promise
import edu.uci.ics.amber.engine.architecture.messaginglayer.ControlOutputPort
import edu.uci.ics.amber.engine.architecture.worker.neo.promisehandlers.PauseHandler
import edu.uci.ics.amber.engine.common.ambermessage.WorkerMessage.ExecutionPaused
import edu.uci.ics.amber.engine.common.ambertag.neo.VirtualIdentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.promise.{
  PromiseHandlerInitializer,
  ControlInvocation,
  PromiseManager,
  WorkflowPromise
}
import edu.uci.ics.amber.engine.common.statetransition.WorkerStateManager

class WorkerPromiseHandlerInitializer(
    val selfID: ActorVirtualIdentity,
    val controlOutputPort: ControlOutputPort,
    val pauseManager: PauseManager,
    val dataProcessor: DataProcessor,
    promiseManager: PromiseManager
) extends PromiseHandlerInitializer(promiseManager)
    with PauseHandler {}
