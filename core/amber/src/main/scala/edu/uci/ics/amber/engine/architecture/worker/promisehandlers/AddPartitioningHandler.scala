package edu.uci.ics.amber.engine.architecture.worker.promisehandlers

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.rpc.controlcommands.{
  AddPartitioningRequest,
  AsyncRPCContext
}
import edu.uci.ics.amber.engine.architecture.rpc.controlreturns.EmptyReturn
import edu.uci.ics.amber.engine.architecture.worker.DataProcessorRPCHandlerInitializer
import edu.uci.ics.amber.engine.architecture.worker.statistics.WorkerState.{PAUSED, READY, RUNNING}

trait AddPartitioningHandler {
  this: DataProcessorRPCHandlerInitializer =>

  override def addPartitioning(
      msg: AddPartitioningRequest,
      ctx: AsyncRPCContext
  ): Future[EmptyReturn] = {
    dp.stateManager.assertState(READY, RUNNING, PAUSED)
    dp.outputManager.addPartitionerWithPartitioning(msg.tag, msg.partitioning)
    EmptyReturn()
  }

}
