package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class CartesianProductOpExec extends OperatorExecutor {
  override def processTexeraTuple(tuple: Either[Tuple, InputExhausted], input: Int, pauseManager: PauseManager, asyncRPCClient: AsyncRPCClient): Iterator[Tuple] = {
    if (input == 0) {
      Iterator()
    } else {
      Iterator()
    }
  }

  override def open(): Unit = ???

  override def close(): Unit = ???
}
