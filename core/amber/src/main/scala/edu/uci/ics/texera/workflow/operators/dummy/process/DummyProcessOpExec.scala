package edu.uci.ics.texera.workflow.operators.dummy.process

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class DummyProcessOpExec() extends OperatorExecutor {

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        input match {
        case 0 =>
          Iterator.empty
        case 1 =>
          Iterator(t)
      }
      case Right(_) => Iterator.empty
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

}
