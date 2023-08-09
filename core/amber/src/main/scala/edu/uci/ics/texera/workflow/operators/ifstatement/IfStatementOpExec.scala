package edu.uci.ics.texera.workflow.operators.ifstatement

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class IfStatementOpExec() extends OperatorExecutor {
  var port = 0
  override def processTuple(
                             tuple: Either[ITuple, InputExhausted],
                             input: Int,
                             pauseManager: PauseManager,
                             asyncRPCClient: AsyncRPCClient
                           ): Iterator[(ITuple, Option[Int])] = {
    tuple match {
      case Left(t) =>
        input match {
          case 0 =>
            port = if (t.getBoolean(0)) 0 else 1
            Iterator.empty
          case 1 =>
            Iterator((t, Some(port)))
        }
      case Right(_) => Iterator.empty
    }

  }


  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(tuple: Either[Tuple, InputExhausted], input: Int, pauseManager: PauseManager, asyncRPCClient: AsyncRPCClient): Iterator[Tuple] = ???
}
