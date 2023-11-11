package edu.uci.ics.texera.workflow.operators.dummy.delay

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class DummyDelayOpExec(val delay: Int) extends OperatorExecutor {

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    Thread.sleep(delay)
    tuple match {
      case Left(t)  => Iterator(t)
      case Right(_) => Iterator.empty
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

}
