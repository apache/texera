package edu.uci.ics.texera.workflow.operators.dummy
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor

abstract class DummyOpExec() extends OperatorExecutor {

  /**
    *  override def processTuple(
    *      tuple: Either[Tuple, InputExhausted],
    *      input: Int,
    *      pauseManager: PauseManager,
    *      asyncRPCClient: AsyncRPCClient
    *  ): Iterator[Tuple] = {
    *    throw new UnsupportedOperationException("Dummy Operator does not support execution.")
    *  }
    */

  override def open(): Unit = {}

  override def close(): Unit = {}
}
