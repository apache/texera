package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait ISinkOperatorExecutor extends IOperatorExecutor {

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[(Tuple, Option[PortIdentity])] = {
    consume(tuple, input)
    Iterator.empty
  }

  def consume(tuple: Either[Tuple, InputExhausted], input: Int): Unit
}
