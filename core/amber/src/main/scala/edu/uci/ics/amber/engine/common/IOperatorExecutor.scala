package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

case class InputExhausted()

trait IOperatorExecutor {

  def open(): Unit

  def close(): Unit

  def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[(TupleLike, Option[PortIdentity])]

  def getParam(query: String): String = { null }

}
