package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait SinkOperatorExecutor extends OperatorExecutor {

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(Tuple, Option[PortIdentity])] = {
    consumeTuple(tuple, port)
    Iterator.empty
  }

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] =
    Iterator.empty

  def consumeTuple(tuple: Tuple, input: Int): Unit

  override def open(): Unit = {}

  override def close(): Unit = {}
}
