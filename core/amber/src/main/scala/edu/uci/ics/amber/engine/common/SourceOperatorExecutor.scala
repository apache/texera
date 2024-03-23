package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait SourceOperatorExecutor extends OperatorExecutor {
  override def open(): Unit = {}

  override def close(): Unit = {}
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator()

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator.empty

  def produceTuple(): Iterator[TupleLike]

  override def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] = {
    // We assume there is only one input port for source operators. The current assumption
    // makes produceTuple to be invoked on each input port finish.
    // We should move this to onFinishAllPorts later.
    produceTuple().map(t => (t, Option.empty))
  }

}
