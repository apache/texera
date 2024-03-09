package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait SourceOperatorExecutor extends IOperatorExecutor {
  override def open(): Unit = {}

  override def close(): Unit = {}
  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = Iterator()

  def produceTuple(): Iterator[TupleLike]

  def onFinishMultiPort(port: Int): Iterator[(TupleLike, Option[PortIdentity])] =
    // The input Tuple for source operator will always be InputExhausted.
    // Source and other operators can share the same processing logic.
    // produceTuple() will be called only once.
    produceTuple().map(t => (t, Option.empty))

}
