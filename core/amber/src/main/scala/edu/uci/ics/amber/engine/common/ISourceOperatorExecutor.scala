package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity

trait ISourceOperatorExecutor extends IOperatorExecutor {

  override def processTupleMultiPort(
      tuple: Either[ITuple, InputExhausted],
      input: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    // The input Tuple for source operator will always be InputExhausted.
    // Source and other operators can share the same processing logic.
    // produceTuple() will be called only once.
    produceTuple().map(t => (t, Option.empty))
  }

  def produceTuple(): Iterator[TupleLike]

}
