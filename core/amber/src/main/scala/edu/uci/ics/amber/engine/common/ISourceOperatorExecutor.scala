package edu.uci.ics.amber.engine.common

import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity

trait ISourceOperatorExecutor extends IOperatorExecutor {

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[(ITuple, Option[LinkIdentity])] = {
    // The input Tuple for source operator will always be InputExhausted.
    // Source and other operators can share the same processing logic.
    // produce() will be called only once.
    produce().map(t => (t, Option.empty))
  }

  def produce(): Iterator[ITuple]

}
