package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.common.{IOperatorExecutor, InputExhausted}
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.tuple.Tuple

trait OperatorExecutor extends IOperatorExecutor {

  override def processTupleMultiPort(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    processTuple(tuple, port).map(t => (t, Option.empty))
  }

  def processTuple(tuple: Either[Tuple, InputExhausted], port: Int): Iterator[TupleLike]

}
