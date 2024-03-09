package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Executes a flatMap() operation.
  * This operation takes a single input Tuple, flattens it, applies a mapping function to each element,
  * and produces an output Tuple for each element.
  */
class FlatMapOpExec extends OperatorExecutor with Serializable {

  var flatMapFunc: Tuple => Iterator[TupleLike] = _

  def setFlatMapFunc(func: Tuple => Iterator[TupleLike]): Unit = flatMapFunc = func

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = flatMapFunc(tuple)

}
