package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
class FlatMapOpExec(
) extends OperatorExecutor
    with Serializable {

  var flatMapFunc: Tuple => Iterator[TupleLike] = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, `setFlatMapFuncJava` should be used instead.
    */
  def setFlatMapFunc(func: Tuple => Iterator[TupleLike]): Unit = {
    this.flatMapFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    tuple match {
      case Left(t)  => flatMapFunc(t)
      case Right(_) => Iterator()
    }
  }

}
