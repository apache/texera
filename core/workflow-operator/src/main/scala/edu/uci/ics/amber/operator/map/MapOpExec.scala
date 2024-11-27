package edu.uci.ics.amber.operator.map

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
abstract class MapOpExec extends OperatorExecutor with Serializable {

  private var mapFunc: Tuple => TupleLike = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, it should be called with:
    * setMapFunc((Function1<Tuple, TupleLike> & Serializable) func)
    */
  def setMapFunc(func: Tuple => TupleLike): Unit = mapFunc = func

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = Iterator(mapFunc(tuple))

}
