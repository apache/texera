package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
abstract class MapOpExec() extends OperatorExecutor with Serializable {

  var mapFunc: Tuple => Tuple = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, it should be called with:
    * setMapFunc((Function1<TexeraTuple, TexeraTuple> & Serializable) func)
    */
  def setMapFunc(func: Tuple => Tuple): Unit = {
    mapFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t)  => Iterator(mapFunc(t))
      case Right(_) => Iterator()
    }
  }
}
