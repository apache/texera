package edu.uci.ics.texera.workflow.common.operators.map

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.TexeraTuple

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
abstract class TexeraMapOpExec()
    extends TexeraOperatorExecutor
    with Serializable {

  var mapFunc: TexeraTuple => TexeraTuple = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, it should be called with:
    * setMapFunc((Function1<TexeraTuple, TexeraTuple> & Serializable) func)
    */
  def setMapFunc(func: TexeraTuple => TexeraTuple): Unit = {
    mapFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t)  => Iterator(mapFunc(t))
      case Right(_) => Iterator()
    }
  }
}
