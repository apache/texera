package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.texera.workflow.common.operators.TexeraOperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.TexeraTuple

import scala.collection.JavaConverters

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
class TexeraFlatMapOpExec(
) extends TexeraOperatorExecutor
    with Serializable {

  var flatMapFunc: TexeraTuple => Iterator[TexeraTuple] = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, `setFlatMapFuncJava` should be used instead.
    */
  def setFlatMapFunc(func: TexeraTuple => Iterator[TexeraTuple]): Unit = {
    this.flatMapFunc = func
  }

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, it should be called with:
    * setFlatMapFuncJava((Function1<TexeraTuple, Iterator<TexeraTuple>> & Serializable) func)
    */
  def setFlatMapFuncJava(func: TexeraTuple => java.util.Iterator[TexeraTuple]): Unit = {
    this.flatMapFunc = t => JavaConverters.asScalaIterator(func(t))
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[TexeraTuple, InputExhausted],
      input: Int
  ): Iterator[TexeraTuple] = {
    tuple match {
      case Left(t)  => flatMapFunc(t)
      case Right(_) => Iterator()
    }
  }

}
