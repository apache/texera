package edu.uci.ics.texera.workflow.common.operators.flatmap

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

/**
  * Common operator executor of a map() function
  * A map() function transforms one input tuple to exactly one output tuple.
  */
class FlatMapOpExec(
) extends OperatorExecutor
    with Serializable {

  var flatMapFunc: Tuple => Iterator[Tuple] = _

  /**
    * Provides the flatMap function of this executor, it should be called in the constructor
    * If the operator executor is implemented in Java, `setFlatMapFuncJava` should be used instead.
    */
  def setFlatMapFunc(func: Tuple => Iterator[Tuple]): Unit = {
    this.flatMapFunc = func
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t)  => flatMapFunc(t)
      case Right(_) => Iterator()
    }
  }

}
