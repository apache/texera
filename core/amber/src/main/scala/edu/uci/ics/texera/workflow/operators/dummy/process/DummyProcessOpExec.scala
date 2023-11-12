package edu.uci.ics.texera.workflow.operators.dummy.process

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class DummyProcessOpExec() extends OperatorExecutor {
  private val hashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private var exhaustedCounter: Int = 0

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        hashSet.add(t)
        Iterator.empty
      case Right(_) =>
        exhaustedCounter += 1
        if (2 == exhaustedCounter) hashSet.iterator
        else Iterator.empty
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}

}
