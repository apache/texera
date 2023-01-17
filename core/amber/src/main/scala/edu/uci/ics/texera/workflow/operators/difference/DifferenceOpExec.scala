package edu.uci.ics.texera.workflow.operators.difference

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.arrow.util.Preconditions

import scala.collection.mutable

class DifferenceOpExec() extends OperatorExecutor {

  private val leftHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private val rightHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private var exhaustedCounter: Int = 0

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    if (input >= 2) {
      throw new IllegalArgumentException("input port should not be more than 2")
    }
    tuple match {
      case Left(t) =>
        if (input == 1) { // right input
          rightHashSet.add(t)
        } else { // left input
          leftHashSet.add(t)
        }
        Iterator()
      case Right(_) =>
        exhaustedCounter += 1
        if (2 == exhaustedCounter) {
          leftHashSet.diff(rightHashSet).iterator
        } else {
          Iterator()
        }
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
