package edu.uci.ics.texera.workflow.operators.difference

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import org.apache.arrow.util.Preconditions

import scala.collection.mutable

class DifferenceOpExec(
    val rightTable: LinkIdentity
) extends OperatorExecutor {

  private val linkIdentityHashSet: mutable.HashSet[LinkIdentity] = new mutable.HashSet()
  private val leftHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private val rightHashSet: mutable.HashSet[Tuple] = new mutable.HashSet()
  private var exhaustedCounter: Int = 0

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    if (!linkIdentityHashSet.contains(input)) {
      linkIdentityHashSet.add(input)
    }
    Preconditions.checkArgument(2 >= linkIdentityHashSet.size)

    tuple match {
      case Left(t) =>
        if (rightTable == input) {
          rightHashSet.add(t)
        } else {
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
