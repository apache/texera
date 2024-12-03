package edu.uci.ics.amber.operator.split

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.workflow.PortIdentity

import scala.util.Random

class SplitOpExec(
    k: Int,
    seed: Int
) extends OperatorExecutor {

  lazy val random = new Random(seed)

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    val isTraining = random.nextInt(100) < k
    // training output port: 0, testing output port: 1
    val port = if (isTraining) PortIdentity(0) else PortIdentity(1)
    Iterator.single((tuple, Some(port)))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[Tuple] = ???

}
