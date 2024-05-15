package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

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
