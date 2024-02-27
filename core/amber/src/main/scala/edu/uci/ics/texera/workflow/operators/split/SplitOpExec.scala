package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.util.Random

class SplitOpExec(
    k: Int,
    worker: Int,
    getSeed: Int => Int
) extends OperatorExecutor {

  lazy val random = new Random(getSeed(worker))

  override def processTupleMultiPort(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    tuple match {
      case Left(iTuple) =>
        val isTraining = random.nextInt(100) < k
        // training output port: 0, testing output port: 1
        val port = if (isTraining) PortIdentity(0) else PortIdentity(1)
        Iterator.single((iTuple, Some(port)))
      case Right(_) => Iterator.empty
    }
  }

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[Tuple] = ???

  override def open(): Unit = {}

  override def close(): Unit = {}
}
