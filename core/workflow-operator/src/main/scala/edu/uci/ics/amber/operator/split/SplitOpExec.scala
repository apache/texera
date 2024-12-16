package edu.uci.ics.amber.operator.split

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.workflow.PortIdentity

import scala.util.Random

class SplitOpExec(
    descString: String
) extends OperatorExecutor {
  val desc: SplitOpDesc = objectMapper.readValue(descString, classOf[SplitOpDesc])
  lazy val random = new Random(desc.seed)

  override def processTupleMultiPort(
      tuple: Tuple,
      port: Int
  ): Iterator[(TupleLike, Option[PortIdentity])] = {
    val isTraining = random.nextInt(100) < desc.k
    // training output port: 0, testing output port: 1
    val port = if (isTraining) PortIdentity(0) else PortIdentity(1)
    Iterator.single((tuple, Some(port)))
  }

  override def processTuple(tuple: Tuple, port: Int): Iterator[Tuple] = ???

}
