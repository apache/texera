package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.util.Random

class SplitOpExec(
    val actor: Int,
    val opDesc: SplitOpDesc,
    val outputMapping: mutable.HashMap[LinkIdentity, (Int, String)]
) extends OperatorExecutor {

  val outputLinkMapping: Map[String, LinkIdentity] =
    this.outputMapping.toMap.mapValues(v => v._2).map(_.swap);

  val random = new Random(opDesc.seeds(actor))

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[(ITuple, Option[LinkIdentity])] = {

    if (tuple.isLeft) {
      val isTraining = random.nextInt(100) < opDesc.k
      val port = if (isTraining) "training" else "testing"
      val outLink = outputLinkMapping.get(port)
      Iterator.single((tuple.left.get, outLink))
    } else {
      Iterator.empty
    }
  }

  def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = ???

  override def open(): Unit = {}

  override def close(): Unit = {}
}
