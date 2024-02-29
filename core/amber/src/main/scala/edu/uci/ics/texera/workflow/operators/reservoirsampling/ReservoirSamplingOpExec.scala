package edu.uci.ics.texera.workflow.operators.reservoirsampling

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.util.Random

class ReservoirSamplingOpExec(actor: Int, kPerActor: Int => Int, seedFunc: Int => Int)
    extends OperatorExecutor {
  private var n: Int = 0
  private val reservoir: Array[Tuple] = Array.ofDim(kPerActor(actor))
  private val rand: Random = new Random(seedFunc(actor))

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    tuple match {
      case Left(t) =>
        if (n < kPerActor(actor)) {
          reservoir(n) = t
        } else {
          val i = rand.nextInt(n)
          if (i < kPerActor(actor)) {
            reservoir(i) = t
          }
        }
        n += 1
        Iterator()
      case Right(_) => reservoir.iterator
    }
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
