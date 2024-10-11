package edu.uci.ics.texera.workflow.operators.reservoirsampling

import edu.uci.ics.amber.engine.common.executor.OperatorExecutor
import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}

import scala.util.Random

class ReservoirSamplingOpExec(actor: Int, kPerActor: Int => Int, seedFunc: Int => Int)
    extends OperatorExecutor {
  private var n: Int = 0
  private val reservoir: Array[Tuple] = Array.ofDim(kPerActor(actor))
  private val rand: Random = new Random(seedFunc(actor))

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (n < kPerActor(actor)) {
      reservoir(n) = tuple
    } else {
      val i = rand.nextInt(n)
      if (i < kPerActor(actor)) {
        reservoir(i) = tuple
      }
    }
    n += 1
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = reservoir.iterator

}
