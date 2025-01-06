package edu.uci.ics.amber.operator.reservoirsampling

import edu.uci.ics.amber.core.executor.OperatorExecutor
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils.equallyPartitionGoal
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.util.Random

class ReservoirSamplingOpExec(descString: String, idx: Int, workerCount: Int)
    extends OperatorExecutor {
  private val desc: ReservoirSamplingOpDesc =
    objectMapper.readValue(descString, classOf[ReservoirSamplingOpDesc])
  private val count: Int = equallyPartitionGoal(desc.k, workerCount)(idx)
  private var n: Int = 0
  private val reservoir: Array[Tuple] = Array.ofDim(count)
  private val rand: Random = new Random(workerCount)

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    if (n < count) {
      reservoir(n) = tuple
    } else {
      val i = rand.nextInt(n)
      if (i < count) {
        reservoir(i) = tuple
      }
    }
    n += 1
    Iterator()
  }

  override def onFinish(port: Int): Iterator[TupleLike] = reservoir.iterator

}
