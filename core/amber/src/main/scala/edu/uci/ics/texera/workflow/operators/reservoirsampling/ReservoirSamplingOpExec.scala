package edu.uci.ics.texera.workflow.operators.reservoirsampling

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.util.Random

class ReservoirSamplingOpExec(val actor: Int, val opDesc: ReservoirSamplingOpDesc)
    extends OperatorExecutor {
  var n: Int = 0

  val reservoir: Array[Tuple] = Array.ofDim(opDesc.getKForActor(actor))
  val rand: Random = new Random(opDesc.getSeed(actor))

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (n < opDesc.getKForActor(actor)) {
          reservoir(n) = t
        } else {
          val i = rand.nextInt(n)
          if (i < opDesc.getKForActor(actor)) {
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
