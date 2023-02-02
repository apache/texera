package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.util.Random

class SplitOpExec(
    val actor: Int,
    val opDesc: SplitOpDesc
) extends OperatorExecutor {

  val random = new Random(opDesc.seeds(actor))

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[(ITuple, Option[Int])] = {

    if (tuple.isLeft) {
      val isTraining = random.nextInt(100) < opDesc.k
      // training output port: 0, testing output port: 1
      val port = if (isTraining) 0 else 1
      Iterator.single((tuple.left.get, Some(port)))
    } else {
      Iterator.empty
    }
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = ???

  override def open(): Unit = {}

  override def close(): Unit = {}
}
