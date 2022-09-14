package edu.uci.ics.texera.workflow.operators.split

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.texera.workflow.common.metadata.OutputPort
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable
import scala.util.Random

class SplitOpExec(
    val actor: Int,
    val opDesc: SplitOpDesc,
    val outputPorts: List[OutputPort],
    val outputMapping: Map[LinkIdentity, Int]
) extends OperatorExecutor {

  val outputLinkMapping: Map[String, LinkIdentity] =
    this.outputMapping.mapValues(index => outputPorts(index).displayName).map(_.swap)

  val random = new Random(opDesc.seeds(actor))

  override def processTuple(
      tuple: Either[ITuple, InputExhausted],
      input: LinkIdentity,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
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
      input: LinkIdentity,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = ???

  override def open(): Unit = {}

  override def close(): Unit = {}
}
