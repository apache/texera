package edu.uci.ics.texera.workflow.common.operators.consolidate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.ProgressiveUtils
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable.ArrayBuffer

class ConsolidateOpExec(operatorSchemaInfo: OperatorSchemaInfo) extends OperatorExecutor {

  private val results = new ArrayBuffer[Tuple]()

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {

    tuple match {
      case Left(t) =>
        val (isInsertion, tupleValue) =
          ProgressiveUtils.getTupleFlagAndValue(t, operatorSchemaInfo)
        if (isInsertion) {
          results += tupleValue
        } else {
          results -= tupleValue
        }
        Iterator()
      case Right(_) =>
        results.iterator
    }

  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}
