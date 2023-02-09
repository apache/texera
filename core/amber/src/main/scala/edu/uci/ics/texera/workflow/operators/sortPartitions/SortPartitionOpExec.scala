package edu.uci.ics.texera.workflow.operators.sortPartitions

import com.twitter.util.Future
import edu.uci.ics.amber.engine.architecture.worker.{PauseManager, PauseType}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.{Constants, InputExhausted}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LinkIdentity}
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo}

import scala.collection.mutable.ArrayBuffer

class SortPartitionOpExec(
    sortAttributeName: String,
    operatorSchemaInfo: OperatorSchemaInfo,
    localIdx: Int,
    domainMin: Long,
    domainMax: Long,
    numberOfWorkers: Int
) extends OperatorExecutor {

  var unorderedTuples: ArrayBuffer[Tuple] = _

  def sortTuples(): Iterator[Tuple] = unorderedTuples.sortWith(compareTuples).iterator

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        unorderedTuples.append(t)
        Iterator()
      case Right(_) =>
        sortTuples()
    }
  }

  def compareTuples(t1: Tuple, t2: Tuple): Boolean = {
    val attributeType = t1.getSchema().getAttribute(sortAttributeName).getType()
    val attributeIndex = t1.getSchema().getIndex(sortAttributeName)
    attributeType match {
      case AttributeType.LONG =>
        t1.getLong(attributeIndex) < t2.getLong(attributeIndex)
      case AttributeType.INTEGER =>
        t1.getInt(attributeIndex) < t2.getInt(attributeIndex)
      case AttributeType.DOUBLE =>
        t1.getDouble(attributeIndex) < t2.getDouble(attributeIndex)
      case _ =>
        true // unsupported type
    }
  }

  override def open = {
    unorderedTuples = new ArrayBuffer[Tuple]()
  }

  override def close = {
    unorderedTuples.clear()
  }

}
