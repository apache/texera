package edu.uci.ics.texera.workflow.operators.sortPartitions

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

  var ownTuples: ArrayBuffer[Tuple] = _

  // used by helper workers to store tuples of skewed workers
  var skewedWorkerTuples: ArrayBuffer[Tuple] = _
  var skewedWorkerIdentity: ActorVirtualIdentity = null

  // used by skewed worker
  var waitingForTuplesFromHelper: Boolean = false
  var numOfTuplesReceivedFromHelper: Long = 0

  def getWorkerIdxForKey(key: Long): Int = {
    val keysPerReceiver = ((domainMax - domainMin) / numberOfWorkers).toLong + 1
    if (key < domainMin) {
      return 0
    }
    if (key > domainMax) {
      return numberOfWorkers - 1
    }
    ((key - domainMin) / keysPerReceiver).toInt
  }

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: LinkIdentity
  ): Iterator[Tuple] = {
    tuple match {
      case Left(t) =>
        if (Constants.reshapeSkewHandlingEnabled) {
          val attributeType = t.getSchema().getAttribute(sortAttributeName).getType()
          val attributeIndex = t.getSchema().getIndex(sortAttributeName)
          var workerIdxForKey = -1
          attributeType match {
            case AttributeType.LONG =>
              workerIdxForKey = getWorkerIdxForKey(t.getLong(attributeIndex))
            case AttributeType.INTEGER =>
              workerIdxForKey = getWorkerIdxForKey(t.getInt(attributeIndex).toLong)
            case AttributeType.DOUBLE =>
              workerIdxForKey = getWorkerIdxForKey(t.getDouble(attributeIndex).toLong)
            case _ =>
              throw new RuntimeException(
                "unsupported attribute type in SortOpExec: " + attributeType.toString()
              )
          }
          if (workerIdxForKey == localIdx) {
            ownTuples.append(t)
          } else {
            skewedWorkerTuples.append(t)
          }
        } else {
          ownTuples.append(t)
        }

        Iterator()
      case Right(_) =>
        ownTuples.sortWith(compareTuples).iterator
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
    ownTuples = new ArrayBuffer[Tuple]()
  }

  override def close = {
    ownTuples.clear()
  }

}
