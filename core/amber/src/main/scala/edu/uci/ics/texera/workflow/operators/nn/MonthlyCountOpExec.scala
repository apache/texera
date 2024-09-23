package edu.uci.ics.texera.workflow.operators.nn

import edu.uci.ics.amber.engine.common.{CheckpointState, CheckpointSupport}
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.amber.engine.common.workflow.PortIdentity
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.amber.engine.common.AmberRuntime
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

import java.sql.Timestamp
import java.util.Calendar
import scala.collection.mutable

class MonthlyCountOpExec(outSchema:Schema) extends OperatorExecutor with Serializable with CheckpointSupport {

  var dedup = mutable.HashSet[Tuple]()
  var currentMonth:Int = 0
  var currentDate: Int = 0


  override def onFinish(port: Int): Iterator[TupleLike] = {
    val count: Long = dedup.size
    if (count > 0) {
      val builder = Tuple.Builder(outSchema).add("count", AttributeType.LONG, count)
      Iterator(builder.build())
    } else {
      Iterator.empty
    }
  }
  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {
    val time = tuple.getField("create_at").asInstanceOf[Timestamp]
    var count = -1L
    if (currentMonth != time.getMonth) {
      currentMonth = time.getMonth
      count = dedup.size
      dedup.clear()
    }
    dedup.add(tuple)
    if (count >= 0) {
      val builder = Tuple.Builder(outSchema).add("count", AttributeType.LONG, count)
      Iterator(builder.build())
    } else {
      Iterator.empty
    }
  }

  override def serializeState(currentIteratorState: Iterator[(TupleLike, Option[PortIdentity])], checkpoint: CheckpointState): Iterator[(TupleLike, Option[PortIdentity])] = {
    checkpoint.save("buffer", dedup)
    checkpoint.save("currentMonth", currentMonth)
    checkpoint.save("currentYear", currentDate)
    val iterArr = currentIteratorState.toArray
    checkpoint.save("currentIter", iterArr)
    iterArr.toIterator
  }

  override def deserializeState(checkpoint: CheckpointState): Iterator[(TupleLike, Option[PortIdentity])] = {
    dedup = checkpoint.load("buffer")
    currentMonth = checkpoint.load("currentMonth")
    currentDate = checkpoint.load("currentYear")
    checkpoint.load("currentIter").asInstanceOf[Array[(TupleLike, Option[PortIdentity])]].toIterator
  }

  override def getEstimatedCheckpointCost: Long = {
    AmberRuntime.serde.serialize(dedup).get.length
  }

  override def open(): Unit = {}

  override def close(): Unit = {}
}