package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class HashJoinBuildOpExec[K](
    val buildAttributeName: String
) extends OperatorExecutor {

  var buildTableHashMap: mutable.HashMap[K, (ListBuffer[Tuple], Boolean)] = _
  var outputSchema: Schema =
    Schema.newBuilder().add("key", AttributeType.ANY).add("value", AttributeType.ANY).build()

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    tuple match {
      case Left(tuple) =>
        building(tuple)
        Iterator()
      case Right(_) =>
        buildTableHashMap.iterator.map {
          case (k, v) =>
            Tuple
              .newBuilder(outputSchema)
              .add("key", AttributeType.ANY, k)
              .add("value", AttributeType.ANY, v)
              .build()
        }
    }
  }

  private def building(tuple: Tuple): Unit = {
    val key = tuple.getField(buildAttributeName).asInstanceOf[K]
    val (storedTuples, _) =
      buildTableHashMap.getOrElseUpdate(key, (new ListBuffer[Tuple](), false))
    storedTuples += tuple
  }

  override def open(): Unit = {
    buildTableHashMap = new mutable.HashMap[K, (mutable.ListBuffer[Tuple], Boolean)]()
  }

  override def close(): Unit = {
    buildTableHashMap.clear()
  }
}
