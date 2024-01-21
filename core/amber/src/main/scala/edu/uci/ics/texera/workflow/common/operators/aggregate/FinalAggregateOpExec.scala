package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.worker.PauseManager
import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.rpc.AsyncRPCClient
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.PartialAggregateOpExec.internalAggObjKey
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}

import scala.collection.mutable

class FinalAggregateOpExec(
    val aggFuncs: List[DistributedAggregation[Object]],
    val groupByKeys: List[String],
    val outputSchema: Schema
) extends OperatorExecutor {

  var groupByKeyAttributes: Array[Attribute] = _
  var schema: Schema = _

  // each value in partialObjectsPerKey has the same length as aggFuncs
  // partialObjectsPerKey(key)[i] corresponds to aggFuncs[i]
  var partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTexeraTuple(
      tuple: Either[Tuple, InputExhausted],
      input: Int,
      pauseManager: PauseManager,
      asyncRPCClient: AsyncRPCClient
  ): Iterator[Tuple] = {
    if (aggFuncs.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    tuple match {
      case Left(t) =>
        val key =
          if (groupByKeys == null || groupByKeys.isEmpty) List()
          else groupByKeys.map(k => t.getField[Object](k))

        val partialObjects =
          aggFuncs.indices.map(i => t.getField[Object](internalAggObjKey(i))).toList
        if (!partialObjectsPerKey.contains(key)) {
          partialObjectsPerKey.put(key, partialObjects)
        } else {
          val updatedPartialObjects = aggFuncs.indices
            .map(i => {
              val aggFunc = aggFuncs(i)
              val partial1 = partialObjectsPerKey(key)(i)
              val partial2 = partialObjects(i)
              aggFunc.merge(partial1, partial2)
            })
            .toList
          partialObjectsPerKey.put(key, updatedPartialObjects)
        }
        Iterator()
      case Right(_) =>
        partialObjectsPerKey.iterator.map(pair => {
          val finalAggValues = aggFuncs.indices.map(i => aggFuncs(i).finalAgg(pair._2(i)))

          val tupleBuilder = Tuple.newBuilder(outputSchema)
          // add group by keys and final agg values
          tupleBuilder.addSequentially((pair._1 ++ finalAggValues).toArray)

          tupleBuilder.build()
        })
    }
  }

}
