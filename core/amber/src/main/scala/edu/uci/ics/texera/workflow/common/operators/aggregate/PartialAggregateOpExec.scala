package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.operators.aggregate.AggregationOperation

import scala.collection.mutable

class PartialAggregateOpExec(
    aggregations: List[AggregationOperation] ,
    groupByKeys: List[String]
) extends OperatorExecutor {

  var partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()
  var aggFuncs :List[DistributedAggregation[Object]]= _
  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    if (aggregations.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    tuple match {
      case Left(t) =>
        val key = Option(groupByKeys)
          .filter(_.nonEmpty)
          .map(_.map(k => t.getField[Object](k)))
          .getOrElse(List.empty)
        println(t.getSchema)
        aggFuncs = aggregations.map(agg => agg.getAggFunc(t.getSchema.getAttribute(agg.attribute).getType))
        val partialObjects =
          partialObjectsPerKey.getOrElseUpdate(key, aggFuncs.map(aggFunc => aggFunc.init()))
        val updatedPartialObjects = aggFuncs.zip(partialObjects).map {
          case (aggFunc, partial) =>
            aggFunc.iterate(partial, t)
        }
        partialObjectsPerKey.put(key, updatedPartialObjects)
        Iterator()
      case Right(_) =>
        partialObjectsPerKey.iterator.map {

          case (group, partial) =>
            TupleLike(group ++ aggFuncs.indices.map(i => aggFuncs(i).finalAgg(partial(i))))
        }
    }
  }

}
