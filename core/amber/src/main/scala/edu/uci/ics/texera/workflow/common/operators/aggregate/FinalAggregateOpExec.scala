package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.common.InputExhausted
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.operators.aggregate.AggregateOpDesc.internalAggObjKey
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class FinalAggregateOpExec(
    val aggFuncs: List[DistributedAggregation[Object]],
    val groupByKeys: List[String]
) extends OperatorExecutor {

  // each value in partialObjectsPerKey has the same length as aggFuncs
  // partialObjectsPerKey(key)[i] corresponds to aggFuncs[i]
  var partialObjectsPerKey = new mutable.HashMap[List[Object], List[Object]]()

  override def open(): Unit = {}
  override def close(): Unit = {}

  override def processTuple(
      tuple: Either[Tuple, InputExhausted],
      port: Int
  ): Iterator[TupleLike] = {
    if (aggFuncs.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    tuple match {
      case Left(t) =>
        val key = Option(groupByKeys)
          .filter(_.nonEmpty)
          .map(_.map(k => t.getField[Object](k)))
          .getOrElse(List.empty)

        val partialObjects =
          aggFuncs.indices.map(i => t.getField[Object](internalAggObjKey(i))).toList

        partialObjectsPerKey.put(
          key,
          partialObjectsPerKey
            .get(key)
            .map(existingPartials =>
              aggFuncs.indices
                .map(i => aggFuncs(i).merge(existingPartials(i), partialObjects(i)))
                .toList
            )
            .getOrElse(partialObjects)
        )
        Iterator()
      case Right(_) =>
        partialObjectsPerKey.iterator.map {
          case (group, partial) =>
            TupleLike(group ++ aggFuncs.indices.map(i => aggFuncs(i).finalAgg(partial(i))))
        }
    }
  }

}
