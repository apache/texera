package edu.uci.ics.texera.workflow.operators.aggregate

import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.OperatorExecutor
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

/**
  * AggregateOpExec performs aggregation operations on input tuples, optionally grouping them by specified keys.
  *
  * @param aggregations a list of aggregation operations to apply on the tuples
  * @param groupByKeys a list of attribute names to group the tuples by
  */
class AggregateOpExec(
    aggregations: List[AggregationOperation],
    groupByKeys: List[String]
) extends OperatorExecutor {

  private val keyedPartialAggregates = new mutable.HashMap[List[Object], List[Object]]()
  private var distributedAggregations: List[DistributedAggregation[Object]] = _

  override def processTuple(tuple: Tuple, port: Int): Iterator[TupleLike] = {

    // Initialize distributedAggregations if it's not yet initialized
    if (distributedAggregations == null) {
      distributedAggregations =
        aggregations.map(agg => agg.getAggFunc(tuple.getSchema.getAttribute(agg.attribute).getType))
    }

    // Construct the group key
    val key = groupByKeys.map(tuple.getField[Object])

    // Get or initialize the partial aggregate for the key
    val partialAggregates =
      keyedPartialAggregates.getOrElseUpdate(key, distributedAggregations.map(_.init()))

    // Update the partial aggregates with the current tuple
    val updatedAggregates = (distributedAggregations zip partialAggregates).map {
      case (aggregation, partial) => aggregation.iterate(partial, tuple)
    }

    keyedPartialAggregates(key) = updatedAggregates
    Iterator.empty

  }

  override def onFinish(port: Int): Iterator[TupleLike] = {
    // Finalize aggregation for all keys and produce the result
    keyedPartialAggregates.iterator.map {
      case (key, partials) =>
        val finalAggregates = partials.zipWithIndex.map {
          case (partial, index) => distributedAggregations(index).finalAgg(partial)
        }
        TupleLike(key ++ finalAggregates)
    }
  }
}
