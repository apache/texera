package texera.common.operators.aggregate

import Engine.Common.tuple.texera.TexeraTuple

/**
  * This class defines the necessary functions required by a distributed aggregation.
  *
  * Explanation using "average" as an example:
  * To compute the average of data on multiple nodes,
  * each node first computes the sum and count of its local data (partial result),
  * then the partial results are sent to one node, where it adds up the sum and count of all nodes,
  * finally, average is computed by calculating sum/count.
  *
  * Corresponding to the functions:
  * init:     initializes partial result:   partial = (sum=0, count=0)
  * iterate:  accumulates each input data:  sum += inputValue, count += 1
  * merge:    adds up all partial results:  sum += partialSum, count += partialCount
  * finalAgg: calculates final result:      average = sum / count
  *
  * Optionally, a list of group by keys can be specified,
  * which will cause the aggregation to be calculated per group
  *
  * These function definitions are from
  * "Distributed Aggregation for Data-Parallel Computing: Interfaces and Implementations"
  * https://www.sigops.org/s/conferences/sosp/2009/papers/yu-sosp09.pdf
  */
case class TexeraDistributedAggregation[P <: AnyRef](
    // () => PartialObject
    init: () => P,
    // PartialObject + Tuple => PartialObject
    iterate: (P, TexeraTuple) => P,
    // PartialObject + PartialObject => PartialObject
    merge: (P, P) => P,
    // PartialObject => FinalObject
    finalAgg: P => TexeraTuple,
    // optional: group by keys
    groupByKeys: Seq[String] = List(),
)
