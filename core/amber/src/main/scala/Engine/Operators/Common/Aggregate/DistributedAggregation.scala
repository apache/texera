package Engine.Operators.Common.Aggregate

import Engine.Common.tuple.Tuple

case class DistributedAggregation(
    // () => PartialObject
    init: () => Tuple,
    // PartialObject + Tuple => PartialObject
    iterate: (Tuple, Tuple) => Tuple,
    // PartialObject + PartialObject => PartialObject
    merge: (Tuple, Tuple) => Tuple,
    // PartialObject => FinalObject
    finalAgg: Tuple => Tuple
)
