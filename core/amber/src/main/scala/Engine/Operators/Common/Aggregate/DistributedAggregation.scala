package Engine.Operators.Common.Aggregate

import Engine.Common.tuple.Tuple

case class DistributedAggregation[T <: Tuple, P <: AnyRef](
    // () => PartialObject
    init: () => P,
    // PartialObject + Tuple => PartialObject
    iterate: (P, T) => P,
    // PartialObject + PartialObject => PartialObject
    merge: (P, P) => P,
    // PartialObject => FinalObject
    finalAgg: P => T
)
