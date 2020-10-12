package texera.common.operators.aggregate

import Engine.Common.tuple.texera.TexeraTuple

case class TexeraDistributedAggregation[P <: AnyRef](
    // () => PartialObject
    init: () => P,
    // PartialObject + Tuple => PartialObject
    iterate: (P, TexeraTuple) => P,
    // PartialObject + PartialObject => PartialObject
    merge: (P, P) => P,
    // PartialObject => FinalObject
    finalAgg: P => TexeraTuple,
)
