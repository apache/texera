package edu.uci.ics.amber.operator.typecasting

import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Tuple, TupleLike}
import edu.uci.ics.amber.operator.map.MapOpExec

class TypeCastingOpExec(typeCastingUnits: List[TypeCastingUnit]) extends MapOpExec {
  this.setMapFunc(castTuple)

  private def castTuple(tuple: Tuple): TupleLike =
    AttributeTypeUtils.tupleCasting(
      tuple,
      typeCastingUnits
        .map(typeCastingUnit => typeCastingUnit.attribute -> typeCastingUnit.resultType)
        .toMap
    )
}
