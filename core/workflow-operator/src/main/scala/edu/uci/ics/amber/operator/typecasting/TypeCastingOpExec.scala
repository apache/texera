package edu.uci.ics.amber.operator.typecasting

import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Tuple, TupleLike}
import edu.uci.ics.amber.operator.map.MapOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class TypeCastingOpExec(descString: String) extends MapOpExec {

  private val desc: TypeCastingOpDesc =
    objectMapper.readValue(descString, classOf[TypeCastingOpDesc])

  this.setMapFunc(castTuple)

  private def castTuple(tuple: Tuple): TupleLike =
    AttributeTypeUtils.tupleCasting(
      tuple,
      desc.typeCastingUnits
        .map(typeCastingUnit => typeCastingUnit.attribute -> typeCastingUnit.resultType)
        .toMap
    )
}
