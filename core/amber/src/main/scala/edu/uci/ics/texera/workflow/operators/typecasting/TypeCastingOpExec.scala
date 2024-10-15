package edu.uci.ics.texera.workflow.operators.typecasting

import edu.uci.ics.amber.engine.common.model.tuple.{AttributeTypeUtils, Tuple, TupleLike}
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec

import scala.jdk.CollectionConverters.CollectionHasAsScala

class TypeCastingOpExec(typeCastingUnits: java.util.List[TypeCastingUnit]) extends MapOpExec {
  this.setMapFunc(castTuple)
  private def castTuple(tuple: Tuple): TupleLike =
    AttributeTypeUtils.tupleCasting(
      tuple,
      typeCastingUnits.asScala
        .map(typeCastingUnit => typeCastingUnit.attribute -> typeCastingUnit.resultType)
        .toMap
    )
}
