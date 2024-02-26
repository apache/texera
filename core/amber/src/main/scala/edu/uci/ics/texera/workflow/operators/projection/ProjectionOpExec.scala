package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class ProjectionOpExec(attributeUnits: List[AttributeUnit]) extends MapOpExec {

  setMapFunc(project)
  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(attributeUnits.nonEmpty)
    TupleLike(
      attributeUnits.map(attributeUnit =>
        tuple.getField[Any](attributeUnit.getOriginalAttribute)
      ): _*
    )
  }

}
