package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.tuple.amber.TupleLike
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

import scala.collection.mutable

class ProjectionOpExec(attributeUnits: List[AttributeUnit]) extends MapOpExec {

  setMapFunc(project)
  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(attributeUnits.nonEmpty)
    val fields = mutable.LinkedHashMap[String, Any]()
    attributeUnits.foreach { attributeUnit =>
      val alias = attributeUnit.getAlias
      if (fields.contains(alias)) {
        throw new RuntimeException("have duplicated attribute name/alias")
      }
      fields(alias) = tuple.getField[Any](attributeUnit.getOriginalAttribute)
    }

    TupleLike(fields.toSeq: _*)
  }

}
