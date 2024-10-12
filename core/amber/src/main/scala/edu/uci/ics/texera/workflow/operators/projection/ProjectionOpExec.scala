package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.model.tuple.{Tuple, TupleLike}
import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec

import scala.collection.mutable

class ProjectionOpExec(
    attributeUnits: List[AttributeUnit],
    dropOption: Boolean = false
) extends MapOpExec {

  setMapFunc(project)
  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(attributeUnits.nonEmpty)
    var selectedUnits: List[AttributeUnit] = List()
    val fields = mutable.LinkedHashMap[String, Any]()
    if (dropOption) {
      val allAttribute = tuple.schema.getAttributeNames
      val selectedAttributes = attributeUnits.map(_.getOriginalAttribute)
      val keepAttributes = allAttribute.diff(selectedAttributes)

      keepAttributes.foreach { attribute =>
        val newList = List(
          new AttributeUnit(attribute, attribute)
        )
        selectedUnits = selectedUnits ::: newList
      }

    } else {

      selectedUnits = attributeUnits
    }

    selectedUnits.foreach { attributeUnit =>
      val alias = attributeUnit.getAlias
      if (fields.contains(alias)) {
        throw new RuntimeException("have duplicated attribute name/alias")
      }
      fields(alias) = tuple.getField[Any](attributeUnit.getOriginalAttribute)
    }

    TupleLike(fields.toSeq: _*)
  }

}
