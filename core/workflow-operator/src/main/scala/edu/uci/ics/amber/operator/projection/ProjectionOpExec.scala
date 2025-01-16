package edu.uci.ics.amber.operator.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.tuple.{Tuple, TupleLike}
import edu.uci.ics.amber.operator.map.MapOpExec
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import scala.collection.mutable

class ProjectionOpExec(
    descString: String
) extends MapOpExec {

  val desc: ProjectionOpDesc = objectMapper.readValue(descString, classOf[ProjectionOpDesc])
  setMapFunc(project)

  def project(tuple: Tuple): TupleLike = {
    Preconditions.checkArgument(desc.attributes.nonEmpty)
    var selectedUnits: List[AttributeUnit] = List()
    val fields = mutable.LinkedHashMap[String, Any]()
    if (desc.isDrop) {
      val allAttribute = tuple.schema.getAttributeNames
      val selectedAttributes = desc.attributes.map(_.getOriginalAttribute)
      val keepAttributes = allAttribute.diff(selectedAttributes)

      keepAttributes.foreach { attribute =>
        val newList = List(
          new AttributeUnit(attribute, attribute)
        )
        selectedUnits = selectedUnits ::: newList
      }

    } else {

      selectedUnits = desc.attributes
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
