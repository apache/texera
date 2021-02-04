package edu.uci.ics.texera.workflow.operators.projection

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple

class ProjectionOpExec(val opDesc: ProjectionOpDesc) extends MapOpExec {
  this.setMapFunc(processTuple)

  def processTuple(tuple: Tuple): Tuple = {
    val builder = Tuple.newBuilder()
    val schema = tuple.getSchema
    schema.getAttributeNames.forEach((attrName: String) => {
      if (opDesc.attributes.contains(attrName))
        builder.add(attrName, schema.getAttribute(attrName).getType, tuple.getField(attrName))
    })
    builder.build()
  }
}
