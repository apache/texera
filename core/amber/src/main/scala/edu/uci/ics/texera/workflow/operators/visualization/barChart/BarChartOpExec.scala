package edu.uci.ics.texera.workflow.operators.visualization.barChart

import edu.uci.ics.texera.workflow.common.operators.map.MapOpExec
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

/**
  * Simply mocks the data.
  * @param opDesc BarChartOpDesc.
  * @param operatorSchemaInfo The descriptor's schema info.
  */
class BarChartOpExec(
    opDesc: BarChartOpDesc,
    operatorSchemaInfo: OperatorSchemaInfo
) extends MapOpExec {

  setMapFunc(this.processTuple)

  def processTuple(t: Tuple): Tuple = {
    val builder = Tuple.newBuilder(operatorSchemaInfo.outputSchema)
    val inputSchema = t.getSchema
    builder.add(inputSchema.getAttribute(opDesc.nameColumn), t.getField(opDesc.nameColumn))
    for (i <- opDesc.resultAttributeNames.indices) {
      val dataName = opDesc.resultAttributeNames.apply(i)
      builder.add(inputSchema.getAttribute(dataName), t.getField(dataName))
    }
    builder.build
  }
}
