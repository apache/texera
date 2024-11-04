package edu.uci.ics.amber.operator.udf.python

import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

class PythonTableReducerOpDesc extends PythonOperatorDescriptor {
  @JsonSchemaTitle("Output columns")
  var lambdaAttributeUnits: List[LambdaAttributeUnit] = List()

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(lambdaAttributeUnits.nonEmpty)
    val outputSchemaBuilder = Schema.builder()
    for (unit <- lambdaAttributeUnits) {
      outputSchemaBuilder.add(unit.attributeName, unit.attributeType)
    }
    outputSchemaBuilder.build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Python Table Reducer",
      "Reduce Table to Tuple",
      OperatorGroupConstants.PYTHON_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def generatePythonCode(): String = {
    var outputTable = "{"
    for (unit <- lambdaAttributeUnits) {
      outputTable += s"""\"${unit.attributeName}\":${unit.expression},"""
    }
    outputTable += "}"
    s"""
from pytexera import *
class ProcessTableOperator(UDFTableOperator):
    @overrides
    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
        yield $outputTable
"""
  }
}
