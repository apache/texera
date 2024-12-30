package edu.uci.ics.amber.operator.udf.python

import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.{AttributeTypeUtils, Schema}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}

class PythonLambdaFunctionOpDesc extends PythonOperatorDescriptor {
  @JsonSchemaTitle("Add/Modify column(s)")
  var lambdaAttributeUnits: List[LambdaAttributeUnit] = List()

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    Preconditions.checkArgument(lambdaAttributeUnits.nonEmpty)
    val inputSchema = schemas(0)
    val outputSchemaBuilder = Schema.builder()
    // keep the same schema from input
    outputSchemaBuilder.add(inputSchema)
    // add new attributes
    for (unit <- lambdaAttributeUnits) {
      if (unit.attributeName.equalsIgnoreCase("Add New Column")) {
        if (inputSchema.containsAttribute(unit.newAttributeName)) {
          throw new RuntimeException(
            "Column name " + unit.newAttributeName + " already exists!"
          )
        }
        if (unit.newAttributeName != null && unit.newAttributeName.nonEmpty)
          outputSchemaBuilder.add(unit.newAttributeName, unit.attributeType)
      }
    }
    var outputSchema = outputSchemaBuilder.build()
    // type casting
    for (unit <- lambdaAttributeUnits) {
      if (!unit.attributeName.equalsIgnoreCase("Add New Column"))
        outputSchema =
          AttributeTypeUtils.SchemaCasting(outputSchema, unit.attributeName, unit.attributeType)
    }
    outputSchema
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Python Lambda Function",
      "Modify or add a new column with more ease",
      OperatorGroupConstants.PYTHON_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

  override def generatePythonCode(): String = {
    // build the python udf code
    var code: String =
      "from pytexera import *\n" +
        "class ProcessTupleOperator(UDFOperatorV2):\n" +
        "    @overrides\n" +
        "    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n"
    if (lambdaAttributeUnits != null) {
      for (unit <- lambdaAttributeUnits) {
        val attrName =
          if (unit.attributeName.equalsIgnoreCase("Add New Column")) unit.newAttributeName
          else unit.attributeName
        if (unit.expression != null && unit.expression.nonEmpty) {
          code += s"""        tuple_['$attrName'] = ${unit.expression}\n"""
        } else
          throw new RuntimeException(
            s"Column name ${attrName}'s expression shouldn't be null or empty!"
          )
      }
    }
    code + "        yield tuple_\n"
  }
}
