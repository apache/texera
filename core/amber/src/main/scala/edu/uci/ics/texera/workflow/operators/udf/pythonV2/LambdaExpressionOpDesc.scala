package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import com.fasterxml.jackson.annotation.{JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

class LambdaExpressionOpDesc extends OperatorDescriptor {
  @JsonSchemaTitle("Add new column(s)")
  @JsonPropertyDescription(
    "Name the new column, select the data type, type the lambda expression"
  )
  var newAttributeUnits: List[NewAttributeUnit] = List()

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val exec = (i: Any) =>
      new PythonUDFOpExecV2(
        PythonCodeBuilder.build(newAttributeUnits),
        operatorSchemaInfo.outputSchemas.head
      )
    new OneToOneOpExecConfig(operatorIdentifier, exec)
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
    val outputSchemaBuilder = Schema.newBuilder
    // keep the same schema from input
    outputSchemaBuilder.add(inputSchema)
    // for any pythonUDFType, it can add custom output columns (attributes).
    if (newAttributeUnits != null) {
      for (unit <- newAttributeUnits) {
        if (inputSchema.containsAttribute(unit.attributeName)) {
          throw new RuntimeException(
            "Column name " + unit.attributeName + " already exists!"
          )
        }
        if (unit.attributeName != null && unit.attributeType != null)
          outputSchemaBuilder.add(unit.attributeName, unit.attributeType)
      }
    }
    outputSchemaBuilder.build
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Lambda Expression",
      "Modify or add a new column",
      OperatorGroupConstants.UDF_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
