package edu.uci.ics.texera.workflow.operators.udf.pythonV2

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import scala.collection.JavaConverters._

class LambdaExpressionOpDesc extends OperatorDescriptor {
  @JsonProperty
  @JsonSchemaTitle("Add new column(s)")
  @JsonPropertyDescription(
    "Name the new column, and select the data type"
  )
  var newColumns: List[Attribute] = List()

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    // build the python udf code
    var code: String =
      "from pytexera import *\n" +
        "class ProcessTupleOperator(UDFOperatorV2):\n" +
        "    @overrides\n" +
        "    def process_tuple(self, tuple_: Tuple, port: int) -> Iterator[Optional[TupleLike]]:\n";
    // if newColumns is not null, add the new column into the tuple
    if (newColumns != null) {
      for (column <- newColumns) {
        code += s"        self.add_column(tuple_, '${column.getName}')\n"
      }
    }
    code += "        yield tuple_\n" +
      "    def add_column(self, tuple_: Tuple, new_column_name: str):\n" +
      "        columns = tuple_.get_field_names()\n" +
      "        if new_column_name in columns:\n" +
      "            raise \"Column name \" + new_column_name + \" already exists!\"\n" +
      "        tuple_[new_column_name] = None\n"
    val exec = (i: Any) => new PythonUDFOpExecV2(code, operatorSchemaInfo.outputSchemas.head)
    new OneToOneOpExecConfig(operatorIdentifier, exec)
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
    val outputSchemaBuilder = Schema.newBuilder
    // keep the same schema from input
    outputSchemaBuilder.add(inputSchema)
    // for any pythonUDFType, it can add custom output columns (attributes).
    if (newColumns != null) {
      for (column <- newColumns) {
        if (inputSchema.containsAttribute(column.getName))
          throw new RuntimeException("Column name " + column.getName + " already exists!")
      }
      outputSchemaBuilder.add(newColumns.asJava).build
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
