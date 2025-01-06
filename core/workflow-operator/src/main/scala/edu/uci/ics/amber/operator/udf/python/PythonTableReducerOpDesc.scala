package edu.uci.ics.amber.operator.udf.python

import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.amber.operator.PythonOperatorDescriptor
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class PythonTableReducerOpDesc extends PythonOperatorDescriptor {
  @JsonSchemaTitle("Output columns")
  var lambdaAttributeUnits: List[LambdaAttributeUnit] = List()

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Preconditions.checkArgument(lambdaAttributeUnits.nonEmpty)
    val outputSchema = lambdaAttributeUnits.foldLeft(Schema()) { (schema, unit) =>
      schema.add(unit.attributeName, unit.attributeType)
    }

    Map(operatorInfo.outputPorts.head.id -> outputSchema)
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
    val outputTable = lambdaAttributeUnits
      .map(unit => s"""\"${unit.attributeName}\": ${unit.expression}""")
      .mkString("{", ", ", "}")

    s"""
       |from pytexera import *
       |class ProcessTableOperator(UDFTableOperator):
       |
       |    @overrides
       |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
       |        yield $outputTable
       |""".stripMargin
  }
}
