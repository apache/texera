package edu.uci.ics.texera.workflow.operators.ifstatement

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.LinkIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.udf.python.PythonUDFOpExecV2
class IfStatementOpDesc extends OperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Expression")
  var exp: String = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = ???
  override def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val reducer = s"""
      |from pytexera import *
      |class ProcessTableOperator(UDFTableOperator):
      |    @overrides
      |    def process_table(self, table: Table, port: int) -> Iterator[Optional[TableLike]]:
      |        yield {"condition":$exp}
      """.stripMargin
    val firstLayer = OpExecConfig
      .manyToOneLayer(
        makeLayer(operatorIdentifier, "reducer"),
        _ =>
          new PythonUDFOpExecV2(
            reducer,
            new Schema(new Attribute("condition", AttributeType.BOOLEAN))
          )
      )
      .copy(inputPorts = List(InputPort("Condition")))

    val finalLayer = OpExecConfig
      .manyToOneLayer(makeLayer(operatorIdentifier, "if"), _ => new IfStatementOpExec())
      .copy(
        inputPorts = List(InputPort("Control"), InputPort("Data")),
        outputPorts = List(OutputPort("True"), OutputPort("False")),
        blockingInputs = List(0),
        dependency = Map(1 -> 0)
      )

    new PhysicalPlan(
      List(firstLayer, finalLayer),
      List(LinkIdentity(firstLayer.id, 0, finalLayer.id, 0))
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "If Statement",
      "If Statement",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("Condition"), InputPort("Data")),
      outputPorts = List(OutputPort("True"), OutputPort("False"))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = ???
  override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] =
    Array(schemas(1), schemas(1))
}
