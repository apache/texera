package edu.uci.ics.amber.operator.ifStatement

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalOp,
  PortIdentity,
  SchemaPropagationFunc
}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

class IfOpDesc extends LogicalOp {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Condition State")
  @JsonPropertyDescription("name of the state variable to evaluate")
  var conditionName: String = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.ifStatement.IfOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(false)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          operatorInfo.outputPorts
            .map(_.id)
            .map(id => id -> inputSchemas(operatorInfo.inputPorts.last.id))
            .toMap
        )
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "If",
      "If",
      OperatorGroupConstants.CONTROL_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), "Condition"),
        InputPort(PortIdentity(1), dependencies = List(PortIdentity()))
      ),
      outputPorts = List(OutputPort(PortIdentity(), "False"), OutputPort(PortIdentity(1), "True"))
    )
}
