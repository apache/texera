package edu.uci.ics.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.UIWidget
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.OutputPort

class TextInputSourceOpDesc extends SourceOperatorDescriptor with TextSourceOpDesc {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var textInput: String = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.source.scan.text.TextInputSourceOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

  override def sourceSchema(): Schema =
    Schema().add(attributeName, attributeType.getType)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Text Input",
      operatorDescription = "Source data from manually inputted text",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
}
