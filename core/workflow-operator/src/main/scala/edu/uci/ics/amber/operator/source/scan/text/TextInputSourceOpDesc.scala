package edu.uci.ics.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.UIWidget
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.OutputPort

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
        OpExecInitInfo((_, _) =>
          new TextInputSourceOpExec(attributeType, textInput, fileScanLimit, fileScanOffset)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )

  override def sourceSchema(): Schema =
    Schema
      .builder()
      .add(new Attribute(attributeName, attributeType.getType))
      .build()

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Text Input",
      operatorDescription = "Source data from manually inputted text",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
}
