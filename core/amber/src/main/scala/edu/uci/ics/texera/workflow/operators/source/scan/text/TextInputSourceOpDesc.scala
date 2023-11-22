package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaDescription, JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}

import javax.validation.constraints.Size

class TextInputSourceOpDesc extends SourceOperatorDescriptor with TextSourceOpDesc {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  var textInput: String = _

  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  var attributeType: AttributeType = AttributeType.STRING

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    OpExecConfig.localLayer(operatorIdentifier, _ => new TextInputSourceOpExec(this))

  override def sourceSchema(): Schema =
    new Schema(new Attribute(attributeName, attributeType))

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Text Input",
      operatorDescription = "Source data from manually inputted text",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      List(OutputPort())
    )
}
