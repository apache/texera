package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaDescription,
  JsonSchemaInject,
  JsonSchemaTitle
}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.UIWidget
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

import java.util.Collections.singletonList
import javax.validation.constraints.Size
import scala.collection.JavaConverters.asScalaBuffer

class TextInputSourceOpDesc extends SourceOperatorDescriptor with TextSourceOpDesc {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text Input")
  @JsonSchemaDescription("Max 1024 Characters, e.g: \"line1\\nline2\"")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  @Size(max = 1024)
  var textInput: String = _

  // indicates the AttributeType of output tuple(s) - supports all AttributeType except ANY and BINARY
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  @JsonPropertyDescription("Attribute type of output tuple(s)")
  var attributeType: TextInputSourceAttributeType = TextInputSourceAttributeType.STRING

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val offsetValue: Int = offsetHideable.getOrElse(0)
    var count: Int = 1 // set num lines to one by default, for outputAsSingleTuple mode
    var defaultAttributeName: String = "text"

    if (!attributeType.isOutputSingleTuple) {
      count = countNumLines(textInput.linesIterator, offsetValue)
      defaultAttributeName = "line"
    }

    OpExecConfig.localLayer(
      operatorIdentifier,
      _ => {
        val startOffset: Int = offsetValue
        val endOffset: Int = offsetValue + count
        new TextInputSourceOpExec(
          this,
          startOffset,
          endOffset,
          if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
          else attributeName.get
        )
      }
    )
  }

  override def sourceSchema(): Schema = {
    val defaultAttributeName: String = if (attributeType.isOutputSingleTuple) "text" else "line"

    Schema
      .newBuilder()
      .add(
        new Attribute(
          if (attributeName.isEmpty || attributeName.get.isEmpty) defaultAttributeName
          else attributeName.get,
          attributeType.getType
        )
      )
      .build()
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Text Input",
      operatorDescription = "Source data from manually inputted text",
      OperatorGroupConstants.SOURCE_GROUP,
      List.empty,
      asScalaBuffer(singletonList(OutputPort(""))).toList
    )
  }
}
