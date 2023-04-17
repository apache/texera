package edu.uci.ics.texera.workflow.operators.source.scan.text

import com.fasterxml.jackson.annotation.JsonProperty
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
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

import java.util.Collections.singletonList
import javax.validation.constraints.Size
import scala.collection.JavaConverters.asScalaBuffer

class TextInputSourceOpDesc extends SourceOperatorDescriptor with TextSourceOpDesc {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Text Input")
  @JsonSchemaDescription("Up to 1024 characters.  Example input : \"line1\\nline2\\nline3\" ")
  @JsonSchemaInject(json = UIWidget.UIWidgetTextArea)
  @Size(max = 1024)
  var textInput: String = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val offsetValue: Int = offsetHideable.getOrElse(0)
    val count: Int = countNumLines(textInput.linesIterator, offsetValue)

    OpExecConfig.localLayer(
      operatorIdentifier,
      _ => {
        val startOffset: Int = offsetValue
        val endOffset: Int = offsetValue + count
        new TextInputSourceOpExec(this, startOffset, endOffset)
      }
    )
  }

  override def sourceSchema(): Schema = {
    Schema
      .newBuilder()
      .add(new Attribute(if (outputAsSingleTuple) "text" else "line", AttributeType.STRING))
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
