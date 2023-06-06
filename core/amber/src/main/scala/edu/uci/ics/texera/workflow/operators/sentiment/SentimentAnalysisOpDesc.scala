package edu.uci.ics.texera.workflow.operators.sentiment

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}

class SentimentAnalysisOpDesc extends MapOpDesc {
  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to perform sentiment analysis on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(
    value = "result attribute",
    required = true,
    defaultValue = "sentiment"
  )
  @JsonPropertyDescription("column name of the sentiment analysis result")
  var resultAttribute: String = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    if (attribute == null)
      throw new RuntimeException("sentiment analysis: attribute is null")
    OpExecConfig.oneToOneLayer(
      operatorIdentifier,
      _ => new SentimentAnalysisOpExec(this, operatorSchemaInfo)
    )
  }

  override def operatorInfo =
    new OperatorInfo(
      "Sentiment Analysis",
      "analysis the sentiment of a text using machine learning",
      OperatorGroupConstants.ANALYTICS_GROUP,
      List(InputPort("")),
      List(OutputPort("")),
      supportReconfiguration = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    if (resultAttribute == null || resultAttribute.trim.isEmpty)
      return null
    Schema.newBuilder.add(schemas(0)).add(resultAttribute, AttributeType.INTEGER).build
  }
}
