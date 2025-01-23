package edu.uci.ics.amber.operator.sentiment

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.AttributeType
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "attribute": {
      "enum": ["string"]
    }
  }
}
""")
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

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    if (attribute == null)
      throw new RuntimeException("sentiment analysis: attribute is null")

    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.sentiment.SentimentAnalysisOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          if (resultAttribute == null || resultAttribute.trim.isEmpty)
            return null
          Map(
            operatorInfo.outputPorts.head.id -> inputSchemas.values.head
              .add(resultAttribute, AttributeType.INTEGER)
          )
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sentiment Analysis",
      "analysis the sentiment of a text using machine learning",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )

}
