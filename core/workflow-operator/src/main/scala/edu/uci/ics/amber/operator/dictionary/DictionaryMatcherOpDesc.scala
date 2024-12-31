package edu.uci.ics.amber.operator.dictionary

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

/**
  * Dictionary matcher operator matches a tuple if the specified column is in the given dictionary.
  * It outputs an extra column to label the tuple if it is matched or not
  * This is the description of the operator
  */
class DictionaryMatcherOpDesc extends MapOpDesc {
  @JsonProperty(value = "Dictionary", required = true)
  @JsonPropertyDescription("dictionary values separated by a comma") var dictionary: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column name to match")
  @AutofillAttributeName var attribute: String = _

  @JsonProperty(value = "result attribute", required = true, defaultValue = "matched")
  @JsonPropertyDescription("column name of the matching result") var resultAttribute: String = _

  @JsonProperty(value = "Matching type", required = true) var matchingType: MatchingType = _

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
          "edu.uci.ics.amber.operator.dictionary.DictionaryMatcherOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          if (resultAttribute == null || resultAttribute.trim.isEmpty) return null
          Map(
            operatorInfo.outputPorts.head.id -> Schema
              .builder()
              .add(inputSchemas.values.head)
              .add(resultAttribute, AttributeType.BOOLEAN)
              .build()
          )
        })
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dictionary matcher",
      "Matches tuples if they appear in a given dictionary",
      OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )
}
