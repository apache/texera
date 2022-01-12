package edu.uci.ics.texera.workflow.operators.dictionary

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig}
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}

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

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) =
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ => new DictionaryMatcherOpExec(this, operatorSchemaInfo)
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Dictionary matcher",
      "Matches tuples if they appear in a given dictionary",
      OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    if (resultAttribute == null || resultAttribute.trim.isEmpty) return null
    Schema.newBuilder.add(schemas(0)).add(resultAttribute, AttributeType.BOOLEAN).build
  }
}
