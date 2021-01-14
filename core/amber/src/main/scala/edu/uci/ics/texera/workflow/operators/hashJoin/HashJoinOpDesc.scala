package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc

class HashJoinOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Larger table join attribute")
  @JsonPropertyDescription("Join attribute name from the larger of the two tables")
  var outerAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Smaller table join attribute")
  @JsonPropertyDescription("Join attribute name from the smaller of the two tables")
  var innerAttribute: String = _

  override def operatorExecutor: OpExecConfig = {}

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Keyword Search",
      operatorDescription = "Search for keyword(s) in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      numInputPorts = 1,
      numOutputPorts = 1
    )
}
