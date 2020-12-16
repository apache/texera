package edu.uci.ics.texera.workflow.operators.keywordSearch

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import com.fasterxml.jackson.annotation.{JsonPropertyDescription, JsonProperty, JsonIgnore}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{OperatorInfo, OperatorGroupConstants}
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig

import scala.util.Random

class KeywordSearchOpDesc extends FilterOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search keyword on")
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("keywords")
  @JsonPropertyDescription("keywords")
  var keyword: String = _

  override def operatorExecutor: OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      this.operatorIdentifier,
      (counter: Int) => new KeywordSearchOpExec(counter, this)
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Keyword Search",
      operatorDescription = "Search for keyword(s) in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      numInputPorts = 1,
      numOutputPorts = 1
    )
}
