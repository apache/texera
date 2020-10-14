package edu.uci.ics.texera.workflow.operators.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaFormat
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.filter.{FilterOpDesc, FilterOpExecConfig}

class RegexOpDesc extends FilterOpDesc {

  @JsonProperty("attribute")
  @JsonPropertyDescription("column to search regex")
  var attribute: String = _

  @JsonProperty("regex")
  @JsonPropertyDescription("regular expression")
  @JsonSchemaFormat("regex")
  var regex: String = _

  override def operatorExecutor: FilterOpExecConfig = {
    new FilterOpExecConfig(this.operatorIdentifier, () => new RegexOpExec(this))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Regular Expression",
      "Search a regular expression in a string column",
      OperatorGroupConstants.SEARCH_GROUP,
      1,
      1
    )
}
