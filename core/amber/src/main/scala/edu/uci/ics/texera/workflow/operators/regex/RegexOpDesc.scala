package edu.uci.ics.texera.workflow.operators.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, TexeraOperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.filter.{TexeraFilterOpDesc, TexeraFilterOpExecConfig}

class RegexOpDesc extends TexeraFilterOpDesc {

  @JsonProperty("attribute")
  @JsonPropertyDescription("column to search regex")
  var attribute: String = _

  @JsonProperty("regex")
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  override def texeraOperatorExecutor: TexeraFilterOpExecConfig = {
    new TexeraFilterOpExecConfig(this.operatorIdentifier, () => new RegexOpExec(this))
  }

  override def texeraOperatorInfo: TexeraOperatorInfo =
    TexeraOperatorInfo(
      "Regular Expression",
      "Search a regular expression in a string column",
      OperatorGroupConstants.SEARCH_GROUP,
      1, 1
    )
}
