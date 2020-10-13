package texera.operators.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import texera.common.metadata.{OperatorGroupConstants, TexeraOperatorInfo}
import texera.common.operators.filter.{TexeraFilterOpDesc, TexeraFilterOpExecConfig}

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
