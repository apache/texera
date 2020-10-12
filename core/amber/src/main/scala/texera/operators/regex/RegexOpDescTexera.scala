package texera.operators.regex

import java.util.regex.Pattern

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import texera.common.metadata.{OperatorGroupConstants, TexeraOperatorInfo}
import texera.common.operators.filter.{TexeraFilterOpDesc, TexeraFilterOpExecConfig}
import texera.common.tuple.TexeraTuple

class RegexOpDescTexera extends TexeraFilterOpDesc {

  @JsonProperty("attribute")
  @JsonPropertyDescription("column to search regex")
  var attribute: String = null

  @JsonProperty("regex")
  @JsonPropertyDescription("regular expression")
  var regex: String = null

  @JsonIgnore
  private var pattern: Pattern = null

  override def texeraOpExec: TexeraFilterOpExecConfig = {
    pattern = Pattern.compile(regex)
    new TexeraFilterOpExecConfig(this.amberOperatorTag, t => this.matchRegex(t))
  }

  def matchRegex(tuple: TexeraTuple): Boolean = {
    val tupleValue = tuple.getField(attribute).toString.trim
    pattern.matcher(tupleValue).find
  }

  override def texeraOperatorInfo =
    TexeraOperatorInfo(
      "Regular Expression",
      "Search a regular expression in a text column",
      OperatorGroupConstants.SEARCH_GROUP,
      1, 1
    )
}
