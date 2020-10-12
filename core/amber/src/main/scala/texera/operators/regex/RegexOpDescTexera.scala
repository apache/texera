package texera.operators.regex

import Engine.Common.tuple.texera.TexeraTuple
import Engine.Operators.OpExecConfig
import com.fasterxml.jackson.annotation.JsonIgnore
import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonPropertyDescription

import scala.collection.immutable.Set
import texera.common.TexeraConstraintViolation
import texera.common.metadata.OperatorGroupConstants
import texera.common.metadata.TexeraOperatorInfo
import texera.common.operators.filter.{TexeraFilterOpDesc, TexeraFilterOpExecConfig}
import java.util.regex.Pattern

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
