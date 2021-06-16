package edu.uci.ics.texera.workflow.operators.regex

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class RegexOpDesc extends FilterOpDesc {

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to search regex on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "regex", required = true)
  @JsonPropertyDescription("regular expression")
  var regex: String = _

  @JsonProperty(required = false, defaultValue = "false")
  @JsonSchemaTitle("Case Insensitive")
  @JsonPropertyDescription("whether the regular expression match is case insensitive")
  var caseInsensitive: Boolean = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(operatorIdentifier, _ => new RegexOpExec(this))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Regular Expression",
      operatorDescription = "Search a regular expression in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
