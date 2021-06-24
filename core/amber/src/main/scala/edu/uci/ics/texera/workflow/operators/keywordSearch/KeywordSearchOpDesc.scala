package edu.uci.ics.texera.workflow.operators.keywordSearch

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

class KeywordSearchOpDesc extends FilterOpDesc {

  @JsonProperty(required = true)
  @JsonSchemaTitle("attribute")
  @JsonPropertyDescription("column to search keyword on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("keywords")
  @JsonPropertyDescription("keywords")
  var keyword: String = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      operatorIdentifier,
      (counter: Int) => new KeywordSearchOpExec(counter, this)
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Keyword Search",
      operatorDescription = "Search for keyword(s) in a string column",
      operatorGroupName = OperatorGroupConstants.SEARCH_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
