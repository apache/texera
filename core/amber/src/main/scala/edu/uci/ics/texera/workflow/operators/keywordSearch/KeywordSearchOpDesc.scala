package edu.uci.ics.texera.workflow.operators.keywordSearch

import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig

import scala.util.Random

class KeywordSearchOpDesc extends FilterOpDesc {

  @JsonProperty(value = "columnName", required = true)
  @JsonPropertyDescription("column to search keyword on")
  var columnName: String = _

  @JsonProperty(value = "keyword", required = true)
  @JsonPropertyDescription("keywords")
  var keyword: String = _

  override def operatorExecutor: OneToOneOpExecConfig = {
//    new OneToOneOpExecConfig(this.operatorIdentifier, (counter: Int) => new KeywordSearchOpExec(this, counter))
    new OneToOneOpExecConfig(this.operatorIdentifier, (counter:Int) => new KeywordSearchOpExec(counter,this))
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
