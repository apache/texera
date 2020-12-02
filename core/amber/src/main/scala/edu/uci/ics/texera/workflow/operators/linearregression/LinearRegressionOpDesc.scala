package edu.uci.ics.texera.workflow.operators.linearregression

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.mlmodel.{MLModelOpDesc, MLModelOpExecConfig}
import edu.uci.ics.texera.workflow.operators.filter.SpecializedFilterOpExec

class LinearRegressionOpDesc extends MLModelOpDesc {

  @JsonProperty(value = "x attribute", required = true)
  @JsonPropertyDescription("column representing x in y=wx+b")
  var xAttr: String = _

  @JsonProperty(value = "y attribute", required = true)
  @JsonPropertyDescription("column representing y in y=wx+b")
  var yAttr: String = _

  @JsonProperty(value = "learning rate", required = true)
  @JsonPropertyDescription("Learning Rate")
  var learningRate: Double = _

  override def operatorExecutor =
    new MLModelOpExecConfig(
      this.operatorIdentifier,
      1,
      () => new LinearRegressionOpExec(xAttr, yAttr, learningRate)
    )

  override def operatorInfo =
    OperatorInfo(
      "Linear Regression",
      "Trains a Linear Regression model",
      OperatorGroupConstants.UTILITY_GROUP,
      1,
      1
    )
}
