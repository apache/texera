package edu.uci.ics.texera.workflow.operators.linearregression

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.mlmodel.MLModelOpDesc
import edu.uci.ics.texera.workflow.common.operators.ManyToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class LinearRegressionOpDesc extends MLModelOpDesc {

  @JsonProperty(value = "x attribute", required = true)
  @JsonPropertyDescription("column representing x in y=wx+b")
  @AutofillAttributeName
  var xAttr: String = _

  @JsonProperty(value = "y attribute", required = true)
  @JsonPropertyDescription("column representing y in y=wx+b")
  @AutofillAttributeName
  var yAttr: String = _

  @JsonProperty(value = "learning rate", required = true)
  @JsonPropertyDescription("Learning Rate")
  var learningRate: Double = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    new ManyToOneOpExecConfig(
      operatorIdentifier,
      _ => new LinearRegressionOpExec(xAttr, yAttr, learningRate)
    )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Linear Regression",
      "Trains a Linear Regression model",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
