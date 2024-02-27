package edu.uci.ics.texera.workflow.operators.linearregression

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.mlmodel.MLModelOpDesc

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

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .manyToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _, _) => new LinearRegressionOpExec(xAttr, yAttr, learningRate))
      )
      .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
      .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Linear Regression",
      "Trains a Linear Regression model",
      OperatorGroupConstants.MACHINE_LEARNING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
