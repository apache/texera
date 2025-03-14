package edu.uci.ics.amber.operator.filter

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.operator.ManualLocationConfiguration

class SpecializedFilterOpDesc extends FilterOpDesc with ManualLocationConfiguration {

  @JsonProperty(value = "predicates", required = true)
  @JsonPropertyDescription("multiple predicates in OR")
  var predicates: List[FilterPredicate] = List.empty

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val baseOp = PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.filter.SpecializedFilterOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)

    applyManualLocation(baseOp)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Filter",
      "Performs a filter operation",
      OperatorGroupConstants.CLEANING_GROUP,
      List(InputPort()),
      List(OutputPort()),
      supportReconfiguration = true
    )
  }
}
