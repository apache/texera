package edu.uci.ics.amber.operator.union

import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalOp, PortIdentity}
import edu.uci.ics.amber.operator.{LogicalOp, ManualLocationConfiguration}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class UnionOpDesc extends LogicalOp with ManualLocationConfiguration {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val baseOp = PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("edu.uci.ics.amber.operator.union.UnionOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)

    applyManualLocation(baseOp)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "Unions the output rows from multiple input operators",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(InputPort(PortIdentity(0), allowMultiLinks = true)),
      outputPorts = List(OutputPort())
    )
}
