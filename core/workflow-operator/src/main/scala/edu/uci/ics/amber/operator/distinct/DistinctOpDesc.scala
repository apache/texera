package edu.uci.ics.amber.operator.distinct

import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{HashPartition, InputPort, OutputPort, PhysicalOp}
import edu.uci.ics.amber.operator.{LogicalOp, ManualLocationConfiguration}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class DistinctOpDesc extends LogicalOp with ManualLocationConfiguration {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val baseOp = PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("edu.uci.ics.amber.operator.distinct.DistinctOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())

    applyManualLocation(baseOp)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Distinct",
      "Remove duplicate tuples",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

}
