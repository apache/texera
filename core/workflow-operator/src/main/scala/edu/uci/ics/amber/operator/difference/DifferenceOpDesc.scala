package edu.uci.ics.amber.operator.difference

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}

class DifferenceOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithClassName("edu.uci.ics.amber.operator.difference.DifferenceOpExec")
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition()), Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        Preconditions.checkArgument(inputSchemas.values.toSet.size == 1)
        val outputSchema = inputSchemas.values.head
        operatorInfo.outputPorts.map(port => port.id -> outputSchema).toMap
      }))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Difference",
      "find the set difference of two inputs",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right")
      ),
      outputPorts = List(OutputPort(blocking = true))
    )
}
