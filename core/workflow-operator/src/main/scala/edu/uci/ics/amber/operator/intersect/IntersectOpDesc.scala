package edu.uci.ics.amber.operator.intersect

import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{HashPartition, PhysicalOp}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PortIdentity}

class IntersectOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new IntersectOpExec())
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition()), Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Intersect",
      "Take the intersect of two inputs",
      OperatorGroupConstants.SET_GROUP,
      inputPorts = List(InputPort(PortIdentity()), InputPort(PortIdentity(1))),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
