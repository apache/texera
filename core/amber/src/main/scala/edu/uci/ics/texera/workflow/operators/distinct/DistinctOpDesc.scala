package edu.uci.ics.texera.workflow.operators.distinct

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.HashPartition

class DistinctOpDesc extends LogicalOp {

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new DistinctOpExec())
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(List(Option(HashPartition())))
      .withDerivePartition(_ => HashPartition())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Distinct",
      "Remove duplicate tuples",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
