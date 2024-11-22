package edu.uci.ics.amber.operator.source.cache

import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.OutputPort
import edu.uci.ics.amber.core.storage.result.OpResultStorage
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants

class CacheSourceOpDesc(val targetSinkStorageId: OperatorIdentity, opResultStorage: OpResultStorage)
    extends SourceOperatorDescriptor {
  assert(null != targetSinkStorageId)
  assert(null != opResultStorage)

  override def sourceSchema(): Schema = opResultStorage.get(targetSinkStorageId).getSchema

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new CacheSourceOpExec(opResultStorage.get(targetSinkStorageId)))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Cache Source Operator",
      "Retrieve the cached output to src",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
}
