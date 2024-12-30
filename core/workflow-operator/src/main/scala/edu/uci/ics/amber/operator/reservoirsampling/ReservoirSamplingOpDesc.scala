package edu.uci.ics.amber.operator.reservoirsampling

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.PhysicalOp
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.operator.util.OperatorDescriptorUtils.equallyPartitionGoal

import scala.util.Random

class ReservoirSamplingOpDesc extends LogicalOp {

  @JsonProperty(value = "number of item sampled in reservoir sampling", required = true)
  @JsonPropertyDescription("reservoir sampling with k items being kept randomly")
  var k: Int = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((idx, workerCount) =>
          new ReservoirSamplingOpExec(
            idx,
            equallyPartitionGoal(k, workerCount),
            Array.fill(workerCount)(Random.nextInt())
          )
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Reservoir Sampling",
      operatorDescription = "Reservoir Sampling with k items being kept randomly",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
