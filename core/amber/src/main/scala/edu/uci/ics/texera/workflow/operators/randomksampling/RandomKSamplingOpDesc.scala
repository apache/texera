package edu.uci.ics.texera.workflow.operators.randomksampling

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.util.Random

class RandomKSamplingOpDesc extends FilterOpDesc {
  // Store random seeds for each executor to satisfy the fault tolerance requirement.
  // If a worker failed, the engine will start a new worker and rerun the computation.
  // Fault tolerance requires that the restarted worker should produce the exactly same output.
  // Therefore the seeds have to be stored.
  @JsonIgnore
  private val seeds: Array[Int] =
    Array.fill(AmberConfig.numWorkerPerOperatorByDefault)(Random.nextInt)

  @JsonProperty(value = "random k sample percentage", required = true)
  @JsonPropertyDescription("random k sampling with given percentage")
  var percentage: Int = _

  @JsonIgnore
  def getSeed(index: Int): Int = seeds(index)

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((idx, _, _) => new RandomKSamplingOpExec(idx, this))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Random K Sampling",
      operatorDescription = "random sampling with given percentage",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = true
    )
}
