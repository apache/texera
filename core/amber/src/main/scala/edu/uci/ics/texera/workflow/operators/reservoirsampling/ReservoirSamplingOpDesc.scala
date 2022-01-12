package edu.uci.ics.texera.workflow.operators.reservoirsampling

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Schema, OperatorSchemaInfo}
import edu.uci.ics.texera.workflow.operators.util.OperatorDescriptorUtils.equallyPartitionGoal

import scala.util.Random

class ReservoirSamplingOpDesc extends OperatorDescriptor {
  // kPerActor needed because one operator can have multiple executor (a.k.a. worker/actor)
  // In order to make sure the total output is k, each executor should produce (k / n) items
  // (n is the number of the executors)
  @JsonIgnore
  private lazy val kPerActor: List[Int] = equallyPartitionGoal(k, Constants.currentWorkerNum)

  // Store random seeds for each executor to satisfy the fault tolerance requirement.
  // If a worker failed, the engine will start a new worker and rerun the computation.
  // Fault tolerance requires that the restarted worker should produce the exactly same output.
  // Therefore the seeds have to be stored.
  @JsonIgnore
  private val seeds: Array[Int] = Array.fill(Constants.currentWorkerNum)(Random.nextInt)

  @JsonProperty(value = "number of item sampled in reservoir sampling", required = true)
  @JsonPropertyDescription("reservoir sampling with k items being kept randomly")
  var k: Int = _

  @JsonIgnore
  def getSeed(index: Int): Int = seeds(index)

  @JsonIgnore
  def getKForActor(actor: Int): Int = {
    kPerActor(actor)
  }

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      operatorIdentifier,
      (actor: Int) => new ReservoirSamplingOpExec(actor, this)
    )
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
