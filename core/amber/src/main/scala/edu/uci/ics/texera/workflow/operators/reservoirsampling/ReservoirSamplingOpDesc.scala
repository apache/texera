package edu.uci.ics.texera.workflow.operators.reservoirsampling

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

import scala.util.Random

class ReservoirSamplingOpDesc extends OperatorDescriptor {
  @JsonProperty(value = "number of item sampled in reservoir sampling", required = true)
  @JsonPropertyDescription("reservoir sampling with k items being kept randomly")
  var k: Int = _

  @JsonIgnore
  private val seeds: Array[Int] = Array.fill(Constants.defaultNumWorkers)(Random.nextInt)

  @JsonIgnore
  def getSeed(index: Int): Int = seeds(index)

  @JsonIgnore
  private lazy val kPerActor: Int = k / Constants.defaultNumWorkers

  @JsonIgnore
  private lazy val kRemainder: Int = k % Constants.defaultNumWorkers

  @JsonIgnore
  def getKForActor(actor: Int): Int = {
    if (actor == Constants.defaultNumWorkers - 1) {
      kPerActor + kRemainder
    } else {
      kPerActor
    }
  }

  override def operatorExecutor: OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(this.operatorIdentifier, (actor: Int) => new ReservoirSamplingOpExec(actor, this))
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Reservoir Sampling",
      operatorDescription = "Reservoir Sampling with k items being kept randomly",
      operatorGroupName =  OperatorGroupConstants.UTILITY_GROUP,
      numInputPorts = 1,
      numOutputPorts = 1
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
