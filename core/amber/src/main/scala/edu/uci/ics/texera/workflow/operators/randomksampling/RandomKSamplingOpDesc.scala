package edu.uci.ics.texera.workflow.operators.randomksampling

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.OneToOneOpExecConfig
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc

import scala.util.Random

class RandomKSamplingOpDesc extends FilterOpDesc {
  @JsonProperty(value = "random k sample percentage", required = true)
  @JsonPropertyDescription("random k sampling with given percentage")
  var percentage: Int = _

  @JsonIgnore
  private val seeds: Array[Int] = Array.fill(Constants.defaultNumWorkers)(Random.nextInt)

  @JsonIgnore
  def getSeed(index: Int): Int = seeds(index)

  override def operatorExecutor: OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(this.operatorIdentifier, (actor: Int) => new RandomKSamplingOpExec(actor, this))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Random K Sampling",
      operatorDescription = "random sampling with given percentage",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      numInputPorts = 1,
      numOutputPorts = 1
    )
}
