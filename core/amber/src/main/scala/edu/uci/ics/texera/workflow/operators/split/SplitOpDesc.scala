package edu.uci.ics.texera.workflow.operators.split

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import scala.util.Random

class SplitOpDesc extends OperatorDescriptor {

  @JsonProperty(value = "training percentage", required = false, defaultValue = "80")
  @JsonPropertyDescription("percentage of training split data (default 80%)")
  var k: Int = 80

  // Store random seeds for each executor to satisfy the fault tolerance requirement.
  @JsonIgnore
  val seeds: Array[Int] = Array.fill(Constants.currentWorkerNum)(Random.nextInt)

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    new SplitOpExecConfig(operatorIdentifier, this)
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Training/Testing Split",
      operatorDescription = "Split training and testing data to two different ports",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort("training"), OutputPort("testing"))
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = throw new NotImplementedError()

  override def getOutputSchemas(schemas: Array[Schema]): Array[Schema] = {
    Preconditions.checkArgument(schemas.length == 1)
    Array(schemas(0), schemas(0))
  }
}
