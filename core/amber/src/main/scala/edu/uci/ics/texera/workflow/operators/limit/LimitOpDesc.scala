package edu.uci.ics.texera.workflow.operators.limit

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.util.OperatorDescriptorUtils.equallyPartitionGoal

class LimitOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Limit")
  @JsonPropertyDescription("the max number of output rows")
  var limit: Int = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    val limitPerWorker = equallyPartitionGoal(limit, Constants.currentWorkerNum)
    OpExecConfig.oneToOneLayer(operatorIdentifier, p => new LimitOpExec(limitPerWorker(p._1)))
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Limit",
      "Limit the number of output rows",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("")),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
