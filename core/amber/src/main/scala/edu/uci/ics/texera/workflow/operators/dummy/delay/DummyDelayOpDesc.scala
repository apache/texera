package edu.uci.ics.texera.workflow.operators.dummy.delay

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecInitInfo}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class DummyDelayOpDesc extends OperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("delay")
  var delay = 1
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    OpExecConfig.manyToOneLayer(operatorIdentifier, OpExecInitInfo(_ => new DummyDelayOpExec(delay)))

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Dummy Delay",
      operatorDescription = "Dummy Delay",
      OperatorGroupConstants.DUMMY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
