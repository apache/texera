package edu.uci.ics.texera.workflow.operators.dummy.source

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

class DummySourceOpDesc extends SourceOperatorDescriptor {
  @JsonProperty(required = true)
  @JsonSchemaTitle("i")
  var i = 1

  @JsonProperty(required = true)
  @JsonSchemaTitle("delay")
  var delay = 1
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig.manyToOneLayer(operatorIdentifier, _ => new DummySourceOpExec(i, delay))
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Dummy Input",
      operatorDescription = "Dummy Input",
      OperatorGroupConstants.DUMMY_GROUP,
      List.empty,
      outputPorts = List(OutputPort())
    )
  }

  override def sourceSchema(): Schema = new Schema(new Attribute("data", AttributeType.INTEGER))
}
