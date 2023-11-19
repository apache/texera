package edu.uci.ics.texera.workflow.operators.dummy.blocking

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class DummyBlockingOutputOpDesc extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig
      .manyToOneLayer(operatorIdentifier, _ => new DummyBlockingOpExec())
      .copy(blockingOutputs = List(0))
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = "Dummy Blocking Output",
      operatorDescription = "Dummy Blocking Output",
      OperatorGroupConstants.DUMMY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
