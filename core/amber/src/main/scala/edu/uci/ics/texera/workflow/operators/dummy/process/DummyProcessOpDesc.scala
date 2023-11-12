package edu.uci.ics.texera.workflow.operators.dummy.process

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class DummyProcessOpDesc extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig =
    OpExecConfig
      .manyToOneLayer(operatorIdentifier, _ => new DummyProcessOpExec())
      .copy(
        inputPorts = operatorInfo.inputPorts,
        outputPorts = operatorInfo.outputPorts
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Dummy Process",
      operatorDescription = "Dummy Process",
      OperatorGroupConstants.DUMMY_GROUP,
      inputPorts = List(InputPort(), InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = schemas(0)
}
