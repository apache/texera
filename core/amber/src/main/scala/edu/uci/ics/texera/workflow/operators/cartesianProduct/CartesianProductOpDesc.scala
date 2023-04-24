package edu.uci.ics.texera.workflow.operators.cartesianProduct

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class CartesianProductOpDesc extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig.manyToOneLayer(operatorIdentifier, _ => new CartesianProductOpExec)
  }

  override def operatorInfo: OperatorInfo = OperatorInfo("Cartesian Product", "Append fields together to get the cartesian product of two inputs", OperatorGroupConstants.UTILITY_GROUP, inputPorts = List(InputPort(), InputPort()), outputPorts = List(OutputPort()))

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    schemas(0)
  }
}
