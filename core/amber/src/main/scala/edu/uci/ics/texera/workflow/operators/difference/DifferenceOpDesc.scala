package edu.uci.ics.texera.workflow.operators.difference

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecInitInfo}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class DifferenceOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    OpExecConfig.oneToOneLayer(operatorIdentifier, (() =>  Left(_ => new DifferenceOpExec())):OpExecInitInfo)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Difference",
      "find the set difference of two inputs",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }
}
