package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

trait PythonOperatorDescriptor extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val exec = (i: Any) =>
      new PythonUDFOpExecV2(
        generatePythonCode(operatorSchemaInfo.inputSchemas),
        operatorSchemaInfo.outputSchemas.head
      )
    new OneToOneOpExecConfig(operatorIdentifier, exec)
  }

  def generatePythonCode(inputSchemas: Array[Schema]): String
}
