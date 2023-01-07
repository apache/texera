package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

trait PythonOperatorDescriptor extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val generatedCode = generatePythonCode(operatorSchemaInfo.inputSchemas)

    new OneToOneOpExecConfig(operatorIdentifier, (_: Any) =>
      new PythonUDFOpExecV2(
        generatedCode,
        operatorSchemaInfo.outputSchemas.head
      ))
  }

  def generatePythonCode(inputSchemas: Array[Schema]): String
}
