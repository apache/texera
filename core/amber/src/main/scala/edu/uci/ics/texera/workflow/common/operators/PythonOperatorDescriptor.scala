package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2

trait PythonOperatorDescriptor extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val generatedCode = generatePythonCode(operatorSchemaInfo)

    new OneToOneOpExecConfig(
      operatorIdentifier,
      (_: Any) =>
        new PythonUDFOpExecV2(
          generatedCode,
          operatorSchemaInfo.outputSchemas.head
        )
    )
  }

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates. It also has access to input and output schema
    * information for reference or validation purposes.
    * @param operatorSchemaInfo the actual input and output schema information of
    *                           this operator.
    * @return a String representation of the executable Python source code.
    */
  def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String

}
