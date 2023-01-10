package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.PythonUDFOpExecV2
import edu.uci.ics.texera.workflow.operators.udf.pythonV2.source.PythonUDFSourceOpExecV2

import scala.collection.mutable

trait PythonOperatorDescriptor extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val generatedCode = generatePythonCode(operatorSchemaInfo)

    new OneToOneOpExecConfig(
      id = operatorIdentifier,
      opExec = (_: Int) =>
        if (asSource()) {
          new PythonUDFSourceOpExecV2(
            generatedCode,
            operatorSchemaInfo.outputSchemas.head
          )
        } else {
          new PythonUDFOpExecV2(
            generatedCode,
            operatorSchemaInfo.outputSchemas.head
          )
        },
      numWorkers(),
      dependency()
    )
  }

  def numWorkers(): Int = Constants.numWorkerPerNode

  def dependency(): mutable.Map[Int, Int] = mutable.Map()

  def asSource(): Boolean = false

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates. It also has access to input and output schema
    * information for reference or validation purposes.
    *
    * @param operatorSchemaInfo the actual input and output schema information of
    *                           this operator.
    * @return a String representation of the executable Python source code.
    */
  def generatePythonCode(operatorSchemaInfo: OperatorSchemaInfo): String

}
