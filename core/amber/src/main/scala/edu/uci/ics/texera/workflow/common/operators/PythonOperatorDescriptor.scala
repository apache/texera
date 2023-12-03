package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable

trait PythonOperatorDescriptor extends OperatorDescriptor {
  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    val generatedCode = generatePythonCode(operatorSchemaInfo)
    if (asSource()) {

      OpExecConfig
        .sourceLayer(
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .copy(numWorkers = numWorkers(), dependency = dependency().toMap)
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    } else {
      OpExecConfig
        .oneToOneLayer(
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .copy(numWorkers = numWorkers(), dependency = dependency().toMap)
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    }
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
