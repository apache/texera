package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.AmberConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

import scala.collection.mutable

trait PythonOperatorDescriptor extends LogicalOp {
  override def getPhysicalOp(
      executionId: Long,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    val generatedCode = generatePythonCode(operatorSchemaInfo)
    if (asSource()) {

      PhysicalOp
        .sourcePhysicalOp(
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .copy(numWorkers = numWorkers(), dependency = dependency().toMap)
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    } else {
      PhysicalOp
        .oneToOnePhysicalOp(
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .copy(numWorkers = numWorkers(), dependency = dependency().toMap)
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    }
  }

  def numWorkers(): Int = AmberConfig.numWorkerPerOperatorByDefault

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
