package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

trait PythonOperatorDescriptor extends LogicalOp {
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalOp = {
    val generatedCode = generatePythonCode(operatorSchemaInfo)
    if (asSource()) {
      PhysicalOp
        .sourcePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .withInputPorts(operatorInfo.inputPorts)
        .withOutputPorts(operatorInfo.outputPorts)
        .withParallelizable(parallelizable())
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    } else {
      PhysicalOp
        .oneToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .withInputPorts(operatorInfo.inputPorts)
        .withOutputPorts(operatorInfo.outputPorts)
        .withParallelizable(parallelizable())
        .withOperatorSchemaInfo(schemaInfo = operatorSchemaInfo)
    }
  }

  def parallelizable(): Boolean = false
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
