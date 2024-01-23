package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

trait PythonOperatorDescriptor extends LogicalOp {
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {

    val generatedCode = generatePythonCode()
    if (asSource()) {
      PhysicalOp
        .sourcePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
        .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
        .withParallelizable(parallelizable())
    } else {
      PhysicalOp
        .oneToOnePhysicalOp(
          workflowId,
          executionId,
          operatorIdentifier,
          OpExecInitInfo(generatedCode)
        )
        .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
        .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
        .withParallelizable(parallelizable())
    }
  }

  def parallelizable(): Boolean = false
  def asSource(): Boolean = false

  /**
    * This method is to be implemented to generate the actual Python source code
    * based on operators predicates.
    *
    * @return a String representation of the executable Python source code.
    */
  def generatePythonCode(): String

}
