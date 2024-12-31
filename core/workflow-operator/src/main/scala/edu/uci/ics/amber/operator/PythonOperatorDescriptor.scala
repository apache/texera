package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OpExecWithCode
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}

trait PythonOperatorDescriptor extends LogicalOp {
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    val physicalOp = if (asSource()) {
      PhysicalOp.sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(generatePythonCode(), "python")
      )
    } else {
      PhysicalOp.oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecWithCode(generatePythonCode(), "python")
      )
    }

    physicalOp
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withParallelizable(parallelizable())
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(
            operatorInfo.outputPorts.head.id -> getOutputSchema(
              operatorInfo.inputPorts.map(_.id).map(inputSchemas(_)).toArray
            )
          )
        )
      )
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
