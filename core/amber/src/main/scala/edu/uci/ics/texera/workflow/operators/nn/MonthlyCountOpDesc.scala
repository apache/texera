package edu.uci.ics.texera.workflow.operators.nn

import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.LogicalOp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, Schema}

class MonthlyCountOpDesc extends LogicalOp {

  private val outSchema = new Schema(new Attribute("count",AttributeType.LONG))

  override def getPhysicalOp(workflowId: WorkflowIdentity,
                             executionId: ExecutionIdentity): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => {
          new MonthlyCountOpExec(outSchema)
        })
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> outSchema)))
      .withParallelizable(false)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Monthly Tweet Count",
      "bluh bluh bluh",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    outSchema
  }
}