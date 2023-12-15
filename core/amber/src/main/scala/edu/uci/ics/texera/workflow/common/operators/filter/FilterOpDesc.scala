package edu.uci.ics.texera.workflow.common.operators.filter

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.common.virtualidentity.ExecutionIdentity
import edu.uci.ics.texera.workflow.common.operators.{LogicalOp, StateTransferFunc}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

import scala.util.{Success, Try}

abstract class FilterOpDesc extends LogicalOp {

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }

  override def runtimeReconfiguration(
      executionId: ExecutionIdentity,
      newOpDesc: LogicalOp,
      operatorSchemaInfo: OperatorSchemaInfo
  ): Try[(PhysicalOp, Option[StateTransferFunc])] = {
    Success(newOpDesc.getPhysicalOp(executionId, operatorSchemaInfo), None)
  }

}
