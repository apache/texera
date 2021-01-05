package edu.uci.ics.texera.workflow.operators.union

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class UnionOpDesc extends OperatorDescriptor {

  override def operatorExecutor: OpExecConfig = {
    new OneToOneOpExecConfig(this.operatorIdentifier, _ => new UnionOpExec())
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Union",
      "unions the output rows from multiple input operators",
      OperatorGroupConstants.UTILITY_GROUP,
      1,
      1,
      allowMultiInputs = true
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    schemas(0)
  }

}
