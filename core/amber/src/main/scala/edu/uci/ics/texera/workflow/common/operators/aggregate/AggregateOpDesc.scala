package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object AggregateOpDesc {

  def opExecPhysicalPlan[P <: AnyRef](
      id: OperatorIdentity,
      aggFunc: DistributedAggregation[P],
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    val partialLayer =
      OpExecConfig
        .oneToOneLayer(
          makeLayer(id, "localAgg"),
          _ => new PartialAggregateOpExec[P](aggFunc)
        )
        .copy(isOneToManyOp = true)

    val finalLayer = if (aggFunc.groupByFunc == null) {
      OpExecConfig
        .localLayer(
          makeLayer(id, "globalAgg"),
          _ => new FinalAggregateOpExec[P](aggFunc)
        )
        .copy(isOneToManyOp = true)
    } else {
      val partitionColumns = aggFunc
        .groupByFunc(operatorSchemaInfo.inputSchemas(0))
        .getAttributes
        .toArray
        .indices
        .toArray
      OpExecConfig
        .hashLayer(
          makeLayer(id, "globalAgg"),
          _ => new FinalAggregateOpExec(aggFunc),
          partitionColumns
        )
        .copy(isOneToManyOp = true)
    }

    new PhysicalPlan(
      List(partialLayer, finalLayer),
      List(LinkIdentity(partialLayer.id, finalLayer.id))
    )
  }

}

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    throw new UnsupportedOperationException("multi-layer op should use operatorExecutorMultiLayer")
  }

  override def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    var plan = aggregateOperatorExecutor(operatorSchemaInfo)
    plan.operators.foreach(op => plan = plan.setOperator(op.copy(isOneToManyOp = true)))
    plan
  }

  def aggregateOperatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan

}
