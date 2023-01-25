package edu.uci.ics.texera.workflow.common.operators.aggregate

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.NewOpExecConfig.NewOpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{NewOpExecConfig, WorkerLayer}
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
      NewOpExecConfig.oneToOneLayer(makeLayer(id, "localAgg"), _ => new PartialAggregateOpExec[P](aggFunc))

    val finalLayer = if (aggFunc.groupByFunc == null) {
      NewOpExecConfig.localLayer(makeLayer(id, "globalAgg"), _ => new FinalAggregateOpExec[P](aggFunc))
    } else {
      val partitionColumns = aggFunc
        .groupByFunc(operatorSchemaInfo.inputSchemas(0))
        .getAttributes
        .toArray
        .indices
        .toArray
      NewOpExecConfig.hashLayer(
        makeLayer(id, "globalAgg"),
        _ => new FinalAggregateOpExec(aggFunc),
        partitionColumns
      )
    }

    new PhysicalPlan(
      List(partialLayer, finalLayer),
      List(LinkIdentity(partialLayer.id, finalLayer.id))
    )
  }

}

abstract class AggregateOpDesc extends OperatorDescriptor {

  override def newOperatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): NewOpExecConfig = {
    throw new UnsupportedOperationException("multi-layer op should use operatorExecutorMultiLayer")
  }

}
