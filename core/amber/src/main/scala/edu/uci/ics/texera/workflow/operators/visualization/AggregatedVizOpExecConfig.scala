package edu.uci.ics.texera.workflow.operators.visualization

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  AggregateOpDesc,
  DistributedAggregation
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

object AggregatedVizOpExecConfig {

  /**
    * Generic config for a visualization operator that supports aggregation internally.
    * @param id A descriptor's OperatorIdentity.
    * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
    * @param exec The final layer, wraps things up for whatever is needed by the frontend.
    * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
    * @tparam P The type of the aggregation data.
    */
  def opExecPhysicalPlan(
      id: OperatorIdentity,
      aggFunc: DistributedAggregation[Object],
      groupByKeys: List[String],
      finalAggValueSchema: Schema,
      vizExec: OpExecConfig,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {

    val aggregateOperators =
      AggregateOpDesc.opExecPhysicalPlan(id, List(aggFunc), groupByKeys, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    new PhysicalPlan(
      vizExec :: aggregateOperators.operators,
      LinkIdentity(tailAggregateOp, 0, vizExec.id, 0) :: aggregateOperators.links
    )
  }

}
