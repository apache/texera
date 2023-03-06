package edu.uci.ics.texera.workflow.operators.visualization

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{OpExecConfig, OpExecFunc}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  AggregateOpDesc,
  DistributedAggregation
}
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import scala.reflect.ClassTag

object AggregatedVizOpExecConfig {

  /**
    * Generic config for a visualization operator that supports aggregation internally.
    * @param id A descriptor's OperatorIdentity.
    * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
    * @param exec The final layer, wraps things up for whatever is needed by the frontend.
    * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
    * @tparam P The type of the aggregation data.
    */
  def opExecPhysicalPlan[P <: AnyRef](
      id: OperatorIdentity,
      aggFunc: DistributedAggregation[P],
      vizExec: OpExecConfig,
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {

    val aggregateOperators = AggregateOpDesc.opExecPhysicalPlan(id, aggFunc, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    new PhysicalPlan(
      vizExec :: aggregateOperators.operators,
      LinkIdentity(tailAggregateOp, vizExec.id) :: aggregateOperators.links
    )
  }

}
