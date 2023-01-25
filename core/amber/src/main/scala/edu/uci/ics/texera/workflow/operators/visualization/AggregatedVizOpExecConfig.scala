package edu.uci.ics.texera.workflow.operators.visualization

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.NewOpExecConfig.NewOpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{NewOpExecConfig, WorkerLayer}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpDesc, DistributedAggregation}
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
  def opExecPhysicalPlan[P <: AnyRef, T <: IOperatorExecutor: ClassTag](
    id: OperatorIdentity,
    aggFunc: DistributedAggregation[P],
    exec: ((Int, NewOpExecConfig)) => T,
    operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {

    val aggregateOperators = AggregateOpDesc.opExecPhysicalPlan(id, aggFunc, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    val vizLayer = NewOpExecConfig.oneToOneLayer(makeLayer(id, "visualize"), exec)

    new PhysicalPlan(
      vizLayer :: aggregateOperators.operators,
      LinkIdentity(tailAggregateOp, vizLayer.id) :: aggregateOperators.links
    )
  }

}


/**
  * Generic config for a visualization operator that supports aggregation internally.
  * @param id A descriptor's OperatorIdentity.
  * @param aggFunc Custom aggregation function to be applied on the data, the first two layers.
  * @param exec The final layer, wraps things up for whatever is needed by the frontend.
  * @param operatorSchemaInfo The descriptor's OperatorSchemaInfo.
  * @tparam P The type of the aggregation data.
  */
class AggregatedVizOpExecConfig[P <: AnyRef](
    id: OperatorIdentity,
    val aggFunc: DistributedAggregation[P],
    exec: IOperatorExecutor,
    operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    throw new NotImplementedError("to be removed")
    //    val partialLayer = new WorkerLayer(
//      makeLayer(id, "localAgg"),
//      _ => new PartialAggregateOpExec(aggFunc),
//      Constants.currentWorkerNum,
//      UseAll(),
//      RoundRobinDeployment()
//    )
//    val finalLayer = new WorkerLayer(
//      makeLayer(id, "globalAgg"),
//      _ => new FinalAggregateOpExec(aggFunc),
//      Constants.currentWorkerNum,
//      FollowPrevious(),
//      RoundRobinDeployment()
//    )
//    val vizLayer = new WorkerLayer(
//      makeLayer(id, "visualize"),
//      _ => exec,
//      Constants.currentWorkerNum,
//      FollowPrevious(),
//      RoundRobinDeployment()
//    )
//    new Topology(
//      Array(
//        partialLayer,
//        finalLayer,
//        vizLayer
//      ),
//      Array(
//        new HashBasedShuffle(
//          partialLayer,
//          finalLayer,
//          Constants.defaultBatchSize,
//          getPartitionColumnIndices(partialLayer.id)
//        ),
//        new OneToOne(
//          finalLayer,
//          vizLayer,
//          Constants.defaultBatchSize
//        )
//      )
//    )
  }

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = {
    aggFunc
      .groupByFunc(operatorSchemaInfo.inputSchemas(0))
      .getAttributes
      .toArray
      .indices
      .toArray
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
