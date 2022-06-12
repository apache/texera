package edu.uci.ics.texera.workflow.operators.sortPartitions

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.{RoundRobinDeployment}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.{OpExecConfig, ShuffleType}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class SortPartitionsOpExecConfig(
    id: OperatorIdentity,
    sortAttributeName: String,
    domainMin: Long,
    domainMax: Long,
    operatorSchemaInfo: OperatorSchemaInfo,
    val numOfWorkers: Int = Constants.currentWorkerNum
) extends OpExecConfig(id) {
  shuffleType = ShuffleType.RANGE_BASED

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          i =>
            new SortPartitionOpExec(
              sortAttributeName,
              operatorSchemaInfo,
              i,
              domainMin,
              domainMax,
              numOfWorkers
            ),
          numOfWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  def getShuffleKey(layer: LayerIdentity): ITuple => String = { t: ITuple =>
    t.asInstanceOf[Tuple].getField(sortAttributeName).asInstanceOf[Float].toString()
  }

  override def getPartitionColumnIndices(layer: LayerIdentity): Array[Int] = {
    Array(operatorSchemaInfo.inputSchemas(0).getIndex(sortAttributeName))
  }

  override def getRangeShuffleMinAndMax: (Long, Long) = (domainMin, domainMax)

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
