package edu.uci.ics.texera.workflow.operators.hashJoin

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.common.tuple.ITuple
import edu.uci.ics.amber.engine.common.virtualidentity.util.{makeLayer, toOperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{
  ActorVirtualIdentity,
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class HashJoinOpExecConfig[K](
    id: OperatorIdentity,
    val probeAttributeName: String,
    val buildAttributeName: String,
    val operatorSchemaInfo: OperatorSchemaInfo
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          null,
          Constants.defaultNumWorkers,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }
  var buildTable: LinkIdentity = _

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val buildLink = inputToOrdinalMapping.find(pair => pair._2 == 0).get._1
    buildTable = buildLink
    val probeLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    workflow.getSources(toOperatorIdentity(probeLink.from)).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(buildLink)
    }
    topology.layers.head.metadata = _ =>
      new HashJoinOpExec[K](
        buildTable,
        buildAttributeName,
        probeAttributeName,
        operatorSchemaInfo
      )
  }

  override def requiredShuffle: Boolean = true

  override def getShuffleHashFunction(layer: LayerIdentity): ITuple => Int = {
    if (layer == buildTable.from.get) { t: ITuple =>
      t.asInstanceOf[Tuple].getField(buildAttributeName).hashCode()
    } else { t: ITuple =>
      t.asInstanceOf[Tuple].getField(probeAttributeName).hashCode()
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
