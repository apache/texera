package edu.uci.ics.texera.workflow.operators.intervalJoin

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, LayerIdentity, LinkIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.tuple.schema.OperatorSchemaInfo

class IntervalJoinOpExecConfig(
                                id: OperatorIdentity,
                                val probeAttributeName: String,
                                val buildAttributeName: String,
                                val operatorSchemaInfo: OperatorSchemaInfo,
                                val constant:Long,
                                val includeLeftBound:Boolean,
                                val includeRightBound:Boolean,
                                val timeIntervalType: TimeIntervalType
                              ) extends OpExecConfig(id) {

  var leftTable: LinkIdentity = _

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          LayerIdentity(id, "main"),
          null,
          1,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }
  override def checkStartDependencies(workflow: Workflow): Unit = {
    val buildLink = inputToOrdinalMapping.find(pair => pair._2 == 0).get._1
    leftTable = buildLink
    val probeLink = inputToOrdinalMapping.find(pair => pair._2 == 1).get._1
    workflow.getSources(probeLink.from.toOperatorIdentity).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(buildLink)
    }

    topology.layers.head.metadata = _ => {
      new IntervalJoinOpExec[Long](leftTable, buildAttributeName, probeAttributeName, operatorSchemaInfo, constant, includeLeftBound, includeRightBound, timeIntervalType)
    }
  }

  override def assignBreakpoint(breakpoint: GlobalBreakpoint[_]): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
