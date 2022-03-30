package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.FollowPrevious
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.util.{makeLayer, toOperatorIdentity}
import edu.uci.ics.amber.engine.common.virtualidentity.{ActorVirtualIdentity, OperatorIdentity}
import edu.uci.ics.amber.engine.common.{Constants, IOperatorExecutor}
import edu.uci.ics.amber.engine.operators.OpExecConfig

class DependentPortOpExecConfig(
    id: OperatorIdentity,
    val opExec: Int => IOperatorExecutor,
    val numWorkers: Int = Constants.currentWorkerNum
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      layers = Array(
        new WorkerLayer(
          makeLayer(id, layerID = "main"),
          initIOperatorExecutor = opExec,
          numWorkers = numWorkers,
          deploymentFilter = FollowPrevious(),
          deployStrategy = RoundRobinDeployment()
        )
      ),
      links = Array()
    )
  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    val proceedingLink = inputToOrdinalMapping.find({ case (_, index) => index == 0 }).get._1

    val succeedingLink = inputToOrdinalMapping.find({ case (_, index) => index == 1 }).get._1
    workflow.getSources(toOperatorIdentity(succeedingLink.from)).foreach { source =>
      workflow.getOperator(source).topology.layers.head.startAfter(proceedingLink)
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    topology.layers(0).identifiers
  }
}
