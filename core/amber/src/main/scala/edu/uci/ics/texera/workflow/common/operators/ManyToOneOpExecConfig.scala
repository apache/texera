package edu.uci.ics.texera.workflow.common.operators

import edu.uci.ics.amber.engine.architecture.breakpoint.globalbreakpoint.GlobalBreakpoint
import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploymentfilter.UseAll
import edu.uci.ics.amber.engine.architecture.deploysemantics.deploystrategy.RoundRobinDeployment
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.WorkerLayer
import edu.uci.ics.amber.engine.common.virtualidentity.ActorVirtualIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.util.{makeLayer, toOperatorIdentity}
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.operators.OpExecConfig

import scala.collection.mutable

class ManyToOneOpExecConfig(
    override val id: OperatorIdentity,
    val opExec: Int => IOperatorExecutor,
    val dependency: mutable.Map[Int, Int] = mutable.Map()
) extends OpExecConfig(id) {

  override lazy val topology: Topology = {
    new Topology(
      Array(
        new WorkerLayer(
          makeLayer(id, "main"),
          opExec,
          1,
          UseAll(),
          RoundRobinDeployment()
        )
      ),
      Array()
    )
  }

  override def checkStartDependencies(workflow: Workflow): Unit = {
    // Map[depender -> dependee]
    // example: 1 -> 0 means port 1 depends on port 0, so that it needs to wait until port 0 finishes.
    for ((dependerOrdinal, dependeeOrdinal) <- dependency) {
      val dependeeLink =
        inputToOrdinalMapping.find({ case (_, (ordinal, _)) => ordinal == dependeeOrdinal }).get._1
      val dependerLink =
        inputToOrdinalMapping.find({ case (_, (ordinal, _)) => ordinal == dependerOrdinal }).get._1
      workflow.getSources(toOperatorIdentity(dependerLink.from)).foreach { dependerSource =>
        val opId = workflow.getOperator(dependeeLink.from.operator).id
        workflow.getSources(opId).foreach { dependeeSource =>
          if (dependerSource != dependeeSource) {
            workflow.getOperator(dependerSource).topology.layers.head.startAfter(dependeeLink)
          }
        }
      }
    }
  }

  override def assignBreakpoint(
      breakpoint: GlobalBreakpoint[_]
  ): Array[ActorVirtualIdentity] = {
    // TODO: take worker states into account
    topology.layers(0).identifiers
  }
}
