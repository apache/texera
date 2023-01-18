package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.NewOpExecConfig.NewOpExecConfig
import edu.uci.ics.amber.engine.architecture.linksemantics.LinkStrategy
import edu.uci.ics.amber.engine.architecture.scheduling.PipelinedRegion
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

object PhysicalPlan {

  def apply(operatorList: Array[NewOpExecConfig], links: Array[LinkIdentity]): PhysicalPlan = {
    new PhysicalPlan(operatorList.toList, links.toList)
  }

  def toOutLinks(
      allLinks: Iterable[(OperatorIdentity, OperatorIdentity)]
  ): Map[OperatorIdentity, Set[OperatorIdentity]] = {
    allLinks
      .groupBy(link => link._1)
      .mapValues(links => links.map(link => link._2).toSet)
  }

}

case class PhysicalPlan(
    operators: List[NewOpExecConfig],
    links: List[LinkIdentity],
    linkStrategies: Map[LinkIdentity, LinkStrategy] = Map(),
    pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = null
) {}
