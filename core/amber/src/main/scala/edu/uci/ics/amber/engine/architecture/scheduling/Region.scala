package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.config.ResourceConfig
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.{PhysicalLink, PortIdentity}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class RegionLink(fromRegionId: RegionIdentity, toRegionId: RegionIdentity)

case class RegionIdentity(id: Long)

case class GlobalPortIdentity(opId: PhysicalOpIdentity, portId: PortIdentity, input: Boolean)
case class Region(
    id: RegionIdentity,
    physicalOps: Set[PhysicalOp],
    physicalLinks: Set[PhysicalLink],
    resourceConfig: Option[ResourceConfig] = None
) {

  private val operators: Map[PhysicalOpIdentity, PhysicalOp] =
    getOperators.map(op => op.id -> op).toMap

  @transient lazy val dag: DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge] = {
    val jgraphtDag = new DirectedAcyclicGraph[PhysicalOpIdentity, DefaultEdge](classOf[DefaultEdge])
    getOperators.foreach(op => jgraphtDag.addVertex(op.id))
    getLinks.foreach(link => jgraphtDag.addEdge(link.fromOpId, link.toOpId))
    jgraphtDag
  }
  def topologicalIterator(): Iterator[PhysicalOpIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }
  def getOperators: Set[PhysicalOp] = physicalOps

  def getLinks: Set[PhysicalLink] = physicalLinks

  def getPorts: Set[GlobalPortIdentity] =
    getLinks
      .flatMap(link =>
        List(
          GlobalPortIdentity(link.fromOpId, link.fromPortId, input = false),
          GlobalPortIdentity(link.toOpId, link.toPortId, input = true)
        )
      )

  def getOperator(physicalOpId: PhysicalOpIdentity): PhysicalOp = {
    operators(physicalOpId)
  }

  /**
    * Effective source operators in a region.
    * The effective source contains operators that have 0 input links in this region.
    */
  def getSourceOperators: Set[PhysicalOp] = {
    getOperators
      .filter(physicalOp =>
        physicalOp
          .getInputLinks()
          .map(link => link.fromOpId)
          .forall(upstreamOpId => !getOperators.map(_.id).contains(upstreamOpId))
      )

  }

}
