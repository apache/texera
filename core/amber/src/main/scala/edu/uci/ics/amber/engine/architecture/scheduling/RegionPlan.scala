package edu.uci.ics.amber.engine.architecture.scheduling
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.jdk.CollectionConverters.IteratorHasAsScala

case class RegionPlan(
    regions: Set[Region],
    regionLinks: Set[RegionLink]
) {

  @transient lazy val dag: DirectedAcyclicGraph[RegionIdentity, RegionLink] = {
    val jgraphtDag = new DirectedAcyclicGraph[RegionIdentity, RegionLink](classOf[RegionLink])
    regionMapping.keys.foreach(regionId => jgraphtDag.addVertex(regionId))
    regionLinks.foreach(l => jgraphtDag.addEdge(l.fromRegionId, l.toRegionId, l))
    jgraphtDag
  }
  @transient private lazy val regionMapping: Map[RegionIdentity, Region] =
    regions.map(region => region.id -> region).toMap

  def getRegionOfLink(link: PhysicalLink): Region = {
    regions.find(region => region.getLinks.contains(link)).get
  }

  def getRegionOfPortId(portId: GlobalPortIdentity): Region = {
    regions.find(region => region.getPorts.contains(portId)).get
  }

  def topologicalIterator(): Iterator[RegionIdentity] = {
    new TopologicalOrderIterator(dag).asScala
  }

  def getRegion(regionId: RegionIdentity): Region = {
    regionMapping(regionId)
  }
}
