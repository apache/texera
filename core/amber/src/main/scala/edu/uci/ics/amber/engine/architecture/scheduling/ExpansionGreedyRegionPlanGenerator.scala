package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.CollectionHasAsScala

class ExpansionGreedyRegionPlanGenerator(
    workflowContext: WorkflowContext,
    initialPhysicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends RegionPlanGenerator(workflowContext, initialPhysicalPlan, opResultStorage)
    with LazyLogging {
  def generate(): (RegionPlan, PhysicalPlan) = {

    val regionDAG = createRegionDAG()

    (
      RegionPlan(
        regions = regionDAG.vertexSet().asScala.toSet,
        regionLinks = regionDAG.edgeSet().asScala.toSet
      ),
      physicalPlan
    )
  }

  /**
    * Takes in a pair of operatorIds, `upstreamOpId` and `downstreamOpId`, finds all regions they each
    * belong to, and creates the order relationships between the Regions of upstreamOpId, with the Regions
    * of downstreamOpId. The relation ship can be N to M.
    *
    * This method does not consider ports.
    *
    * Returns pairs of (upstreamRegion, downstreamRegion) indicating the order from
    * upstreamRegion to downstreamRegion.
    */
  private def toRegionOrderPairs(
      upstreamOpId: PhysicalOpIdentity,
      downstreamOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[(Region, Region)] = {

    val upstreamRegions = getRegions(upstreamOpId, regionDAG)
    val downstreamRegions = getRegions(downstreamOpId, regionDAG)

    upstreamRegions.flatMap { upstreamRegion =>
      downstreamRegions
        .filterNot(regionDAG.getDescendants(upstreamRegion).contains(_))
        .map(downstreamRegion => (upstreamRegion, downstreamRegion))
    }
  }

  /**
    * Create Regions based on the PhysicalPlan. The Region are to be added to regionDAG separately.
    */
  private def createRegions(physicalPlan: PhysicalPlan): Set[Region] = {
    val dependeeLinksRemovedDAG = physicalPlan.getDependeeLinksRemovedDAG
    val connectedComponents = new BiconnectivityInspector[PhysicalOpIdentity, PhysicalLink](
      dependeeLinksRemovedDAG.dag
    ).getConnectedComponents.asScala.toSet
    connectedComponents.zipWithIndex.map {
      case (connectedSubDAG, idx) =>
        val operatorIds = connectedSubDAG.vertexSet().asScala.toSet
        val links = operatorIds
          .flatMap(operatorId => {
            physicalPlan.getUpstreamPhysicalLinks(operatorId) ++ physicalPlan
              .getDownstreamPhysicalLinks(operatorId)
          })
          .filter(link => operatorIds.contains(link.fromOpId))
        val operators = operatorIds.map(operatorId => physicalPlan.getOperator(operatorId))
        Region(
          id = RegionIdentity(idx),
          physicalOps = operators,
          physicalLinks = links
        )
    }
  }

  /**
    * Try connect the regions in the DAG while respecting the dependencies of PhysicalLinks (e.g., HashJoin).
    * This function returns either a successful connected region DAG, or a list of PhysicalLinks that should be
    * replaced for materialization.
    *
    * This function builds a region DAG from scratch. It first adds all the regions into the DAG. Then it starts adding
    * edges on the DAG. To do so, it examines each PhysicalOp and checks its input links. The links will be problematic
    * if the link's toOp (this PhysicalOp) has another link that has higher priority to run than this link (i.e., it has
    * a dependency). If such links are found, the function will terminate after this PhysicalOp and return the set of
    * links.
    *
    * If the function finds no such links for all PhysicalOps, it will return the connected Region DAG.
    *
    *  @return Either a partially connected region DAG, or a set of PhysicalLinks for materialization replacement.
    */
  private def tryConnectRegionDAG()
      : Either[DirectedAcyclicGraph[Region, RegionLink], Set[PhysicalLink]] = {

    // creates an empty regionDAG
    val regionDAG = new DirectedAcyclicGraph[Region, RegionLink](classOf[RegionLink])

    // add Regions as vertices
    createRegions(physicalPlan).foreach(region => regionDAG.addVertex(region))

    // add regionLinks as edges, if failed, return the problematic PhysicalLinks.
    physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        (handleDependentLinks(physicalOpId, regionDAG))
          .map(links => return Right(links))
      })

    // if success, a partially connected region DAG without edges between materialization operators is returned.
    // The edges between materialization are to be added later.
    Left(regionDAG)
  }

  private def handleDependentLinks(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Option[Set[PhysicalLink]] = {
    // for operators like HashJoin that have an order among their blocking and pipelined inputs
    physicalPlan
      .getOperator(physicalOpId)
      .getInputLinksInProcessingOrder
      .sliding(2, 1)
      .foreach {
        case List(prevLink, nextLink) =>
          // Create edges between regions
          val regionOrderPairs = toRegionOrderPairs(prevLink.fromOpId, nextLink.fromOpId, regionDAG)
          // Attempt to add edges to regionDAG
          try {
            regionOrderPairs.foreach {
              case (fromRegion, toRegion) =>
                regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
            }
          } catch {
            case _: IllegalArgumentException =>
              // adding the edge causes cycle. return the link for materialization replacement
              return Some(Set(nextLink))
          }
        case _ =>
      }
    None
  }

  /**
    * This function creates and connects a region DAG while conducting materialization replacement.
    * It keeps attempting to create a region DAG from the given PhysicalPlan. When failed, a list
    * of PhysicalLinks that causes the failure will be given to conduct materialization replacement,
    * which changes the PhysicalPlan. It keeps attempting with the updated PhysicalPLan until a
    * region DAG is built after connecting materialized pairs.
    *
    * @return a fully connected region DAG.
    */
  private def createRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {

    val matReaderWriterPairs =
      new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
    @tailrec
    def recConnectRegionDAG(): DirectedAcyclicGraph[Region, RegionLink] = {
      tryConnectRegionDAG() match {
        case Left(dag) => dag
        case Right(links) =>
          links.foreach { link =>
            physicalPlan = replaceLinkWithMaterialization(
              link,
              matReaderWriterPairs
            )
          }
          recConnectRegionDAG()
      }
    }

    // the region is partially connected successfully.
    val regionDAG: DirectedAcyclicGraph[Region, RegionLink] = recConnectRegionDAG()

    // try to add dependencies between materialization writer and reader regions
    try {
      matReaderWriterPairs.foreach {
        case (writer, reader) =>
          toRegionOrderPairs(writer, reader, regionDAG).foreach {
            case (fromRegion, toRegion) =>
              regionDAG.addEdge(fromRegion, toRegion, RegionLink(fromRegion.id, toRegion.id))
          }
      }
    } catch {
      case _: IllegalArgumentException =>
        // a cycle is detected. it should not reach here.
        throw new WorkflowRuntimeException(
          "Cyclic dependency between regions detected"
        )
    }

    // mark links that go to downstream regions
    populateDependeeLinks(regionDAG)

    // allocate resources on regions
    allocateResource(regionDAG)

    regionDAG
  }
}
