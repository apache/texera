package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.ExpansionGreedyRegionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.annotation.tailrec
import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}

object ExpansionGreedyRegionPlanGenerator {

  def replaceVertex(
      graph: DirectedAcyclicGraph[Region, RegionLink],
      oldVertex: Region,
      newVertex: Region
  ): Unit = {
    if (oldVertex.equals(newVertex)) {
      return
    }
    graph.addVertex(newVertex)
    graph
      .outgoingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val dest = graph.getEdgeTarget(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(newVertex, dest, RegionLink(newVertex.id, dest.id))
      })
    graph
      .incomingEdgesOf(oldVertex)
      .asScala
      .toList
      .foreach(oldEdge => {
        val source = graph.getEdgeSource(oldEdge)
        graph.removeEdge(oldEdge)
        graph.addEdge(source, newVertex, RegionLink(source.id, newVertex.id))
      })
    graph.removeVertex(oldVertex)
  }

}

class ExpansionGreedyRegionPlanGenerator(
    workflowContext: WorkflowContext,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends RegionPlanGenerator(workflowContext, physicalPlan, opResultStorage)
    with LazyLogging {

  private val executionClusterInfo = new ExecutionClusterInfo()
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
    val nonBlockingDAG = physicalPlan.removeBlockingLinks()
    val connectedComponents = new BiconnectivityInspector[PhysicalOpIdentity, DefaultEdge](
      nonBlockingDAG.dag
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
    * if they have one of the following two properties:
    *   1. The link's toOp (this PhysicalOp) has and only has blocking links;
    *   2. The link's toOp (this PhysicalOp) has another link that has higher priority to run than this link
    *   (aka, it has a dependency).
    * If such links are found, the function will terminate after this PhysicalOp and return the set of links.
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
        (handleAllBlockingInput(physicalOpId) ++ handleDependentLinks(physicalOpId, regionDAG))
          .map(links => return Right(links))
      })

    // if success, a partially connected region DAG without edges between materialization operators is returned.
    // The edges between materialization are to be added later.
    Left(regionDAG)
  }

  private def handleAllBlockingInput(
      physicalOpId: PhysicalOpIdentity
  ): Option[Set[PhysicalLink]] = {
    if (physicalPlan.areAllInputBlocking(physicalOpId)) {
      // for operators that have only blocking input links return all links for materialization replacement
      return Some(
        physicalPlan
          .getUpstreamPhysicalOpIds(physicalOpId)
          .flatMap { upstreamPhysicalOpId =>
            physicalPlan.getLinksBetween(upstreamPhysicalOpId, physicalOpId)
          }
      )
    }
    None
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
        case List(_) | Nil =>
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
    populateDownstreamLinks(regionDAG)

    // allocate resources on regions
    allocateResource(regionDAG)

    regionDAG
  }

  private def allocateResource(regionDAG: DirectedAcyclicGraph[Region, RegionLink]): Unit = {
    val resourceAllocator = new DefaultResourceAllocator(physicalPlan, executionClusterInfo)
    // generate the region configs
    new TopologicalOrderIterator(regionDAG).asScala
      .foreach(region => {
        val (newRegion, _) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  private def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[Region] = {
    regionDAG
      .vertexSet()
      .asScala
      .filter(region => region.getOperators.map(_.id).contains(physicalOpId))
      .toSet
  }

  private def populateDownstreamLinks(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): DirectedAcyclicGraph[Region, RegionLink] = {

    val blockingLinks = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))
        }
      }
      .toSet

    blockingLinks
      .flatMap { link => getRegions(link.fromOpId, regionDAG).map(region => region -> link) }
      .groupBy(_._1)
      .view
      .mapValues(_.map(_._2))
      .foreach {
        case (region, links) =>
          val newRegion = region.copy(
            physicalLinks = region.physicalLinks ++ links,
            physicalOps =
              region.getOperators ++ links.map(_.toOpId).map(id => physicalPlan.getOperator(id))
          )
          replaceVertex(regionDAG, region, newRegion)
      }
    regionDAG
  }

  private def replaceLinkWithMaterialization(
      physicalLink: PhysicalLink,
      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
  ): PhysicalPlan = {
    val fromOp = physicalPlan.getOperator(physicalLink.fromOpId)
    val fromPortId = physicalLink.fromPortId

    val toOp = physicalPlan.getOperator(physicalLink.toOpId)
    val toPortId = physicalLink.toPortId

    val outputSchema = fromOp.outputPorts(fromPortId)._3
    val matWriterPhysicalOp: PhysicalOp =
      createMatWriter(outputSchema, fromOp.id.logicalOpId)
    val matReaderPhysicalOp: PhysicalOp =
      createMatReader(outputSchema, matWriterPhysicalOp.id.logicalOpId)

    // create 2 links for materialization
    val readerToDestLink =
      PhysicalLink(
        matReaderPhysicalOp.id,
        matReaderPhysicalOp.outputPorts.keys.head,
        toOp.id,
        toPortId
      )
    val sourceToWriterLink =
      PhysicalLink(
        fromOp.id,
        fromPortId,
        matWriterPhysicalOp.id,
        matWriterPhysicalOp.inputPorts.keys.head
      )

    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id

    physicalPlan
      .removeLink(physicalLink)
      .addOperator(matWriterPhysicalOp)
      .addOperator(matReaderPhysicalOp)
      .addLink(readerToDestLink)
      .addLink(sourceToWriterLink)
      .setOperatorUnblockPort(toOp.id, toPortId)

  }

  private def createMatReader(
      inputSchema: Schema,
      matWriterLogicalOpId: OperatorIdentity
  ): PhysicalOp = {
    val matReader = new CacheSourceOpDesc(
      matWriterLogicalOpId,
      opResultStorage: OpResultStorage
    )
    matReader.setContext(workflowContext)
    matReader.setOperatorId("cacheSource_" + matWriterLogicalOpId.id)
    matReader.schema = inputSchema

    // expect exactly one output port
    matReader.outputPortToSchemaMapping(matReader.operatorInfo.outputPorts.head.id) =
      matReader.getOutputSchema(Array())

    matReader.getPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId
    )
  }

  private def createMatWriter(
      inputSchema: Schema,
      fromLogicalOpId: OperatorIdentity
  ): PhysicalOp = {
    val matWriter = new ProgressiveSinkOpDesc()
    matWriter.setContext(workflowContext)
    matWriter.setOperatorId("materialized_" + fromLogicalOpId.id)

    // expect exactly one input port and one output port
    val inputPort = matWriter.operatorInfo().inputPorts.head
    val outputPort = matWriter.operatorInfo().outputPorts.head
    matWriter.inputPortToSchemaMapping(inputPort.id) = inputSchema
    matWriter.outputPortToSchemaMapping(outputPort.id) =
      matWriter.getOutputSchema(Array(inputSchema))
    matWriter.setStorage(
      opResultStorage.create(
        key = matWriter.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    opResultStorage.get(matWriter.operatorIdentifier).setSchema(inputSchema)

    matWriter.getPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId
    )

  }
}
