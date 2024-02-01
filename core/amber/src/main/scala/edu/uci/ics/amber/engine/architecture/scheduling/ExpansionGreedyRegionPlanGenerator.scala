package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.ExpansionGreedyRegionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.amber.engine.common.workflow.{OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.jdk.CollectionConverters.asScalaIteratorConverter

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
      .toList
      .foreach(oldEdge => {
        val dest = graph.getEdgeTarget(oldEdge)
        graph.addEdge(newVertex, dest, RegionLink(newVertex, dest))
        graph.removeEdge(oldEdge)
      })
    graph
      .incomingEdgesOf(oldVertex)
      .toList
      .foreach(oldEdge => {
        val source = graph.getEdgeSource(oldEdge)
        graph.addEdge(source, newVertex, RegionLink(source, newVertex))
        graph.removeEdge(oldEdge)
      })
    graph.removeVertex(oldVertex)
  }

}

class ExpansionGreedyRegionPlanGenerator(
    logicalPlan: LogicalPlan,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends RegionPlanGenerator(
      logicalPlan,
      physicalPlan,
      opResultStorage
    )
    with LazyLogging {

  private val executionClusterInfo = new ExecutionClusterInfo()

  /**
    * Create RegionLinks between the regions of operators `upstreamOpId` and `downstreamOpId`.
    * The links are to be added to the region DAG separately.
    */
  private def createLinks(
      upstreamOpId: PhysicalOpIdentity,
      downstreamOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[RegionLink] = {

    val upstreamRegions = getRegions(upstreamOpId, regionDAG)
    val downstreamRegions = getRegions(downstreamOpId, regionDAG)

    upstreamRegions.flatMap { upstreamRegion =>
      downstreamRegions
        .filterNot(regionDAG.getDescendants(upstreamRegion).contains(_))
        .map(downstreamRegion => RegionLink(upstreamRegion, downstreamRegion))
    }
  }

  /**
    * Create Regions based on the PhysicalPlan. The Region are to be added to regionDAG separately.
    */
  private def createRegions(physicalPlan: PhysicalPlan): Set[Region] = {
    val nonBlockingDAG = physicalPlan.removeBlockingLinks()
    new BiconnectivityInspector[PhysicalOpIdentity, DefaultEdge](
      nonBlockingDAG.dag
    ).getConnectedComponents.toSet.zipWithIndex.map {
      case (connectedSubDAG, idx) =>
        val operatorIds = connectedSubDAG.vertexSet().toSet
        val links = operatorIds.flatMap(operatorId => {
          physicalPlan.getUpstreamPhysicalLinks(operatorId) ++ physicalPlan
            .getDownstreamPhysicalLinks(operatorId)
        })
        val operators = operatorIds.map(operatorId => physicalPlan.getOperator(operatorId))
        Region(RegionIdentity(idx), operators, links)
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
          val regionLinks = createLinks(prevLink.fromOpId, nextLink.fromOpId, regionDAG)
          // Attempt to add edges to regionDAG
          try {
            regionLinks.foreach(link => regionDAG.addEdge(link.fromRegion, link.toRegion, link))
          } catch {
            case _: IllegalArgumentException =>
              // adding the edge causes cycle. return the link for materialization replacement
              return Some(Set(nextLink))
          }
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
  private def createRegionDAG(
      context: WorkflowContext
  ): DirectedAcyclicGraph[Region, RegionLink] = {

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
              context,
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
          createLinks(writer, reader, regionDAG).foreach(link =>
            regionDAG.addEdge(link.fromRegion, link.toRegion, link)
          )
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
        val (newRegion, estimationCost) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  private def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[Region] = {
    regionDAG
      .vertexSet()
      .filter(region => region.physicalOps.map(_.id).contains(physicalOpId))
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
      .mapValues(_.map(_._2))
      .foreach {
        case (region, links) =>
          val newRegion = region.copy(
            downstreamLinks = links,
            downstreamOps = links.map(_.toOpId).map(id => physicalPlan.getOperator(id))
          )
          replaceVertex(regionDAG, region, newRegion)
      }
    regionDAG
  }
  def generate(context: WorkflowContext): (RegionPlan, PhysicalPlan) = {

    val regionDAG = createRegionDAG(context)

    (
      RegionPlan(
        regions = regionDAG.iterator().asScala.toList,
        regionLinks = regionDAG.edgeSet().toSet
      ),
      physicalPlan
    )
  }

  private def replaceLinkWithMaterialization(
      physicalLink: PhysicalLink,
      context: WorkflowContext,
      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
  ): PhysicalPlan = {
    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val fromOp = physicalPlan.getOperator(physicalLink.fromOpId)
    val fromPortId = physicalLink.fromPortId

    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val toOp = physicalPlan.getOperator(physicalLink.toOpId)
    val toPortId = physicalLink.toPortId

    val (matWriterLogicalOp: ProgressiveSinkOpDesc, matWriterPhysicalOp: PhysicalOp) =
      createMatWriter(fromOp, fromPortId, context)

    val matReaderPhysicalOp: PhysicalOp = createMatReader(matWriterLogicalOp, context)

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
      matWriterLogicalOp: ProgressiveSinkOpDesc,
      context: WorkflowContext
  ): PhysicalOp = {
    val materializationReader = new CacheSourceOpDesc(
      matWriterLogicalOp.operatorIdentifier,
      opResultStorage: OpResultStorage
    )
    materializationReader.setContext(context)
    materializationReader.setOperatorId("cacheSource_" + matWriterLogicalOp.operatorIdentifier.id)
    materializationReader.schema = matWriterLogicalOp.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array()).head
    materializationReader.outputPortToSchemaMapping(
      materializationReader.operatorInfo.outputPorts.head.id
    ) = matReaderOutputSchema

    val matReaderOp = materializationReader
      .getPhysicalOp(
        context.workflowId,
        context.executionId
      )
      .withOutputPorts(List(OutputPort()), materializationReader.outputPortToSchemaMapping)

    matReaderOp
  }

  private def createMatWriter(
      fromOp: PhysicalOp,
      fromPortId: PortIdentity,
      context: WorkflowContext
  ): (ProgressiveSinkOpDesc, PhysicalOp) = {
    val matWriterLogicalOp = new ProgressiveSinkOpDesc()
    matWriterLogicalOp.setContext(context)
    matWriterLogicalOp.setOperatorId("materialized_" + fromOp.id.logicalOpId.id)
    val fromLogicalOp = logicalPlan.getOperator(fromOp.id.logicalOpId)
    val fromOpInputSchema: Array[Schema] =
      if (!fromLogicalOp.isInstanceOf[SourceOperatorDescriptor]) {
        fromLogicalOp.inputPortToSchemaMapping.values.toArray
      } else {
        Array()
      }
    val matWriterInputSchema = fromLogicalOp.getOutputSchemas(fromOpInputSchema)(fromPortId.id)
    // we currently expect only one output schema
    val inputPort = matWriterLogicalOp.operatorInfo().inputPorts.head
    val outputPort = matWriterLogicalOp.operatorInfo().outputPorts.head
    matWriterLogicalOp.inputPortToSchemaMapping(inputPort.id) = matWriterInputSchema
    val matWriterOutputSchema = matWriterLogicalOp.getOutputSchema(Array(matWriterInputSchema))
    matWriterLogicalOp.outputPortToSchemaMapping(outputPort.id) = matWriterOutputSchema
    val matWriterPhysicalOp = matWriterLogicalOp
      .getPhysicalOp(
        context.workflowId,
        context.executionId
      )
      .withInputPorts(List(inputPort), matWriterLogicalOp.inputPortToSchemaMapping)
      .withOutputPorts(List(outputPort), matWriterLogicalOp.outputPortToSchemaMapping)
    matWriterLogicalOp.setStorage(
      opResultStorage.create(
        key = matWriterLogicalOp.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    opResultStorage.get(matWriterLogicalOp.operatorIdentifier).setSchema(matWriterInputSchema)
    (matWriterLogicalOp, matWriterPhysicalOp)
  }
}
