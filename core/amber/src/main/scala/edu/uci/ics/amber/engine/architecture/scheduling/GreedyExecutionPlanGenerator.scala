package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.scheduling.GreedyExecutionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalOpIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, asScalaSetConverter}

object GreedyExecutionPlanGenerator {

  def replaceVertex[V](
      graph: DirectedAcyclicGraph[V, DefaultEdge],
      oldVertex: V,
      newVertex: V
  ): Unit = {
    if (oldVertex.equals(newVertex)) {
      return
    }
    graph.addVertex(newVertex)
    graph
      .outgoingEdgesOf(oldVertex)
      .forEach(edge => {
        graph.addEdge(newVertex, graph.getEdgeTarget(edge))
      })
    graph
      .incomingEdgesOf(oldVertex)
      .forEach(edge => {
        graph.addEdge(graph.getEdgeSource(edge), newVertex)
      })
    graph.removeVertex(oldVertex)
  }

}

class GreedyExecutionPlanGenerator(
    workflowId: WorkflowIdentity,
    workflowContext: WorkflowContext,
    logicalPlan: LogicalPlan,
    var physicalPlan: PhysicalPlan,
    opResultStorage: OpResultStorage
) extends ExecutionPlanGenerator(
      workflowId,
      workflowContext,
      logicalPlan,
      physicalPlan,
      opResultStorage
    )
    with LazyLogging {

  /**
    * Create RegionLinks between the regions of operators `prevInOrderOpId` and `nextInOrderOpId`.
    * The links are to be added to the region DAG separately.
    */
  private def createLinks(
      prevInOrderOpId: PhysicalOpIdentity,
      nextInOrderOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, DefaultEdge]
  ): List[RegionLink] = {

    val prevInOrderRegions = getRegions(prevInOrderOpId, regionDAG)
    val nextInOrderRegions = getRegions(nextInOrderOpId, regionDAG)

    prevInOrderRegions.flatMap { prevRegion =>
      nextInOrderRegions
        .filterNot(nextRegion => regionDAG.getDescendants(prevRegion).contains(nextRegion))
        .map(nextRegion => RegionLink(prevRegion, nextRegion))
    }.toList
  }

  /**
    * Create Regions based on the PhysicalPlan. The Region are to be added to regionDAG separately.
    */
  private def createRegions(physicalPlan: PhysicalPlan): List[Region] = {
    val nonBlockingDAG = physicalPlan.removeBlockingEdges()
    nonBlockingDAG.getSourceOperatorIds.zipWithIndex
      .map {
        case (sourcePhysicalOpId, index) =>
          val operatorsInRegion =
            nonBlockingDAG.getDescendantPhysicalOpIds(sourcePhysicalOpId) :+ sourcePhysicalOpId
          Region(RegionIdentity(workflowId, (index + 1).toString), operatorsInRegion)
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
    * If such links are found, the function will terminate after this PhysicalOp and return the list of links.
    *
    * If the function finds no such links for all PhysicalOps, it will return the connected Region DAG.
    *
    *  @return Either a partially connected region DAG, or a list of PhysicalLinks for materialization replacement.
    */
  private def tryConnectRegionDAG()
      : Either[DirectedAcyclicGraph[Region, DefaultEdge], List[PhysicalLink]] = {

    // creates an empty regionDAG
    val regionDAG = new DirectedAcyclicGraph[Region, DefaultEdge](classOf[DefaultEdge])

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
  ): Option[List[PhysicalLink]] = {
    // for operators that have only blocking input links
    val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
    if (physicalPlan.areAllInputBlocking(physicalOpId)) {
      // return all links for materialization replacement
      return Some(
        upstreamPhysicalOpIds
          .flatMap { upstreamPhysicalOpId =>
            physicalPlan.getLinksBetween(upstreamPhysicalOpId, physicalOpId)
          }
      )
    }
    None
  }

  private def handleDependentLinks(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, DefaultEdge]
  ): Option[List[PhysicalLink]] = {
    // for operators like HashJoin that have an order among their blocking and pipelined inputs
    physicalPlan
      .getOperator(physicalOpId)
      .getInputLinksInProcessingOrder
      .sliding(2, 1)
      .foreach {
        case List(prevLink, nextLink) =>
          // Create edges between regions
          val regionLinks = createLinks(prevLink.fromOp.id, nextLink.fromOp.id, regionDAG)
          // Attempt to add edges to regionDAG
          try {
            regionLinks.foreach(link => regionDAG.addEdge(link.fromRegion, link.toRegion))
          } catch {
            case _: IllegalArgumentException =>
              // adding the edge causes cycle. return the link materialization replacement
              return Some(List(nextLink))
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
  private def connectRegionDAG(): DirectedAcyclicGraph[Region, DefaultEdge] = {
    val matReaderWriterPairs =
      new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
    @tailrec
    def recConnectRegionDAG(): DirectedAcyclicGraph[Region, DefaultEdge] = {
      tryConnectRegionDAG() match {
        case Left(dag) => dag
        case Right(links) =>
          links.foreach { link =>
            physicalPlan = replaceLinkWithMaterialization(link, matReaderWriterPairs)
          }
          recConnectRegionDAG()
      }
    }

    // the region is partially connected successfully.
    val regionDAG: DirectedAcyclicGraph[Region, DefaultEdge] = recConnectRegionDAG()

    // try to add dependencies between materialization writer and reader regions
    try {
      matReaderWriterPairs.foreach {
        case (writer, reader) =>
          createLinks(writer, reader, regionDAG).foreach(link =>
            regionDAG.addEdge(link.fromRegion, link.toRegion)
          )
      }
      regionDAG
    } catch {
      case _: java.lang.IllegalArgumentException =>
        // a cycle is detected. it should not reach here.
        throw new WorkflowRuntimeException(
          "PipelinedRegionsBuilder: Cyclic dependency between regions detected"
        )
    }

  }

  private def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, DefaultEdge]
  ): Set[Region] = {
    regionDAG.vertexSet().filter(region => region.contains(physicalOpId)).toSet
  }

  private def populateTerminalOperatorsForBlockingLinks(
      regionDAG: DirectedAcyclicGraph[Region, DefaultEdge]
  ): DirectedAcyclicGraph[Region, DefaultEdge] = {
    val regionTerminalOperatorInOtherRegions =
      mutable.HashMap[Region, ArrayBuffer[PhysicalOpIdentity]]()

    physicalPlan.topologicalIterator().foreach { physicalOpId =>
      val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
      upstreamPhysicalOpIds.foreach { upstreamPhysicalOpId =>
        val blockingLinks = physicalPlan
          .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
          .filter(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))

        blockingLinks.foreach { _ =>
          val prevInOrderRegions = getRegions(upstreamPhysicalOpId, regionDAG)
          prevInOrderRegions.foreach { prevInOrderRegion =>
            val regionEntry = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
              prevInOrderRegion,
              ArrayBuffer[PhysicalOpIdentity]()
            )
            if (!regionEntry.contains(physicalOpId)) {
              regionEntry.append(physicalOpId)
              regionTerminalOperatorInOtherRegions(prevInOrderRegion) = regionEntry
            }
          }
        }
      }
    }

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      val newRegion = region.copy(blockingDownstreamPhysicalOpIdsInOtherRegions =
        terminalOps.toArray.map(opId => (opId, 0))
      )
      replaceVertex(regionDAG, region, newRegion)
    }
    regionDAG
  }

  def generate(): (ExecutionPlan, PhysicalPlan) = {
    val regionDAG = populateTerminalOperatorsForBlockingLinks(connectRegionDAG())
    val regions = regionDAG.iterator().asScala.toList
    val ancestors = regions.map { region =>
      region -> regionDAG.getAncestors(region).asScala.toSet
    }.toMap
    (
      new ExecutionPlan(regionsToSchedule = regions, regionAncestorMapping = ancestors),
      physicalPlan
    )
  }

  private def replaceLinkWithMaterialization(
      physicalLink: PhysicalLink,
      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
  ): PhysicalPlan = {
    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val fromOp = physicalPlan.getOperator(physicalLink.id.from)
    val fromOutputPort = fromOp.getPortIdxForOutputLinkId(physicalLink.id)

    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val toOp = physicalPlan.getOperator(physicalLink.id.to)
    val toInputPort = toOp.getPortIdxForInputLinkId(physicalLink.id)

    val (matWriterLogicalOp: ProgressiveSinkOpDesc, matWriterPhysicalOp: PhysicalOp) =
      createMatWriter(fromOp, fromOutputPort)

    val matReaderPhysicalOp: PhysicalOp = createMatReader(matWriterLogicalOp)

    // create 2 links for materialization
    val readerToDestLink = PhysicalLink(matReaderPhysicalOp, 0, toOp, toInputPort)
    val sourceToWriterLink = PhysicalLink(fromOp, fromOutputPort, matWriterPhysicalOp, 0)

    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id

    physicalPlan
      .removeEdge(physicalLink)
      .addOperator(matWriterPhysicalOp)
      .addOperator(matReaderPhysicalOp)
      .addEdge(readerToDestLink)
      .addEdge(sourceToWriterLink)
      .setOperatorUnblockPort(toOp.id, toInputPort)
      .populatePartitioningOnLinks()

  }

  private def createMatReader(matWriterLogicalOp: ProgressiveSinkOpDesc): PhysicalOp = {
    val materializationReader = new CacheSourceOpDesc(
      matWriterLogicalOp.operatorIdentifier,
      opResultStorage: OpResultStorage
    )
    materializationReader.setContext(workflowContext)
    materializationReader.schema = matWriterLogicalOp.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOp = materializationReader.getPhysicalOp(
      workflowContext.executionId,
      OperatorSchemaInfo(Array(), matReaderOutputSchema)
    )
    matReaderOp
  }

  private def createMatWriter(
      fromOp: PhysicalOp,
      fromOutputPortIdx: Int
  ): (ProgressiveSinkOpDesc, PhysicalOp) = {
    val matWriterLogicalOp = new ProgressiveSinkOpDesc()
    matWriterLogicalOp.setContext(workflowContext)
    val fromLogicalOp = logicalPlan.getOperator(fromOp.id.logicalOpId)
    val fromOpInputSchema: Array[Schema] =
      if (!fromLogicalOp.isInstanceOf[SourceOperatorDescriptor]) {
        logicalPlan.getOpInputSchemas(fromLogicalOp.operatorIdentifier).map(s => s.get).toArray
      } else {
        Array()
      }
    val matWriterInputSchema = fromLogicalOp.getOutputSchemas(fromOpInputSchema)(fromOutputPortIdx)
    // we currently expect only one output schema
    val matWriterOutputSchema =
      matWriterLogicalOp.getOutputSchemas(Array(matWriterInputSchema)).head
    val matWriterPhysicalOp = matWriterLogicalOp.getPhysicalOp(
      workflowContext.executionId,
      OperatorSchemaInfo(Array(matWriterInputSchema), Array(matWriterOutputSchema))
    )
    matWriterLogicalOp.setStorage(
      opResultStorage.create(
        key = matWriterLogicalOp.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )
    opResultStorage.get(matWriterLogicalOp.operatorIdentifier).setSchema(matWriterOutputSchema)
    (matWriterLogicalOp, matWriterPhysicalOp)
  }
}
