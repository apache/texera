package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.controller.ControllerConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.scheduling.ExpansionGreedyRegionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.common.{AmberConfig, VirtualIdentityUtils}
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.PhysicalOpIdentity
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph

import scala.annotation.tailrec
import scala.collection.convert.ImplicitConversions.{
  `collection AsScalaIterable`,
  `iterable AsScalaIterable`
}
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
    opResultStorage: OpResultStorage,
    controllerConfig: ControllerConfig
) extends RegionPlanGenerator(
      logicalPlan,
      physicalPlan,
      opResultStorage
    )
    with LazyLogging {

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
    nonBlockingDAG.getSourceOperatorIds.zipWithIndex
      .map {
        case (sourcePhysicalOpId, index) =>
          val operatorIds =
            nonBlockingDAG.getDescendantPhysicalOpIds(sourcePhysicalOpId) ++ Set(sourcePhysicalOpId)
          val linkIds = operatorIds.flatMap(operatorId => {
            physicalPlan.getUpstreamPhysicalLinkIds(operatorId) ++ physicalPlan
              .getDownstreamPhysicalLinkIds(operatorId)
          })
          Region(RegionIdentity((index + 1).toString), operatorIds, linkIds)
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
          val regionLinks = createLinks(prevLink.fromOp.id, nextLink.fromOp.id, regionDAG)
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
            physicalPlan = replaceLinkWithMaterialization(link, context, matReaderWriterPairs)
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
      case _: java.lang.IllegalArgumentException =>
        // a cycle is detected. it should not reach here.
        throw new WorkflowRuntimeException(
          "Cyclic dependency between regions detected"
        )
    }

    // mark source operators in each region
    populateSourceOperators(regionDAG)

    // mark links that go to downstream regions
    populateDownstreamLinks(regionDAG)

    // generate the region configs
    populateRegionConfigs(regionDAG)
  }

  private def populateSourceOperators(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): DirectedAcyclicGraph[Region, RegionLink] = {
    regionDAG
      .vertexSet()
      .toList
      .foreach(region => {
        val sourceOpIds = region.physicalOpIds
          .filter(physicalOpId =>
            physicalPlan
              .getUpstreamPhysicalOpIds(physicalOpId)
              .forall(upstreamOpId => !region.physicalOpIds.contains(upstreamOpId))
          )
        val newRegion = region.copy(sourcePhysicalOpIds = sourceOpIds)
        replaceVertex(regionDAG, region, newRegion)
      })
    regionDAG
  }

  private def getRegions(
      physicalOpId: PhysicalOpIdentity,
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): Set[Region] = {
    regionDAG.vertexSet().filter(region => region.physicalOpIds.contains(physicalOpId)).toSet
  }

  private def populateDownstreamLinks(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): DirectedAcyclicGraph[Region, RegionLink] = {

    val blockingLinkIds = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))
            .map(_.id)
        }
      }
      .toSet

    blockingLinkIds
      .flatMap { linkId => getRegions(linkId.from, regionDAG).map(region => region -> linkId) }
      .groupBy(_._1)
      .mapValues(_.map(_._2))
      .foreach {
        case (region, links) =>
          val newRegion = region.copy(downstreamLinkIds = links.toSet)
          replaceVertex(regionDAG, region, newRegion)
      }
    regionDAG
  }

  private def populateRegionConfigs(
      regionDAG: DirectedAcyclicGraph[Region, RegionLink]
  ): DirectedAcyclicGraph[Region, RegionLink] = {
    regionDAG
      .vertexSet()
      .toList
      .foreach(region => {
        val config = RegionConfig(
          region.getEffectiveOperators
            .map(physicalOpId => physicalPlan.getOperator(physicalOpId))
            .map { physicalOp =>
              {
                val workerCount =
                  if (physicalOp.suggestedWorkerNum.isDefined) {
                    physicalOp.suggestedWorkerNum.get
                  } else if (physicalOp.parallelizable) {
                    AmberConfig.numWorkerPerOperatorByDefault
                  } else {
                    1
                  }

                physicalOp.id -> (0 until workerCount)
                  .map(idx => {
                    val workerId = VirtualIdentityUtils
                      .createWorkerIdentity(physicalOp.executionId, physicalOp.id, idx)
                    WorkerConfig(
                      controllerConfig.workerRestoreConfMapping(workerId),
                      controllerConfig.workerLoggingConfMapping(workerId)
                    )
                  })
                  .toList
              }
            }
            .toMap
        )
        val newRegion = region.copy(config = Some(config))
        replaceVertex(regionDAG, region, newRegion)
      })
    regionDAG
  }

  def generate(context: WorkflowContext): (RegionPlan, PhysicalPlan) = {

    val regionDAG = createRegionDAG(context)

    regionDAG.toList.foreach(region =>
      region.config.get.workerConfigs.foreach {
        case (physicalOpId, workerConfigs) =>
          physicalPlan.getOperator(physicalOpId).assignWorkers(workerConfigs.length)
      }
    )
    physicalPlan = physicalPlan.populatePartitioningOnLinks()

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
    val fromOp = physicalPlan.getOperator(physicalLink.id.from)
    val fromOutputPort = fromOp.getPortIdxForOutputLinkId(physicalLink.id)

    // get the actual Op from the physical plan. the operators on the link and that on the physical plan
    // are different due to partial rewrite
    val toOp = physicalPlan.getOperator(physicalLink.id.to)
    val toInputPort = toOp.getPortIdxForInputLinkId(physicalLink.id)

    val (matWriterLogicalOp: ProgressiveSinkOpDesc, matWriterPhysicalOp: PhysicalOp) =
      createMatWriter(fromOp, fromOutputPort, context)

    val matReaderPhysicalOp: PhysicalOp = createMatReader(matWriterLogicalOp, context)

    // create 2 links for materialization
    val readerToDestLink = PhysicalLink(matReaderPhysicalOp, 0, toOp, toInputPort)
    val sourceToWriterLink = PhysicalLink(fromOp, fromOutputPort, matWriterPhysicalOp, 0)

    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id

    physicalPlan
      .removeLink(physicalLink)
      .addOperator(matWriterPhysicalOp)
      .addOperator(matReaderPhysicalOp)
      .addLink(readerToDestLink)
      .addLink(sourceToWriterLink)
      .setOperatorUnblockPort(toOp.id, toInputPort)
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
    materializationReader.setOperatorId("cacheSource-" + matWriterLogicalOp.operatorIdentifier.id)
    materializationReader.schema = matWriterLogicalOp.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOp = materializationReader.getPhysicalOp(
      context.executionId,
      OperatorSchemaInfo(Array(), matReaderOutputSchema)
    )
    matReaderOp
  }

  private def createMatWriter(
      fromOp: PhysicalOp,
      fromOutputPortIdx: Int,
      context: WorkflowContext
  ): (ProgressiveSinkOpDesc, PhysicalOp) = {
    val matWriterLogicalOp = new ProgressiveSinkOpDesc()
    matWriterLogicalOp.setContext(context)
    matWriterLogicalOp.setOperatorId("materialized-" + fromOp.id.logicalOpId.id)
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
      context.executionId,
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
