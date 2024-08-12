package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.scheduling.RegionPlanGenerator.replaceVertex
import edu.uci.ics.amber.engine.architecture.scheduling.resourcePolicies.{
  DefaultResourceAllocator,
  ExecutionClusterInfo
}
import edu.uci.ics.amber.engine.common.virtualidentity.{OperatorIdentity, PhysicalOpIdentity}
import edu.uci.ics.amber.engine.common.workflow.PhysicalLink
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.graph.DirectedAcyclicGraph
import org.jgrapht.traverse.TopologicalOrderIterator

import scala.collection.mutable
import scala.jdk.CollectionConverters.{CollectionHasAsScala, IteratorHasAsScala}


import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import edu.uci.ics.texera.web.SqlServer
import edu.uci.ics.texera.web.model.jooq.generated.tables.daos.WorkflowDao
import org.jooq.types.UInteger
import play.api.libs.json._

object RegionPlanGenerator {
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

abstract class RegionPlanGenerator(
                                    workflowContext: WorkflowContext,
                                    var physicalPlan: PhysicalPlan,
                                    opResultStorage: OpResultStorage
                                  ) {
  private val executionClusterInfo = new ExecutionClusterInfo()

  final private lazy val context = SqlServer.createDSLContext()
  final private lazy val workflowDao = new WorkflowDao(context.configuration)

  def generate(): (RegionPlan, PhysicalPlan)

  def allocateResource(
                        regionDAG: DirectedAcyclicGraph[Region, RegionLink]
                      ): Unit = {
    //在database里找最新的content，里的batch size
    val workflowId = UInteger.valueOf(workflowContext.workflowId.id)
    val batchSize = getLatestBatchSize(workflowId)
    println("+++++++")
    println(batchSize)

    val resourceAllocator = new DefaultResourceAllocator(physicalPlan, executionClusterInfo, batchSize)
    // generate the resource configs
    new TopologicalOrderIterator(regionDAG).asScala
      .foreach(region => {
        val (newRegion, _) = resourceAllocator.allocate(region)
        replaceVertex(regionDAG, region, newRegion)
      })
  }

  def getLatestBatchSize(workflowId: UInteger): Int = {
    val workflowRecord = workflowDao.fetchOneByWid(workflowId)
    if (workflowRecord != null) {
      val content: String = workflowRecord.getContent
      val json = Json.parse(content)
      (json \ "batchSize").validate[Int] match {
        case JsSuccess(batchSize, _) => batchSize
        case JsError(_) => 400 // 返回默认值400
      }
    } else {
      400 // 如果没有找到对应的workflowId，也返回默认值400
    }
  }

  def getRegions(
                  physicalOpId: PhysicalOpIdentity,
                  regionDAG: DirectedAcyclicGraph[Region, RegionLink]
                ): Set[Region] = {
    regionDAG
      .vertexSet()
      .asScala
      .filter(region => region.getOperators.map(_.id).contains(physicalOpId))
      .toSet
  }

  /**
   * For a dependee input link, although it connects two regions A->B, we include this link and its toOp in region A
   *  so that the dependee link will be completed first.
   */
  def populateDependeeLinks(
                             regionDAG: DirectedAcyclicGraph[Region, RegionLink]
                           ): Unit = {

    val dependeeLinks = physicalPlan
      .topologicalIterator()
      .flatMap { physicalOpId =>
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.flatMap { upstreamPhysicalOpId =>
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .filter(link =>
              !physicalPlan.getOperator(physicalOpId).isSinkOperator && (physicalPlan
                .getOperator(physicalOpId)
                .isInputLinkDependee(link))
            )
        }
      }
      .toSet

    dependeeLinks
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
  }

  def replaceLinkWithMaterialization(
                                      physicalLink: PhysicalLink,
                                      writerReaderPairs: mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]
                                    ): PhysicalPlan = {

    val fromOp = physicalPlan.getOperator(physicalLink.fromOpId)
    val fromPortId = physicalLink.fromPortId

    val toOp = physicalPlan.getOperator(physicalLink.toOpId)
    val toPortId = physicalLink.toPortId

    var newPhysicalPlan = physicalPlan
      .removeLink(physicalLink)

    // create cache writer and link
    val matWriterPhysicalOp: PhysicalOp =
      createMatWriter(physicalLink)
    val sourceToWriterLink =
      PhysicalLink(
        fromOp.id,
        fromPortId,
        matWriterPhysicalOp.id,
        matWriterPhysicalOp.inputPorts.keys.head
      )
    newPhysicalPlan = newPhysicalPlan
      .addOperator(matWriterPhysicalOp)
      .addLink(sourceToWriterLink)

    // create cache reader and link
    val matReaderPhysicalOp: PhysicalOp =
      createMatReader(matWriterPhysicalOp.id.logicalOpId, physicalLink)
    val readerToDestLink =
      PhysicalLink(
        matReaderPhysicalOp.id,
        matReaderPhysicalOp.outputPorts.keys.head,
        toOp.id,
        toPortId
      )
    // add the pair to the map for later adding edges between 2 regions.
    writerReaderPairs(matWriterPhysicalOp.id) = matReaderPhysicalOp.id
    newPhysicalPlan
      .addOperator(matReaderPhysicalOp)
      .addLink(readerToDestLink)
  }

  def createMatReader(
                       matWriterLogicalOpId: OperatorIdentity,
                       physicalLink: PhysicalLink
                     ): PhysicalOp = {
    val matReader = new CacheSourceOpDesc(
      matWriterLogicalOpId,
      opResultStorage: OpResultStorage
    )
    matReader.setContext(workflowContext)
    matReader.setOperatorId(s"cacheSource_${getMatIdFromPhysicalLink(physicalLink)}")

    matReader
      .getPhysicalOp(
        workflowContext.workflowId,
        workflowContext.executionId
      )
      .propagateSchema()

  }

  def createMatWriter(
                       physicalLink: PhysicalLink
                     ): PhysicalOp = {
    val matWriter = new ProgressiveSinkOpDesc()
    matWriter.setContext(workflowContext)
    matWriter.setOperatorId(s"materialized_${getMatIdFromPhysicalLink(physicalLink)}")

    // expect exactly one input port and one output port

    matWriter.setStorage(
      opResultStorage.create(
        key = matWriter.operatorIdentifier,
        mode = OpResultStorage.defaultStorageMode
      )
    )

    matWriter.getPhysicalOp(
      workflowContext.workflowId,
      workflowContext.executionId
    )

  }

  private def getMatIdFromPhysicalLink(physicalLink: PhysicalLink) =
    s"${physicalLink.fromOpId.logicalOpId}_${physicalLink.fromOpId.layerName}_" +
      s"${physicalLink.fromPortId.id}_" +
      s"${physicalLink.toOpId.logicalOpId}_${physicalLink.toOpId.layerName}_" +
      s"${physicalLink.toPortId.id}"

}
