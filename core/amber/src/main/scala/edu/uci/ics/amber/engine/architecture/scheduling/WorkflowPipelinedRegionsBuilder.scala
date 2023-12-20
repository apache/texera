package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder.replaceVertex
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{
  PhysicalLinkIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.texera.workflow.common.workflow.{
  LogicalPlan,
  MaterializationRewriter,
  PhysicalPlan
}
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.{asScalaIteratorConverter, asScalaSetConverter}

object WorkflowPipelinedRegionsBuilder {

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

class WorkflowPipelinedRegionsBuilder(
    val workflowId: WorkflowIdentity,
    var logicalPlan: LogicalPlan,
    var physicalPlan: PhysicalPlan,
    val materializationRewriter: MaterializationRewriter
) extends LazyLogging {
  private var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] =
    new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )

  private val materializationWriterReaderPairs =
    new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()

  /**
    * create a DAG similar to the physical DAG but with all blocking links removed.
    *
    * @return
    */
  private def getBlockingEdgesRemovedDAG: PhysicalPlan = {
    val edgesToRemove = new mutable.MutableList[PhysicalLinkIdentity]()

    physicalPlan.operators
      .map(physicalOp => physicalOp.id)
      .foreach(physicalOpId => {
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.foreach(upstreamPhysicalOpId => {
          physicalPlan.links
            .filter(l => l.fromOp.id == upstreamPhysicalOpId && l.toOp.id == physicalOpId)
            .foreach(link => {
              if (physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link)) {
                edgesToRemove += link.id
              }
            })
        })
      })

    val linksAfterRemoval = physicalPlan.links.filter(link => !edgesToRemove.contains(link.id))

    new PhysicalPlan(physicalPlan.operators, linksAfterRemoval)
  }

  /**
    * Adds an edge between the regions of operator `prevInOrderOperator` to the regions of the operator `nextInOrderOperator`.
    * Throws IllegalArgumentException when the addition of an edge causes a cycle.
    */
  @throws(classOf[java.lang.IllegalArgumentException])
  private def addEdgeBetweenRegions(
      prevInOrderOperator: PhysicalOpIdentity,
      nextInOrderOperator: PhysicalOpIdentity
  ): Unit = {
    val prevInOrderRegions = getPipelinedRegionsFromOperatorId(prevInOrderOperator)
    val nextInOrderRegions = getPipelinedRegionsFromOperatorId(nextInOrderOperator)
    for (prevInOrderRegion <- prevInOrderRegions) {
      for (nextInOrderRegion <- nextInOrderRegions) {
        if (!pipelinedRegionsDAG.getDescendants(prevInOrderRegion).contains(nextInOrderRegion)) {
          pipelinedRegionsDAG.addEdge(prevInOrderRegion, nextInOrderRegion)
        }
      }
    }
  }

  /**
    * Returns a new DAG with materialization writer and reader operators added, if needed. These operators
    * are added to force dependent input links of an operator to come from different regions.
    */
  private def addMaterializationOperatorIfNeeded(): Boolean = {
    // create regions
    val dagWithoutBlockingEdges = getBlockingEdgesRemovedDAG
    val sourcePhysicalOpIds = dagWithoutBlockingEdges.getSourceOperatorIds
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 1
    sourcePhysicalOpIds.foreach(sourcePhysicalOpId => {
      val operatorsInRegion =
        dagWithoutBlockingEdges.getDescendantPhysicalOpIds(sourcePhysicalOpId) :+ sourcePhysicalOpId
      val regionId = PipelinedRegionIdentity(workflowId, regionCount.toString)
      pipelinedRegionsDAG.addVertex(PipelinedRegion(regionId, operatorsInRegion.toSet.toArray))
      regionCount += 1
    })

    // add dependencies among regions
    physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        // For operators like HashJoin that have an order among their blocking and pipelined inputs
        val inputProcessingOrderForOp =
          physicalPlan.getOperator(physicalOpId).getInputLinksInProcessingOrder
        if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
          for (i <- 1 until inputProcessingOrderForOp.length) {
            try {
              addEdgeBetweenRegions(
                inputProcessingOrderForOp(i - 1).fromOp.id,
                inputProcessingOrderForOp(i).fromOp.id
              )
            } catch {
              case _: java.lang.IllegalArgumentException =>
                logger.info(
                  "trying to add materialziations, current pairs" + materializationWriterReaderPairs.size
                )
                // edge causes a cycle
                this.physicalPlan = materializationRewriter
                  .addMaterializationToLink(
                    physicalPlan,
                    logicalPlan,
                    inputProcessingOrderForOp(i),
                    materializationWriterReaderPairs
                  )
                return false
            }
          }
        }

        // For operators that have only blocking input links. add materialization to all input links.
        val upstreamPhysicalOpIds = physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)

        val allInputBlocking =
          upstreamPhysicalOpIds.nonEmpty && upstreamPhysicalOpIds.forall(upstreamPhysicalOpId =>
            physicalPlan
              .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
              .forall(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))
          )
        if (allInputBlocking) {
          upstreamPhysicalOpIds.foreach(upstreamPhysicalOpId => {
            physicalPlan.getLinksBetween(upstreamPhysicalOpId, physicalOpId).foreach { link =>
              this.physicalPlan = materializationRewriter
                .addMaterializationToLink(
                  physicalPlan,
                  logicalPlan,
                  link,
                  materializationWriterReaderPairs
                )
            }
          })
          return false
        }
      })

    // add dependencies between materialization writer and reader regions
    for ((writer, reader) <- materializationWriterReaderPairs) {
      try {
        addEdgeBetweenRegions(writer, reader)
      } catch {
        case _: java.lang.IllegalArgumentException =>
          // edge causes a cycle. Code shouldn't reach here.
          throw new WorkflowRuntimeException(
            s"PipelinedRegionsBuilder: Cyclic dependency between regions of ${writer.logicalOpId.id} and ${reader.logicalOpId.id}"
          )
      }
    }

    true
  }

  private def findAllPipelinedRegionsAndAddDependencies(): Unit = {
    var traversedAllOperators = addMaterializationOperatorIfNeeded()
    while (!traversedAllOperators) {
      traversedAllOperators = addMaterializationOperatorIfNeeded()
    }
  }

  private def getPipelinedRegionsFromOperatorId(opId: PhysicalOpIdentity): Set[PipelinedRegion] = {
    val regionsForOperator = new mutable.HashSet[PipelinedRegion]()
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators.contains(opId)) {
          regionsForOperator.add(region)
        }
      )
    regionsForOperator.toSet
  }

  private def populateTerminalOperatorsForBlockingLinks(): Unit = {
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[PhysicalOpIdentity]]()
    this.physicalPlan
      .topologicalIterator()
      .foreach(physicalOpId => {
        val upstreamPhysicalOpIds = this.physicalPlan.getUpstreamPhysicalOpIds(physicalOpId)
        upstreamPhysicalOpIds.foreach(upstreamPhysicalOpId => {
          physicalPlan
            .getLinksBetween(upstreamPhysicalOpId, physicalOpId)
            .foreach(upstreamPhysicalLink => {
              if (
                physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(upstreamPhysicalLink)
              ) {
                val prevInOrderRegions = getPipelinedRegionsFromOperatorId(upstreamPhysicalOpId)
                for (prevInOrderRegion <- prevInOrderRegions) {
                  if (
                    !regionTerminalOperatorInOtherRegions.contains(
                      prevInOrderRegion
                    ) || !regionTerminalOperatorInOtherRegions(prevInOrderRegion)
                      .contains(physicalOpId)
                  ) {
                    val terminalOps = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
                      prevInOrderRegion,
                      new ArrayBuffer[PhysicalOpIdentity]()
                    )
                    terminalOps.append(physicalOpId)
                    regionTerminalOperatorInOtherRegions(prevInOrderRegion) = terminalOps
                  }
                }
              }
            })

        })
      })

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      val newRegion = region.copy(blockingDownstreamPhysicalOpIdsInOtherRegions =
        terminalOps.toArray.map(opId => (opId, 0))
      )
      replaceVertex(pipelinedRegionsDAG, region, newRegion)
    }
  }

  def buildPipelinedRegions(): ExecutionPlan = {
    findAllPipelinedRegionsAndAddDependencies()
    populateTerminalOperatorsForBlockingLinks()
    val allRegions = pipelinedRegionsDAG.iterator().asScala.toList
    val ancestors = pipelinedRegionsDAG
      .iterator()
      .asScala
      .map { region =>
        region -> pipelinedRegionsDAG.getAncestors(region).asScala.toSet
      }
      .toMap
    new ExecutionPlan(regionsToSchedule = allRegions, regionAncestorMapping = ancestors)
  }
}
