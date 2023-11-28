package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder.replaceVertex
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  LinkIdentity,
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
    val logicalPlan: LogicalPlan,
    var physicalPlan: PhysicalPlan,
    val materializationRewriter: MaterializationRewriter
) {
  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] =
    new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )

  var materializationWriterReaderPairs = new mutable.HashMap[LayerIdentity, LayerIdentity]()

  /**
    * Uses the outLinks and operatorToOpExecConfig to create a DAG similar to the workflow but with all
    * blocking links removed.
    *
    * @return
    */
  private def getBlockingEdgesRemovedDAG(): PhysicalPlan = {
    val edgesToRemove = new mutable.MutableList[LinkIdentity]()

    physicalPlan.allOperatorIds.foreach(opId => {
      val upstreamOps = physicalPlan.getUpstream(opId)
      upstreamOps.foreach(upOpId => {

        physicalPlan.links
          .filter(l => l.from == upOpId && l.to == opId)
          .foreach(link => {
            if (physicalPlan.operatorMap(opId).isInputBlocking(link)) {
              edgesToRemove += link
            }
          })
      })
    })

    val linksAfterRemoval = physicalPlan.links.filter(link => !edgesToRemove.contains(link))
    new PhysicalPlan(physicalPlan.operatorMap.values.toList, linksAfterRemoval)
  }

  /**
    * Adds an edge between the regions of operator `prevInOrderOperator` to the regions of the operator `nextInOrderOperator`.
    * Throws IllegalArgumentException when the addition of an edge causes a cycle.
    */
  @throws(classOf[java.lang.IllegalArgumentException])
  private def addEdgeBetweenRegions(
      prevInOrderOperator: LayerIdentity,
      nextInOrderOperator: LayerIdentity
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
    val dagWithoutBlockingEdges = getBlockingEdgesRemovedDAG()
    val sourceOperators = dagWithoutBlockingEdges.sourceOperators
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 1
    sourceOperators.foreach(sourceOp => {
      val operatorsInRegion =
        dagWithoutBlockingEdges.getDescendants(sourceOp) :+ sourceOp
      val regionId = PipelinedRegionIdentity(workflowId, regionCount.toString)
      pipelinedRegionsDAG.addVertex(PipelinedRegion(regionId, operatorsInRegion.toSet.toArray))
      regionCount += 1
    })

    // add dependencies among regions
    physicalPlan
      .topologicalIterator()
      .foreach(opId => {
        // For operators like HashJoin that have an order among their blocking and pipelined inputs
        val inputProcessingOrderForOp = physicalPlan.operatorMap(opId).getInputProcessingOrder()
        if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
          for (i <- 1 until inputProcessingOrderForOp.length) {
            try {
              addEdgeBetweenRegions(
                inputProcessingOrderForOp(i - 1).from,
                inputProcessingOrderForOp(i).from
              )
            } catch {
              case _: java.lang.IllegalArgumentException =>
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
        val upstreamOps = physicalPlan.getUpstream(opId)

        val allInputBlocking = upstreamOps.nonEmpty && upstreamOps.forall(upstreamOp =>
          findAllLinks(upstreamOp, opId)
            .forall(link => physicalPlan.operatorMap(opId).isInputBlocking(link))
        )
        if (allInputBlocking) {
          upstreamOps.foreach(upstreamOp => {
            findAllLinks(upstreamOp, opId).foreach { link =>
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
            s"PipelinedRegionsBuilder: Cyclic dependency between regions of ${writer.operator} and ${reader.operator}"
          )
      }
    }

    true
  }

  private def findAllLinks(from: LayerIdentity, to: LayerIdentity): List[LinkIdentity] = {
    physicalPlan.links.filter(link => link.from == from && link.to == to)

  }

  private def findAllPipelinedRegionsAndAddDependencies(): Unit = {
    var traversedAllOperators = addMaterializationOperatorIfNeeded()
    while (!traversedAllOperators) {
      traversedAllOperators = addMaterializationOperatorIfNeeded()
    }
  }

  private def getPipelinedRegionsFromOperatorId(opId: LayerIdentity): Set[PipelinedRegion] = {
    val regionsForOperator = new mutable.HashSet[PipelinedRegion]()
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators().contains(opId)) {
          regionsForOperator.add(region)
        }
      )
    regionsForOperator.toSet
  }

  private def populateTerminalOperatorsForBlockingLinks(): Unit = {
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[LayerIdentity]]()
    this.physicalPlan
      .topologicalIterator()
      .foreach(opId => {
        val upstreamOps = this.physicalPlan.getUpstream(opId)
        upstreamOps.foreach(upstreamOp => {
          findAllLinks(upstreamOp, opId).foreach(linkFromUpstreamOp => {
            if (physicalPlan.operatorMap(opId).isInputBlocking(linkFromUpstreamOp)) {
              val prevInOrderRegions = getPipelinedRegionsFromOperatorId(upstreamOp)
              for (prevInOrderRegion <- prevInOrderRegions) {
                if (
                  !regionTerminalOperatorInOtherRegions.contains(
                    prevInOrderRegion
                  ) || !regionTerminalOperatorInOtherRegions(prevInOrderRegion).contains(opId)
                ) {
                  val terminalOps = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
                    prevInOrderRegion,
                    new ArrayBuffer[LayerIdentity]()
                  )
                  terminalOps.append(opId)
                  regionTerminalOperatorInOtherRegions(prevInOrderRegion) = terminalOps
                }
              }
            }
          })

        })
      })

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      val newRegion = region.copy(blockingDownstreamOperatorsInOtherRegions =
        terminalOps.toArray.map(opId => (opId, 0))
      )
      replaceVertex(pipelinedRegionsDAG, region, newRegion)
    }
  }

  def buildPipelinedRegions(): PhysicalPlan = {
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
    this.physicalPlan.copy(regionsToSchedule = allRegions, regionAncestorMapping = ancestors)
  }
}
