package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.VirtualIdentityUtil.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LayerIdentity,
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan.toOutLinks
import edu.uci.ics.texera.workflow.common.workflow.{
  LogicalPlan,
  MaterializationRewriter,
  PhysicalPlan
}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.Graph
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.JavaConverters._
import java.util
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
        val linkFromUpstreamOp = LinkIdentity(
          physicalPlan.operators(upOpId).id,
          physicalPlan.operators(opId).id
        )
        if (physicalPlan.operators(opId).isInputBlocking(linkFromUpstreamOp)) {
          edgesToRemove += linkFromUpstreamOp
        }
      })
    })

    val linksAfterRemoval = physicalPlan.links.filter(link => !edgesToRemove.contains(link))
    new PhysicalPlan(physicalPlan.operators.values.toList, linksAfterRemoval)
  }

  private def createPipelinedRegionFromComponent(
      component: Graph[LayerIdentity, DefaultEdge],
      regionCount: Int
  ): PipelinedRegion = {
    val regionId = PipelinedRegionIdentity(workflowId, regionCount.toString())
    val operatorArray = new ArrayBuffer[LayerIdentity]()
    component.vertexSet().forEach(opId => operatorArray.append(opId))
    new PipelinedRegion(regionId, operatorArray.toArray)
  }

  /**
    * Returns a new DAG with materialization writer and reader operators added, if needed. These operators
    * are added to force dependent ipnut links of an operator to come from different regions.
    */
  private def addMaterializationOperatorIfNeeded(): Boolean = {
    val connectedComponents = new BiconnectivityInspector[LayerIdentity, DefaultEdge](
      getBlockingEdgesRemovedDAG().dag
    ).getConnectedComponents()
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 1
    connectedComponents.asScala.foreach(component => {
      pipelinedRegionsDAG.addVertex(createPipelinedRegionFromComponent(component, regionCount))
      regionCount += 1
    })
    physicalPlan
      .topologicalIterator()
      .foreach(opId => {
        val inputProcessingOrderForOp = physicalPlan.operators(opId).getInputProcessingOrder()
        if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
          for (i <- 1 to inputProcessingOrderForOp.length - 1) {
            val prevInOrder = getPipelinedRegionFromOperatorId(
              inputProcessingOrderForOp(i - 1).from
            )
            val nextInOrder = getPipelinedRegionFromOperatorId(
              inputProcessingOrderForOp(i).from
            )
            try {
              if (!pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)) {
                pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
              }
            } catch {
              case e: java.lang.IllegalArgumentException =>
                // edge causes a cycle
                this.physicalPlan = materializationRewriter
                  .addMaterializationToLink(physicalPlan, logicalPlan, inputProcessingOrderForOp(i))
                return false
            }
          }
        }
      })

    true
  }

  private def findAllPipelinedRegions(): Unit = {
    var traversedAllOperators = addMaterializationOperatorIfNeeded()
    while (!traversedAllOperators) {
      traversedAllOperators = addMaterializationOperatorIfNeeded()
    }
  }

  private def getPipelinedRegionFromOperatorId(operatorId: LayerIdentity): PipelinedRegion = {
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators().contains(operatorId)) {
          return region
        }
      )
    null
  }

  private def addDependenciesBetweenIOOfBlockingOperator(): Unit = {
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[LayerIdentity]]()
    physicalPlan
      .topologicalIterator()
      .foreach(opId => {
        // Find dependencies due to blocking operators.
        // e.g. The region before and after a sort operator has dependencies
        physicalPlan
          .getUpstream(opId)
          .foreach(upstreamOp => {
            val linkFromUpstreamOp = LinkIdentity(
              physicalPlan.operators(upstreamOp).id,
              physicalPlan.operators(opId).id
            )
            if (physicalPlan.operators(opId).isInputBlocking(linkFromUpstreamOp)) {
              val prevInOrder = getPipelinedRegionFromOperatorId(upstreamOp)
              val nextInOrder = getPipelinedRegionFromOperatorId(opId)

              try {
                if (!pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)) {
                  pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
                }
                if (
                  !regionTerminalOperatorInOtherRegions.contains(
                    prevInOrder
                  ) || !regionTerminalOperatorInOtherRegions(prevInOrder).contains(opId)
                ) {

                  val terminalOps = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
                    prevInOrder,
                    new ArrayBuffer[LayerIdentity]()
                  )
                  terminalOps.append(opId)
                  regionTerminalOperatorInOtherRegions(prevInOrder) = terminalOps
                }
              } catch {
                case e: java.lang.IllegalArgumentException =>
                  // edge causes a cycle
                  throw new WorkflowRuntimeException(
                    s"PipelinedRegionsBuilder: Cyclic dependency between regions of ${upstreamOp
                      .toString()} and ${opId.toString()}"
                  )
              }
            }
          })
      })

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      region.blockingDowstreamOperatorsInOtherRegions = terminalOps.toArray
    }
  }

  def buildPipelinedRegions()
      : (PhysicalPlan, DirectedAcyclicGraph[PipelinedRegion, DefaultEdge]) = {
    findAllPipelinedRegions()
    addDependenciesBetweenIOOfBlockingOperator()
    (this.physicalPlan, pipelinedRegionsDAG)
  }
}
