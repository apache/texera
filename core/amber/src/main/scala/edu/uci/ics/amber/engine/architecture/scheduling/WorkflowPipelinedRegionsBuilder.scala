package edu.uci.ics.amber.engine.architecture.scheduling

import com.typesafe.scalalogging.LazyLogging
import edu.uci.ics.amber.engine.architecture.deploysemantics.{PhysicalLink, PhysicalOp}
import edu.uci.ics.amber.engine.architecture.scheduling.WorkflowPipelinedRegionsBuilder.replaceVertex
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.{PhysicalOpIdentity, WorkflowIdentity}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{LogicalPlan, PhysicalPlan}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
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
    val opResultStorage: OpResultStorage
) extends LazyLogging {
  private var pipelinedRegionsDAG: DirectedAcyclicGraph[Region, DefaultEdge] =
    new DirectedAcyclicGraph[Region, DefaultEdge](
      classOf[DefaultEdge]
    )

  /**
    * create a DAG similar to the physical DAG but with all blocking links removed.
    *
    * @return
    */
  private def removeBlockingEdges(): PhysicalPlan = {
    val edgesToRemove = physicalPlan.operators
      .map(_.id)
      .flatMap { physicalOpId =>
        {
          physicalPlan
            .getUpstreamPhysicalOpIds(physicalOpId)
            .flatMap { upstreamPhysicalOpId =>
              physicalPlan.links
                .filter(l => l.fromOp.id == upstreamPhysicalOpId && l.toOp.id == physicalOpId)
                .filter(link => physicalPlan.getOperator(physicalOpId).isInputLinkBlocking(link))
                .map(_.id)
            }
        }
      }

    new PhysicalPlan(
      physicalPlan.operators,
      physicalPlan.links.filterNot(e => edgesToRemove.contains(e.id))
    )
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

    val matReaderWriterPairs =
      new mutable.HashMap[PhysicalOpIdentity, PhysicalOpIdentity]()
    // create regions
    val nonBlockingDAG = removeBlockingEdges()

    pipelinedRegionsDAG = new DirectedAcyclicGraph[Region, DefaultEdge](
      classOf[DefaultEdge]
    )
    nonBlockingDAG.getSourceOperatorIds.zipWithIndex.map {
      case (sourcePhysicalOpId, index) =>
        val operatorsInRegion =
          nonBlockingDAG.getDescendantPhysicalOpIds(sourcePhysicalOpId) :+ sourcePhysicalOpId
        Region(RegionIdentity(workflowId, (index + 1).toString), operatorsInRegion)
    }.foreach(region=>pipelinedRegionsDAG.addVertex(region))

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
                // edge causes a cycle
                this.physicalPlan = addMaterializationToLink(
                  inputProcessingOrderForOp(i),
                  matReaderWriterPairs
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
              this.physicalPlan = addMaterializationToLink(
                link,
                matReaderWriterPairs
              )
            }
          })
          return false
        }
      })

    // add dependencies between materialization writer and reader regions
    for ((writer, reader) <- matReaderWriterPairs) {
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

  private def getPipelinedRegionsFromOperatorId(opId: PhysicalOpIdentity): Set[Region] = {
    val regionsForOperator = new mutable.HashSet[Region]()
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
      new mutable.HashMap[Region, ArrayBuffer[PhysicalOpIdentity]]()
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

  def addMaterializationToLink(
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
    materializationReader.setContext(logicalPlan.context)
    materializationReader.schema = matWriterLogicalOp.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOp = materializationReader.getPhysicalOp(
      logicalPlan.context.executionId,
      OperatorSchemaInfo(Array(), matReaderOutputSchema)
    )
    matReaderOp
  }

  private def createMatWriter(
      fromOp: PhysicalOp,
      fromOutputPortIdx: Int
  ) = {
    val matWriterLogicalOp = new ProgressiveSinkOpDesc()
    matWriterLogicalOp.setContext(logicalPlan.context)
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
      logicalPlan.context.executionId,
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
