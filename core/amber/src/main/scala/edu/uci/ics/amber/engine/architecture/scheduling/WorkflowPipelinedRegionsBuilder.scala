package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{
  LinkIdentity,
  OperatorIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.Graph
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}
import org.jgrapht.traverse.TopologicalOrderIterator

import java.util
import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowPipelinedRegionsBuilder(
                                       workflowContext: WorkflowContext,
                                       operatorIdToDesc: Map[String, OperatorDescriptor],
                                       inputSchemaMap: Map[OperatorDescriptor, List[Option[Schema]]],
                                       workflowId: WorkflowIdentity,
                                       operatorToOpExecConfig: mutable.Map[OperatorIdentity, OpExecConfig],
                                       outLinks: mutable.Map[OperatorIdentity, mutable.Set[OperatorIdentity]],
                                       opResultStorage: OpResultStorage
                                     ) {
  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] =
    new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )

  var materializationWriterReaderPairs = new mutable.HashMap[OperatorIdentity, OperatorIdentity]()

  private def inLinks(): Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(
      outLinks.map({ case (operatorId, links) => operatorId -> links.toSet }).toMap
    )

  private def getAllOperatorIds: Iterable[OperatorIdentity] = operatorToOpExecConfig.keys

  private def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    inLinks.getOrElse(opID, Set())

  private def getTopologicallyOrderedOperators(): Array[OperatorIdentity] = {
    val dag = new DirectedAcyclicGraph[OperatorIdentity, DefaultEdge](classOf[DefaultEdge])
    getAllOperatorIds.foreach(opId => dag.addVertex(opId))
    outLinks.foreach(entry => {
      entry._2.foreach(toOpId => dag.addEdge(entry._1, toOpId))
    })
    val regionsScheduleOrderIterator =
      new TopologicalOrderIterator[OperatorIdentity, DefaultEdge](dag)

    val orderedArray = new ArrayBuffer[OperatorIdentity]()
    while (regionsScheduleOrderIterator.hasNext()) {
      orderedArray.append(regionsScheduleOrderIterator.next())
    }
    orderedArray.toArray
  }

  /**
    * Uses the outLinks and operatorToOpExecConfig to create a DAG similar to the workflow but with all
    * blocking links removed.
    *
    * @return
    */
  private def getBlockingEdgesRemovedDAG(): DirectedAcyclicGraph[OperatorIdentity, DefaultEdge] = {
    val dag = new DirectedAcyclicGraph[OperatorIdentity, DefaultEdge](classOf[DefaultEdge])
    getAllOperatorIds.foreach(opId => dag.addVertex(opId))
    getAllOperatorIds.foreach(opId => {
      val upstreamOps = getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upOpId => {
        val linkFromUpstreamOp = LinkIdentity(
          operatorToOpExecConfig(upOpId).topology.layers.last.id,
          operatorToOpExecConfig(opId).topology.layers.head.id
        )
        if (!operatorToOpExecConfig(opId).isInputBlocking(linkFromUpstreamOp)) {
          dag.addEdge(upOpId, opId)
        }
      })
    })
    dag
  }

  private def getSourcesOfRegions(
                                   blockingEdgesRemovedDAG: DirectedAcyclicGraph[OperatorIdentity, DefaultEdge]
                                 ): Set[OperatorIdentity] = {
    blockingEdgesRemovedDAG
      .vertexSet()
      .filter(opId => blockingEdgesRemovedDAG.getAncestors(opId).isEmpty())
      .toSet
  }

  private def getRegionOperatorsFromSource(
                                            sourceOperator: OperatorIdentity,
                                            blockingEdgesRemovedDAG: DirectedAcyclicGraph[OperatorIdentity, DefaultEdge]
                                          ): mutable.HashSet[OperatorIdentity] = {
    val visited = new mutable.HashSet[OperatorIdentity]()
    val toVisit = new mutable.Queue[OperatorIdentity]()
    toVisit.enqueue(sourceOperator)
    while (toVisit.nonEmpty) {
      val opToExplore = toVisit.dequeue()
      visited.add(opToExplore)
      val descendentOps = blockingEdgesRemovedDAG.getDescendants(opToExplore)
      descendentOps.foreach(descendentOp =>
        if (!visited.contains(descendentOp)) {
          toVisit.enqueue(descendentOp)
        }
      )
    }
    visited
  }

  /**
    * When a materialization writer and reader have to be inserted between two operators, then the
    * port maps in the OpExecConfig of the operators have to be updated.
    */
  private def updatePortLinking(
                                 originalSrcOpId: OperatorIdentity,
                                 originalDestOpId: OperatorIdentity,
                                 matWriterOpId: OperatorIdentity,
                                 matReaderOpId: OperatorIdentity
                               ): Unit = {
    val originalLink = LinkIdentity(
      operatorToOpExecConfig(originalSrcOpId).topology.layers.last.id,
      operatorToOpExecConfig(originalDestOpId).topology.layers.head.id
    )

    val outputPortMappings =
      operatorToOpExecConfig(originalSrcOpId).outputToOrdinalMapping(originalLink)
    val sourcePortId = outputPortMappings._1
    val sourcePortName = outputPortMappings._2

    val inputPortMappings =
      operatorToOpExecConfig(originalDestOpId).inputToOrdinalMapping(originalLink)
    val destPortId = inputPortMappings._1
    val destPortName = inputPortMappings._2

    operatorToOpExecConfig(originalSrcOpId).outputToOrdinalMapping.remove(originalLink)
    operatorToOpExecConfig(originalDestOpId).inputToOrdinalMapping.remove(originalLink)

    val originalSrcToMatWriterLink = LinkIdentity(
      operatorToOpExecConfig(originalSrcOpId).topology.layers.last.id,
      operatorToOpExecConfig(matWriterOpId).topology.layers.head.id
    )
    operatorToOpExecConfig(originalSrcOpId).setOutputToOrdinalMapping(
      originalSrcToMatWriterLink,
      sourcePortId,
      sourcePortName
    )
    operatorToOpExecConfig(matWriterOpId).setInputToOrdinalMapping(
      originalSrcToMatWriterLink,
      0,
      ""
    )

    val matReaderToOriginalDestLink = LinkIdentity(
      operatorToOpExecConfig(matReaderOpId).topology.layers.last.id,
      operatorToOpExecConfig(originalDestOpId).topology.layers.head.id
    )
    operatorToOpExecConfig(matReaderOpId).setOutputToOrdinalMapping(
      matReaderToOriginalDestLink,
      0,
      ""
    )
    operatorToOpExecConfig(originalDestOpId).setInputToOrdinalMapping(
      matReaderToOriginalDestLink,
      destPortId,
      destPortName
    )
  }

  private def addMaterializationToLink(linkId: LinkIdentity): Unit = {
    val fromOpId = toOperatorIdentity(linkId.from)
    val toOpId = toOperatorIdentity(linkId.to)
    val fromPortIndex = operatorToOpExecConfig(fromOpId).outputToOrdinalMapping(linkId)._1

    val matWriter = new ProgressiveSinkOpDesc()
    matWriter.setContext(workflowContext)
    val inputSchemaMapStr = inputSchemaMap.map(kv => (kv._1.operatorID, kv._2))
    val fromOpIdInputSchema: Array[Schema] =
      if (!operatorIdToDesc(fromOpId.operator).isInstanceOf[SourceOperatorDescriptor])
        inputSchemaMapStr(fromOpId.operator).map(s => s.get).toArray
      else Array()
    val matWriterInputSchema = operatorIdToDesc(fromOpId.operator).getOutputSchemas(
      fromOpIdInputSchema
    )(fromPortIndex)
    val matWriterOutputSchema = matWriter.getOutputSchemas(Array(matWriterInputSchema))(0)
    matWriter.setStorage(
      opResultStorage.create(matWriter.operatorID, matWriterOutputSchema)
    )
    val matWriterOpExecConfig =
      matWriter.operatorExecutor(
        OperatorSchemaInfo(Array(matWriterInputSchema), Array(matWriterOutputSchema))
      )
    operatorToOpExecConfig.put(matWriterOpExecConfig.id, matWriterOpExecConfig)

    val materializationReader = new CacheSourceOpDesc(
      matWriter.operatorID,
      opResultStorage: OpResultStorage
    )
    materializationReader.setContext(workflowContext)
    materializationReader.schema = matWriter.getStorage.getSchema
    val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
    val matReaderOpExecConfig: OpExecConfig =
      materializationReader.operatorExecutor(
        OperatorSchemaInfo(Array(), matReaderOutputSchema)
      )
    operatorToOpExecConfig.put(matReaderOpExecConfig.id, matReaderOpExecConfig)

    // Create new links
    val downstreamOps = outLinks(fromOpId)
    downstreamOps.remove(toOpId)
    downstreamOps.add(matWriterOpExecConfig.id)
    outLinks(fromOpId) = downstreamOps
    outLinks(matReaderOpExecConfig.id) = mutable.Set(toOpId)

    materializationWriterReaderPairs(matWriter.operatorIdentifier) =
      materializationReader.operatorIdentifier

    // update port linkage
    updatePortLinking(fromOpId, toOpId, matWriterOpExecConfig.id, matReaderOpExecConfig.id)
  }

  /**
    * Returns a new DAG with materialization writer and reader operators added, if needed. These operators
    * are added to force dependent ipnut links of an operator to come from different regions.
    */
  private def addMaterializationOperatorIfNeeded(): Boolean = {
    val dagWithoutBlockingEdges = getBlockingEdgesRemovedDAG()
    val sourceOperators = getSourcesOfRegions(dagWithoutBlockingEdges)
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 1
    sourceOperators.foreach(sourceOp => {
      val operatorsInRegion = getRegionOperatorsFromSource(sourceOp, dagWithoutBlockingEdges)
      val regionId = PipelinedRegionIdentity(workflowId, regionCount.toString())
      pipelinedRegionsDAG.addVertex(new PipelinedRegion(regionId, operatorsInRegion.toArray))
      regionCount += 1
    })
    getTopologicallyOrderedOperators().foreach(opId => {
      val inputProcessingOrderForOp = operatorToOpExecConfig(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          val prevInOrderRegions = getPipelinedRegionsFromOperatorId(
            toOperatorIdentity(inputProcessingOrderForOp(i - 1).from)
          )
          val nextInOrderRegions = getPipelinedRegionsFromOperatorId(
            toOperatorIdentity(inputProcessingOrderForOp(i).from)
          )
          for (prevInOrderRegion <- prevInOrderRegions) {
            for (nextInOrderRegion <- nextInOrderRegions) {
              try {
                if (
                  !pipelinedRegionsDAG.getDescendants(prevInOrderRegion).contains(nextInOrderRegion)
                ) {
                  pipelinedRegionsDAG.addEdge(prevInOrderRegion, nextInOrderRegion)
                }
              } catch {
                case e: java.lang.IllegalArgumentException =>
                  // edge causes a cycle
                  addMaterializationToLink(inputProcessingOrderForOp(i))
                  return false
              }
            }
          }
        }
      }
    })

    // add dependencies between materialization writer and reader regions
    for ((writer, reader) <- materializationWriterReaderPairs) {
      val prevInOrderRegions = getPipelinedRegionsFromOperatorId(writer)
      val nextInOrderRegions = getPipelinedRegionsFromOperatorId(reader)
      for (prevInOrderRegion <- prevInOrderRegions) {
        for (nextInOrderRegion <- nextInOrderRegions) {
          try {
            if (
              !pipelinedRegionsDAG.getDescendants(prevInOrderRegion).contains(nextInOrderRegion)
            ) {
              pipelinedRegionsDAG.addEdge(prevInOrderRegion, nextInOrderRegion)
            }
          } catch {
            case e: java.lang.IllegalArgumentException =>
              // edge causes a cycle
              throw new WorkflowRuntimeException(
                s"PipelinedRegionsBuilder: Cyclic dependency between regions of ${
                  writer.operator
                } and ${reader.operator}"
              )
          }
        }
      }
    }

    true
  }

  private def findAllPipelinedRegions(): Unit = {
    var traversedAllOperators = addMaterializationOperatorIfNeeded()
    while (!traversedAllOperators) {
      traversedAllOperators = addMaterializationOperatorIfNeeded()
    }
  }

  private def getPipelinedRegionsFromOperatorId(
                                                 operatorId: OperatorIdentity
                                               ): Set[PipelinedRegion] = {
    val regionsForOperator = new mutable.HashSet[PipelinedRegion]()
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators().contains(operatorId)) {
          regionsForOperator.add(region)
        }
      )
    return regionsForOperator.toSet
  }

  private def addDependenciesBetweenIOOfBlockingOperator(): Unit = {
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[OperatorIdentity]]()
    getTopologicallyOrderedOperators().foreach(opId => {
      // Find dependencies due to blocking operators.
      // e.g. The region before and after a sort operator has dependencies
      val upstreamOps = getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upstreamOp => {
        val linkFromUpstreamOp = LinkIdentity(
          operatorToOpExecConfig(upstreamOp).topology.layers.last.id,
          operatorToOpExecConfig(opId).topology.layers.head.id
        )
        if (operatorToOpExecConfig(opId).isInputBlocking(linkFromUpstreamOp)) {
          val prevInOrderRegions = getPipelinedRegionsFromOperatorId(upstreamOp)
          val nextInOrderRegions = getPipelinedRegionsFromOperatorId(opId)

          for (prevInOrderRegion <- prevInOrderRegions) {
            for (nextInOrderRegion <- nextInOrderRegions) {
              try {
                if (
                  !pipelinedRegionsDAG.getDescendants(prevInOrderRegion).contains(nextInOrderRegion)
                ) {
                  pipelinedRegionsDAG.addEdge(prevInOrderRegion, nextInOrderRegion)
                }
              } catch {
                case e: java.lang.IllegalArgumentException =>
                  // edge causes a cycle
                  throw new WorkflowRuntimeException(
                    s"PipelinedRegionsBuilder: Cyclic dependency between regions of ${
                      upstreamOp
                        .toString()
                    } and ${opId.toString()}"
                  )
              }
            }
            if (
              !regionTerminalOperatorInOtherRegions.contains(
                prevInOrderRegion
              ) || !regionTerminalOperatorInOtherRegions(prevInOrderRegion).contains(opId)
            ) {
              val terminalOps = regionTerminalOperatorInOtherRegions.getOrElseUpdate(
                prevInOrderRegion,
                new ArrayBuffer[OperatorIdentity]()
              )
              terminalOps.append(opId)
              regionTerminalOperatorInOtherRegions(prevInOrderRegion) = terminalOps
            }
          }
        }
      })
    })

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      region.blockingDowstreamOperatorsInOtherRegions = terminalOps.toArray
    }
  }

  private def addDependenciesBetweenMatWriterAndReaderRegions(): Unit = {

  }

  def buildPipelinedRegions(): DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = {
    findAllPipelinedRegions()
    addDependenciesBetweenIOOfBlockingOperator()
    pipelinedRegionsDAG
  }
}
