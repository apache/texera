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
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.Graph
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

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
  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = null

  private def inLinks(): Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(
      outLinks.map({ case (operatorId, links) => operatorId -> links.toSet }).toMap
    )

  private def getAllOperatorIds: Iterable[OperatorIdentity] = operatorToOpExecConfig.keys

  private def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    inLinks.getOrElse(opID, Set())

  /**
    * Uses the outLinks and operatorToOpExecConfig to create a DAG similar to the workflow but with all
    * blocking links removed.
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

  private def getComponentFromOperatorId(
      operatorId: OperatorIdentity,
      components: java.util.Set[Graph[OperatorIdentity, DefaultEdge]]
  ): Graph[OperatorIdentity, DefaultEdge] = {
    components.foreach(component => {
      if (component.containsVertex(operatorId)) {
        return component
      }
    })
    null
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

  /**
    * Returns a new DAG with materialization writer and reader operators added, if needed. These operators
    * are added to force dependent ipnut links of an operator to come from different regions.
    */
  private def addMaterializationOperatorIfNeeded(
      initialDAG: DirectedAcyclicGraph[OperatorIdentity, DefaultEdge]
  ): DirectedAcyclicGraph[OperatorIdentity, DefaultEdge] = {
    val biconnectivityInspector =
      new BiconnectivityInspector[OperatorIdentity, DefaultEdge](initialDAG)
    val connectedComponents = biconnectivityInspector.getConnectedComponents()

    val linksToAddMaterialization = new mutable.HashSet[LinkIdentity]()
    getAllOperatorIds.foreach(opId => {
      val inputProcessingOrderForOp = operatorToOpExecConfig(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          if (
            getComponentFromOperatorId(
              toOperatorIdentity(inputProcessingOrderForOp(i - 1).from),
              connectedComponents
            )
              .containsVertex(toOperatorIdentity(inputProcessingOrderForOp(i).from))
          ) {
            linksToAddMaterialization.add(inputProcessingOrderForOp(i))
          }
        }
      }
    })

    // Add materialization operators to links which were in same region as the links they depend on
    if (linksToAddMaterialization.nonEmpty) {
      linksToAddMaterialization.foreach(linkId => {
        val fromOpId = toOperatorIdentity(linkId.from)
        val toOpId = toOperatorIdentity(linkId.to)
        val materializationWriter = new ProgressiveSinkOpDesc()
        materializationWriter.setContext(workflowContext)
        val matWriterInputSchemas =
          inputSchemaMap(operatorIdToDesc(fromOpId.operator)).map(s => s.get).toArray
        val matWriterOutputSchemas = materializationWriter.getOutputSchemas(matWriterInputSchemas)
        materializationWriter.setStorage(
          opResultStorage.create(materializationWriter.operatorID, matWriterOutputSchemas(0))
        )
        val matWriterOpExecConfig =
          materializationWriter.operatorExecutor(
            OperatorSchemaInfo(matWriterInputSchemas, matWriterOutputSchemas)
          )
        operatorToOpExecConfig.put(matWriterOpExecConfig.id, matWriterOpExecConfig)

        val materializationReader = new CacheSourceOpDesc(
          materializationWriter.operatorID,
          opResultStorage: OpResultStorage
        )
        materializationReader.setContext(workflowContext)
        materializationReader.schema = materializationWriter.getStorage.getSchema
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

        // update port linkage
        updatePortLinking(fromOpId, toOpId, matWriterOpExecConfig.id, matReaderOpExecConfig.id)
      })
    }
    getBlockingEdgesRemovedDAG()
  }

  private def findAllPipelinedRegions(): Unit = {
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 0
    val modifiedDAG = addMaterializationOperatorIfNeeded(getBlockingEdgesRemovedDAG())
    val biconnectivityInspector =
      new BiconnectivityInspector[OperatorIdentity, DefaultEdge](modifiedDAG)
    biconnectivityInspector
      .getConnectedComponents()
      .forEach(component => {
        val regionId = PipelinedRegionIdentity(workflowId, regionCount.toString())
        val operatorArray = new ArrayBuffer[OperatorIdentity]()
        component.vertexSet().forEach(opId => operatorArray.append(opId))
        pipelinedRegionsDAG.addVertex(new PipelinedRegion(regionId, operatorArray.toArray))
        regionCount += 1
      })
  }

  private def getPipelinedRegionFromOperatorId(operatorId: OperatorIdentity): PipelinedRegion = {
    pipelinedRegionsDAG
      .vertexSet()
      .forEach(region =>
        if (region.getOperators().contains(operatorId)) {
          return region
        }
      )
    null
  }

  private def findDependenciesBetweenRegions(): Unit = {
    val regionTerminalOperatorInOtherRegions =
      new mutable.HashMap[PipelinedRegion, ArrayBuffer[OperatorIdentity]]()
    val allOperatorIds = getAllOperatorIds
    allOperatorIds.foreach(opId => {
      // 1. Find dependencies between pipelined regions enforced by inputs of operators.
      // e.g 2 phase hash join requires build input to come first.
      val inputProcessingOrderForOp = operatorToOpExecConfig(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          val prevInOrder = getPipelinedRegionFromOperatorId(
            toOperatorIdentity(inputProcessingOrderForOp(i - 1).from)
          )
          val nextInOrder =
            getPipelinedRegionFromOperatorId(toOperatorIdentity(inputProcessingOrderForOp(i).from))

          if (prevInOrder.getId() == nextInOrder.getId()) {
            throw new WorkflowRuntimeException(
              "PipelinedRegionsBuilder: Inputs having a precedence order are in the same region"
            )
          } else {
            if (!pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)) {
              pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
            }
          }
        }
      }

      // 2. Find dependencies due to blocking operators.
      // e.g. The region before and after a sort operator has dependencies
      val upstreamOps = getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upstreamOp => {
        val linkFromUpstreamOp = LinkIdentity(
          operatorToOpExecConfig(upstreamOp).topology.layers.last.id,
          operatorToOpExecConfig(opId).topology.layers.head.id
        )
        if (operatorToOpExecConfig(opId).isInputBlocking(linkFromUpstreamOp)) {
          val prevInOrder = getPipelinedRegionFromOperatorId(upstreamOp)
          val nextInOrder = getPipelinedRegionFromOperatorId(opId)
          if (prevInOrder.getId() != nextInOrder.getId()) {
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
                new ArrayBuffer[OperatorIdentity]()
              )
              terminalOps.append(opId)
              regionTerminalOperatorInOtherRegions(prevInOrder) = terminalOps
            }

          } else {
            throw new WorkflowRuntimeException(
              "PipelinedRegionsBuilder: Operators separated by blocking link are in the same region"
            )
          }
        }
      })
    })

    for ((region, terminalOps) <- regionTerminalOperatorInOtherRegions) {
      region.blockingDowstreamOperatorsInOtherRegions = terminalOps.toArray
    }
  }

  def buildPipelinedRegions(): DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = {
    findAllPipelinedRegions()
    findDependenciesBetweenRegions()
    pipelinedRegionsDAG
  }
}
