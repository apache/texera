package edu.uci.ics.amber.engine.architecture.scheduling

import edu.uci.ics.amber.engine.architecture.controller.Workflow
import edu.uci.ics.amber.engine.common.AmberUtils
import edu.uci.ics.amber.engine.common.amberexception.WorkflowRuntimeException
import edu.uci.ics.amber.engine.common.virtualidentity.util.toOperatorIdentity
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.WorkflowInfo.WorkflowDAG
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import org.jgrapht.alg.connectivity.BiconnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DirectedAcyclicGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class WorkflowPipelinedRegionsBuilder(
                                       operatorIdToDesc: Map[String, OperatorDescriptor],
                                       inputSchemaMap: Map[OperatorDescriptor, List[Option[Schema]]],
                                       workflowId: WorkflowIdentity,
                                       operatorToOpExecConfig: mutable.Map[OperatorIdentity, OpExecConfig],
                                       outLinks: Map[OperatorIdentity, Set[OperatorIdentity]],
                                       opResultStorage: OpResultStorage
                                     ) {
  var pipelinedRegionsDAG: DirectedAcyclicGraph[PipelinedRegion, DefaultEdge] = null

  private val inLinks: Map[OperatorIdentity, Set[OperatorIdentity]] =
    AmberUtils.reverseMultimap(outLinks)

  private def getAllOperatorIds: Iterable[OperatorIdentity] = operatorToOpExecConfig.keys

  private def getOperator(opID: OperatorIdentity): OpExecConfig = operatorToOpExecConfig(opID)

  private def getDirectUpstreamOperators(opID: OperatorIdentity): Iterable[OperatorIdentity] =
    inLinks.getOrElse(opID, Set())

  private def getBlockingEdgesRemovedDAG(): DirectedAcyclicGraph[OperatorIdentity, DefaultEdge] = {
    val dag = new DirectedAcyclicGraph[OperatorIdentity, DefaultEdge](classOf[DefaultEdge])
    getAllOperatorIds.foreach(opId => dag.addVertex(opId))
    getAllOperatorIds.foreach(opId => {
      val upstreamOps = getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upOpId => {
        val linkFromUpstreamOp = LinkIdentity(
          getOperator(upOpId).topology.layers.last.id,
          getOperator(opId).topology.layers.head.id
        )
        if (!getOperator(opId).isInputBlocking(linkFromUpstreamOp)) {
          dag.addEdge(upOpId, opId)
        }
      })
    })
    dag
  }

  private def findAllPipelinedRegions(): Unit = {
    pipelinedRegionsDAG = new DirectedAcyclicGraph[PipelinedRegion, DefaultEdge](
      classOf[DefaultEdge]
    )
    var regionCount = 0
    val biconnectivityInspector =
      new BiconnectivityInspector[OperatorIdentity, DefaultEdge](getBlockingEdgesRemovedDAG())
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
      val linksToAddMaterialization = new mutable.HashSet[LinkIdentity]()
      val inputProcessingOrderForOp = getOperator(opId).getInputProcessingOrder()
      if (inputProcessingOrderForOp != null && inputProcessingOrderForOp.length > 1) {
        for (i <- 1 to inputProcessingOrderForOp.length - 1) {
          val prevInOrder = getPipelinedRegionFromOperatorId(
            toOperatorIdentity(inputProcessingOrderForOp(i - 1).from)
          )
          val nextInOrder =
            getPipelinedRegionFromOperatorId(toOperatorIdentity(inputProcessingOrderForOp(i).from))

          if (prevInOrder.getId() == nextInOrder.getId()) {
            linksToAddMaterialization.add(inputProcessingOrderForOp(i))
          } else {
            if (!pipelinedRegionsDAG.getDescendants(prevInOrder).contains(nextInOrder)) {
              pipelinedRegionsDAG.addEdge(prevInOrder, nextInOrder)
            }
          }
        }
      }

      // Add materialization operators to links which were in same region as the links they depend on
      if (linksToAddMaterialization.nonEmpty) {
        linksToAddMaterialization.foreach(linkId => {
          val fromOpId = toOperatorIdentity(linkId.from)
          val toOpId = toOperatorIdentity(linkId.to)
          val materializationWriter = new ProgressiveSinkOpDesc()
          val matWriterInputSchemas = inputSchemaMap(operatorIdToDesc(fromOpId.operator)).map(s => s.get).toArray
          val matWriterOutputSchemas = materializationWriter.getOutputSchemas(matWriterInputSchemas)
          materializationWriter.setStorage(opResultStorage.create(materializationWriter.operatorID, matWriterOutputSchemas(0)))
          val matWriterOpExecConfig =
            materializationWriter.operatorExecutor(OperatorSchemaInfo(matWriterInputSchemas, matWriterOutputSchemas))
          operatorToOpExecConfig.put(matWriterOpExecConfig.id, matWriterOpExecConfig)

          val materializationReader = new CacheSourceOpDesc(
            materializationWriter.operatorID,
            opResultStorage: OpResultStorage
          )
          materializationReader.schema = materializationWriter.getStorage.getSchema
          val matReaderOutputSchema = materializationReader.getOutputSchemas(Array())
          val matReaderOpExecConfig: OpExecConfig =
            materializationReader.operatorExecutor(OperatorSchemaInfo(Array(), matReaderOutputSchema))
          operatorToOpExecConfig.put(matReaderOpExecConfig.id, matReaderOpExecConfig)


        })
      }

      // 2. Find dependencies due to blocking operators.
      // e.g. The region before and after a sort operator has dependencies
      val upstreamOps = getDirectUpstreamOperators(opId)
      upstreamOps.foreach(upstreamOp => {
        val linkFromUpstreamOp = LinkIdentity(
          getOperator(upstreamOp).topology.layers.last.id,
          getOperator(opId).topology.layers.head.id
        )
        if (getOperator(opId).isInputBlocking(linkFromUpstreamOp)) {
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
