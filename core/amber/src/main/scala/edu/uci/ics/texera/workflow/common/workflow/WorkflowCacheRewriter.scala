package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.Utils.objectMapper
import edu.uci.ics.texera.web.service.{ExecutionsMetadataPersistService, WorkflowCacheChecker}
import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.VisualizationConstants

object WorkflowCacheRewriter {

  def transform(
      logicalPlan: LogicalPlan,
      lastCompletedPlan: Option[LogicalPlan],
      storage: OpResultStorage,
      opsToReuseCache: Set[OperatorIdentity]
  ): LogicalPlan = {
    val validCachesFromLastExecution =
      new WorkflowCacheChecker(lastCompletedPlan, logicalPlan).getValidCacheReuse

    var resultPlan = logicalPlan
    // an operator can reuse cache if
    // 1: the user wants the operator to reuse past result
    // 2: the operator is equivalent to the last run
    val opsCanUseCache = opsToReuseCache.intersect(validCachesFromLastExecution)

    // remove sinks directly connected to operators that are already reusing cache
    val unnecessarySinks = resultPlan.getTerminalOperatorIds.filter(sink => {
      opsCanUseCache.contains(resultPlan.getUpstreamOps(sink).head.operatorIdentifier)
    })
    unnecessarySinks.foreach(o => {
      resultPlan = resultPlan.removeOperator(o)
    })

    opsCanUseCache.foreach(opId => {
      val materializationReader = new CacheSourceOpDesc(opId, storage)
      resultPlan = resultPlan.addOperator(materializationReader)
      // replace the connection of all outgoing edges of opId with the cache
      val edgesToReplace = resultPlan.getDownstreamEdges(opId)
      edgesToReplace.foreach(e => {
        resultPlan = resultPlan.removeEdge(
          e.origin.operatorId,
          e.destination.operatorId,
          e.origin.portOrdinal,
          e.destination.portOrdinal
        )
        resultPlan = resultPlan.addEdge(
          materializationReader.operatorIdentifier,
          e.destination.operatorId,
          0,
          e.destination.portOrdinal
        )
      })
    })

    // after an operator is replaced with reading from cached result
    // its upstream operators can be removed if it's not used by other sinks
    val allOperators = resultPlan.operators.map(op => op.operatorIdentifier).toSet
    val sinkOps =
      resultPlan.operators.filter(op => op.isInstanceOf[SinkOpDesc]).map(o => o.operatorIdentifier)
    val usefulOperators = sinkOps ++ sinkOps.flatMap(o => resultPlan.getAncestorOpIds(o)).toSet
    // remove operators that are no longer reachable by any sink
    allOperators
      .diff(usefulOperators.toSet)
      .foreach(o => {
        resultPlan = resultPlan.removeOperator(o)
      })

    assert(
      resultPlan.getTerminalOperatorIds.forall(o =>
        resultPlan.getOperator(o).isInstanceOf[SinkOpDesc]
      )
    )

    resultPlan.propagateWorkflowSchema(None)

    // assign sink storage to the logical plan after cache rewrite
    // as it will be converted to the actual physical plan
    assignSinkStorage(resultPlan, storage, opsCanUseCache)
    // also assign sink storage to the original logical plan, as the original logical plan
    // will be used to be compared to the subsequent runs
    assignSinkStorage(logicalPlan, storage, opsCanUseCache)
    resultPlan

  }

  private def assignSinkStorage(
      logicalPlan: LogicalPlan,
      storage: OpResultStorage,
      reuseStorageSet: Set[OperatorIdentity] = Set()
  ): Unit = {
    // create a JSON object that holds pointers to the workflow's results in Mongo
    // TODO in the future, will extract this logic from here when we need pointers to the stats storage
    val resultsJSON = objectMapper.createObjectNode()
    val sinksPointers = objectMapper.createArrayNode()
    // assign storage to texera-managed sinks before generating exec config
    logicalPlan.operators.foreach {
      case o @ (sink: ProgressiveSinkOpDesc) =>
        val storageKey = sink.getUpstreamId.getOrElse(o.operatorIdentifier)
        // due to the size limit of single document in mongoDB (16MB)
        // for sinks visualizing HTMLs which could possibly be large in size, we always use the memory storage.
        val storageType = {
          if (sink.getChartType.contains(VisualizationConstants.HTML_VIZ)) OpResultStorage.MEMORY
          else OpResultStorage.defaultStorageMode
        }
        if (reuseStorageSet.contains(storageKey) && storage.contains(storageKey)) {
          sink.setStorage(storage.get(storageKey))
        } else {
          sink.setStorage(
            storage.create(
              o.getContext.executionId + "_",
              storageKey,
              storageType
            )
          )
          sink.getStorage.setSchema(logicalPlan.getOpOutputSchemas(o.operatorIdentifier).head)
          // add the sink collection name to the JSON array of sinks
          sinksPointers.add(o.getContext.executionId + "_" + storageKey)
        }
        storage.get(storageKey)

      case _ =>
    }
    // update execution entry in MySQL to have pointers to the mongo collections
    resultsJSON.set("results", sinksPointers)
    ExecutionsMetadataPersistService.updateExistingExecutionVolumePointers(
      logicalPlan.context.executionId,
      resultsJSON.toString
    )
  }

}
