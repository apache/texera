package edu.uci.ics.texera.workflow.common.workflow

import edu.uci.ics.texera.workflow.common.storage.OpResultStorage
import edu.uci.ics.texera.workflow.operators.sink.SinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.cache.CacheSourceOpDesc

object WorkflowCacheRewriter {

  def transform(
      logicalPlan: LogicalPlan,
      storage: OpResultStorage,
      availableCache: Set[String]
  ): LogicalPlan = {
    var resultPlan = logicalPlan

    // an operator can reuse cache if
    // 1: the user wants the operator to reuse past result
    // 2: the operator is equivalent to the last run
    val opsToUseCache = logicalPlan.opsToReuseCache.toSet.intersect(availableCache)

    // remove sinks directly connected to operators that are already reusing cache
    val unnecessarySinks = resultPlan.getTerminalOperators.filter(sink => {
      opsToUseCache.contains(resultPlan.getUpstream(sink).head.operatorID)
    })
    unnecessarySinks.foreach(o => {
      resultPlan = resultPlan.removeOperator(o)
    })

    opsToUseCache.foreach(opId => {
      val materializationReader = new CacheSourceOpDesc(opId, storage)
      resultPlan = resultPlan.addOperator(materializationReader)
      // replace the connection of all outgoing edges of opId with the cache
      val edgesToReplace = resultPlan.getDownstreamEdges(opId)
      edgesToReplace.foreach(e => {
        resultPlan = resultPlan.removeEdge(
          e.origin.operatorID,
          e.destination.operatorID,
          e.origin.portOrdinal,
          e.destination.portOrdinal
        )
        resultPlan = resultPlan.addEdge(
          materializationReader.operatorID,
          e.destination.operatorID,
          0,
          e.destination.portOrdinal
        )
      })
    })

    // after an operator is replaced with reading from cached result
    // its upstream operators can be removed if it's not used by other sinks
    val allOperators = resultPlan.operators.map(op => op.operatorID).toSet
    val sinkOps =
      resultPlan.operators.filter(op => op.isInstanceOf[SinkOpDesc]).map(o => o.operatorID)
    val usefulOperators = sinkOps ++ sinkOps.flatMap(o => resultPlan.getAncestorOpIds(o)).toSet
    // remove operators that are no longer reachable by any sink
    allOperators
      .diff(usefulOperators.toSet)
      .foreach(o => {
        resultPlan = resultPlan.removeOperator(o)
      })

    assert(
      resultPlan.terminalOperators.forall(o => resultPlan.getOperator(o).isInstanceOf[SinkOpDesc])
    )

    resultPlan
  }

}
