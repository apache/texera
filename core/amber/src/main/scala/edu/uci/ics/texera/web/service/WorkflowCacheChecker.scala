package edu.uci.ics.texera.web.service

import edu.uci.ics.texera.web.model.websocket.request.EditingTimeCompilationRequest
import edu.uci.ics.texera.workflow.common.workflow.LogicalPlan

import scala.collection.mutable

object WorkflowCacheChecker {

  def handleCacheStatusUpdate(
      oldPlan: Option[LogicalPlan],
      newPlan: LogicalPlan,
      request: EditingTimeCompilationRequest
  ): Map[String, String] = {
    val validCacheOps = new WorkflowCacheChecker(oldPlan, newPlan).getValidCacheReuse()
    val cacheUpdateResult = request.opsToReuseResult
      .map(o => (o, if (validCacheOps.contains(o)) "cache valid" else "cache invalid"))
      .toMap
    cacheUpdateResult
  }

}

class WorkflowCacheChecker(oldWorkflowOpt: Option[LogicalPlan], newWorkflow: LogicalPlan) {

  private val equivalenceClass = new mutable.HashMap[String, Int]()
  private var nextClassId: Int = 0

  private def getNextClassId(): Int = {
    nextClassId += 1
    nextClassId
  }

  // checks the validity of the cache given the old plan and the new plan
  // returns a set of operator IDs that can be reused
  // the operatorID is also the storage key
  def getValidCacheReuse(): Set[String] = {
    if (oldWorkflowOpt.isEmpty) {
      return Set()
    }

    val oldWorkflow = oldWorkflowOpt.get
    // for each operator in the old workflow, add it to its own equivalence class
    oldWorkflow.jgraphtDag
      .iterator()
      .forEachRemaining(opId => {
        val oldId = "old-" + opId
        equivalenceClass.put(oldId, nextClassId)
        nextClassId += 1
      })

    // for each operator in the new workflow
    // check if
    // 1: an operator with the same content can be found in the old workflow, and
    // 2: the input operators are also in the same equivalence class
    //
    // if both conditions are met, then the two operators are equal,
    // else a new equivalence class is created
    newWorkflow.jgraphtDag
      .iterator()
      .forEachRemaining(opId => {
        val newOp = newWorkflow.getOperator(opId)
        val newOpUpstreamClasses = newWorkflow
          .getUpstream(opId)
          .map(op => equivalenceClass("new-" + op.operatorID))
        val oldOp = oldWorkflow.operators.find(op => op.equals(newOp)).orNull

        // check if the old workflow contains the same operator content
        val newOpClassId = if (oldOp == null) {
          getNextClassId() // operator not found, create a new class
        } else {
          // check its inputs are all in the same equivalence class
          val oldId = "old-" + oldOp.operatorID
          val oldOpUpstreamClasses = oldWorkflow
            .getUpstream(oldOp.operatorID)
            .map(op => equivalenceClass("old-" + op.operatorID))
          if (oldOpUpstreamClasses.equals(newOpUpstreamClasses)) {
            equivalenceClass(oldId) // same equivalence class
          } else {
            getNextClassId() // inputs are no the same, new class
          }
        }
        equivalenceClass.put("new-" + opId, newOpClassId)
      })

    // for each cached operator in the old workflow,
    // check if it can be still used in the new workflow
    oldWorkflow.terminalOperators
      .map(sinkOpId => {
        val opId = oldWorkflow.getUpstream(sinkOpId).head.operatorID
        val oldCachedOpId = "old-" + opId
        // find its equivalence class
        val oldClassId = equivalenceClass(oldCachedOpId)
        // find the corresponding operator that can still use this cache
        val newOpId = equivalenceClass
          .find(p => p._2 == oldClassId && p._1 != oldCachedOpId)
          .map(p => p._1)
          .orNull
        if (newOpId == null) null else opId
      })
      .filter(o => o != null)
      .toSet
  }

}
