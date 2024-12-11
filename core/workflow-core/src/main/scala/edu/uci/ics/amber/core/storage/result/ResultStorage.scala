package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.virtualidentity.WorkflowIdentity

import scala.collection.mutable

object ResultStorage {

  private val workflowIdToOpResultMapping: mutable.Map[WorkflowIdentity, OpResultStorage] =
    mutable.Map.empty

  def getOpResultStorage(workflowIdentity: WorkflowIdentity): OpResultStorage = {
    workflowIdToOpResultMapping.getOrElseUpdate(workflowIdentity, new OpResultStorage())
  }
}
