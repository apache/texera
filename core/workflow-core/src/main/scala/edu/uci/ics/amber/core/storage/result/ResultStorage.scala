package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.virtualidentity.WorkflowIdentity

object ResultStorage {

  val workflowToOpResultMapping: scala.collection.mutable.Map[
    edu.uci.ics.amber.virtualidentity.WorkflowIdentity,
    edu.uci.ics.amber.core.storage.result.OpResultStorage
  ] = scala.collection.mutable.Map.empty

  def getOpResultStorage (workflowIdentity: WorkflowIdentity) : OpResultStorage = {
    workflowToOpResultMapping.getOrElseUpdate(workflowIdentity, new OpResultStorage())
  }
}
