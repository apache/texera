package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.virtualidentity.WorkflowIdentity

import scala.collection.mutable

/**
  * ResultStorage is a singleton for accessing storage objects. It maintains a mapping from workflow ID to OpResultStorage.
  *
  * Using a workflow ID and an operator ID, the corresponding OpResultStorage object can be resolved and retrieved globally.
  *
  * This design has one limitation: the singleton is only accessible on the master node. Consequently, all sink executors
  * must execute on the master node. While this aligns with the current system design, improvements are needed in the
  * future to enhance scalability and flexibility.
  *
  * TODO: Move the storage mappings to an external, distributed, and persistent location to eliminate the master-node
  *   dependency and enable sink executors to run on other nodes.
  */
object ResultStorage {

  private val workflowIdToOpResultMapping: mutable.Map[WorkflowIdentity, OpResultStorage] =
    mutable.Map.empty

  def getOpResultStorage(workflowIdentity: WorkflowIdentity): OpResultStorage = {
    workflowIdToOpResultMapping.getOrElseUpdate(workflowIdentity, new OpResultStorage())
  }
}
