package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

import java.net.URI
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
object ExecutionResourcesMapping {

  private val executionIdToExecutionResourcesMapping: mutable.Map[ExecutionIdentity, List[URI]] =
    mutable.Map.empty

  def getResourceURIs(executionIdentity: ExecutionIdentity): List[URI] = {
    executionIdToExecutionResourcesMapping.getOrElseUpdate(executionIdentity, List())
  }

  def addResourceUri(executionIdentity: ExecutionIdentity, uri: URI): Unit = {
    executionIdToExecutionResourcesMapping.updateWith(executionIdentity) {
      case Some(existingUris) => Some(uri :: existingUris) // Prepend URI to the existing list
      case None               => Some(List(uri)) // Create a new list if key doesn't exist
    }
  }
}
