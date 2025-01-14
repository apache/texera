package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}

import java.net.URI
import scala.collection.mutable

/**
  * ExecutionResourcesMapping is a singleton for keeping track of resources associated with each execution.
  *   It maintains a mapping from execution ID to a list of URIs, which point to resources like the result storage.
  *
  * Currently, this mapping is only used during the resource clean-up phase.
  *
  * This design has one limitation: the singleton is only accessible on the master node. Consequently, all sink executors
  * must execute on the master node. While this aligns with the current system design, improvements are needed in the
  * future to enhance scalability and flexibility.
  *
  * TODO: Move the mappings to an external, distributed, and persistent location to eliminate the master-node
  *   dependency and enable sink executors to run on other nodes.
  */
object ExecutionResourcesMapping {

  private val executionIdToExecutionResourcesMapping: mutable.Map[ExecutionIdentity, List[URI]] =
    mutable.Map.empty

  /**
    * Get the URIs of given execution Id
    * @param executionIdentity the target execution id
    * @return
    */
  def getResourceURIs(executionIdentity: ExecutionIdentity): List[URI] = {
    executionIdToExecutionResourcesMapping.getOrElseUpdate(executionIdentity, List())
  }

  /**
    * Add the URI to the mapping
    * @param executionIdentity the target execution
    * @param uri the URI of the resource
    */
  def addResourceUri(executionIdentity: ExecutionIdentity, uri: URI): Unit = {
    executionIdToExecutionResourcesMapping.updateWith(executionIdentity) {
      case Some(existingUris) => Some(uri :: existingUris) // Prepend URI to the existing list
      case None               => Some(List(uri)) // Create a new list if key doesn't exist
    }
  }
}
