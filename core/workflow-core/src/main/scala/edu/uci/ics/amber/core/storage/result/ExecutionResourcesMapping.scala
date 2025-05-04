/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package edu.uci.ics.amber.core.storage.result

import edu.uci.ics.amber.core.virtualidentity.ExecutionIdentity

import java.net.URI
import scala.collection.mutable

/**
  * ExecutionResourcesMapping is a singleton for keeping track of resources associated with each execution.
  *   It maintains a mapping from execution ID to a list of URIs, which point to resources like the result storage.
  *
  * Currently, this mapping is only used during the resource clean-up phase.
  *
  * This design has one limitation: the singleton is only accessible on the master node.
  * While this aligns with the current system design, improvements are needed in the
  * future to enhance scalability and flexibility.
  *
  * TODO: Move the mappings to an external, distributed, and persistent location to eliminate the master-node
  *   dependency.
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

  /**
    * Remove all resources associated with a given execution ID.
    *
    * @param executionIdentity the target execution ID
    * @return true if the entry was removed, false if it did not exist
    */
  def removeExecutionResources(executionIdentity: ExecutionIdentity): Boolean = {
    executionIdToExecutionResourcesMapping.remove(executionIdentity).isDefined
  }
}
