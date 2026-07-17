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

package org.apache.texera.amber.util

import org.apache.texera.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\d+)-(.+)-(\w+)-(\d+)".r
  private val operatorUUIDPattern: Regex = raw"(\w+)-(.+)-(\w+)".r
  private val MATERIALIZATION_READER_ACTOR_PREFIX: String = "MATERIALIZATION_READER_"
  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      operator: String,
      layerName: String,
      workerId: Int
  ): ActorVirtualIdentity = {
    require(
      !layerName.contains('-'),
      s"layerName must not contain '-' (worker-name parsing relies on this): $layerName"
    )
    ActorVirtualIdentity(
      s"Worker:WF${workflowId.id}-$operator-$layerName-$workerId"
    )
  }

  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      physicalOpId: PhysicalOpIdentity,
      workerId: Int
  ): ActorVirtualIdentity = {
    createWorkerIdentity(
      workflowId,
      physicalOpId.logicalOpId.id,
      physicalOpId.layerName,
      workerId
    )
  }

  def getPhysicalOpId(workerId: ActorVirtualIdentity): PhysicalOpIdentity = {
    workerId.name match {
      case workerNamePattern(_, operator, layerName, _) =>
        PhysicalOpIdentity(OperatorIdentity(operator), layerName)
      case other =>
        // for special actorId such as SELF, COORDINATOR
        PhysicalOpIdentity(OperatorIdentity("__DummyOperator"), "__DummyLayer")
    }
  }

  /**
    * Extract the logical operator id from a worker actor id of the form
    * `Worker:WF<workflowId>-<operatorId>-<layerName>-<workerIndex>`.
    *
    * Returns the logical operator id only (the `<operatorId>` segment);
    * the physical operator id additionally carries the `<layerName>` and
    * is exposed by [[getPhysicalOpId]]. Method name parallels
    * `getPhysicalOpId` so callers can distinguish the two at the call
    * site; the Python sibling is `core.util.virtual_identity.get_logical_op_id`.
    *
    * The Python helper raises `ValueError` on a non-match for fail-loud
    * semantics; this Scala helper preserves the existing sentinel-on-miss
    * behavior (`"__DummyOperator"`) so it stays a drop-in replacement for
    * the inline `getPhysicalOpId(workerId).logicalOpId.id` pattern at
    * call sites.
    */
  def getLogicalOpId(workerId: ActorVirtualIdentity): String = {
    getPhysicalOpId(workerId).logicalOpId.id
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Option[Int] = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        Some(idx.toInt)
      case _ =>
        // for special actorId such as SELF, COORDINATOR
        None
    }
  }

  def toShorterString(workerId: ActorVirtualIdentity): String = {
    workerId.name match {
      case workerNamePattern(workflowId, operatorName, layerName, workerIndex) =>
        val shorterName = if (operatorName.length > 6) {
          operatorName match {
            case operatorUUIDPattern(op, _, postfix) => op + "-" + postfix.takeRight(6)
            case _                                   => operatorName.takeRight(6)
          }
        } else {
          operatorName
        }

        s"WF$workflowId-$shorterName-$layerName-$workerIndex"
      case _ => workerId.name
    }
  }

  /**
    * An input port materialization reader thread mimics the behavior of an upstream worker.
    * Each thread has a virtual actor id. This method creates such a virtual actor id.
    * @param storageURIStr The materialization location to read from.
    * @param toWorkerActorId The worker actor that the thread belongs to.
    * @return
    */
  def getFromActorIdForInputPortStorage(
      storageURIStr: String,
      toWorkerActorId: ActorVirtualIdentity
  ): ActorVirtualIdentity = {
    ActorVirtualIdentity(MATERIALIZATION_READER_ACTOR_PREFIX + storageURIStr + toWorkerActorId.name)
  }
}
