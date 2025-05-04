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

package edu.uci.ics.amber.util

import edu.uci.ics.amber.core.virtualidentity.{
  ActorVirtualIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}

import scala.util.matching.Regex

object VirtualIdentityUtils {

  private val workerNamePattern: Regex = raw"Worker:WF(\d+)-(.+)-(\w+)-(\d+)".r
  private val operatorUUIDPattern: Regex = raw"(\w+)-(.+)-(\w+)".r
  def createWorkerIdentity(
      workflowId: WorkflowIdentity,
      operator: String,
      layerName: String,
      workerId: Int
  ): ActorVirtualIdentity = {

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
        // for special actorId such as SELF, CONTROLLER
        PhysicalOpIdentity(OperatorIdentity("__DummyOperator"), "__DummyLayer")
    }
  }

  def getWorkerIndex(workerId: ActorVirtualIdentity): Int = {
    workerId.name match {
      case workerNamePattern(_, _, _, idx) =>
        idx.toInt
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
}
