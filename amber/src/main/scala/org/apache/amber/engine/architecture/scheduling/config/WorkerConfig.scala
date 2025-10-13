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

package org.apache.amber.engine.architecture.scheduling.config

import org.apache.amber.config.ApplicationConfig
import org.apache.amber.core.virtualidentity.ActorVirtualIdentity
import org.apache.amber.core.workflow.PhysicalOp
import org.apache.amber.util.VirtualIdentityUtils

case object WorkerConfig {
  def generateWorkerConfigs(physicalOp: PhysicalOp): List[WorkerConfig] = {
    val workerCount = if (physicalOp.parallelizable) {
      physicalOp.suggestedWorkerNum match {
        // Keep suggested number of workers
        case Some(num) => num
        // If no suggested number, use default value
        case None => ApplicationConfig.numWorkerPerOperatorByDefault
      }
    } else {
      // Non parallelizable operator has only 1 worker
      1
    }

    (0 until workerCount).toList.map(idx =>
      WorkerConfig(
        VirtualIdentityUtils.createWorkerIdentity(physicalOp.workflowId, physicalOp.id, idx)
      )
    )
  }
}

case class WorkerConfig(
    workerId: ActorVirtualIdentity
)
