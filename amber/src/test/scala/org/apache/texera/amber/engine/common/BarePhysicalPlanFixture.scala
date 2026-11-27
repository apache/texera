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

package org.apache.texera.amber.engine.common

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  PhysicalLink,
  PhysicalOp,
  PortIdentity
}
import org.apache.texera.amber.engine.architecture.scheduling.{Region, RegionIdentity}

/** Shared bare physical-op, link, global-port, and single-operator region constructors for engine unit tests. */
trait BarePhysicalPlanFixture {

  protected def physicalOpId(opId: String, layerName: String = "main"): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(opId), layerName)

  protected def op(
      opId: String,
      layerName: String = "main",
      workflowId: WorkflowIdentity = WorkflowIdentity(0),
      executionId: ExecutionIdentity = ExecutionIdentity(0)
  ): PhysicalOp =
    PhysicalOp(physicalOpId(opId, layerName), workflowId, executionId, OpExecInitInfo.Empty)

  protected def region(
      regionId: Long,
      opId: String,
      ports: Set[GlobalPortIdentity] = Set.empty
  ): Region =
    Region(RegionIdentity(regionId), Set(op(opId)), Set.empty, ports = ports)

  protected def link(fromOp: String, toOp: String): PhysicalLink =
    PhysicalLink(physicalOpId(fromOp), PortIdentity(0), physicalOpId(toOp), PortIdentity(0))

  protected def globalPort(opId: String, input: Boolean = true): GlobalPortIdentity =
    GlobalPortIdentity(physicalOpId(opId), PortIdentity(0), input = input)
}
