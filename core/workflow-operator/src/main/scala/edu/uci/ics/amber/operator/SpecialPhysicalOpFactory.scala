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

package edu.uci.ics.amber.operator

import edu.uci.ics.amber.core.executor.OpExecSource
import edu.uci.ics.amber.core.storage.VFSURIFactory
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow._

import java.net.URI

object SpecialPhysicalOpFactory {
  def newSourcePhysicalOp(
      workflowIdentity: WorkflowIdentity,
      executionIdentity: ExecutionIdentity,
      uri: URI,
      downstreamOperator: PhysicalOpIdentity,
      downstreamPort: PortIdentity,
      schema: Schema
  ): PhysicalOp = {

    val (_, _, globalPortIdOption, _) = VFSURIFactory.decodeURI(uri)
    val globalPortId = globalPortIdOption.get
    val outputPort = OutputPort()
    PhysicalOp
      .sourcePhysicalOp(
        PhysicalOpIdentity(
          globalPortId.opId.logicalOpId,
          s"${globalPortId.opId.layerName}_source_${globalPortId.portId.id}_${downstreamOperator.logicalOpId.id
            .replace('-', '_')}_${downstreamPort.id}"
        ),
        workflowIdentity,
        executionIdentity,
        OpExecSource(uri.toString, workflowIdentity)
      )
      .withInputPorts(List.empty)
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(outputPort.id -> schema))
      )
      .propagateSchema()

  }

}
