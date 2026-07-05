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

package org.apache.texera.amber.operator

import org.apache.texera.amber.core.storage.VFSURIFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{GlobalPortIdentity, PortIdentity, PreferController}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SpecialPhysicalOpFactorySpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(42L)
  private val executionId = ExecutionIdentity(7L)
  private val schema = Schema().add(new Attribute("col", AttributeType.STRING))

  private def portUri(opName: String, layer: String, portId: Int) = {
    val gpid = GlobalPortIdentity(
      PhysicalOpIdentity(OperatorIdentity(opName), layer),
      PortIdentity(portId),
      input = false
    )
    VFSURIFactory.resultURI(VFSURIFactory.createPortBaseURI(workflowId, executionId, gpid))
  }

  "SpecialPhysicalOpFactory.newSourcePhysicalOp" should
    "derive a source op that wires the decoded port identity, ports, and schema" in {
    val uri = portUri("srcOp", "layerA", 3)
    val downstream = PhysicalOpIdentity(OperatorIdentity("down-stream"), "main")
    val op = SpecialPhysicalOpFactory.newSourcePhysicalOp(
      workflowId,
      executionId,
      uri,
      downstream,
      PortIdentity(5),
      schema
    )
    op.id.logicalOpId shouldBe OperatorIdentity("srcOp")
    // layerName: "${layerName}_source_${portId.id}_${downstreamLogicalId}_${downstreamPort.id}"
    // with '-' in the downstream logical id replaced by '_'
    op.id.layerName shouldBe "layerA_source_3_down_stream_5"
    op.workflowId shouldBe workflowId
    op.executionId shouldBe executionId
    op.isSourceOperator shouldBe true
    op.inputPorts shouldBe empty
    op.outputPorts.keySet shouldBe Set(PortIdentity(0))
    op.locationPreference shouldBe Some(PreferController)
    op.parallelizable shouldBe false
    op.outputPorts(PortIdentity(0))._3 shouldBe Right(schema)
  }

  it should "replace every dash in the downstream operator id when building the layer name" in {
    val op = SpecialPhysicalOpFactory.newSourcePhysicalOp(
      workflowId,
      executionId,
      portUri("srcOp", "layerA", 3),
      PhysicalOpIdentity(OperatorIdentity("a-b-c"), "main"),
      PortIdentity(9),
      schema
    )
    op.id.layerName shouldBe "layerA_source_3_a_b_c_9"
  }

  it should "fail when the URI carries no global port identity" in {
    val noPortUri = VFSURIFactory.createRuntimeStatisticsURI(workflowId, executionId)
    intercept[NoSuchElementException] {
      SpecialPhysicalOpFactory.newSourcePhysicalOp(
        workflowId,
        executionId,
        noPortUri,
        PhysicalOpIdentity(OperatorIdentity("down"), "main"),
        PortIdentity(0),
        schema
      )
    }
  }
}
