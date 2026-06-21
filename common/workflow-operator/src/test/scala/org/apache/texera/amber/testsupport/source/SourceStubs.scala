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

package org.apache.texera.amber.testsupport.source

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity}
import org.apache.texera.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import org.apache.texera.amber.operator.source.{
  PythonSourceOperatorDescriptor,
  SourceOperatorDescriptor
}

// Both stubs deliberately live outside `org.apache.texera.amber.operator`:
// PythonReflectionUtils.scanCandidates scopes itself to that package prefix
// (see PythonCodeRawInvalidTextSpec / SklearnOpDescRegistrySpec) and would
// otherwise try to instantiate and codegen these test-only fixtures.

object SourceStubs {
  val testSchema: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))
}

/** Minimal concrete `SourceOperatorDescriptor` for contract tests. */
class StubSource extends SourceOperatorDescriptor {
  override def sourceSchema(): Schema = SourceStubs.testSchema
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Stub",
      "stub source",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List.empty
    )
  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    throw new NotImplementedError(
      "getPhysicalOp is not needed for the SourceOperatorDescriptor contract test"
    )
}

/** Minimal concrete `PythonSourceOperatorDescriptor` for composition tests. */
class StubPythonSource extends PythonSourceOperatorDescriptor {
  override def sourceSchema(): Schema = SourceStubs.testSchema
  override def generatePythonCode(): String = "yield {'col': 'value'}"
  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] =
    Map(PortIdentity() -> SourceStubs.testSchema)
  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "StubPySrc",
      "stub python source",
      OperatorGroupConstants.INPUT_GROUP,
      inputPorts = List.empty,
      outputPorts = List.empty
    )
}
