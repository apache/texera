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

package org.apache.texera.amber.operator.udf

import org.apache.texera.amber.core.executor.OpExecWithCode
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{PhysicalOp, PortIdentity, UnknownPartition}
import org.apache.texera.amber.operator.{LogicalOp, PortDescription}
import org.apache.texera.amber.operator.metadata.OperatorInfo
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.matchers.should.Matchers

import scala.reflect.ClassTag

private[udf] trait UdfDescriptorSpecSupport extends Matchers {
  protected val workflowId = WorkflowIdentity(1L)
  protected val executionId = ExecutionIdentity(1L)

  protected final def buildPhysicalOp(desc: LogicalOp): PhysicalOp =
    desc.getPhysicalOp(workflowId, executionId)

  protected final def expectOperatorInfo(
      desc: LogicalOp,
      name: String,
      group: String,
      inputs: Int
  ): OperatorInfo = {
    val info = desc.operatorInfo
    info.userFriendlyName shouldBe name
    info.operatorGroupName shouldBe group
    info.inputPorts should have length inputs
    info.outputPorts should have length 1
    info
  }

  protected final def roundTrip[D <: LogicalOp: ClassTag](desc: D): D = {
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(desc), classOf[LogicalOp])
    restored shouldBe a[D]
    restored.asInstanceOf[D]
  }

  protected final def expectInvalidWorkerCount[D <: LogicalOp](desc: D)(
      configure: D => Unit
  ): Unit = {
    configure(desc)
    intercept[IllegalArgumentException] { buildPhysicalOp(desc) }
  }

  protected final def resultColumn: Attribute = new Attribute("res", AttributeType.INTEGER)

  protected final def inputSchema: Schema =
    Schema().add(new Attribute("in", AttributeType.STRING))

  protected final def expectOutputColumns(desc: LogicalOp, retainInput: Boolean): Unit =
    expectOutputColumns(desc, retainInput, Map(desc.operatorInfo.inputPorts.head.id -> inputSchema))

  protected final def expectOutputColumns(
      desc: LogicalOp,
      retainInput: Boolean,
      inputs: Map[PortIdentity, Schema]
  ): Unit = {
    val expected = (if (retainInput) inputSchema else Schema()).add(resultColumn)
    desc.getExternalOutputSchemas(inputs) shouldBe Map(
      desc.operatorInfo.outputPorts.head.id -> expected
    )
  }

  protected final def portDescription(
      id: String,
      name: String,
      disallowMultiInputs: Boolean = false,
      dependencies: List[Int] = Nil
  ): PortDescription =
    PortDescription(
      id,
      name,
      disallowMultiInputs,
      isDynamicPort = false,
      UnknownPartition(),
      dependencies
    )

  protected val tupleUdfCode =
    """from pytexera import *
      |
      |class ProcessTupleOperator(UDFOperatorV2):
      |    def process_tuple(self, tuple_, port):
      |        yield tuple_
      |""".stripMargin

  protected final def uiParameter(name: String, value: String): UiUDFParameter =
    parameter(name, value, AttributeType.INTEGER, "")

  protected final def datasetParameter(name: String, value: String): UiUDFParameter =
    parameter(name, value, AttributeType.STRING, "dataset")

  private def parameter(
      name: String,
      value: String,
      attributeType: AttributeType,
      inputType: String
  ): UiUDFParameter = {
    val parameter = new UiUDFParameter
    parameter.attribute = new Attribute(name, attributeType)
    parameter.inputType = inputType
    parameter.value = value
    parameter
  }

  protected final def expectInjectedPython(desc: LogicalOp): Unit =
    buildPhysicalOp(desc).opExecInitInfo match {
      case OpExecWithCode(code, "python") =>
        code should include("def _texera_injected_ui_parameters")
        code should include("self.decode_python_template")
      case other => fail(s"expected Python OpExecWithCode, got $other")
    }
}
