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

package org.apache.texera.amber.operator.source.scan.json

import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.storage.FileResolver
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JSONLScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "JSONLScanSourceOpDesc.operatorInfo" should
    "advertise the JSONL file-scan name in the Data Input group with no input and one output" in {
    val info = (new JSONLScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "JSONL File Scan"
    info.operatorDescription shouldBe "Scan data from a JSONL file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "JSONLScanSourceOpDesc" should "default the flatten flag, encoding, and scan window" in {
    val d = new JSONLScanSourceOpDesc
    d.flatten shouldBe false
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("JSONL")
  }

  "JSONLScanSourceOpDesc.sourceSchema" should "prompt for a file before one is resolved" in {
    val ex = intercept[IllegalArgumentException]((new JSONLScanSourceOpDesc).sourceSchema())
    ex.getMessage should include("No file selected")
  }

  "JSONLScanSourceOpDesc.getPhysicalOp" should
    "wire the JSONL exec as a source op with no input port and one output port" in {
    val d = new JSONLScanSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.json.JSONLScanSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.parallelizable shouldBe true
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  "JSONLScanSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new JSONLScanSourceOpDesc
    d.flatten = true
    d.fileEncoding = FileDecodingMethod.UTF_16
    d.limit = Some(10)
    d.offset = Some(5)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[JSONLScanSourceOpDesc]
    val r = restored.asInstanceOf[JSONLScanSourceOpDesc]
    r.flatten shouldBe true
    r.fileEncoding shouldBe FileDecodingMethod.UTF_16
    r.limit shouldBe Some(10)
    r.offset shouldBe Some(5)
  }

  // The limit bounded the sample the inference reads as well as the rows the
  // operator emits, so a Limit of 0 had nothing to infer from and the operator
  // declared a schema of no columns at all. A file's columns do not depend on
  // how many of its rows were asked for.
  "JSONLScanSourceOpDesc.generateStandaloneCode" should "keep the file's columns when the window asks for no rows" in {
    val data = Files.createTempFile("jsonl-zero-window-", ".jsonl")
    data.toFile.deleteOnExit()
    Files.write(
      data,
      "{\"id\":1,\"name\":\"alice\"}\n{\"id\":2,\"name\":\"bob\"}\n".getBytes(
        StandardCharsets.UTF_8
      )
    )

    val op = new JSONLScanSourceOpDesc
    op.fileName = Some(data.toString)
    op.setResolvedFileName(FileResolver.resolve(data.toString))
    op.limit = Some(0)

    val schema = op.sourceSchema()
    schema.getAttributeNames shouldBe List("id", "name")
    schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
  }
}
