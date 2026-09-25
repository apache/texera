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

package org.apache.texera.amber.operator.source.scan.csvOld

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

class CSVOldScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "CSVOldScanSourceOpDesc.operatorInfo" should
    "advertise the CSVOld file-scan name in the Data Input group with no input and one output" in {
    val info = (new CSVOldScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "CSVOld File Scan"
    info.operatorDescription shouldBe "Scan data from a CSVOld file"
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    info.outputPorts should have length 1
  }

  "CSVOldScanSourceOpDesc" should "default the delimiter, header flag, and scan window" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter shouldBe Some(",")
    d.hasHeader shouldBe true
    d.fileName shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileTypeName shouldBe Some("CSVOld")
  }

  "CSVOldScanSourceOpDesc.sourceSchema" should "prompt for a file before one is resolved" in {
    val ex = intercept[IllegalArgumentException]((new CSVOldScanSourceOpDesc).sourceSchema())
    ex.getMessage should include("No file selected")
  }

  "CSVOldScanSourceOpDesc.getPhysicalOp" should
    "wire the CSVOld exec as a source op with no input port and one output port" in {
    val d = new CSVOldScanSourceOpDesc
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.source.scan.csvOld.CSVOldScanSourceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe empty
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
  }

  it should "fall back to a comma when the configured delimiter is empty" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter = Some("")
    d.getPhysicalOp(workflowId, executionId)
    d.customDelimiter shouldBe Some(",")
  }

  "CSVOldScanSourceOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new CSVOldScanSourceOpDesc
    d.customDelimiter = Some(";")
    d.hasHeader = false
    d.fileEncoding = FileDecodingMethod.UTF_16
    d.limit = Some(10)
    d.offset = Some(5)
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[CSVOldScanSourceOpDesc]
    val r = restored.asInstanceOf[CSVOldScanSourceOpDesc]
    r.customDelimiter shouldBe Some(";")
    r.hasHeader shouldBe false
    r.fileEncoding shouldBe FileDecodingMethod.UTF_16
    r.limit shouldBe Some(10)
    r.offset shouldBe Some(5)
  }

  // The limit bounded the sample the inference reads as well as the rows the
  // operator emits, so a Limit of 0 had nothing to infer from. The types came
  // back empty while the header still asked each column for one, and the
  // operator threw before a row was read. A file's columns do not depend on how
  // many of its rows were asked for.
  "CSVOldScanSourceOpDesc.generateStandaloneCode" should "keep the file's columns when the window asks for no rows" in {
    val d = describing(writeCsv("id,name\n1,alice\n2,bob\n"))
    d.limit = Some(0)

    val schema = d.sourceSchema()
    schema.getAttributeNames shouldBe List("id", "name")
    schema.getAttribute("id").getType shouldBe AttributeType.INTEGER
    schema.getAttribute("name").getType shouldBe AttributeType.STRING
  }

  private def writeCsv(content: String): String = {
    val file = Files.createTempFile("csv-old-", ".csv")
    file.toFile.deleteOnExit()
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    file.toString
  }

  private def describing(path: String): CSVOldScanSourceOpDesc = {
    val d = new CSVOldScanSourceOpDesc
    d.fileName = Some(path)
    d.customDelimiter = Some(",")
    d.hasHeader = true
    d.setResolvedFileName(FileResolver.resolve(path))
    d
  }
}
