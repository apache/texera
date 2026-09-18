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

  // scala-csv hands back the text of every field and nothing else: a blank cell is
  // "", not a null, and "NA" is the country code it says it is. pandas reads both
  // as missing by default, so the export read a column of codes as a column of
  // nulls and turned the blank into NaN.
  "CSVOldScanSourceOpDesc.generateStandaloneCode" should
    "read a literal NA as text and a blank as an empty string, as its reader does" in {
    val d = describing(writeCsv("code,note\nNA,x\n,y\n"))

    rowsFromEngine(d) shouldBe List(List("NA", "x"), List("", "y"))

    val code = d.generateStandaloneCode()
    code should include("keep_default_na=False")
    // No na_values: where the other CSV readers null a blank, this one keeps it.
    code should not include "na_values"
  }

  // The same reason keeps a large integer exact here: a blank types the column
  // STRING, and with no missing value named, pandas reads every cell as the text
  // it is. Nothing widens through a float, so 9007199254740993 stays itself.
  it should "keep a nullable large integer exact, as text" in {
    val d = describing(writeCsv("id,big\n1,9007199254740993\n2,\n3,9007199254740995\n"))

    d.sourceSchema().getAttribute("big").getType shouldBe AttributeType.STRING
    rowsFromEngine(d).map(_(1)) shouldBe List("9007199254740993", "", "9007199254740995")
  }

  // sourceSchema names a blank header column-N; pandas names it "Unnamed: N", and a
  // downstream operator asks for the name the schema gave.
  it should "give the frame the names the schema gives it" in {
    val d = describing(writeCsv("id,name,,age\n1,Alice,x,30\n"))
    d.generateStandaloneCode() should include(
      """out1df.columns = ["id", "name", "column-3", "age"]"""
    )
  }

  // Only the property editor refuses a negative window; a plan posted to the API
  // arrives with one intact. pandas rejects a negative nrows outright, where this
  // reader's take just keeps no rows, so the export asks for the empty window.
  it should "ask pandas for the empty window a negative limit means to the reader" in {
    val d = describing(writeCsv("id\n1\n2\n3\n"))
    d.limit = Some(-1)
    d.offset = Some(-1)

    val code = d.generateStandaloneCode()
    code should include("nrows=0")
    code should include("skiprows=range(1, 1)")
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

  private def rowsFromEngine(d: CSVOldScanSourceOpDesc): List[List[Any]] = {
    val exec = new CSVOldScanSourceOpExec(objectMapper.writeValueAsString(d))
    exec.open()
    try exec.produceTuple().map(_.getFields.toList).toList
    finally exec.close()
  }
}
