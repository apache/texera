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

package org.apache.texera.amber.operator.source.scan

import org.apache.texera.amber.core.workflow.OutputPort
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.source.scan.csv.{
  CSVScanSourceOpDesc,
  ParallelCSVScanSourceOpDesc
}
import org.apache.texera.amber.operator.source.scan.file.FileScanSourceOpDesc
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URI

class ScanSourceOpDescSpec extends AnyFlatSpec with Matchers {

  // Minimal concrete subclasses: ScanSourceOpDesc is abstract but needs no overrides
  // to construct. Construction never throws: LogicalOp's operatorVersion lookup
  // (OPVersion.getVersion) either finds a git commit for the operator path or, when
  // the git lookup fails, lands in its NullPointerException catch and memoizes "N/A"
  // per simple class name. The second, unrelated subclass exercises the cross-class
  // equals case.
  private class TestScanSourceOpDesc extends ScanSourceOpDesc
  private class OtherScanSourceOpDesc extends ScanSourceOpDesc

  "ScanSourceOpDesc.operatorInfo" should
    "fall back to the Unknown file-scan name when no file type is set" in {
    val info = (new TestScanSourceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Unknown File Scan"
    // "Unknown" starts with 'U', which is in the vowel list, so the "an" branch applies
    info.operatorDescription shouldBe "Scan data from an Unknown file"
  }

  it should "drop the type prefix entirely for an empty file-type name" in {
    val d = new TestScanSourceOpDesc
    d.fileTypeName = Some("")
    val info = d.operatorInfo
    // isEmpty is checked before charAt(0), so the empty string picks the plain
    // wording instead of throwing StringIndexOutOfBoundsException
    info.userFriendlyName shouldBe "File Scan"
    info.operatorDescription shouldBe "Scan data from a file"
  }

  it should "choose the article from the first letter of the file-type name" in {
    // mutating one stub between assertions also pins that operatorInfo is
    // recomputed on every call rather than cached
    val d = new TestScanSourceOpDesc
    d.fileTypeName = Some("Orc")
    d.operatorInfo.operatorDescription shouldBe "Scan data from an Orc file"
    d.fileTypeName = Some("avro")
    d.operatorInfo.operatorDescription shouldBe "Scan data from an avro file"
    d.fileTypeName = Some("Parquet")
    d.operatorInfo.operatorDescription shouldBe "Scan data from a Parquet file"
    d.operatorInfo.userFriendlyName shouldBe "Parquet File Scan"
  }

  it should "sit in the Data Input group with no input port and one default output port" in {
    val info = (new TestScanSourceOpDesc).operatorInfo
    info.operatorGroupName shouldBe OperatorGroupConstants.INPUT_GROUP
    info.inputPorts shouldBe empty
    // the full default port, not just the count: a change to blocking, mode or
    // reuseStorage here would alter the runtime behavior of every scan operator
    info.outputPorts shouldBe List(OutputPort())
  }

  it should "name the concrete CSV, parallel CSV, and generic file scans" in {
    val csvInfo = (new CSVScanSourceOpDesc).operatorInfo
    csvInfo.userFriendlyName shouldBe "CSV File Scan"
    csvInfo.operatorDescription shouldBe "Scan data from a CSV file"
    val parallelCsvInfo = (new ParallelCSVScanSourceOpDesc).operatorInfo
    parallelCsvInfo.userFriendlyName shouldBe "CSV File Scan"
    parallelCsvInfo.operatorDescription shouldBe "Scan data from a CSV file"
    val fileInfo = (new FileScanSourceOpDesc).operatorInfo
    fileInfo.userFriendlyName shouldBe "File Scan"
    fileInfo.operatorDescription shouldBe "Scan data from a file"
  }

  "ScanSourceOpDesc" should "default to an unresolved UTF-8 scan over the whole file" in {
    val d = new TestScanSourceOpDesc
    d.INFER_READ_LIMIT shouldBe 100
    d.fileName shouldBe None
    d.fileTypeName shouldBe None
    d.limit shouldBe None
    d.offset shouldBe None
    d.fileEncoding shouldBe FileDecodingMethod.UTF_8
  }

  "ScanSourceOpDesc.sourceSchema" should
    "default to null (every registered subclass overrides it)" in {
    (new TestScanSourceOpDesc).sourceSchema() shouldBe null
  }

  "ScanSourceOpDesc.setResolvedFileName" should "store the resolved URI as the file name" in {
    val d = new TestScanSourceOpDesc
    d.setResolvedFileName(new URI("file:///tmp/input.csv"))
    d.fileName shouldBe Some("file:///tmp/input.csv")
  }

  it should "store the ASCII form of a URI with non-ASCII characters" in {
    val d = new TestScanSourceOpDesc
    // toASCIIString (not toString): a single slash for the null authority and
    // UTF-8 percent escapes for the non-ASCII path characters
    d.setResolvedFileName(new URI("file", null, "/tmp/años.csv", null))
    d.fileName shouldBe Some("file:/tmp/a%C3%B1os.csv")
  }

  it should "overwrite a previously resolved file name unconditionally" in {
    val d = new TestScanSourceOpDesc
    d.setResolvedFileName(new URI("file:///tmp/first.csv"))
    d.setResolvedFileName(new URI("file:///tmp/second.csv"))
    d.fileName shouldBe Some("file:///tmp/second.csv")
  }

  "ScanSourceOpDesc.fileResolved" should "be false while no file name is set" in {
    (new TestScanSourceOpDesc).fileResolved() shouldBe false
  }

  it should "turn true once a scheme-qualified file name replaces a relative one" in {
    val d = new TestScanSourceOpDesc
    d.fileName = Some("relative/path.csv")
    d.fileResolved() shouldBe false
    d.setResolvedFileName(new URI("file:///tmp/a.csv"))
    d.fileResolved() shouldBe true
  }

  "ScanSourceOpDesc.equals" should "tell freshly constructed descriptors apart" in {
    // LogicalOp.operatorId embeds a random UUID and is not excluded from
    // reflectionEquals, so two new instances never compare equal
    (new TestScanSourceOpDesc) should not equal (new TestScanSourceOpDesc)
  }

  it should "treat descriptors with aligned operator ids as equal" in {
    val a = new TestScanSourceOpDesc
    val b = new TestScanSourceOpDesc
    a.setOperatorId("scan-1")
    b.setOperatorId("scan-1")
    a shouldEqual b
    b shouldEqual a
    a shouldEqual a
    a.hashCode shouldBe b.hashCode
  }

  it should "still compare the JSON-ignored tuning knob and the scan window" in {
    val a = new TestScanSourceOpDesc
    val b = new TestScanSourceOpDesc
    a.setOperatorId("scan-1")
    b.setOperatorId("scan-1")
    b.INFER_READ_LIMIT = 99
    a should not equal b
    b.INFER_READ_LIMIT = a.INFER_READ_LIMIT
    a shouldEqual b
    b.limit = Some(1)
    a should not equal b
  }

  it should "reject null and unrelated sibling subclasses" in {
    val a = new TestScanSourceOpDesc
    val other = new OtherScanSourceOpDesc
    a.setOperatorId("scan-1")
    other.setOperatorId("scan-1")
    a.equals(null) shouldBe false
    a should not equal other
  }

  // The JSON tests use the registered CSVScanSourceOpDesc: a local stub has no
  // @JsonSubTypes name, so its JSON could never round-trip through the
  // polymorphic objectMapper the way saved workflows do.
  "A CSVScanSourceOpDesc" should "serialize only the JSON-visible scan fields" in {
    val d = new CSVScanSourceOpDesc
    d.fileName = Some("file:///tmp/a.csv")
    d.limit = Some(2)
    d.offset = Some(3)
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(d))
    tree.has("fileName") shouldBe true
    tree.has("fileEncoding") shouldBe true
    tree.has("limit") shouldBe true
    tree.has("offset") shouldBe true
    // both are @JsonIgnore on the base class
    tree.has("INFER_READ_LIMIT") shouldBe false
    tree.has("fileTypeName") shouldBe false
  }

  it should "omit unset optional fields entirely, the shape saved workflows store" in {
    // objectMapper is configured NON_NULL + NON_ABSENT
    val tree = objectMapper.readTree(objectMapper.writeValueAsString(new CSVScanSourceOpDesc))
    tree.has("fileName") shouldBe false
    tree.has("limit") shouldBe false
    tree.has("offset") shouldBe false
  }
}
