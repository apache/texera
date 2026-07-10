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

import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class JSONLScanSourceOpExecSpec extends AnyFlatSpec {

  private def writeJsonl(lines: String*): URI = {
    val path = Files.createTempFile("jsonl-scan-", ".jsonl")
    path.toFile.deleteOnExit()
    Files.write(path, lines.mkString("\n").getBytes(StandardCharsets.UTF_8))
    path.toFile.toURI
  }

  private def descString(
      uri: URI,
      flatten: Boolean = false,
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): String = {
    val desc = new JSONLScanSourceOpDesc
    desc.setResolvedFileName(uri)
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc.flatten = flatten
    desc.limit = limit
    desc.offset = offset
    objectMapper.writeValueAsString(desc)
  }

  private def drain(exec: JSONLScanSourceOpExec): List[Seq[Any]] = {
    exec.open()
    try exec.produceTuple().map(_.getFields.toSeq).toList
    finally exec.close()
  }

  "JSONLScanSourceOpExec" should "read each JSON line, ordering fields by sorted attribute name" in {
    // keys are written name-then-id; the output must be reordered to id-then-name
    val exec = new JSONLScanSourceOpExec(
      descString(writeJsonl("""{"name":"a","id":1}""", """{"name":"b","id":2}"""))
    )
    val rows = drain(exec)
    assert(rows.size == 2)
    assert(rows.head == Seq(1, "a"))
    assert(rows(1) == Seq(2, "b"))
  }

  it should "partition rows across workers" in {
    val uri = writeJsonl("""{"v":0}""", """{"v":1}""", """{"v":2}""", """{"v":3}""")
    val worker0 = new JSONLScanSourceOpExec(descString(uri), idx = 0, workerCount = 2)
    val worker1 = new JSONLScanSourceOpExec(descString(uri), idx = 1, workerCount = 2)
    assert(drain(worker0).map(_.head) == Seq(0, 1))
    assert(drain(worker1).map(_.head) == Seq(2, 3))
  }

  it should "apply the row limit" in {
    val uri = writeJsonl("""{"v":0}""", """{"v":1}""", """{"v":2}""", """{"v":3}""", """{"v":4}""")
    val exec = new JSONLScanSourceOpExec(descString(uri, limit = Some(2)))
    assert(drain(exec).map(_.head) == Seq(0, 1))
  }

  it should "fail loudly when a row cannot be parsed into the inferred schema" in {
    // Type inference samples only the first INFER_READ_LIMIT (=100) rows; those
    // have integer "v" values, so "v" is inferred as INTEGER. Row 101 holds a
    // non-integer, which the inferred schema cannot accept, so the scan must
    // abort loudly rather than dropping the row.
    val cleanRows = (0 until 100).map(i => s"""{"v":$i}""")
    val badRow = """{"v":"oops"}"""
    val exec = new JSONLScanSourceOpExec(descString(writeJsonl((cleanRows :+ badRow): _*)))
    val ex = intercept[RuntimeException](drain(exec))

    // Column-identified message (no row number for this exec): value, column,
    // expected type, then the actionable fix.
    assert(ex.getMessage.startsWith("Value 'oops'")) // the offending value, up front
    assert(ex.getMessage.contains("'v'")) // the offending column's name
    assert(ex.getMessage.contains("cannot be read as"))
    assert(ex.getMessage.contains("INTEGER")) // the inferred/expected type
    assert(ex.getMessage.contains("clean the data before scanning")) // actionable fix
    assert(ex.getCause != null) // original parse exception preserved as the cause
  }

  it should "fall back to a generic row error when a line is not valid JSON" in {
    // A syntactically malformed line yields no fields at all (readTree throws
    // before extraction), so no single offending column can be identified and
    // the generic fallback message must be used. The bad line sits after the
    // first 100 rows so schema inference (which also parses lines) never sees it.
    val cleanRows = (0 until 100).map(i => s"""{"v":$i}""")
    val badRow = """{not valid json"""
    val exec = new JSONLScanSourceOpExec(descString(writeJsonl((cleanRows :+ badRow): _*)))
    val ex = intercept[RuntimeException](drain(exec))

    assert(ex.getMessage.contains("could not be parsed into the inferred schema"))
    assert(ex.getMessage.contains("clean the data before scanning"))
    assert(ex.getCause != null) // original parse exception preserved as the cause
  }
}
