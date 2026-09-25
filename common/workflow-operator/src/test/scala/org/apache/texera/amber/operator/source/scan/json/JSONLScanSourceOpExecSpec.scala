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

  it should "start at the offset and keep every row after it" in {
    val uri = writeJsonl("""{"v":0}""", """{"v":1}""", """{"v":2}""", """{"v":3}""", """{"v":4}""")
    val exec = new JSONLScanSourceOpExec(descString(uri, offset = Some(2)))
    assert(drain(exec).map(_.head) == Seq(2, 3, 4))
  }

  it should "apply the limit relative to the offset" in {
    val uri = writeJsonl("""{"v":0}""", """{"v":1}""", """{"v":2}""", """{"v":3}""", """{"v":4}""")
    // The window is shorter than the offset itself, which used to empty it out.
    val exec = new JSONLScanSourceOpExec(descString(uri, limit = Some(2), offset = Some(2)))
    assert(drain(exec).map(_.head) == Seq(2, 3))
  }

  it should "split the offset window across workers, losing no row to either end" in {
    val uri = writeJsonl("""{"v":0}""", """{"v":1}""", """{"v":2}""", """{"v":3}""", """{"v":4}""")
    val desc = descString(uri, offset = Some(1))
    val worker0 = new JSONLScanSourceOpExec(desc, idx = 0, workerCount = 2)
    val worker1 = new JSONLScanSourceOpExec(desc, idx = 1, workerCount = 2)
    assert(drain(worker0).map(_.head) == Seq(1, 2))
    assert(drain(worker1).map(_.head) == Seq(3, 4))
  }

  it should "give the last worker the remainder of the offset-and-limit window" in {
    val uri = writeJsonl(
      """{"v":0}""",
      """{"v":1}""",
      """{"v":2}""",
      """{"v":3}""",
      """{"v":4}""",
      """{"v":5}""",
      """{"v":6}"""
    )
    // Four rows over three workers: one each, and the odd row goes to the last.
    val desc = descString(uri, limit = Some(4), offset = Some(2))
    val workers =
      (0 until 3).map(i => new JSONLScanSourceOpExec(desc, idx = i, workerCount = 3))
    assert(workers.map(drain(_).map(_.head)) == Seq(Seq(2), Seq(3), Seq(4, 5)))
  }

  it should "skip a post-inference line whose value does not match the inferred type and report it" in {
    // 100 clean integer lines fix "v" at INTEGER (INFER_READ_LIMIT=100); line 101
    // holds a string and must be skipped but reported with its absolute line number.
    val clean = (1 to 100).map(i => s"""{"v":$i}""")
    val exec = new JSONLScanSourceOpExec(descString(writeJsonl(clean :+ """{"v":"oops"}""": _*)))
    val rows = drain(exec)

    assert(rows.size == 100)
    val warnings = exec.getWarnings
    assert(warnings.size == 1)
    assert(warnings.head.startsWith("WARNING: "))
    assert(warnings.head.contains("row 101"))
    assert(warnings.head.contains("'oops'"))
    assert(warnings.head.contains("column 'v'"))
    assert(warnings.head.contains("INTEGER"))
  }

  it should "skip a malformed JSON line and report it with the generic fallback" in {
    val clean = (1 to 100).map(i => s"""{"v":$i}""")
    val exec = new JSONLScanSourceOpExec(descString(writeJsonl(clean :+ """{"v": not json""": _*)))
    val rows = drain(exec)

    assert(rows.size == 100)
    val warnings = exec.getWarnings
    assert(warnings.size == 1)
    assert(warnings.head.contains("row 101"))
    assert(warnings.head.contains("could not be parsed into the inferred schema"))
  }

  it should "report the absolute line number when an offset is set" in {
    // offset=2 skips lines 1-2; inference then samples lines 3-102 (100 clean
    // integers), and the bad value sits at file line 103.
    val clean = (1 to 102).map(i => s"""{"v":$i}""")
    val exec = new JSONLScanSourceOpExec(
      descString(writeJsonl(clean :+ """{"v":"oops"}""": _*), offset = Some(2))
    )
    assert(drain(exec).size == 100)
    assert(exec.getWarnings.head.contains("row 103"))
  }
}
