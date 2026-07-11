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

package org.apache.texera.amber.operator.source.scan.csv

import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.nio.charset.StandardCharsets
import java.nio.file.Files

class ParallelCSVScanSourceOpExecSpec extends AnyFlatSpec {

  private def writeCsv(content: String): String = {
    val file = Files.createTempFile("parallel-csv-", ".csv")
    file.toFile.deleteOnExit()
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    file.toFile.toURI.toString
  }

  private def descString(uri: String, delimiter: String = ",", header: Boolean = true): String = {
    val desc = new ParallelCSVScanSourceOpDesc()
    desc.fileName = Some(uri)
    desc.customDelimiter = Some(delimiter)
    desc.hasHeader = header
    desc.setResolvedFileName(new java.net.URI(uri))
    objectMapper.writeValueAsString(desc)
  }

  private def drain(exec: ParallelCSVScanSourceOpExec): List[Seq[Any]] = {
    exec.open()
    try exec.produceTuple().map(_.getFields.toSeq).toList
    finally exec.close()
  }

  "ParallelCSVScanSourceOpExec" should "read every data row of a headered file" in {
    val exec =
      new ParallelCSVScanSourceOpExec(descString(writeCsv("id,name,age\n1,Alice,30\n2,Bob,25\n")))
    val rows = drain(exec)
    assert(rows.size == 2)
    assert(rows.head == Seq(1, "Alice", 30))
    assert(rows(1) == Seq(2, "Bob", 25))
  }

  it should "not skip the first row when the file has no header" in {
    val exec =
      new ParallelCSVScanSourceOpExec(
        descString(writeCsv("1,Alice,30\n2,Bob,25\n"), header = false)
      )
    assert(drain(exec).size == 2)
  }

  it should "pad short rows with trailing nulls to the schema width" in {
    // the wide first data row fixes the schema at 3 columns; the short row is padded
    val exec = new ParallelCSVScanSourceOpExec(descString(writeCsv("a,b,c\n1,2,3\n4,5\n")))
    val rows = drain(exec)
    assert(rows.size == 2)
    assert(rows(1).length == 3)
    assert(rows(1)(2) == null)
  }

  it should "discard all-null (blank) lines" in {
    val exec =
      new ParallelCSVScanSourceOpExec(descString(writeCsv("name,city\nAlice,NYC\n\nBob,LA\n")))
    val rows = drain(exec)
    assert(rows.size == 2)
    assert(rows.map(_.head) == Seq("Alice", "Bob"))
  }

  it should "partition a headerless file across workers by byte range" in {
    // Two workers over the same file: worker 0 (idx != workerCount-1) takes the
    // first byte-range chunk; worker 1 seeks past its chunk start and drops the
    // leading partial line. Rows are deliberately uneven widths so the split
    // point (totalBytes/2 = 12) lands *inside* the second row rather than on a
    // newline, exercising the partial-line skip. Exact boundary rows depend on
    // the block reader reading one line past the boundary, so assert only that
    // the union covers all rows without duplication.
    val uri = writeCsv("1,aaaa\n2,bbb\n3,ccccc\n4,d\n")
    val rows0 = drain(
      new ParallelCSVScanSourceOpExec(descString(uri, header = false), idx = 0, workerCount = 2)
    )
    val rows1 = drain(
      new ParallelCSVScanSourceOpExec(descString(uri, header = false), idx = 1, workerCount = 2)
    )
    val keys = (rows0 ++ rows1).map(_.head)
    assert(keys.toSet == Set(1, 2, 3, 4))
    assert(keys.size == 4) // each row read exactly once
    assert(rows0.nonEmpty && rows1.nonEmpty)
  }
}
