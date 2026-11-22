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

import com.github.tototoshi.csv.MalformedCSVException
import org.apache.texera.amber.core.tuple.AttributeType
import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.Files

class CSVOldScanSourceOpExecSpec extends AnyFlatSpec {

  private def descString(
      content: String,
      header: Boolean = true,
      delimiter: String = ",",
      limit: Option[Int] = None,
      offset: Option[Int] = None
  ): String = {
    val file = Files.createTempFile("csvold-", ".csv")
    file.toFile.deleteOnExit()
    Files.write(file, content.getBytes(StandardCharsets.UTF_8))
    val desc = new CSVOldScanSourceOpDesc
    desc.setResolvedFileName(file.toFile.toURI)
    desc.customDelimiter = Some(delimiter)
    desc.hasHeader = header
    desc.fileEncoding = FileDecodingMethod.UTF_8
    desc.limit = limit
    desc.offset = offset
    objectMapper.writeValueAsString(desc)
  }

  private def drain(exec: CSVOldScanSourceOpExec): List[Seq[Any]] = {
    exec.open()
    try exec.produceTuple().map(_.getFields.toSeq).toList
    finally exec.close()
  }

  "CSVOldScanSourceOpExec" should "infer the schema from the header at construction" in {
    val exec = new CSVOldScanSourceOpExec(descString("a,b\n1,2\n3,4\n", header = true))
    assert(exec.desc.hasHeader)
    assert(exec.schema.getAttributeNames == List("a", "b"))
  }

  it should "read every data row, skipping the header" in {
    val exec = new CSVOldScanSourceOpExec(descString("a,b\n1,2\n3,4\n", header = true))
    val rows = drain(exec)
    assert(rows.size == 2)
    assert(rows.head == Seq(1, 2))
    assert(rows(1) == Seq(3, 4))
  }

  it should "not skip the first row when the file has no header" in {
    val exec = new CSVOldScanSourceOpExec(descString("1,2\n3,4\n", header = false))
    assert(drain(exec).size == 2)
  }

  it should "honor the row limit" in {
    val exec = new CSVOldScanSourceOpExec(descString("a,b\n1,2\n3,4\n5,6\n", limit = Some(1)))
    assert(drain(exec).size == 1)
  }

  it should "treat close before open as a no-op" in {
    val exec = new CSVOldScanSourceOpExec(descString("a\n1\n"))
    exec.close() // reader is still null -> guarded, no exception
  }

  it should "skip a post-inference row that does not parse and report row, value, column, type" in {
    // 100 clean integer rows fix the column type at INTEGER (INFER_READ_LIMIT=100);
    // data row 101 holds a non-integer and must be skipped but reported.
    val clean = (1 to 100).map(_.toString).mkString("\n")
    val exec = new CSVOldScanSourceOpExec(descString(s"a\n$clean\noops\n"))
    val rows = drain(exec)

    assert(rows.size == 100)
    val warnings = exec.getWarnings
    assert(warnings.size == 1)
    assert(warnings.head.startsWith("WARNING: "))
    assert(warnings.head.contains("row 101"))
    assert(warnings.head.contains("'oops'"))
    assert(warnings.head.contains("column 'a'"))
    assert(warnings.head.contains("INTEGER"))
  }

  it should "report the absolute data-row number when an offset is set" in {
    // offset=2 shifts both inference and reading to data row 3; the inference sample
    // (rows 3..102) is all integers, so "oops" at data row 103 is skipped. The
    // reported number must count from the start of the data, not from the offset.
    val clean = (1 to 102).map(_.toString).mkString("\n")
    val exec = new CSVOldScanSourceOpExec(descString(s"a\n$clean\noops\n", offset = Some(2)))
    val rows = drain(exec)

    assert(rows.size == 100)
    assert(exec.getWarnings.size == 1)
    assert(exec.getWarnings.head.contains("row 103"))
  }

  it should "remain exhausted after close for every scan limit" in {
    for (limit <- List(None, Some(0), Some(1), Some(2), Some(3))) {
      val exec = new CSVOldScanSourceOpExec(
        descString("id,name\n1,猫\n2,\"line one\nline two\"\n", limit = limit)
      )
      assert(exec.schema.getAttributeNames == List("id", "name"))
      assert(
        exec.schema.getAttributes.map(_.getType) == List(
          AttributeType.INTEGER,
          AttributeType.STRING
        )
      )
      exec.open()
      val output = exec.produceTuple()
      try {
        val expected = List(Seq(1, "猫"), Seq(2, "line one\nline two"))
        assert(output.map(_.getFields.toSeq).toList == expected.take(limit.getOrElse(2)))
        assert(!output.hasNext)
        exec.close()
        (1 to 3).foreach(_ => assert(!output.hasNext))
        intercept[NoSuchElementException](output.next())
      } finally exec.close()
    }
  }

  it should "remain exhausted without a header and reset the iterator when reopened" in {
    val exec = new CSVOldScanSourceOpExec(descString("1,2\n3,4\n", header = false))
    for (_ <- 1 to 2) {
      exec.open()
      val output = exec.produceTuple()
      try {
        assert(output.map(_.getFields.toSeq).toList == List(Seq(1, 2), Seq(3, 4)))
        exec.close()
        (1 to 3).foreach(_ => assert(!output.hasNext))
      } finally exec.close()
    }
  }

  it should "preserve empty-file failure and the header-only schema after EOF" in {
    intercept[java.util.NoSuchElementException](new CSVOldScanSourceOpExec(descString("")))
    val exec = new CSVOldScanSourceOpExec(descString("a,b\n"))
    assert(exec.schema.getAttributeNames == List("a", "b"))
    assert(
      exec.schema.getAttributes.map(_.getType) == List(AttributeType.STRING, AttributeType.STRING)
    )
    exec.open()
    val output = exec.produceTuple()
    try {
      assert(!output.hasNext)
      exec.close()
      assert(!output.hasNext)
    } finally exec.close()
  }

  it should "propagate a reader failure before EOF has been observed" in {
    val exec = new CSVOldScanSourceOpExec(descString("a,b\n1,2\n3,4\n"))
    exec.open()
    val output = exec.produceTuple()
    exec.close()
    intercept[IOException](output.hasNext)
    intercept[IOException](output.hasNext)
  }

  it should "propagate malformed quoted CSV after the inference sample" in {
    val clean = (1 to 100).map(_.toString).mkString("\n")
    val exec = new CSVOldScanSourceOpExec(descString(s"a\n$clean\n" + "\"unterminated\n"))
    exec.open()
    val output = exec.produceTuple()
    try {
      assert(
        (1 to 100).map(_ => output.next().getFields.toSeq).toList ==
          (1 to 100).map(i => Seq(i)).toList
      )
      intercept[MalformedCSVException](output.hasNext)
      assert(exec.getWarnings.isEmpty)
    } finally exec.close()
  }

  it should "preserve short rows and report rejected values before remaining exhausted" in {
    val clean = (1 to 100).map(i => s"$i,$i").mkString("\n")
    val exec = new CSVOldScanSourceOpExec(descString(s"a,b\n$clean\n101\noops,102\n103,103\n"))
    exec.open()
    val output = exec.produceTuple()
    try {
      val expected = (1 to 100).map(i => Seq(i, i)).toList ++ List(Seq(101), Seq(103, 103))
      assert(output.map(_.getFields.toSeq).toList == expected)
      assert(exec.getWarnings.size == 1)
      assert(exec.getWarnings.head.contains("row 102"))
      assert(exec.getWarnings.head.contains("'oops'"))
      exec.close()
      assert(!output.hasNext)
    } finally exec.close()
  }
}
