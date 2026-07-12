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

import org.apache.texera.amber.operator.source.scan.FileDecodingMethod
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec

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
}
