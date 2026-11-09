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

package org.apache.texera.amber.operator.source.scan.arrow

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.ipc.ArrowFileWriter
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.ArrowUtils
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.{File, FileOutputStream}
import java.nio.channels.Channels
import java.nio.file.Files

class ArrowSourceOpExecSpec extends AnyFlatSpec with Matchers {

  private val schema = Schema(List(new Attribute("s", AttributeType.STRING)))

  private def writeArrowFile(rows: Seq[String]): File = {
    val file = File.createTempFile("arrow-src-", ".arrow")
    file.deleteOnExit()
    val allocator = new RootAllocator()
    val root = VectorSchemaRoot.create(ArrowUtils.fromTexeraSchema(schema), allocator)
    val out = new FileOutputStream(file)
    val writer = new ArrowFileWriter(root, null, Channels.newChannel(out))
    try {
      writer.start()
      root.allocateNew()
      rows.zipWithIndex.foreach {
        case (value, i) =>
          ArrowUtils.setTexeraTuple(
            Tuple.builder(schema).addSequentially(Array[Any](value)).build(),
            i,
            root
          )
      }
      root.setRowCount(rows.size)
      writer.writeBatch()
      writer.end()
    } finally {
      writer.close()
      root.close()
      allocator.close()
      out.close()
    }
    file
  }

  private def descString(
      file: File,
      offset: Option[Int] = None,
      limit: Option[Int] = None
  ): String = {
    val desc = new ArrowSourceOpDesc()
    desc.fileName = Some(file.toURI.toString)
    desc.offset = offset
    desc.limit = limit
    objectMapper.writeValueAsString(desc)
  }

  private def values(exec: ArrowSourceOpExec): List[String] =
    exec.produceTuple().toList.map(_.asInstanceOf[Tuple].getField[String]("s"))

  "ArrowSourceOpExec" should "read every row of a single-batch Arrow file" in {
    val exec = new ArrowSourceOpExec(descString(writeArrowFile(Seq("a", "b", "c"))))
    exec.open()
    try assert(values(exec) == List("a", "b", "c"))
    finally exec.close()
  }

  it should "apply offset and limit" in {
    val exec =
      new ArrowSourceOpExec(
        descString(writeArrowFile(Seq("a", "b", "c", "d", "e")), Some(1), Some(2))
      )
    exec.open()
    try assert(values(exec) == List("b", "c"))
    finally exec.close()
  }

  it should "wrap failures to open an invalid Arrow file" in {
    val bogus = File.createTempFile("not-arrow-", ".arrow")
    bogus.deleteOnExit()
    Files.write(bogus.toPath, "this is not arrow".getBytes)
    val exec = new ArrowSourceOpExec(descString(bogus))
    val ex = intercept[RuntimeException](exec.open())
    ex.getMessage shouldBe "Failed to open Arrow source"
    ex.getCause should not be null
  }

  it should "treat close before open as a safe no-op" in {
    val exec = new ArrowSourceOpExec(descString(writeArrowFile(Seq("a"))))
    noException should be thrownBy exec.close()
  }
}
