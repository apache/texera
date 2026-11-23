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

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File

class ArrowSourceOpExecSpec extends AnyFlatSpec with Matchers {

  private val schema = Schema(List(new Attribute("s", AttributeType.STRING)))

  /** Writes one record batch per argument, so a file can hold several or none. */
  private def writeArrowFile(batches: Seq[String]*): File =
    ArrowSourceFixtures.writeArrowBatches(schema, batches.map(_.map(value => Array[Any](value))))

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

  it should "read every record batch in order, each row with the file's schema" in {
    val exec =
      new ArrowSourceOpExec(descString(writeArrowFile(Seq("a", "b"), Seq("c"), Seq("d", "e"))))
    exec.open()
    try {
      val tuples = exec.produceTuple().toList.map(_.asInstanceOf[Tuple])
      tuples.map(_.getField[String]("s")) shouldBe List("a", "b", "c", "d", "e")
      tuples.map(_.getSchema).distinct shouldBe List(schema)
    } finally exec.close()
  }

  it should "apply offset and limit across record batches" in {
    val exec = new ArrowSourceOpExec(
      descString(writeArrowFile(Seq("a", "b"), Seq("c"), Seq("d", "e")), Some(1), Some(3))
    )
    exec.open()
    try assert(values(exec) == List("b", "c", "d"))
    finally exec.close()
  }

  it should "read no rows from a file without record batches" in {
    val exec = new ArrowSourceOpExec(descString(writeArrowFile()))
    exec.open()
    try assert(values(exec).isEmpty)
    finally exec.close()
  }

  it should "wrap failures to open an invalid Arrow file" in {
    val bogus = ArrowSourceFixtures.invalidArrowFile()
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
