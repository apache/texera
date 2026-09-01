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

package org.apache.texera.amber.translator.verify

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.distinct.DistinctOpDesc
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

/** The two ways of running one operator, and the file format they meet in.
  *
  * `Distinct` is the operator under test throughout, because what is being
  * tested is the harness rather than the operator: it takes one input, needs no
  * configuration, and its answer is short enough to state in full.
  */
class HarnessSpec extends AnyFlatSpec with Matchers {

  private val schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING)
  )

  private def tuple(id: Int, name: String): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), name)
    b.build()
  }

  /** Four rows, the last a repeat of the second. */
  private val rows = Seq(tuple(1, "a"), tuple(2, "b"), tuple(3, "c"), tuple(2, "b"))

  private def withInput(test: (Path, Path) => Unit): Unit = {
    val dir = Files.createTempDirectory("harness-spec-")
    val input = dir.resolve("input_port_0.jsonl")
    TupleIO.writeTuples(input, rows.iterator, schema)
    test(dir, input)
  }

  "TupleIO" should "read back the rows and the schema it wrote" in {
    withInput { (_, input) =>
      // The schema travels in a sidecar rather than in the JSONL, which carries
      // values alone and so cannot say a column is INTEGER rather than a number.
      TupleIO.readSchemaSidecar(input) shouldBe schema
      val read = TupleIO.readTuples(input, schema).toSeq
      read should have length 4
      read.map(_.getField[Integer]("id").intValue) shouldBe Seq(1, 2, 3, 2)
    }
  }

  "OpExecHarness" should "run an operator and write one file per output port" in {
    withInput { (dir, input) =>
      val out = dir.resolve("actual")
      val result =
        OpExecHarness.execute(new DistinctOpDesc, Map(PortIdentity(0) -> input), out)

      result.outputs should have size 1
      val produced = result.outputs(PortIdentity(0))
      Files.exists(produced) shouldBe true

      val written = TupleIO.readTuples(produced, result.outputSchemas(PortIdentity(0))).toSeq
      written.map(_.getField[Integer]("id").intValue) shouldBe Seq(1, 2, 3)
    }
  }

  "StandaloneRunner" should "run the generated script and reach the same answer" in {
    withInput { (dir, input) =>
      val work = dir.resolve("standalone")
      Files.createDirectories(work)
      val result = StandaloneRunner.run(
        opDesc = new DistinctOpDesc,
        inputs = Map(1 -> input),
        outputPortCount = 1,
        workDir = work
      )

      // The script is kept where it ran, so a failing operator can be opened as
      // generated rather than described second-hand.
      Files.exists(work.resolve("script.py")) shouldBe true

      val produced = result.outputs(1)
      val lines = Files.readAllLines(produced)
      lines should have size 3
      lines.get(0) should include("\"id\":1")
      lines.get(2) should include("\"id\":3")
    }
  }
}
