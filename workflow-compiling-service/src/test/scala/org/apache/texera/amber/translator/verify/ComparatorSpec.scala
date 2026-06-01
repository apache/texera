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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path}

class ComparatorSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema = Schema()
    .add(new Attribute("id", AttributeType.INTEGER))
    .add(new Attribute("name", AttributeType.STRING))

  private val idAttr = new Attribute("id", AttributeType.INTEGER)
  private val nameAttr = new Attribute("name", AttributeType.STRING)

  private def row(id: Int, name: String): Tuple =
    Tuple
      .builder(schema)
      .add(idAttr, Int.box(id))
      .add(nameAttr, name)
      .build()

  private def writeJsonl(dir: Path, name: String, rows: Seq[Tuple]): Path = {
    val p = dir.resolve(name)
    TupleIO.writeTuples(p, rows.iterator, schema)
    p
  }

  "Comparator.assertEqual" should "pass when JSONL files contain identical rows" in {
    val dir = Files.createTempDirectory("comparator-spec-equal-")
    val rows = Seq(row(1, "alice"), row(2, "bob"))
    val a = writeJsonl(dir, "a.jsonl", rows)
    val b = writeJsonl(dir, "b.jsonl", rows)
    noException should be thrownBy Comparator.assertEqual(a, b)
  }

  it should "throw ComparatorMismatchException when JSONL files differ" in {
    val dir = Files.createTempDirectory("comparator-spec-diff-")
    val a = writeJsonl(dir, "a.jsonl", Seq(row(1, "alice"), row(2, "bob")))
    val b = writeJsonl(dir, "b.jsonl", Seq(row(1, "alice"), row(2, "carol")))
    intercept[ComparatorMismatchException] {
      Comparator.assertEqual(a, b)
    }
  }
}
