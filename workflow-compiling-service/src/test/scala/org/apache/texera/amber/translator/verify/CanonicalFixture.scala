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

import java.nio.file.Path

/**
  * The shared input dataset for auto-configured transform verification.
  * Properties are deliberate (see CanonicalFixtureSpec): enough rows and
  * partial port-0/port-1 overlap to defeat hash-coincidence false passes on
  * set ops and joins, and the canonical value "1" present in some-but-not-all
  * rows so ConfigGenerator-filled free-form predicates match a proper subset.
  */
object CanonicalFixture {

  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE)
  )

  /** Schemas ConfigGenerator resolves @AutofillAttributeName fields against. */
  val schemasByPort: Map[Int, Schema] = Map(0 -> schema, 1 -> schema)

  // "1" appears twice per cycle so it recurs but never dominates.
  private val names =
    Vector("1", "alice", "bob", "carol", "dave", "1", "eve", "frank", "grace", "heidi")

  private def tup(id: Int): Tuple = {
    val b = Tuple.builder(schema)
    b.add(schema.getAttribute("id"), Int.box(id))
    b.add(schema.getAttribute("name"), names((id - 1) % names.size))
    b.add(schema.getAttribute("score"), Double.box(id * 0.5))
    b.build()
  }

  def port0Rows: Seq[Tuple] = (1 to 25).map(tup)
  def port1Rows: Seq[Tuple] = (10 to 40).map(tup)

  /** Write one JSONL fixture per 0-based input port. At most 2 ports. */
  def writeInputs(testRoot: Path, inputPortCount: Int): Map[PortIdentity, Path] = {
    require(
      inputPortCount >= 1 && inputPortCount <= 2,
      s"unsupported input port count: $inputPortCount"
    )
    (0 until inputPortCount).map { port =>
      val rows = if (port == 0) port0Rows else port1Rows
      val path = testRoot.resolve(s"input_port_$port.jsonl")
      TupleIO.writeTuples(path, rows.iterator, schema)
      PortIdentity(port) -> path
    }.toMap
  }
}
