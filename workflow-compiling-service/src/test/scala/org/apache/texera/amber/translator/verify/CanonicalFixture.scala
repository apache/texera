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

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity

import java.nio.file.Path
import java.sql.Timestamp
import scala.jdk.CollectionConverters._

/**
  * The shared input dataset for auto-configured transform verification.
  * Properties are deliberate (see CanonicalFixtureSpec): enough rows and
  * partial port-0/port-1 overlap to defeat hash-coincidence false passes on
  * set ops and joins, and the canonical value "1" present in some-but-not-all
  * rows so ConfigGenerator-filled free-form predicates match a proper subset.
  */
object CanonicalFixture {

  // Columns are semantically named and type-correct so @SampleColumn-tagged or
  // type-constrained fields can be filled with realistic input (a valid OHLC
  // block, real ISO country codes, real dates) instead of a degenerate
  // first-column pick. Ordering is deliberate: id/name/score lead so the
  // first-column fallback AND the type-rule tier ("first column of a matching
  // type") are unchanged for un-annotated fields — the domain-specific columns
  // that follow are only reached via an explicit @SampleColumn.
  val schema: Schema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("name", AttributeType.STRING),
    new Attribute("score", AttributeType.DOUBLE),
    new Attribute("open", AttributeType.DOUBLE),
    new Attribute("high", AttributeType.DOUBLE),
    new Attribute("low", AttributeType.DOUBLE),
    new Attribute("close", AttributeType.DOUBLE),
    new Attribute("iso_country", AttributeType.STRING),
    new Attribute("trade_date", AttributeType.STRING),
    // --- Domain-specific columns (reached only via @SampleColumn) ---
    new Attribute("pvalue", AttributeType.DOUBLE), // strictly in (0,1): p-values
    new Attribute("log2fc", AttributeType.DOUBLE), // signed, centered on 0: fold-change
    new Attribute("comp_a", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("comp_b", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("comp_c", AttributeType.DOUBLE), // >0 ternary simplex component
    new Attribute("uvec", AttributeType.DOUBLE), // any real: a 4th numeric (Quiver u/v)
    new Attribute(
      "edge_pair",
      AttributeType.STRING
    ), // "[parent, child]" literals, single-rooted tree
    new Attribute("node_src", AttributeType.STRING), // edge source id (Sankey/Network)
    new Attribute("node_dst", AttributeType.STRING), // edge target id, overlaps node_src (a DAG)
    new Attribute(
      "start_ts",
      AttributeType.TIMESTAMP
    ), // real timestamp; Gantt start / TimeSeries axis
    new Attribute(
      "finish_ts",
      AttributeType.TIMESTAMP
    ), // always > start_ts; Gantt finish (bar width)
    new Attribute(
      "uniq_name",
      AttributeType.STRING
    ), // distinct per row: Pie/name-keyed ops need no duplicates
    new Attribute(
      "simplex_a",
      AttributeType.DOUBLE
    ), // >0 and simplex_a+simplex_b+simplex_c == 100 (ternary-contour)
    new Attribute("simplex_b", AttributeType.DOUBLE), // >0 simplex component summing to 100
    new Attribute("simplex_c", AttributeType.DOUBLE), // >0 simplex component summing to 100
    // ── text + iris-numeric columns for Hugging Face model operators ──
    new Attribute(
      "short_text",
      AttributeType.STRING
    ), // one sentence: sentiment / spam-detection input
    new Attribute(
      "long_text",
      AttributeType.STRING
    ), // a multi-sentence paragraph: summarization input
    new Attribute("petal_length", AttributeType.DOUBLE), // iris petal length in cm (~1.3–6.5)
    new Attribute("petal_width", AttributeType.DOUBLE), // iris petal width in cm (~0.2–2.4)
    new Attribute(
      "csv_list",
      AttributeType.STRING
    ), // comma-delimited, 1–4 tokens per row: split/explode ops need real fan-out
    new Attribute(
      "mixed_case",
      AttributeType.STRING
    ) // a third lower-case, a third upper, a third letterless: a case flag has to
    // change WHICH rows match, and on any other column it changes nothing
  )

  /** Schemas ConfigGenerator resolves @AutofillAttributeName fields against. */
  val schemasByPort: Map[Int, Schema] = Map(0 -> schema, 1 -> schema)

  // ── Data source ──
  // The rows are NOT generated at runtime — they live in a single, checked-in,
  // human-readable JSON file that IS the source of truth:
  //   src/test/resources/verify/canonical_fixture.json   (15 rows, ids 1..15)
  // Open it to see the exact table; edit it to change the data. The
  // CanonicalFixtureSpec invariants guard every semantic constraint (valid OHLC
  // block, pvalue ∈ (0,1), ternary parts summing to 100, finish_ts > start_ts,
  // etc.), so a hand-edit that breaks one fails the build. `schema` above stays
  // authoritative for column types: JSON has no TIMESTAMP, so start_ts/finish_ts
  // are stored as JDBC strings ("2024-01-01 00:00:00.0") and coerced back here.
  private val fixtureResource = "/verify/canonical_fixture.json"

  private val allRows: Vector[Tuple] = {
    val stream = Option(getClass.getResourceAsStream(fixtureResource))
      .getOrElse(sys.error(s"canonical fixture not found on classpath: $fixtureResource"))
    val root =
      try new ObjectMapper().readTree(stream)
      finally stream.close()
    root
      .elements()
      .asScala
      .map { node =>
        val b = Tuple.builder(schema)
        schema.getAttributes.foreach { attr =>
          val cell = node.get(attr.getName)
          require(cell != null, s"fixture row missing column '${attr.getName}'")
          val value: AnyRef = attr.getType match {
            case AttributeType.INTEGER   => Int.box(cell.asInt())
            case AttributeType.LONG      => Long.box(cell.asLong())
            case AttributeType.DOUBLE    => Double.box(cell.asDouble())
            case AttributeType.BOOLEAN   => Boolean.box(cell.asBoolean())
            case AttributeType.TIMESTAMP => Timestamp.valueOf(cell.asText())
            case _                       => cell.asText() // STRING
          }
          b.add(attr, value)
        }
        b.build()
      }
      .toVector
  }

  // Each port is a 10-row window over the 15-row table (kept small to minimize
  // per-test Python runtime), overlapping by 5 rows so joins and set ops can't
  // pass by hash coincidence. Rows sit out of id order in the file, so the
  // windows are positional — not id ranges.
  def port0Rows: Seq[Tuple] = allRows.slice(0, 10)
  def port1Rows: Seq[Tuple] = allRows.slice(5, 15)

  /** Write one JSONL fixture per 0-based input port. At most 2 ports. */
  def writeInputs(testRoot: Path, inputPortCount: Int): Map[PortIdentity, Path] =
    writeInputs(testRoot, inputPortCount, withGaps = false)

  /** As [[writeInputs]], but with one cell per column emptied.
    *
    * The table above has every cell filled, so nothing in the default run says
    * what an operator does with a value that isn't there — and an empty cell is
    * ordinary input: a blank in a CSV reaches an operator as null, which
    * `AttributeTypeUtils.parseField` passes through by design. The two paths do
    * not agree on it for free, since a JVM null and a pandas NaN are different
    * things, so it takes a fixture of its own to hold them to it.
    *
    * `id` keeps every value: it is what joins and set operations match on, and
    * emptying it would change which rows pair up rather than what a null does.
    */
  def writeInputsWithGaps(testRoot: Path, inputPortCount: Int): Map[PortIdentity, Path] =
    writeInputs(testRoot, inputPortCount, withGaps = true)

  private def writeInputs(
      testRoot: Path,
      inputPortCount: Int,
      withGaps: Boolean
  ): Map[PortIdentity, Path] = {
    require(
      inputPortCount >= 1 && inputPortCount <= 2,
      s"unsupported input port count: $inputPortCount"
    )
    (0 until inputPortCount).map { port =>
      val rows = if (port == 0) port0Rows else port1Rows
      val path = testRoot.resolve(s"input_port_$port.jsonl")
      TupleIO.writeTuples(
        path,
        (if (withGaps) emptyOneCellPerColumn(rows) else rows).iterator,
        schema
      )
      PortIdentity(port) -> path
    }.toMap
  }

  /** One empty cell per column, spread across rows so no row is wholly empty —
    * an operator that reads two columns should still meet a row where one is
    * filled and the other is not. Placement is by column position, so it is the
    * same on every run.
    */
  private[verify] def emptyOneCellPerColumn(rows: Seq[Tuple]): Seq[Tuple] = {
    val holes: Map[Int, Set[String]] = schema.getAttributes.zipWithIndex
      .filter { case (attr, _) => attr.getName != "id" }
      .map { case (attr, i) => (i % rows.size) -> attr.getName }
      .groupBy(_._1)
      .map { case (row, pairs) => row -> pairs.map(_._2).toSet }
    rows.zipWithIndex.map {
      case (t, rowIdx) =>
        val emptied = holes.getOrElse(rowIdx, Set.empty)
        if (emptied.isEmpty) t
        else {
          val b = Tuple.builder(schema)
          schema.getAttributes.foreach { attr =>
            val v: AnyRef =
              if (emptied.contains(attr.getName)) null else t.getField[AnyRef](attr.getName)
            b.add(attr, v)
          }
          b.build()
        }
    }
  }
}
