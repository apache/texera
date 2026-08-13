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

import org.apache.texera.amber.core.tuple.{Schema, Tuple}
import org.apache.texera.amber.core.workflow.PortIdentity

import java.nio.file.Path

/**
  * A checked-in table a whole FAMILY of operators runs on, as opposed to one
  * written for a single operator.
  *
  * Which table an operator runs on is its own axis, separate from who writes its
  * config. [[CanonicalFixture]] is the wide mixed-type table every operator takes;
  * the sklearn families take [[ProjectedFixture]] views of it, since
  * `X = table.drop(target)` feeds every remaining column to `fit`, which a string
  * or timestamp column ends.
  */
trait SharedFixture {

  def schema: Schema

  /** The rows port `port` gets. Ports may take different windows of the table
    * (canonical overlaps them partially, to defeat hash-coincidence passes on
    * joins) or the same rows twice.
    */
  def rowsFor(port: Int): Seq[Tuple]

  /** Every row of the table, ports aside — what a [[ProjectedFixture]] of it
    * narrows. A table whose ports read windows says so by overriding; by default
    * a port already sees the whole of it.
    */
  def allRows: Seq[Tuple] = rowsFor(0)

  /** Columns [[write]] never empties, because their VALUE is what the table was
    * built to arrange rather than data under test: canonical's `id` is what joins
    * and set operations pair rows on, and a sklearn table's label is what its
    * estimator fits against. Emptying one of those changes what the test asks
    * instead of asking what an operator does with a null.
    */
  def keepFilled: Set[String]

  /** Write one JSONL file per 0-based input port under `dir`. At most 2 ports. */
  final def write(
      dir: Path,
      inputPortCount: Int,
      withGaps: Boolean
  ): Map[PortIdentity, Path] = {
    require(
      inputPortCount >= 1 && inputPortCount <= 2,
      s"unsupported input port count: $inputPortCount"
    )
    (0 until inputPortCount).map { port =>
      val rows = rowsFor(port)
      val path = dir.resolve(s"input_port_$port.jsonl")
      TupleIO.writeTuples(
        path,
        (if (withGaps) emptyOneCellPerColumn(rows) else rows).iterator,
        schema
      )
      PortIdentity(port) -> path
    }.toMap
  }

  /** Schemas ConfigGenerator resolves @AutofillAttributeName fields against.
    * Every port sees the same columns: a fixture's ports differ in which ROWS
    * they get, not in shape.
    */
  final def schemasByPort: Map[Int, Schema] = Map(0 -> schema, 1 -> schema)

  /** Write one JSONL fixture per 0-based input port, every cell filled. */
  final def writeInputs(dir: Path, inputPortCount: Int): Map[PortIdentity, Path] =
    write(dir, inputPortCount, withGaps = false)

  /** How many rows port 0 gets — what a row-count-sensitive knob (`limit`,
    * `offset`) is sized against so its value keeps some rows and drops some.
    */
  final def port0RowCount: Int = rowsFor(0).size

  /** This table's rows with [[SharedFixture.emptyOneCellPerColumn]] applied. */
  private[verify] def emptyOneCellPerColumn(rows: Seq[Tuple]): Seq[Tuple] =
    SharedFixture.emptyOneCellPerColumn(rows, schema, keepFilled)
}

/**
  * A column subset of another table: the same rows in the same order, keeping
  * only the named columns, in the order named.
  *
  * The sklearn families need one. Their generated code is
  * `X = table.drop(target, axis=1)`, so every column that is not the target
  * reaches `fit`, and a string or a timestamp ends it. A projection hands them a
  * table an estimator can fit without a second dataset to keep in step: the rows
  * are still [[CanonicalFixture]]'s, only narrower.
  */
final case class ProjectedFixture(
    source: SharedFixture,
    columns: Seq[String],
    keepFilled: Set[String]
) extends SharedFixture {

  val schema: Schema = new Schema(columns.map(c => source.schema.getAttribute(c)): _*)

  private val rows: Vector[Tuple] = source.allRows.map { t =>
    val b = Tuple.builder(schema)
    schema.getAttributes.foreach(a => b.add(a, t.getField[AnyRef](a.getName)))
    b.build()
  }.toVector

  /** Every port gets the whole table. An estimator pair trains on port 0 and
    * tests on port 1, and the point of the pair is the two ports rather than two
    * datasets: what the comparison sees is the fitted model, which port 1 has no
    * hand in, so giving the ports different rows buys nothing.
    *
    * The whole table rather than the source's ten-row window, because the
    * estimators that cross-validate pass no fold count and so take sklearn's
    * default of five: the window would leave the smaller class at four, and one
    * fold holding none of a class is a fold that asks nothing (sklearn warns and
    * splits anyway rather than refusing).
    */
  override def rowsFor(port: Int): Seq[Tuple] = rows
}

object SharedFixture {

  /** One empty cell per column, spread across rows so no row is wholly empty — an
    * operator that reads two columns should still meet a row where one is filled
    * and the other is not. Placement is by column position, so it is the same on
    * every run.
    *
    * Free-standing rather than a member, because a curated handler's table has no
    * [[SharedFixture]] behind it: the runner reads back the rows the handler wrote
    * and punches the holes here.
    */
  def emptyOneCellPerColumn(
      rows: Seq[Tuple],
      schema: Schema,
      keepFilled: Set[String]
  ): Seq[Tuple] = {
    if (rows.isEmpty) return rows
    val holes: Map[Int, Set[String]] = schema.getAttributes.zipWithIndex
      .filterNot { case (attr, _) => keepFilled.contains(attr.getName) }
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
