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
  * config. [[CanonicalFixture]] is the wide mixed-type table every operator
  * takes; the sklearn families take [[ProjectedFixture]] views of it.
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
  ): Map[PortIdentity, Path] =
    writeShaped(dir, inputPortCount, rows => if (withGaps) emptyOneCellPerColumn(rows) else rows)

  /** [[write]] with the rows put through `shape` first, so a caller that wants a
    * differently shaped table writes it the same way and gets the same sidecars.
    */
  private def writeShaped(
      dir: Path,
      inputPortCount: Int,
      shape: Seq[Tuple] => Seq[Tuple]
  ): Map[PortIdentity, Path] = {
    require(
      inputPortCount >= 1 && inputPortCount <= 2,
      s"unsupported input port count: $inputPortCount"
    )
    (0 until inputPortCount).map { port =>
      val path = dir.resolve(s"input_port_$port.jsonl")
      TupleIO.writeTuples(path, shape(rowsFor(port)).iterator, schema)
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

  /** The same columns with no rows under them. An upstream filter that matches
    * nothing hands an operator exactly this, and it is not the same table as one
    * with holes in it: a column with no values has no minimum, no quantile and no
    * inferable type, so the code that reads one either answers with an empty
    * result or raises.
    */
  final def writeEmptyInputs(dir: Path, inputPortCount: Int): Map[PortIdentity, Path] =
    writeShaped(dir, inputPortCount, _ => Seq.empty)

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

  /** Every port gets the whole table. What the comparison sees is the fitted
    * model, which port 1 has no hand in, so giving the ports different rows buys
    * nothing.
    *
    * The whole table rather than the source's ten-row window, because a
    * cross-validating estimator passes no fold count and takes sklearn's default
    * of five: the window would leave the smaller class at four, and a fold
    * holding none of a class asks nothing.
    */
  override def rowsFor(port: Int): Seq[Tuple] = rows
}

object HostileColumn {

  /** Columns of [[CanonicalFixture]] whose NAMES hold the characters that end a
    * Python string literal, so an operator pointed at one has to escape the name
    * it writes. They are the table's own columns renamed rather than columns added
    * beside them: the arrangements an operator needs already live here, and a
    * second set under hostile names would be the same table twice.
    */
  private val prefix = "a\"b\\c_"

  /** Three components summing to 100 in every row (the ternary family). */
  val Numeric: Seq[String] = Seq("simplex_a", "simplex_b", "simplex_c").map(prefix + _)

  val IntegerLike: Seq[String] = Seq(prefix + "species_pred")

  /** Carries a single quote and a newline besides, the two that end a `'...'`
    * literal and a `#` comment. Only one column needs them to put the question.
    */
  val Text: Seq[String] = Seq("a\"b'c\\d\ne_uniq_name")

  val Timestamp: Seq[String] = Seq(prefix + "finish_ts")

  val all: Seq[String] = Numeric ++ IntegerLike ++ Text ++ Timestamp

  /** The ones carrying `t`, in the order a caller hands them to siblings. */
  def forType(t: org.apache.texera.amber.core.tuple.AttributeType): Seq[String] = {
    import org.apache.texera.amber.core.tuple.AttributeType._
    t match {
      case STRING         => Text
      case TIMESTAMP      => Timestamp
      case INTEGER | LONG => IntegerLike
      case DOUBLE         => Numeric
      case _              => Seq.empty
    }
  }
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
