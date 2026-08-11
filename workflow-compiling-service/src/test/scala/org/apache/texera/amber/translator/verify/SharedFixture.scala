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
  * config. [[CanonicalFixture]] is the wide mixed-type table most operators take;
  * the sklearn families share two narrow ones because `X = table.drop(target)`
  * feeds every remaining column to `fit`, which a string or timestamp column ends.
  * Both are shared tables, so both can carry what a shared table makes affordable:
  * a derived table is one derivation for a family rather than one per operator.
  */
trait SharedFixture {

  def schema: Schema

  /** The rows port `port` gets. Ports may take different windows of the table
    * (canonical overlaps them partially, to defeat hash-coincidence passes on
    * joins) or the same rows twice.
    */
  def rowsFor(port: Int): Seq[Tuple]

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

  /** One empty cell per column, spread across rows so no row is wholly empty — an
    * operator that reads two columns should still meet a row where one is filled
    * and the other is not. Placement is by column position, so it is the same on
    * every run.
    */
  private[verify] def emptyOneCellPerColumn(rows: Seq[Tuple]): Seq[Tuple] = {
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
