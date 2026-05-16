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

package org.apache.texera.amber.core.storage.result.iceberg

import org.apache.iceberg.Schema
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.types.Type
import org.apache.iceberg.types.Types
import org.apache.texera.amber.core.storage.model.ColumnFilter

/**
  * Translates [[ColumnFilter]]s (string-typed-over-the-wire) into Iceberg [[Expression]]s
  * suitable for predicate pushdown via `Scan.filter(...)`.
  *
  * Iceberg pushes the resulting predicates into the Parquet reader and prunes whole
  * data files using min/max stats, so accurate type parsing here is the single biggest
  * lever for read performance on filtered queries. Filters whose semantics Iceberg
  * cannot express (`contains`, `endsWith`) are reported back to the caller via
  * [[buildPushdownAndResidual]] so the caller can run them as an in-memory pass.
  */
object IcebergPredicateBuilder {

  /** Operators we can fully express as an Iceberg [[Expression]]. */
  private val PushdownOps: Set[String] =
    Set("eq", "ne", "lt", "le", "gt", "ge", "startsWith", "isNull", "isNotNull", "in")

  /** Operators we evaluate after the scan because Iceberg has no native equivalent. */
  private val ResidualOps: Set[String] = Set("contains", "endsWith")

  case class ParseError(columnName: String, value: String, expectedType: String) extends RuntimeException(
        s"Cannot interpret '$value' as $expectedType for column '$columnName'"
      )

  /**
    * Split the filters into (pushdownExpression, residualFilters):
    *   - pushdownExpression: ANDed Iceberg predicate suitable for `Scan.filter`
    *   - residualFilters: filters that must be applied in memory over scan output
    *
    * Throws [[ParseError]] for malformed values so the caller can surface a typed
    * error back to the UI rather than silently returning wrong rows.
    */
  def buildPushdownAndResidual(
      filters: Seq[ColumnFilter],
      schema: Schema
  ): (Option[Expression], Seq[ColumnFilter]) = {
    if (filters.isEmpty) return (None, Seq.empty)

    val pushable = filters.filter(f => PushdownOps.contains(f.op))
    val residual = filters.filterNot(f => PushdownOps.contains(f.op))

    residual.foreach { f =>
      if (!ResidualOps.contains(f.op)) {
        throw new IllegalArgumentException(s"Unsupported filter op '${f.op}' on column '${f.columnName}'")
      }
    }

    val expression = pushable
      .map(toExpression(_, schema))
      .reduceOption[Expression]((acc, e) => Expressions.and(acc, e))

    (expression, residual)
  }

  /**
    * Convert a single pushdown-capable filter to an Iceberg [[Expression]].
    * Caller is responsible for filtering to PushdownOps first.
    */
  def toExpression(filter: ColumnFilter, schema: Schema): Expression = {
    val field = Option(schema.findField(filter.columnName))
      .getOrElse(throw new IllegalArgumentException(s"Unknown column: ${filter.columnName}"))
    val icebergType = field.`type`()

    filter.op match {
      case "isNull"    => Expressions.isNull(filter.columnName)
      case "isNotNull" => Expressions.notNull(filter.columnName)
      case "in" =>
        val parsed = filter.values
          .getOrElse(
            throw new IllegalArgumentException(s"`in` filter requires `values` (column: ${filter.columnName})")
          )
          .map(v => parseValue(filter.columnName, v, icebergType))
        Expressions.in(filter.columnName, parsed: _*)
      case op =>
        val raw = filter.value.getOrElse(
          throw new IllegalArgumentException(s"`$op` filter requires `value` (column: ${filter.columnName})")
        )
        val parsed = parseValue(filter.columnName, raw, icebergType)
        op match {
          case "eq"         => Expressions.equal(filter.columnName, parsed)
          case "ne"         => Expressions.notEqual(filter.columnName, parsed)
          case "lt"         => Expressions.lessThan(filter.columnName, parsed)
          case "le"         => Expressions.lessThanOrEqual(filter.columnName, parsed)
          case "gt"         => Expressions.greaterThan(filter.columnName, parsed)
          case "ge"         => Expressions.greaterThanOrEqual(filter.columnName, parsed)
          case "startsWith" => Expressions.startsWith(filter.columnName, raw)
          case _            => throw new IllegalArgumentException(s"Op `$op` is not pushdown-capable")
        }
    }
  }

  /**
    * Parse a string value into the JVM type Iceberg expects for the given column type.
    * Throws [[ParseError]] when the value doesn't fit the type — letting the websocket
    * layer translate that into a structured client error.
    */
  def parseValue(columnName: String, raw: String, icebergType: Type): AnyRef = {
    try {
      icebergType match {
        case _ if icebergType == Types.IntegerType.get()              => Integer.valueOf(raw.trim)
        case _ if icebergType == Types.LongType.get()                 => java.lang.Long.valueOf(raw.trim)
        case _ if icebergType == Types.DoubleType.get()               => java.lang.Double.valueOf(raw.trim)
        case _ if icebergType == Types.FloatType.get()                => java.lang.Float.valueOf(raw.trim)
        case _ if icebergType == Types.BooleanType.get()              => java.lang.Boolean.valueOf(raw.trim)
        case _ if icebergType == Types.StringType.get()               => raw
        case _ if icebergType == Types.TimestampType.withoutZone()    => parseTimestampMicros(raw)
        case _ if icebergType == Types.TimestampType.withZone()       => parseTimestampMicros(raw)
        case _ if icebergType == Types.DateType.get()                 => Integer.valueOf(java.time.LocalDate.parse(raw.trim).toEpochDay.toInt)
        case _                                                        => raw
      }
    } catch {
      case _: NumberFormatException | _: java.time.format.DateTimeParseException =>
        throw ParseError(columnName, raw, icebergType.toString)
    }
  }

  /**
    * Iceberg stores TIMESTAMP as microseconds-since-epoch. Accept either a numeric
    * micros value, an ISO-8601 instant, or a millis-since-epoch number (sniffed by
    * magnitude). All three are common shapes for ag-grid's date filter values.
    */
  private def parseTimestampMicros(raw: String): java.lang.Long = {
    val trimmed = raw.trim
    if (trimmed.forall(_.isDigit) || (trimmed.startsWith("-") && trimmed.drop(1).forall(_.isDigit))) {
      val n = java.lang.Long.parseLong(trimmed)
      // Numbers below year-3000 in millis (< ~3.3e13) come from JS Date.getTime();
      // anything larger we treat as already-micros.
      if (math.abs(n) < 100000000000000L) java.lang.Long.valueOf(n * 1000L) else java.lang.Long.valueOf(n)
    } else {
      val instant = java.time.Instant.parse(trimmed)
      java.lang.Long.valueOf(instant.getEpochSecond * 1000000L + instant.getNano / 1000L)
    }
  }
}
