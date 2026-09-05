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

package org.apache.texera.amber.operator.source.scan

import org.apache.texera.amber.core.tuple.{Attribute, AttributeTypeUtils, Schema}

import scala.util.Try

/**
  * Builds actionable, per-row warnings for scan sources when a row's values do not
  * parse into the inferred schema. The scan skips such a row (rather than failing the
  * whole run) and surfaces one of these warnings so the user knows exactly which row
  * was dropped, and why. Messages lead with the essential facts (row number, offending
  * value, column, expected type) because the console title line truncates.
  */
object ScanRowParseError {

  /** Prefix that makes a PRINT console message surface as a warning in the UI. */
  private val WarningPrefix = "WARNING: "

  /**
    * Builds the warning for a single skipped row.
    *
    * The failing column is identified by re-parsing each raw field individually; this
    * only runs on a row that has already failed, so the extra cost is irrelevant. If no
    * single column can be identified (e.g. a malformed JSON line or a structural row
    * error), a generic fallback carrying the original reason is used instead.
    *
    * @param rawFields       raw field values of the failing row, in schema order
    *                        (may be empty or shorter than the schema)
    * @param schema          the inferred schema of the scan
    * @param inferSampleSize number of rows actually used for type inference
    *                        (desc.inferSampleSize)
    * @param rowNumber       1-based row number, if cheaply available
    * @param cause           the original parse exception
    */
  def skipWarning(
      rawFields: Seq[Any],
      schema: Schema,
      inferSampleSize: Int,
      rowNumber: Option[Int],
      cause: Throwable
  ): String = {
    val where = rowNumber.map(n => s"row $n").getOrElse("a row")
    findFailingColumn(rawFields, schema) match {
      case Some((attribute, value)) =>
        s"${WarningPrefix}skipped $where — value '$value' in column '${attribute.getName}' " +
          s"cannot be read as ${attribute.getType.name()}. " +
          s"Column types were inferred from an initial sample of $inferSampleSize rows, " +
          "and this value does not match."
      case None =>
        val reason = Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
        s"${WarningPrefix}skipped $where — could not be parsed into the inferred schema: $reason. " +
          s"Column types were inferred from an initial sample of $inferSampleSize rows."
    }
  }

  /**
    * Summary warning appended when the per-row detail list is capped, so the user still
    * learns the true total even though only the first rows are listed individually.
    */
  def moreSkipped(hidden: Int, total: Int): String =
    s"${WarningPrefix}...and $hidden more row(s) skipped ($total total). " +
      "Fix the values in the file, or clean the data before scanning."

  /**
    * Re-parses each raw field against its attribute type; the first failure identifies
    * the offending column. Missing trailing fields are ignored (equivalent to treating
    * them as null for failure detection) and thus never fail.
    */
  private def findFailingColumn(
      rawFields: Seq[Any],
      schema: Schema
  ): Option[(Attribute, Any)] = {
    schema.getAttributes.iterator
      .zip(rawFields.iterator)
      .find {
        case (attribute, value) =>
          Try(AttributeTypeUtils.parseField(value, attribute.getType)).isFailure
      }
  }
}
