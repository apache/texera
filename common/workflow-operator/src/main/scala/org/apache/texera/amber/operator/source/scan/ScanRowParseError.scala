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
  * Builds actionable errors for scan sources when a row's values do not parse into
  * the inferred schema. The console title line in the UI truncates, so the essential
  * facts (row number, offending value, column name, expected type) come first.
  */
object ScanRowParseError {

  /**
    * Builds a RuntimeException describing why a row failed to parse.
    *
    * The failing column is identified by re-parsing each raw field individually;
    * this only runs on a row that has already failed, so the extra cost is irrelevant.
    * If no single column can be identified (e.g. a malformed JSON line or a structural
    * row error), a generic fallback message carrying the original reason is used.
    *
    * @param rawFields      raw field values of the failing row, in schema order
    *                       (may be empty or shorter than the schema)
    * @param schema         the inferred schema of the scan
    * @param inferReadLimit number of rows used for type inference (desc.INFER_READ_LIMIT)
    * @param rowNumber      1-based row number, if cheaply available
    * @param cause          the original parse exception
    */
  def build(
      rawFields: Seq[Any],
      schema: Schema,
      inferReadLimit: Int,
      rowNumber: Option[Int],
      cause: Throwable
  ): RuntimeException = {
    val message = findFailingColumn(rawFields, schema) match {
      case Some((attribute, value)) =>
        val prefix = rowNumber.map(n => s"Row $n: value").getOrElse("Value")
        s"$prefix '$value' in column '${attribute.getName}' cannot be read as " +
          s"${attribute.getType.name()}. " +
          s"Column types were inferred from the first $inferReadLimit rows of the file, " +
          "and this value does not match. " +
          "Fix the value in the file, or clean the data before scanning."
      case None =>
        val reason = Option(cause.getMessage).getOrElse(cause.getClass.getSimpleName)
        val prefix = rowNumber.map(n => s"Row $n").getOrElse("A row")
        s"$prefix could not be parsed into the inferred schema: $reason. " +
          s"Column types were inferred from the first $inferReadLimit rows of the file. " +
          "Fix the row in the file, or clean the data before scanning."
    }
    new RuntimeException(message, cause)
  }

  /**
    * Re-parses each raw field against its attribute type; the first failure identifies
    * the offending column. Missing trailing fields are treated as null (as the scan does)
    * and thus never fail.
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
