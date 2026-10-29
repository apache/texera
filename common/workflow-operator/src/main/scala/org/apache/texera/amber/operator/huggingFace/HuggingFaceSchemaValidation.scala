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

package org.apache.texera.amber.operator.huggingFace

import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}

/**
  * Configuration checks shared by the Spam Detection, Sentiment Analysis, Text
  * Summarization and Iris Logistic Regression operators' `getOutputSchemas`.
  *
  * Each check throws. The compiler runs `getOutputSchemas` inside a `Try` and stores
  * a thrown error on the operator's output port, which is how an operator that is
  * not configured yet reports why. Messages name the field as the form shows it.
  */
private[huggingFace] object HuggingFaceSchemaValidation {

  private val NUMERIC_TYPES = Set(AttributeType.INTEGER, AttributeType.LONG, AttributeType.DOUBLE)

  /** Require a column-name field to be set and not blank. */
  def requireNonEmpty(fieldTitle: String, value: String): Unit =
    if (value == null || value.trim.isEmpty) {
      throw new RuntimeException(s"$fieldTitle cannot be empty")
    }

  /**
    * Require the column a field names to be in the input table, spelled exactly as
    * it is there: Schema lookups ignore case, but the generated Python does not.
    */
  def requireInputColumn(schema: Schema, fieldTitle: String, column: String): Attribute = {
    requireNonEmpty(fieldTitle, column)
    schema.attributes
      .find(_.getName == column)
      .getOrElse(
        throw new RuntimeException(
          s"$fieldTitle '$column' is not in the input table. " +
            s"Available columns: ${schema.getAttributeNames.mkString(", ")}"
        )
      )
  }

  /** As [[requireInputColumn]], and the column must also be numeric. */
  def requireNumericInputColumn(schema: Schema, fieldTitle: String, column: String): Unit = {
    val columnType = requireInputColumn(schema, fieldTitle, column).getType
    if (!NUMERIC_TYPES.contains(columnType)) {
      throw new RuntimeException(
        s"$fieldTitle '$column' must be numeric (integer, long or double), not ${columnType.getName}"
      )
    }
  }
}
