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

package org.apache.texera.amber.operator.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaInject
import org.apache.texera.amber.core.tuple.{AttributeType, AttributeTypeUtils, Schema, Tuple}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName

import scala.util.Try

/** A typed row predicate for one aggregate measure, compiled once per input schema. */
class AggregationCondition {
  @JsonProperty(required = true)
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaInject(
    json =
      """{"enum":["=","!=","<","<=",">",">=","contains","not contains","is null","is not null"]}"""
  )
  var condition: String = _

  @JsonProperty
  @JsonPropertyDescription(
    "Comparison value; not needed for is null or is not null. Text is case-sensitive."
  )
  var value: String = _

  @JsonIgnore
  def compile(schema: Schema): Tuple => Boolean = {
    require(
      attribute != null && schema.containsAttribute(attribute),
      s"Condition column does not exist: $attribute"
    )
    val column = attribute
    val comparison = condition
    if (comparison == "is null") return tuple => tuple.getField[Any](column) == null
    if (comparison == "is not null") return tuple => tuple.getField[Any](column) != null

    val orderedComparisons = Set("=", "!=", "<", "<=", ">", ">=")
    require(
      orderedComparisons.contains(comparison) ||
        comparison == "contains" || comparison == "not contains",
      "Condition operator is invalid"
    )
    require(value != null, "Condition value is required")
    val expected = value
    val attrType = schema.getAttribute(column).getType

    def matches(order: Int): Boolean =
      comparison match {
        case "="  => order == 0
        case "!=" => order != 0
        case "<"  => order < 0
        case "<=" => order <= 0
        case ">"  => order > 0
        case ">=" => order >= 0
      }

    val evaluate: Any => Boolean = if (comparison == "contains" || comparison == "not contains") {
      require(attrType == AttributeType.STRING, "Condition contains requires a string column")
      actual => actual.asInstanceOf[String].contains(expected) == (comparison == "contains")
    } else {
      attrType match {
        case AttributeType.STRING =>
          actual => matches(actual.asInstanceOf[String].compareTo(expected))
        case AttributeType.INTEGER | AttributeType.LONG | AttributeType.DOUBLE =>
          // Do not coerce LONG to DOUBLE: distinct 64-bit values could otherwise compare equal.
          val threshold = Try(BigDecimal(expected)).getOrElse(
            throw new IllegalArgumentException("Condition value must be a finite number")
          )
          actual => {
            val number = Try(BigDecimal(actual.toString)).getOrElse(
              throw new IllegalArgumentException(
                s"Condition column $column must contain finite numbers"
              )
            )
            matches(number.compare(threshold))
          }
        case AttributeType.BOOLEAN =>
          require(
            comparison == "=" || comparison == "!=",
            "Condition boolean comparison requires = or !="
          )
          require(
            expected == "true" || expected == "false",
            "Condition boolean value must be true or false"
          )
          actual =>
            matches(java.lang.Boolean.compare(actual.asInstanceOf[Boolean], expected == "true"))
        case AttributeType.TIMESTAMP =>
          val threshold = Try(AttributeTypeUtils.parseTimestamp(expected)).getOrElse(
            throw new IllegalArgumentException("Condition value must be a valid timestamp")
          )
          actual => matches(AttributeTypeUtils.parseTimestamp(actual).compareTo(threshold))
        case _ =>
          throw new IllegalArgumentException(s"Condition comparison does not support $attrType")
      }
    }
    tuple => {
      val actual = tuple.getField[Any](column)
      actual != null && evaluate(actual)
    }
  }
}
