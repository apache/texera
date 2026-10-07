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
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  AttributeTypeUtils,
  Schema,
  Tuple
}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName

import javax.validation.constraints.NotNull

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}
case class ConcatPartialObj(value: String, leadingEmptyCount: Long) extends Serializable

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "attribute": {
      "allOf": [
        {
          "if": {
            "aggFunction": {
              "valEnum": ["sum", "average", "min", "max"]
            }
          },
          "then": {
            "enum": ["integer", "long", "double", "timestamp"]
          }
        },
        {
          "if": {
            "aggFunction": {
              "valEnum": ["concat"]
            }
          },
          "then": {
            "enum": ["string"]
          }
        }
      ]
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "aggFunction": { "const": "count" }
        }
      },
      "then": {},
      "else": {
        "required": ["attribute"],
        "properties": {
          "attribute": { "pattern": "\\S" }
        }
      }
    }
  ]
}
""")
class AggregationOperation {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Aggregate Func")
  @JsonPropertyDescription("sum, count, average, min, max, or concat")
  var aggFunction: AggregationFunction = _

  @JsonProperty(value = "attribute")
  @JsonPropertyDescription("column to aggregate on")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "result attribute", required = true)
  @JsonPropertyDescription("column name of average result")
  @NotNull(message = "result attribute is required")
  var resultAttribute: String = _

  @JsonProperty
  @JsonSchemaTitle("Conditions")
  @JsonPropertyDescription(
    "Only matching rows contribute to this measure. Empty conditions include every row. Null values match only explicit null checks."
  )
  var conditions: List[AggregationCondition] = List.empty

  @JsonProperty
  @JsonSchemaTitle("Match conditions")
  @JsonSchemaInject(json = """{"enum":["all","any"],"default":"all"}""")
  @JsonPropertyDescription(
    "Require all or any conditions to match. Does not affect other measures or remove groups."
  )
  var conditionMatch: String = "all"

  @JsonIgnore
  def compileConditions(inputSchema: Schema): Tuple => Boolean = {
    require(
      conditions != null && !conditions.contains(null),
      "Conditions and their rules cannot be null"
    )
    require(
      conditionMatch == "all" || conditionMatch == "any",
      "Condition match must be all or any"
    )
    // Compile every rule now, even if runtime evaluation can short-circuit.
    val predicates = conditions.map(_.compile(inputSchema))
    if (predicates.isEmpty) _ => true
    else if (conditionMatch == "all") tuple => predicates.forall(_(tuple))
    else tuple => predicates.exists(_(tuple))
  }

  @JsonIgnore
  def getAggFunc(attrType: AttributeType, inputSchema: Schema): DistributedAggregation[Object] = {
    val matches = compileConditions(inputSchema)
    val aggregate = getAggFunc(attrType)
    aggregate.copy(iterate =
      (partial, tuple) => if (matches(tuple)) aggregate.iterate(partial, tuple) else partial
    )
  }

  @JsonIgnore
  def getAggregationAttribute(attrType: AttributeType): Attribute = {
    val resultAttrType = this.aggFunction match {
      case AggregationFunction.SUM     => attrType
      case AggregationFunction.COUNT   => AttributeType.INTEGER
      case AggregationFunction.AVERAGE => AttributeType.DOUBLE
      case AggregationFunction.MIN     => attrType
      case AggregationFunction.MAX     => attrType
      case AggregationFunction.CONCAT  => AttributeType.STRING
      case _                           => throw new RuntimeException("Unknown aggregation function: " + this.aggFunction)
    }
    new Attribute(resultAttribute, resultAttrType)
  }

  @JsonIgnore
  def getAggFunc(attrType: AttributeType): DistributedAggregation[Object] = {
    val aggFunc = aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg()
      case AggregationFunction.COUNT   => countAgg()
      case AggregationFunction.MAX     => maxAgg(attrType)
      case AggregationFunction.MIN     => minAgg(attrType)
      case AggregationFunction.SUM     => sumAgg(attrType)
      case AggregationFunction.CONCAT  => concatAgg()
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
    aggFunc.asInstanceOf[DistributedAggregation[Object]]
  }

  @JsonIgnore
  def getFinal: AggregationOperation = {
    val newAggFunc = aggFunction match {
      // COUNT emits partial counts locally; the global stage sums them.
      case AggregationFunction.COUNT => AggregationFunction.SUM
      case a: AggregationFunction    => a
    }
    val res = new AggregationOperation()
    res.aggFunction = newAggFunc
    res.resultAttribute = resultAttribute
    res.attribute = resultAttribute
    res
  }

  private def sumAgg(attributeType: AttributeType): DistributedAggregation[Object] = {
    if (
      attributeType != AttributeType.INTEGER &&
      attributeType != AttributeType.DOUBLE &&
      attributeType != AttributeType.LONG &&
      attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException(
        "Unsupported attribute type for sum aggregation: " + attributeType
      )
    }
    new DistributedAggregation[Object](
      () => AttributeTypeUtils.zeroValue(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        AttributeTypeUtils.add(partial, value, attributeType)
      },
      (partial1, partial2) => AttributeTypeUtils.add(partial1, partial2, attributeType),
      partial => partial
    )
  }

  private def countAgg(): DistributedAggregation[Integer] = {
    // An empty attribute means COUNT(*): count every row. Otherwise count only the
    // rows whose attribute value is non-null (COUNT(column)).
    val countAllRows = attribute == null || attribute.trim.isEmpty
    new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) =>
        partial + (if (countAllRows || tuple.getField(attribute) != null) 1 else 0),
      (partial1, partial2) => partial1 + partial2,
      partial => partial
    )
  }

  private def concatAgg(): DistributedAggregation[ConcatPartialObj] = {
    new DistributedAggregation[ConcatPartialObj](
      () => ConcatPartialObj("", 0L),
      (partial, tuple) => {
        val value = Option(tuple.getField[Object](attribute)).fold("")(_.toString)
        if (partial.value.isEmpty && value.isEmpty)
          partial.copy(leadingEmptyCount = Math.addExact(partial.leadingEmptyCount, 1L))
        else if (partial.value.isEmpty) partial.copy(value = value)
        else partial.copy(value = partial.value + "," + value)
      },
      (partial1, partial2) => {
        // Leading null/empty values disappear in a single stream, but become
        // interior slots if an earlier worker supplied non-empty text. Preserve
        // their count so a matched blank is not confused with zero matches.
        if (partial1.value.isEmpty)
          partial2.copy(leadingEmptyCount =
            Math.addExact(partial1.leadingEmptyCount, partial2.leadingEmptyCount)
          )
        else
          partial1.copy(value =
            partial1.value + ",".repeat(Math.toIntExact(partial2.leadingEmptyCount)) +
              (if (partial2.value.nonEmpty) "," + partial2.value else "")
          )
      },
      partial => partial.value
    )
  }

  private def minAgg(attributeType: AttributeType): DistributedAggregation[Object] = {
    if (
      attributeType != AttributeType.INTEGER &&
      attributeType != AttributeType.DOUBLE &&
      attributeType != AttributeType.LONG &&
      attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException(
        "Unsupported attribute type for min aggregation: " + attributeType
      )
    }
    new DistributedAggregation[Object](
      () => null,
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = AttributeTypeUtils.compare(value, partial, attributeType)
        val isNaN = attributeType == AttributeType.DOUBLE && value != null &&
          value.asInstanceOf[java.lang.Double].isNaN
        if (value != null && !isNaN && (partial == null || comp < 0)) value else partial
      },
      (partial1, partial2) =>
        if (
          partial2 == null || (partial1 != null &&
          AttributeTypeUtils.compare(partial1, partial2, attributeType) < 0)
        ) partial1
        else partial2,
      partial => partial
    )
  }

  private def maxAgg(attributeType: AttributeType): DistributedAggregation[Object] = {
    if (
      attributeType != AttributeType.INTEGER &&
      attributeType != AttributeType.DOUBLE &&
      attributeType != AttributeType.LONG &&
      attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException(
        "Unsupported attribute type for max aggregation: " + attributeType
      )
    }
    new DistributedAggregation[Object](
      () => null,
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = AttributeTypeUtils.compare(value, partial, attributeType)
        if (value != null && (partial == null || comp > 0)) value else partial
      },
      (partial1, partial2) =>
        if (AttributeTypeUtils.compare(partial1, partial2, attributeType) > 0) partial1
        else partial2,
      partial => partial
    )
  }

  private def getNumericalValue(tuple: Tuple): Option[Double] = {
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return None

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      Option(AttributeTypeUtils.parseTimestamp(value.toString).getTime.toDouble)
    else Option(value.toString.toDouble)
  }

  private def averageAgg(): DistributedAggregation[AveragePartialObj] = {
    new DistributedAggregation[AveragePartialObj](
      () => AveragePartialObj(0, 0),
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        AveragePartialObj(
          partial.sum + (if (value.isDefined) value.get else 0),
          partial.count + (if (value.isDefined) 1 else 0)
        )
      },
      (partial1, partial2) =>
        AveragePartialObj(partial1.sum + partial2.sum, partial1.count + partial2.count),
      partial => {
        val ret: java.lang.Double = if (partial.count == 0d) null else partial.sum / partial.count
        ret
      }
    )
  }
}
