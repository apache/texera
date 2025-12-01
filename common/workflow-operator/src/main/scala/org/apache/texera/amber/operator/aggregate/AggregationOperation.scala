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
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, AttributeTypeUtils, Tuple}
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName

import javax.validation.constraints.NotNull

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

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
  }
}
""")
class AggregationOperation {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Aggregate Func")
  @JsonPropertyDescription("sum, count, average, min, max, or concat")
  var aggFunction: AggregationFunction = _

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to calculate average value")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "result attribute", required = true)
  @JsonPropertyDescription("column name of average result")
  @NotNull(message = "result attribute is required")
  var resultAttribute: String = _

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
    new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        val inc =
          if (attribute == null) 1
          else if (tuple.getField(attribute) != null) 1
          else 0
        partial + inc
      },
      (partial1, partial2) => partial1 + partial2,
      partial => partial
    )
  }

  private def concatAgg(): DistributedAggregation[String] = {
    new DistributedAggregation[String](
      () => "",
      (partial, tuple) => {
        if (partial == "") {
          if (tuple.getField(attribute) != null) tuple.getField(attribute).toString else ""
        } else {
          partial + "," + (if (tuple.getField(attribute) != null)
                             tuple.getField(attribute).toString
                           else "")
        }
      },
      (partial1, partial2) => {
        if (partial1 != "" && partial2 != "") {
          partial1 + "," + partial2
        } else {
          partial1 + partial2
        }
      },
      partial => partial
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
      () => AttributeTypeUtils.maxValue(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = AttributeTypeUtils.compare(value, partial, attributeType)
        if (value != null && comp < 0) value else partial
      },
      (partial1, partial2) =>
        if (AttributeTypeUtils.compare(partial1, partial2, attributeType) < 0) partial1
        else partial2,
      partial => if (partial == AttributeTypeUtils.maxValue(attributeType)) null else partial
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
      () => AttributeTypeUtils.minValue(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = AttributeTypeUtils.compare(value, partial, attributeType)
        if (value != null && comp > 0) value else partial
      },
      (partial1, partial2) =>
        if (AttributeTypeUtils.compare(partial1, partial2, attributeType) > 0) partial1
        else partial2,
      partial => if (partial == AttributeTypeUtils.maxValue(attributeType)) null else partial
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
