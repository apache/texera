package edu.uci.ics.amber.operator.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.tuple.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Tuple}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

import java.sql.Timestamp
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
      () => zero(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        add(partial, value, attributeType)
      },
      (partial1, partial2) => add(partial1, partial2, attributeType),
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
      () => maxValue(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = compare(value, partial, attributeType)
        if (value != null && comp < 0) value else partial
      },
      (partial1, partial2) =>
        if (compare(partial1, partial2, attributeType) < 0) partial1 else partial2,
      partial => if (partial == maxValue(attributeType)) null else partial
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
      () => minValue(attributeType),
      (partial, tuple) => {
        val value = tuple.getField[Object](attribute)
        val comp = compare(value, partial, attributeType)
        if (value != null && comp > 0) value else partial
      },
      (partial1, partial2) =>
        if (compare(partial1, partial2, attributeType) > 0) partial1 else partial2,
      partial => if (partial == maxValue(attributeType)) null else partial
    )
  }

  private def getNumericalValue(tuple: Tuple): Option[Double] = {
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return None

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      Option(parseTimestamp(value.toString).getTime.toDouble)
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

  // return a.compare(b),
  // < 0 if a < b,
  // > 0 if a > b,
  //   0 if a = b
  private def compare(a: Object, b: Object, attributeType: AttributeType): Int = {
    if (a == null && b == null) {
      return 0
    } else if (a == null) {
      return -1
    } else if (b == null) {
      return 1
    }
    attributeType match {
      case AttributeType.INTEGER => a.asInstanceOf[Integer].compareTo(b.asInstanceOf[Integer])
      case AttributeType.DOUBLE =>
        a.asInstanceOf[java.lang.Double].compareTo(b.asInstanceOf[java.lang.Double])
      case AttributeType.LONG =>
        a.asInstanceOf[java.lang.Long].compareTo(b.asInstanceOf[java.lang.Long])
      case AttributeType.TIMESTAMP =>
        a.asInstanceOf[Timestamp].getTime.compareTo(b.asInstanceOf[Timestamp].getTime)
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported attribute type for comparison: " + attributeType
        )
    }
  }

  private def add(a: Object, b: Object, attributeType: AttributeType): Object = {
    if (a == null && b == null) {
      return zero(attributeType)
    } else if (a == null) {
      return b
    } else if (b == null) {
      return a
    }
    attributeType match {
      case AttributeType.INTEGER =>
        Integer.valueOf(a.asInstanceOf[Integer] + b.asInstanceOf[Integer])
      case AttributeType.DOUBLE =>
        java.lang.Double.valueOf(
          a.asInstanceOf[java.lang.Double] + b.asInstanceOf[java.lang.Double]
        )
      case AttributeType.LONG =>
        java.lang.Long.valueOf(a.asInstanceOf[java.lang.Long] + b.asInstanceOf[java.lang.Long])
      case AttributeType.TIMESTAMP =>
        new Timestamp(a.asInstanceOf[Timestamp].getTime + b.asInstanceOf[Timestamp].getTime)
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported attribute type for addition: " + attributeType
        )
    }
  }

  private def zero(attributeType: AttributeType): Object =
    attributeType match {
      case AttributeType.INTEGER   => java.lang.Integer.valueOf(0)
      case AttributeType.DOUBLE    => java.lang.Double.valueOf(0)
      case AttributeType.LONG      => java.lang.Long.valueOf(0)
      case AttributeType.TIMESTAMP => new Timestamp(0)
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported attribute type for zero value: " + attributeType
        )
    }

  private def maxValue(attributeType: AttributeType): Object =
    attributeType match {
      case AttributeType.INTEGER   => Integer.MAX_VALUE.asInstanceOf[Object]
      case AttributeType.DOUBLE    => java.lang.Double.MAX_VALUE.asInstanceOf[Object]
      case AttributeType.LONG      => java.lang.Long.MAX_VALUE.asInstanceOf[Object]
      case AttributeType.TIMESTAMP => new Timestamp(java.lang.Long.MAX_VALUE)
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported attribute type for max value: " + attributeType
        )
    }

  private def minValue(attributeType: AttributeType): Object =
    attributeType match {
      case AttributeType.INTEGER   => Integer.MIN_VALUE.asInstanceOf[Object]
      case AttributeType.DOUBLE    => java.lang.Double.MIN_VALUE.asInstanceOf[Object]
      case AttributeType.LONG      => java.lang.Long.MIN_VALUE.asInstanceOf[Object]
      case AttributeType.TIMESTAMP => new Timestamp(0)
      case _ =>
        throw new UnsupportedOperationException(
          "Unsupported attribute type for min value: " + attributeType
        )
    }

}
