package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.aggregate.PartialObj.zero

import java.io.Serializable

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

case class PartialObj(value: Object, attributeType: AttributeType) {
  def get: Object = {
    attributeType match {
      case AttributeType.TIMESTAMP => new java.sql.Timestamp(value.asInstanceOf[java.lang.Long])
      case _ => value
    }
  }

  private def compare(a: Object, b: Object): Int = {
    if (a == null && b == null) {
      return 0
    } else if (a == null) {
      return -1
    } else if (b == null) {
      return 1
    }
    attributeType match {
      case AttributeType.INTEGER => a.asInstanceOf[Integer].compareTo(b.asInstanceOf[Integer])
      case AttributeType.DOUBLE => a.asInstanceOf[java.lang.Double].compareTo(b.asInstanceOf[java.lang.Double])
      case AttributeType.LONG => a.asInstanceOf[java.lang.Long].compareTo(b.asInstanceOf[java.lang.Long])
      // java.sql.Timestamp is not comparable
      case AttributeType.TIMESTAMP => a.asInstanceOf[java.lang.Long].compareTo(b.asInstanceOf[java.lang.Long])
      case _ => throw new UnsupportedOperationException("Unsupported attribute type for comparison: " + attributeType)
    }
  }

  def <(that: PartialObj): Boolean = compare(this.value, that.value) < 0

  def >(that: PartialObj): Boolean = compare(this.value, that.value) > 0

  def ==(that: PartialObj): Boolean = compare(this.value, that.value) == 0

  private def add(a: Object, b: Object): Object = {
    if (a == null && b == null) {
      return zero(attributeType).get
    } else if (a == null) {
      return b
    } else if (b == null) {
      return a
    }
    attributeType match {
      case AttributeType.INTEGER => Integer.valueOf(a.asInstanceOf[Integer] + b.asInstanceOf[Integer])
      case AttributeType.DOUBLE => java.lang.Double.valueOf(a.asInstanceOf[java.lang.Double] + b.asInstanceOf[java.lang.Double])
      case AttributeType.LONG => java.lang.Long.valueOf(a.asInstanceOf[java.lang.Long] + b.asInstanceOf[java.lang.Long])
      // java.sql.Timestamp is not comparable
      case AttributeType.TIMESTAMP => java.lang.Long.valueOf(a.asInstanceOf[java.lang.Long] + b.asInstanceOf[java.lang.Long])
      case _ => throw new UnsupportedOperationException("Unsupported attribute type for addition: " + attributeType)
    }
  }
  def +(that: PartialObj): PartialObj = PartialObj(add(this.value, that.value), attributeType)

}

object PartialObj {
  def maxValue(attributeType: AttributeType): PartialObj =
    PartialObj(attributeType match {
      case AttributeType.INTEGER => Integer.MAX_VALUE.asInstanceOf[Object]
      case AttributeType.DOUBLE => java.lang.Double.MAX_VALUE.asInstanceOf[Object]
      case AttributeType.LONG => java.lang.Long.MAX_VALUE.asInstanceOf[Object]
      // java.sql.Timestamp is not comparable
      case AttributeType.TIMESTAMP => java.lang.Long.MAX_VALUE.asInstanceOf[Object]
      case _ => throw new UnsupportedOperationException("Unsupported attribute type for max value: " + attributeType)
    }, attributeType)

  def minValue(attributeType: AttributeType): PartialObj =
    PartialObj(attributeType match {
      case AttributeType.INTEGER => Integer.MIN_VALUE.asInstanceOf[Object]
      case AttributeType.DOUBLE => java.lang.Double.MIN_VALUE.asInstanceOf[Object]
      case AttributeType.LONG => java.lang.Long.MIN_VALUE.asInstanceOf[Object]
      // java.sql.Timestamp is not comparable
      case AttributeType.TIMESTAMP => java.lang.Long.MIN_VALUE.asInstanceOf[Object]
      case _ => throw new UnsupportedOperationException("Unsupported attribute type for min value: " + attributeType)
    }, attributeType)

  def zero(attributeType: AttributeType): PartialObj =
    PartialObj(attributeType match {
      case AttributeType.INTEGER => Integer.valueOf(0).asInstanceOf[Object]
      case AttributeType.DOUBLE => java.lang.Double.valueOf(0).asInstanceOf[Object]
      case AttributeType.LONG => java.lang.Long.valueOf(0).asInstanceOf[Object]
      // java.sql.Timestamp is not comparable
      case AttributeType.TIMESTAMP => java.lang.Long.valueOf(0).asInstanceOf[Object]
      case _ => throw new UnsupportedOperationException("Unsupported attribute type for zero value: " + attributeType)
    }, attributeType)
}

class AggregationOperation() {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Aggregation Function")
  @JsonPropertyDescription("sum, count, average, min, max, or concat")
  var aggFunction: AggregationFunction = _

  @JsonProperty(value = "attribute", required = true)
  @JsonPropertyDescription("column to calculate average value")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "result attribute", required = true)
  @JsonPropertyDescription("column name of average result")
  var resultAttribute: String = _

  @JsonIgnore
  def getAggregationAttribute(schema: Schema): Attribute = {
    if (this.aggFunction == AggregationFunction.COUNT)
      new Attribute(resultAttribute, AttributeType.INTEGER)
    else if (this.aggFunction == AggregationFunction.CONCAT)
      new Attribute(resultAttribute, AttributeType.STRING)
    else if (this.aggFunction == AggregationFunction.AVERAGE)
      new Attribute(resultAttribute, AttributeType.DOUBLE)
    else
      new Attribute(resultAttribute, schema.getAttribute(attribute).getType)
  }

  @JsonIgnore
  def getAggFunc(
      finalAggValueSchema: Schema,
      groupByFunc: Schema => Schema
  ): DistributedAggregation[_ <: AnyRef] = {
    val attributeType = finalAggValueSchema.getAttribute(resultAttribute).getType
    aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg(groupByFunc)
      case AggregationFunction.COUNT   => countAgg(groupByFunc)
      case AggregationFunction.MAX     => maxAgg(groupByFunc, attributeType)
      case AggregationFunction.MIN     => minAgg(groupByFunc, attributeType)
      case AggregationFunction.SUM     => sumAgg(groupByFunc, attributeType)
      case AggregationFunction.CONCAT  => concatAgg(groupByFunc)
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
  }

  private def sumAgg(groupByFunc: Schema => Schema, attributeType: AttributeType) = {
    if (
      attributeType != AttributeType.INTEGER &&
        attributeType != AttributeType.DOUBLE &&
        attributeType != AttributeType.LONG &&
        attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException("Unsupported attribute type for sum aggregation: " + attributeType)
    }
      new DistributedAggregation[PartialObj](
        () => PartialObj.zero(attributeType),
        (partial, tuple) => {
          val value: Object = tuple.getField(attribute)
          partial + (if (value != null) PartialObj(value, attributeType) else PartialObj.zero(attributeType))
        },
        (partial1, partial2) => partial1 + partial2,
        (partial) => {
          val schema = Schema.newBuilder().add(resultAttribute, attributeType).build()
          Tuple
            .newBuilder(schema)
            .add(resultAttribute, attributeType, partial.get)
            .build()
        },
        groupByFunc
      )
  }

  private def countAgg(groupByFunc: Schema => Schema) = {
    new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        partial + (if (tuple.getField(attribute) != null) 1 else 0)
      },
      (partial1, partial2) => partial1 + partial2,
      (partial) => {
        val schema = Schema.newBuilder().add(resultAttribute, AttributeType.INTEGER).build()
        Tuple
          .newBuilder(schema)
          .add(resultAttribute, AttributeType.INTEGER, partial)
          .build()
      },
      groupByFunc
    )
  }

  private def concatAgg(groupByFunc: Schema => Schema) = {
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
      (partial) => {
        val schema = Schema.newBuilder().add(resultAttribute, AttributeType.STRING).build()
        Tuple
          .newBuilder(schema)
          .add(resultAttribute, AttributeType.STRING, partial)
          .build()
      },
      groupByFunc
    )
  }

  private def minAgg(groupByFunc: Schema => Schema, attributeType: AttributeType) = {
    if (
      attributeType != AttributeType.INTEGER &&
      attributeType != AttributeType.DOUBLE &&
      attributeType != AttributeType.LONG &&
      attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException("Unsupported attribute type for min aggregation: " + attributeType)
    }
    new DistributedAggregation[PartialObj](
      () => PartialObj.maxValue(attributeType),
      (partial, tuple) => {
        val value: Object = tuple.getField(attribute)
        if (value != null && PartialObj(value, attributeType) < partial) PartialObj(value, attributeType) else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      (partial) => {
        val schema = Schema.newBuilder().add(resultAttribute, attributeType).build()
        Tuple
          .newBuilder(schema)
          .add(
            resultAttribute,
            attributeType,
            if (partial == PartialObj.maxValue(attributeType)) null else partial.get
          )
          .build()
      },
      groupByFunc
    )
  }

  private def maxAgg(groupByFunc: Schema => Schema, attributeType: AttributeType) = {
    if (
      attributeType != AttributeType.INTEGER &&
      attributeType != AttributeType.DOUBLE &&
      attributeType != AttributeType.LONG &&
      attributeType != AttributeType.TIMESTAMP
    ) {
      throw new UnsupportedOperationException("Unsupported attribute type for max aggregation: " + attributeType)
    }
    new DistributedAggregation[PartialObj](
      () => PartialObj.minValue(attributeType),
      (partial, tuple) => {
        val value: Object = tuple.getField(attribute)
        if (value != null && PartialObj(value, attributeType) > partial) PartialObj(value, attributeType) else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      (partial) => {
        val schema = Schema.newBuilder().add(resultAttribute, attributeType).build()
        Tuple
          .newBuilder(schema)
          .add(
            resultAttribute,
            attributeType,
            if (partial == PartialObj.minValue(attributeType)) null else partial.get
          )
          .build()
      },
      groupByFunc
    )
  }



  private def averageAgg(groupByFunc: Schema => Schema) = {
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
      (partial) => {
        val schema = Schema.newBuilder().add(resultAttribute, AttributeType.DOUBLE).build()
        Tuple
          .newBuilder(schema)
          .add(
            resultAttribute,
            AttributeType.DOUBLE,
            if (partial.count == 0) null else partial.sum / partial.count
          )
          .build()
      },
      groupByFunc
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
}
