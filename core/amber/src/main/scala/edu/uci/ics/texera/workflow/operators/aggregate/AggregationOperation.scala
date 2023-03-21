package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.aggregate.DistributedAggregation
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType}

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
  def getAggregationAttribute: Attribute = {
    if (this.aggFunction == AggregationFunction.COUNT)
      new Attribute(resultAttribute, AttributeType.INTEGER)
    else if (this.aggFunction == AggregationFunction.CONCAT)
      new Attribute(resultAttribute, AttributeType.STRING)
    else
      new Attribute(resultAttribute, AttributeType.DOUBLE)
  }

  @JsonIgnore
  def getAggFunc(): DistributedAggregation[Object] = {
    val aggFunc = aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg()
      case AggregationFunction.COUNT   => countAgg()
      case AggregationFunction.MAX     => maxAgg()
      case AggregationFunction.MIN     => minAgg()
      case AggregationFunction.SUM     => sumAgg()
      case AggregationFunction.CONCAT  => concatAgg()
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
    aggFunc.asInstanceOf[DistributedAggregation[Object]]
  }

  private def sumAgg(): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => 0,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        partial + (if (value.isDefined) value.get else 0)

      },
      (partial1, partial2) => partial1 + partial2,
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

  private def minAgg(): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => Double.MaxValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get < partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      partial => if (partial == Double.MaxValue) null else partial
    )
  }

  private def maxAgg(): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => Double.MinValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get > partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      partial => if (partial == Double.MinValue) null else partial
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

}
