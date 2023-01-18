package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}

class AggregationOperator() {

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
  def getAggFunc(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[_ <: AnyRef] = {
    aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg(finalAggValueSchema, groupByFunc)
      case AggregationFunction.COUNT   => countAgg(finalAggValueSchema, groupByFunc)
      case AggregationFunction.MAX     => maxAgg(finalAggValueSchema, groupByFunc)
      case AggregationFunction.MIN     => minAgg(finalAggValueSchema, groupByFunc)
      case AggregationFunction.SUM     => sumAgg(finalAggValueSchema, groupByFunc)
      case AggregationFunction.CONCAT  => concatAgg(finalAggValueSchema, groupByFunc)
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
  }

  private def sumAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => 0,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        partial + (if (value.isDefined) value.get else 0)

      },
      (partial1, partial2) => partial1 + partial2,
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.DOUBLE, partial)
        }
      },
      groupByFunc
    )
//    new AggregateOpExecConfig[java.lang.Double](
//      operatorIdentifier,
//      aggregation,
//      operatorSchemaInfo
//    )
  }

  private def countAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[Integer] = {
    new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        partial + (if (tuple.getField(attribute) != null) 1 else 0)
      },
      (partial1, partial2) => partial1 + partial2,
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.INTEGER, partial)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.INTEGER, partial)
        }
      },
      groupByFunc
    )
//    new AggregateOpExecConfig[Integer](
//      operatorIdentifier,
//      aggregation,
//      operatorSchemaInfo
//    )
  }

  private def concatAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[String] = {
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
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.STRING, partial)
        }
      },
      groupByFunc
    )
//    new AggregateOpExecConfig[String](
//      operatorIdentifier,
//      aggregation,
//      operatorSchemaInfo
//    )
  }

  private def minAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => Double.MaxValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get < partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      // TODO: Check Max Value
//      partial => {
//        if (partial == Double.MaxValue) null
//        else
//          Tuple
//            .newBuilder(finalAggValueSchema)
//            .add(resultAttribute, AttributeType.DOUBLE, partial)
//            .build
//      },
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.DOUBLE, partial)
        }
      },
      groupByFunc
    )
//    new AggregateOpExecConfig[java.lang.Double](
//      operatorIdentifier,
//      aggregation,
//      operatorSchemaInfo
//    )
  }

  private def maxAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[java.lang.Double] = {
    new DistributedAggregation[java.lang.Double](
      () => Double.MinValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get > partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      // TODO: Check min value
//      partial => {
//        if (partial == Double.MinValue) null
//        else
//          Tuple
//            .newBuilder(finalAggValueSchema)
//            .add(resultAttribute, AttributeType.DOUBLE, partial)
//            .build
//      },
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.DOUBLE, partial)
        }
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

  private def averageAgg(finalAggValueSchema: Schema, groupByFunc: Schema => Schema): DistributedAggregation[AveragePartialObj] = {
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
      // TODO: Check
//      partial => {
//        val value = if (partial.count == 0) null else partial.sum / partial.count
//        Tuple
//          .newBuilder(finalAggValueSchema)
//          .add(resultAttribute, AttributeType.DOUBLE, value)
//          .build
//      },
      (partial, tupleBuilder) => {
        if (tupleBuilder == null) {
          Tuple
            .newBuilder(finalAggValueSchema)
        } else {
          tupleBuilder.add(resultAttribute, AttributeType.DOUBLE, if (partial.count == 0) null else partial.sum / partial.count)
        }
      },
      groupByFunc
    )
  }

}