package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{
  AggregateOpDesc,
  DistributedAggregation
}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import java.io.Serializable

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAverageOpDesc extends AggregateOpDesc {

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

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

  @JsonIgnore
  private var groupBySchema: Schema = _
  @JsonIgnore
  private var finalAggValueSchema: Schema = _

  override def aggregateOperatorExecutor(
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    this.groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    this.finalAggValueSchema = getFinalAggValueSchema

    aggFunction match {
      case AggregationFunction.AVERAGE => averageAgg(operatorSchemaInfo)
      case AggregationFunction.COUNT   => countAgg(operatorSchemaInfo)
      case AggregationFunction.MAX     => maxAgg(operatorSchemaInfo)
      case AggregationFunction.MIN     => minAgg(operatorSchemaInfo)
      case AggregationFunction.SUM     => sumAgg(operatorSchemaInfo)
      case AggregationFunction.CONCAT  => concatAgg(operatorSchemaInfo)
      case _ =>
        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
    }
  }

  def sumAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => 0,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        partial + (if (value.isDefined) value.get else 0)

      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.DOUBLE, partial)
          .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def countAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[Integer](
      () => 0,
      (partial, tuple) => {
        partial + (if (tuple.getField(attribute) != null) 1 else 0)
      },
      (partial1, partial2) => partial1 + partial2,
      partial => {
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.INTEGER, partial)
          .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[Integer](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def concatAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[String](
      () => "",
      (partial, tuple) => {
        if (partial == "") {
          if (tuple.getField(attribute) != null) tuple.getField(attribute).toString() else ""
        } else {
          partial + "," + (if (tuple.getField(attribute) != null)
                             tuple.getField(attribute).toString()
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
      partial => {
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.STRING, partial)
          .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[String](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def minAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MaxValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get < partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 < partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MaxValue) null
        else
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
            .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def maxAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[java.lang.Double](
      () => Double.MinValue,
      (partial, tuple) => {
        val value = getNumericalValue(tuple)
        if (value.isDefined && value.get > partial) value.get else partial
      },
      (partial1, partial2) => if (partial1 > partial2) partial1 else partial2,
      partial => {
        if (partial == Double.MinValue) null
        else
          Tuple
            .newBuilder(finalAggValueSchema)
            .add(resultAttribute, AttributeType.DOUBLE, partial)
            .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[java.lang.Double](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  def groupByFunc(): Schema => Schema = {
    if (this.groupByKeys == null) null
    else
      schema => {
        // Since this is a partially evaluated tuple, there is no actual schema for this
        // available anywhere. Constructing it once for re-use
        if (groupBySchema == null) {
          val schemaBuilder = Schema.newBuilder()
          groupByKeys.foreach(key => schemaBuilder.add(schema.getAttribute(key)))
          groupBySchema = schemaBuilder.build
        }
        groupBySchema
      }
  }

  private def getNumericalValue(tuple: Tuple): Option[Double] = {
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return None

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      Option(parseTimestamp(value.toString).getTime.toDouble)
    else Option(value.toString.toDouble)
  }

  def averageAgg(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    val aggregation = new DistributedAggregation[AveragePartialObj](
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
        val value = if (partial.count == 0) null else partial.sum / partial.count
        Tuple
          .newBuilder(finalAggValueSchema)
          .add(resultAttribute, AttributeType.DOUBLE, value)
          .build
      },
      groupByFunc()
    )
    AggregateOpDesc.opExecPhysicalPlan[AveragePartialObj](
      operatorIdentifier,
      aggregation,
      operatorSchemaInfo
    )
  }

  private def getGroupByKeysSchema(schemas: Array[Schema]): Schema = {
    if (groupByKeys == null) {
      groupByKeys = List()
    }
    Schema
      .newBuilder()
      .add(groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*)
      .build()
  }

  private def getFinalAggValueSchema: Schema = {
    if (this.aggFunction.equals(AggregationFunction.COUNT)) {
      Schema
        .newBuilder()
        .add(resultAttribute, AttributeType.INTEGER)
        .build()
    } else if (this.aggFunction.equals(AggregationFunction.CONCAT)) {
      Schema
        .newBuilder()
        .add(resultAttribute, AttributeType.STRING)
        .build()
    } else {
      Schema
        .newBuilder()
        .add(resultAttribute, AttributeType.DOUBLE)
        .build()
    }
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "Calculate different types of aggregation values",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (resultAttribute == null || resultAttribute.trim.isEmpty) {
      return null
    }
    Schema
      .newBuilder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(getFinalAggValueSchema.getAttributes)
      .build()
  }

}
