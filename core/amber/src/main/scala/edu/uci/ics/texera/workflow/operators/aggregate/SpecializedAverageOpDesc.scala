package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpDesc, AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}

import java.io.Serializable
import scala.jdk.CollectionConverters.asJavaIterableConverter

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAverageOpDesc extends AggregateOpDesc {

//  @JsonProperty(required = true)
//  @JsonSchemaTitle("Aggregation Function")
//  @JsonPropertyDescription("sum, count, average, min, max, or concat")
//  var aggFunction: AggregationFunction = _
//
//  @JsonProperty(value = "attribute", required = true)
//  @JsonPropertyDescription("column to calculate average value")
//  @AutofillAttributeName
//  var attribute: String = _
//
//  @JsonProperty(value = "result attribute", required = true)
//  @JsonPropertyDescription("column name of average result")
//  var resultAttribute: String = _

//  @JsonProperty(value = "aggregations", required = true)
//  @JsonPropertyDescription("multiple predicates in OR")
//  var aggregations: List[AggregationOperator] = List()
  var aggregations: List[AggregationOperator] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

//  @JsonIgnore
//  private var groupBySchema: Schema = _
//  @JsonIgnore
//  private var finalAggValueSchema: Schema = _

  override def operatorExecutor(
      operatorSchemaInfo: OperatorSchemaInfo
  ): AggregateOpExecConfig[java.lang.Double] = {
    var groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    var finalAggValueSchema = getFinalAggValueSchema

    var groupByFunc: Schema => Schema = {
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

    new AggregateOpExecConfig(
      operatorIdentifier,
      aggregations.map(_.getAggFunc(finalAggValueSchema, groupByFunc)),
      operatorSchemaInfo
    )

//    aggFunction match {
//      case AggregationFunction.AVERAGE => averageAgg(operatorSchemaInfo)
//      case AggregationFunction.COUNT   => countAgg(operatorSchemaInfo)
//      case AggregationFunction.MAX     => maxAgg(operatorSchemaInfo)
//      case AggregationFunction.MIN     => minAgg(operatorSchemaInfo)
//      case AggregationFunction.SUM     => sumAgg(operatorSchemaInfo)
//      case AggregationFunction.CONCAT  => concatAgg(operatorSchemaInfo)
//      case _ =>
//        throw new UnsupportedOperationException("Unknown aggregation function: " + aggFunction)
//    }
  }

//  def groupByFunc(): Schema => Schema = {
//    if (this.groupByKeys == null) null
//    else
//      schema => {
//        // Since this is a partially evaluated tuple, there is no actual schema for this
//        // available anywhere. Constructing it once for re-use
//        if (groupBySchema == null) {
//          val schemaBuilder = Schema.newBuilder()
//          groupByKeys.foreach(key => schemaBuilder.add(schema.getAttribute(key)))
//          groupBySchema = schemaBuilder.build
//        }
//        groupBySchema
//      }
//  }


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
    // TODO: Remove for loop
//    val schema = Schema.newBuilder()
//    for (agg <- aggregations) {
//      schema.add(agg.getAggregationSchema)
//    }
//    schema.build()
    Schema.newBuilder()
      .add(aggregations.map(_.getAggregationAttribute).asJava)
      .build()
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
    if (aggregations.length == 0 || aggregations(0).resultAttribute == null || aggregations(0).resultAttribute.trim.isEmpty) {
      return null
    }
    Schema
      .newBuilder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(getFinalAggValueSchema.getAttributes)
      .build()
  }

}
