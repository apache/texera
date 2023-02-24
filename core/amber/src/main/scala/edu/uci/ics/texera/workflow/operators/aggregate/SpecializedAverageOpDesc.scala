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
  AggregateOpExecConfig,
  DistributedAggregation
}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}

import java.io.Serializable
import scala.jdk.CollectionConverters.asJavaIterableConverter

class SpecializedAverageOpDesc extends AggregateOpDesc {

  @JsonProperty(value = "aggregations", required = true)
  @JsonPropertyDescription("multiple aggregation functions")
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

  override def operatorExecutor(
      operatorSchemaInfo: OperatorSchemaInfo
  ): AggregateOpExecConfig = {
    if (aggregations.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }

    var groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    val finalAggValueSchema = getFinalAggValueSchema(operatorSchemaInfo.inputSchemas)

    val groupByFunc: Schema => Schema = {
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
      aggregations.map(
        _.getAggFunc(finalAggValueSchema, groupByFunc).asInstanceOf[DistributedAggregation[Object]]
      ),
      finalAggValueSchema,
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

  private def getFinalAggValueSchema(schemas: Array[Schema]): Schema = {
    Schema
      .newBuilder()
      .add(aggregations.map(_.getAggregationAttribute(schemas(0))).asJava)
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
    if (
      aggregations.exists(agg => (agg.resultAttribute == null || agg.resultAttribute.trim.isEmpty))
    ) {
      return null
    }
    Schema
      .newBuilder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(getFinalAggValueSchema(schemas).getAttributes)
      .build()
  }

}
