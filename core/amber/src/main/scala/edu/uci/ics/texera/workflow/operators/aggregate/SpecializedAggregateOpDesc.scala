package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.ProgressiveUtils
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameList
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.aggregate.AggregateOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

import java.io.Serializable
import scala.jdk.CollectionConverters.asJavaIterableConverter

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAggregateOpDesc extends AggregateOpDesc {

  @JsonProperty(value = "aggregations", required = true)
  @JsonPropertyDescription("multiple aggregation functions")
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

  override def aggregateOperatorExecutor(
      operatorSchemaInfo: OperatorSchemaInfo
  ): PhysicalPlan = {
    if (aggregations.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    AggregateOpDesc.opExecPhysicalPlan(
      operatorIdentifier,
      aggregations.map(agg => agg.getAggFunc(operatorSchemaInfo.inputSchemas(0))),
      groupByKeys,
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
      .add(ProgressiveUtils.insertRetractFlagAttr)
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(aggregations.map(agg => agg.getAggregationAttribute(schemas(0))).asJava)
      .build()
  }

}
