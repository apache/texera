package texera.operators.aggregate

import java.io.Serializable

import Engine.Common.tuple.texera.TexeraTuple
import Engine.Common.tuple.texera.schema.{AttributeType, Schema}
import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import texera.common.metadata.{OperatorGroupConstants, TexeraOperatorInfo}
import texera.common.operators.TexeraOperatorDescriptor
import texera.common.operators.aggregate.{TexeraAggregateOpDesc, TexeraAggregateOpExecConfig, TexeraDistributedAggregation}

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class AverageOpDesc extends TexeraAggregateOpDesc {

  @JsonProperty("attribute")
  @JsonPropertyDescription("column to calculate average value")
  var attribute: String = _

  @JsonProperty("result attribute")
  @JsonPropertyDescription("column name of average result")
  var resultAttribute: String = _

  @JsonProperty("group by keys")
  @JsonPropertyDescription("group by columns")
  var groupByKeys: List[String] = _

  override def texeraOpExec: TexeraAggregateOpExecConfig[AveragePartialObj] = {
    val groupByKeyList = if (this.groupByKeys == null) List() else this.groupByKeys
    val aggregation = new TexeraDistributedAggregation[AveragePartialObj](
      () => AveragePartialObj(0, 0),
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        AveragePartialObj(partial.sum + value, partial.count + 1)
      },
      (partial1, partial2) =>
        AveragePartialObj(partial1.sum + partial2.sum, partial1.count + partial2.count),
      partial => {
        val value = if (partial.count == 0) null else partial.sum / partial.count
        TexeraTuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, value).build
      },
      groupByKeyList
    )
    new TexeraAggregateOpExecConfig[AveragePartialObj](
      amberOperatorTag,
      aggregation
    )
  }

  override def texeraOperatorInfo: TexeraOperatorInfo =
    TexeraOperatorInfo(
      "Average",
      "calculate the average value of a column",
      OperatorGroupConstants.UTILITY_GROUP,
      1, 1
    )

  override def transformSchema(schemas: Schema*): Schema = { null }

}
