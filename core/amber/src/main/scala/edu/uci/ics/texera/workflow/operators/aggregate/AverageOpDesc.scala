package edu.uci.ics.texera.workflow.operators.aggregate

import java.io.Serializable

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.aggregate.{AggregateOpDesc, AggregateOpExecConfig, DistributedAggregation}
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, Schema}

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class AverageOpDesc extends AggregateOpDesc {

  @JsonProperty("attribute")
  @JsonPropertyDescription("column to calculate average value")
  var attribute: String = _

  @JsonProperty("result attribute")
  @JsonPropertyDescription("column name of average result")
  var resultAttribute: String = _

  @JsonProperty("group by keys")
  @JsonPropertyDescription("group by columns")
  var groupByKeys: List[String] = _

  override def operatorExecutor: AggregateOpExecConfig[AveragePartialObj] = {
    val aggregation = new DistributedAggregation[AveragePartialObj](
      () => AveragePartialObj(0, 0),
      (partial, tuple) => {
        val value = tuple.getField(attribute).toString.toDouble
        AveragePartialObj(partial.sum + value, partial.count + 1)
      },
      (partial1, partial2) =>
        AveragePartialObj(partial1.sum + partial2.sum, partial1.count + partial2.count),
      partial => {
        val value = if (partial.count == 0) null else partial.sum / partial.count
        Tuple.newBuilder.add(resultAttribute, AttributeType.DOUBLE, value).build
      },
      if (this.groupByKeys == null) null
      else
        tuple => {
          val builder = Tuple.newBuilder()
          groupByKeys.foreach(key =>
            builder.add(tuple.getSchema.getAttribute(key), tuple.getField(key))
          )
          builder.build()
        }
    )
    new AggregateOpExecConfig[AveragePartialObj](
      operatorIdentifier,
      aggregation
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Average",
      "calculate the average value of a column",
      OperatorGroupConstants.UTILITY_GROUP,
      1,
      1
    )

  override def getOutputSchema(schemas: Schema*): Schema = { null }

}
