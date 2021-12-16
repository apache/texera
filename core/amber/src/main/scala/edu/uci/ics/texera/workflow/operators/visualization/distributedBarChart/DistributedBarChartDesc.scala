package edu.uci.ics.texera.workflow.operators.visualization.distributedBarChart

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameList}
import edu.uci.ics.texera.workflow.common.operators.aggregate.DistributedAggregation
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{AttributeType, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.operators.visualization.{VisualizationConstants, VisualizationOperator}
import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

class DistributedBarChartDesc extends VisualizationOperator{
  @JsonProperty(value = "name column", required = true)
  @JsonPropertyDescription("column of name (for x-axis)")
  @AutofillAttributeName var nameColumn: String = _

  @JsonProperty(value = "data column(s)", required = true)
  @JsonPropertyDescription("column(s) of data (for y-axis)")
  @AutofillAttributeNameList var dataColumns: List[String] = _

  @JsonIgnore
  private var resultAttribute = "AggregatedDataColumn"

  override def chartType: String = VisualizationConstants.BAR

  @JsonIgnore
  private var groupBySchema: Schema = _
  @JsonIgnore
  private var finalAggValueSchema: Schema = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    if (nameColumn == null) {
      throw new RuntimeException("bar chart: name column is null")
    }
    if (dataColumns == null || dataColumns.isEmpty) {
      throw new RuntimeException("bar chart: data column is null or empty")
    }

    this.groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    this.finalAggValueSchema = getFinalAggValueSchema

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

    new DistributedBarChartOpExecConfig(operatorIdentifier, aggregation,this, operatorSchemaInfo)
  }

  override def operatorInfo: OperatorInfo = OperatorInfo(
    "Distributed Bar Chart",
    "View the result in bar chart",
    OperatorGroupConstants.VISUALIZATION_GROUP,
    asScalaBuffer(singletonList(InputPort(""))).toList,
    asScalaBuffer(singletonList(OutputPort(""))).toList
  )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Schema
      .newBuilder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(getFinalAggValueSchema.getAttributes)
      .build()
  }

  private def getNumericalValue(tuple: Tuple): Option[Double] = {
    val attribute = dataColumns.head
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return None

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      Option(parseTimestamp(value.toString).getTime.toDouble)
    else Option(value.toString.toDouble)
  }

  private def getGroupByKeysSchema(schemas: Array[Schema]): Schema = {
    val groupByKeys = List(this.nameColumn)
    Schema
      .newBuilder()
      .add(groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*)
      .build()
  }

  private def getFinalAggValueSchema: Schema = {
    Schema
      .newBuilder()
      .add(resultAttribute, AttributeType.DOUBLE)
      .build()
  }

  def groupByFunc(): Schema => Schema = {
    schema => {
      // Since this is a partially evaluated tuple, there is no actual schema for this
      // available anywhere. Constructing it once for re-use
      if (groupBySchema == null) {
        val schemaBuilder = Schema.newBuilder()
        schemaBuilder.add(schema.getAttribute(nameColumn))
        groupBySchema = schemaBuilder.build
      }
      groupBySchema
    }
  }
}
