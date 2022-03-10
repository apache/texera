package edu.uci.ics.texera.workflow.operators.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.operators.OpExecConfig
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
import edu.uci.ics.texera.workflow.common.operators.aggregate.DistributedAggregation
import edu.uci.ics.texera.workflow.common.tuple.Tuple
import edu.uci.ics.texera.workflow.common.tuple.schema.AttributeTypeUtils.parseTimestamp
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  AggregatedVizOpExecConfig,
  VisualizationConstants,
  VisualizationOperator
}

import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

class LineChartOpDesc extends VisualizationOperator {
  @JsonProperty(value = "name column", required = true)
  @JsonPropertyDescription("column of name (for x-axis)")
  @AutofillAttributeName var nameColumn: String = _

  @JsonProperty(value = "data column(s)", required = false)
  @JsonPropertyDescription("column(s) of data (for y-axis)")
  @AutofillAttributeNameList var dataColumns: List[String] = _

  @JsonProperty(value = "chart style", required = true, defaultValue = VisualizationConstants.LINE)
  var lineChartEnum: LineChartEnum = _

  @JsonIgnore
  private var groupBySchema: Schema = _
  @JsonIgnore
  private var finalAggValueSchema: Schema = _

  override def chartType: String = lineChartEnum.getChartStyle

  def noDataCol: Boolean = dataColumns == null || dataColumns.isEmpty

  def resultAttributeNames: List[String] = if (noDataCol) List("count") else dataColumns

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    if (nameColumn == null || nameColumn == "") {
      throw new RuntimeException("line chart: name column is null or empty")
    }

    this.groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    this.finalAggValueSchema = getFinalAggValueSchema

    val aggregation =
      if (noDataCol)
        new DistributedAggregation[Integer](
          () => 0,
          (partial, tuple) => {
            partial + (if (tuple.getField(nameColumn) != null) 1 else 0)
          },
          (partial1, partial2) => partial1 + partial2,
          partial => {
            Tuple
              .newBuilder(finalAggValueSchema)
              .add(resultAttributeNames.head, AttributeType.INTEGER, partial)
              .build
          },
          groupByFunc()
        )
      else
        new DistributedAggregation[Array[Double]](
          () => Array.fill(dataColumns.length)(0),
          (partial, tuple) => {
            for (i <- dataColumns.indices) {
              partial(i) = partial(i) + getNumericalValue(tuple, dataColumns(i))
            }
            partial
          },
          (partial1, partial2) => partial1.zip(partial2).map { case (x, y) => x + y },
          partial => {
            val resultBuilder = Tuple.newBuilder(finalAggValueSchema)
            for (i <- dataColumns.indices) {
              resultBuilder.add(resultAttributeNames(i), AttributeType.DOUBLE, partial(i))
            }
            resultBuilder.build()
          },
          groupByFunc()
        )
    new AggregatedVizOpExecConfig(
      operatorIdentifier,
      aggregation,
      new LineChartOpExec(this, operatorSchemaInfo),
      operatorSchemaInfo
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Line Chart",
      "View the result in line chart",
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

  private def getNumericalValue(tuple: Tuple, attribute: String): Double = {
    val value: Object = tuple.getField(attribute)
    if (value == null)
      return 0

    if (tuple.getSchema.getAttribute(attribute).getType == AttributeType.TIMESTAMP)
      parseTimestamp(value.toString).getTime.toDouble
    else value.toString.toDouble
  }

  private def getGroupByKeysSchema(schemas: Array[Schema]): Schema = {
    val groupByKeys = List(this.nameColumn)
    Schema
      .newBuilder()
      .add(groupByKeys.map(key => schemas(0).getAttribute(key)).toArray: _*)
      .build()
  }

  private def getFinalAggValueSchema: Schema = {
    if (noDataCol) {
      Schema
        .newBuilder()
        .add(resultAttributeNames.head, AttributeType.INTEGER)
        .build()
    } else {
      Schema
        .newBuilder()
        .add(resultAttributeNames.map(key => new Attribute(key, AttributeType.DOUBLE)).toArray: _*)
        .build()
    }
  }

  def groupByFunc(): Schema => Schema = { schema =>
    {
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
