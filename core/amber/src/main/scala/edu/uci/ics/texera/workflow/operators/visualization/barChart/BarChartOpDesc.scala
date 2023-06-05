package edu.uci.ics.texera.workflow.operators.visualization.barChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameList
}
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.operators.aggregate.{
  AggregationFunction,
  AggregationOperation,
  SpecializedAggregateOpDesc
}
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

import java.util.Collections.singletonList
import scala.jdk.CollectionConverters.asScalaBuffer

/**
  * Supports a bar chart with internal aggregation for one name column (label, x-axis) and multiple data columns (y-axis).
  * If no data column is provided, the count of each label is returned; else the aggregated sum over each data column,
  * grouped by each label is returned.
  */
class BarChartOpDesc extends VisualizationOperator {
  @JsonProperty(value = "name column", required = true)
  @JsonPropertyDescription("column of name (for x-axis)")
  @AutofillAttributeName var nameColumn: String = _

  @JsonProperty(value = "data column(s)", required = false)
  @JsonPropertyDescription("column(s) of data (for y-axis)")
  @AutofillAttributeNameList var dataColumns: List[String] = _

  override def chartType: String = VisualizationConstants.BAR

  def noDataCol: Boolean = dataColumns == null || dataColumns.isEmpty

  def resultAttributeNames: List[String] = if (noDataCol) List("count") else dataColumns

  override def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo) = {
    if (nameColumn == null || nameColumn == "") {
      throw new RuntimeException("bar chart: name column is null or empty")
    }

    val aggOperator = new SpecializedAggregateOpDesc()
    aggOperator.context = this.context
    aggOperator.operatorID = this.operatorID
    if (noDataCol) {
      val aggOperation = new AggregationOperation()
      aggOperation.aggFunction = AggregationFunction.COUNT
      aggOperation.attribute = nameColumn
      aggOperation.resultAttribute = resultAttributeNames.head
      aggOperator.aggregations = List(aggOperation)
      aggOperator.groupByKeys = List(nameColumn)
    } else {
      val aggOperations = dataColumns.map(dataCol => {
        val aggOperation = new AggregationOperation()
        aggOperation.aggFunction = AggregationFunction.SUM
        aggOperation.attribute = dataCol
        aggOperation.resultAttribute = dataCol
        aggOperation
      })
      aggOperator.aggregations = aggOperations
      aggOperator.groupByKeys = List(nameColumn)
    }

    val aggPlan = aggOperator.aggregateOperatorExecutor(
      OperatorSchemaInfo(
        operatorSchemaInfo.inputSchemas,
        Array(aggOperator.getOutputSchema(operatorSchemaInfo.inputSchemas))
      )
    )

    val barChartViz = OpExecConfig.oneToOneLayer(
      makeLayer(operatorIdentifier, "visualize"),
      _ => new BarChartOpExec(this, operatorSchemaInfo)
    )

    val finalAggOp = aggPlan.sinkOperators.head
    aggPlan.addOperator(barChartViz).addEdge(finalAggOp, barChartViz.id)
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Bar Chart",
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

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    throw new UnsupportedOperationException("multi layer operators use operatorExecutorMultiLayer")
  }
}
