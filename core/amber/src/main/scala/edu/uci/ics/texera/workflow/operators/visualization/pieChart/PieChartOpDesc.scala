package edu.uci.ics.texera.workflow.operators.visualization.pieChart

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.util.makeLayer
import edu.uci.ics.amber.engine.common.virtualidentity.{LinkIdentity, OperatorIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
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
import edu.uci.ics.texera.workflow.common.tuple.schema.{
  Attribute,
  AttributeType,
  OperatorSchemaInfo,
  Schema
}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.operators.visualization.{
  VisualizationConstants,
  VisualizationOperator
}

import java.util.Collections.singletonList
import scala.collection.JavaConverters.asScalaBuffer
import scala.reflect.ClassTag

/**
  * PieChart is a visualization operator that can be used to get tuples for pie chart.
  * PieChart returns tuples with name of data (String) and a number (the input can be int, double or String number,
  * but the output will be Double).
  * Note here we assume every name has exactly one data.
  * @author Mingji Han, Xiaozhen Liu
  */
class PieChartOpDesc extends VisualizationOperator {
  @JsonProperty(value = "name column", required = true)
  @JsonPropertyDescription("column of name (for chart label)")
  @AutofillAttributeName var nameColumn: String = _

  @JsonProperty(value = "data column")
  @JsonPropertyDescription("column of data")
  @AutofillAttributeName var dataColumn: String = _

  @JsonProperty(value = "prune ratio", required = true, defaultValue = "1")
  @JsonPropertyDescription("names below this ratio will be grouped into one \"Other\" category")
  var pruneRatio = .0

  @JsonProperty(value = "chart style", required = true, defaultValue = VisualizationConstants.PIE)
  var pieChartEnum: PieChartEnum = _

  @JsonIgnore
  private var groupBySchema: Schema = _
  @JsonIgnore
  private var finalAggValueSchema: Schema = _

  override def chartType: String = pieChartEnum.getChartStyle
  def noDataCol: Boolean = dataColumn == null || dataColumn.equals("")
  def resultAttributeNames: List[String] = if (noDataCol) List("count") else List(dataColumn)

  override def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo) = {
    if (nameColumn == null) throw new RuntimeException("pie chart: name column is null")
    if (pruneRatio < 0 || pruneRatio > 1)
      throw new RuntimeException("pie chart: prune ratio not within in [0,1]")
    this.groupBySchema = getGroupByKeysSchema(operatorSchemaInfo.inputSchemas)
    this.finalAggValueSchema = getFinalAggValueSchema
    def dataColumns: List[String] = List(dataColumn)

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

    val aggregateOperators =
      AggregateOpDesc.opExecPhysicalPlan(this.operatorIdentifier, aggregation, operatorSchemaInfo)
    val tailAggregateOp = aggregateOperators.sinkOperators.last

    val partialLayer = OpExecConfig
      .oneToOneLayer(
        makeLayer(this.operatorIdentifier, "localPieChartProcessor"),
        _ => new PieChartOpPartialExec(nameColumn, dataColumn)
      )
      .copy(isOneToManyOp = true)
    val finalLayer = OpExecConfig
      .localLayer(
        makeLayer(this.operatorIdentifier, "globalPieChartProcessor"),
        _ => new PieChartOpFinalExec(pruneRatio, dataColumn)
      )
      .copy(isOneToManyOp = true)

    new PhysicalPlan(
      aggregateOperators.operators :+ partialLayer :+ finalLayer,
      aggregateOperators.links :+ LinkIdentity(tailAggregateOp, partialLayer.id) :+ LinkIdentity(
        partialLayer.id,
        finalLayer.id
      )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Pie Chart",
      "View the result in pie chart",
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

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    throw new UnsupportedOperationException("implemented in operatorExecutorMultiLayer")
  }

}
