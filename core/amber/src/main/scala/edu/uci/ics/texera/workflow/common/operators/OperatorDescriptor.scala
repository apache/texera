package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.amber.engine.common.IOperatorExecutor
import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.web.OPversion
import edu.uci.ics.texera.workflow.common.metadata.{OperatorInfo, PropertyNameConstants}
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan
import edu.uci.ics.texera.workflow.common.WorkflowContext
import edu.uci.ics.texera.workflow.operators.aggregate.SpecializedAggregateOpDesc
import edu.uci.ics.texera.workflow.operators.cartesianProduct.CartesianProductOpDesc
import edu.uci.ics.texera.workflow.operators.dictionary.DictionaryMatcherOpDesc
import edu.uci.ics.texera.workflow.operators.difference.DifferenceOpDesc
import edu.uci.ics.texera.workflow.operators.distinct.DistinctOpDesc
import edu.uci.ics.texera.workflow.operators.download.BulkDownloaderOpDesc
import edu.uci.ics.texera.workflow.operators.filter.SpecializedFilterOpDesc
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.intersect.IntersectOpDesc
import edu.uci.ics.texera.workflow.operators.intervalJoin.IntervalJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.limit.LimitOpDesc
import edu.uci.ics.texera.workflow.operators.linearregression.LinearRegressionOpDesc
import edu.uci.ics.texera.workflow.operators.projection.ProjectionOpDesc
import edu.uci.ics.texera.workflow.operators.randomksampling.RandomKSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
import edu.uci.ics.texera.workflow.operators.reservoirsampling.ReservoirSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.sentiment.SentimentAnalysisOpDesc
import edu.uci.ics.texera.workflow.operators.sink.managed.ProgressiveSinkOpDesc
import edu.uci.ics.texera.workflow.operators.sortPartitions.SortPartitionsOpDesc
import edu.uci.ics.texera.workflow.operators.source.apis.reddit.RedditSearchSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.apis.twitter.v2.{
  TwitterFullArchiveSearchSourceOpDesc,
  TwitterSearchSourceOpDesc
}
import edu.uci.ics.texera.workflow.operators.source.fetcher.URLFetcherOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csv.CSVScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.csvOld.CSVOldScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.json.JSONLScanSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.scan.text.{
  TextInputSourceOpDesc,
  TextScanSourceOpDesc
}
import edu.uci.ics.texera.workflow.operators.source.sql.asterixdb.AsterixDBSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.mysql.MySQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.source.sql.postgresql.PostgreSQLSourceOpDesc
import edu.uci.ics.texera.workflow.operators.split.SplitOpDesc
import edu.uci.ics.texera.workflow.operators.symmetricDifference.SymmetricDifferenceOpDesc
import edu.uci.ics.texera.workflow.operators.typecasting.TypeCastingOpDesc
import edu.uci.ics.texera.workflow.operators.udf.python.source.PythonUDFSourceOpDescV2
import edu.uci.ics.texera.workflow.operators.udf.python.{
  DualInputPortsPythonUDFOpDescV2,
  PythonLambdaFunctionOpDesc,
  PythonTableReducerOpDesc,
  PythonUDFOpDescV2
}
import edu.uci.ics.texera.workflow.operators.union.UnionOpDesc
import edu.uci.ics.texera.workflow.operators.unneststring.UnnestStringOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.boxPlot.BoxPlotOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.barChart.BarChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.htmlviz.HtmlVizOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.lineChart.LineChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.pieChart.PieChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.scatterplot.ScatterplotOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.timeseries.TimeSeriesOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.ganttChart.GanttChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.urlviz.UrlVizOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.DotPlot.DotPlotOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.wordCloud.{
  WordCloudOpDesc,
  WordCloudV2OpDesc
}
import edu.uci.ics.texera.workflow.operators.visualization.filledAreaPlot.FilledAreaPlotOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.bubbleChart.BubbleChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.ImageViz.ImageVisualizerOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.hierarchychart.HierarchyChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.dumbbellPlot.DumbbellPlotOpDesc
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}

import java.util.UUID
import scala.util.Try

trait StateTransferFunc
    extends ((IOperatorExecutor, IOperatorExecutor) => Unit)
    with java.io.Serializable

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[CSVScanSourceOpDesc], name = "CSVFileScan"),
    // disabled the ParallelCSVScanSourceOpDesc so that it does not confuse user. it can be re-enabled when doing experiments.
    // new Type(value = classOf[ParallelCSVScanSourceOpDesc], name = "ParallelCSVFileScan"),
    new Type(value = classOf[JSONLScanSourceOpDesc], name = "JSONLFileScan"),
    new Type(value = classOf[TextScanSourceOpDesc], name = "TextFileScan"),
    new Type(value = classOf[TextInputSourceOpDesc], name = "TextInput"),
    new Type(
      value = classOf[TwitterFullArchiveSearchSourceOpDesc],
      name = "TwitterFullArchiveSearch"
    ),
    new Type(
      value = classOf[TwitterSearchSourceOpDesc],
      name = "TwitterSearch"
    ),
    new Type(value = classOf[ProgressiveSinkOpDesc], name = "SimpleSink"),
    new Type(value = classOf[SplitOpDesc], name = "Split"),
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
    new Type(value = classOf[SpecializedFilterOpDesc], name = "Filter"),
    new Type(value = classOf[SentimentAnalysisOpDesc], name = "SentimentAnalysis"),
    new Type(value = classOf[ProjectionOpDesc], name = "Projection"),
    new Type(value = classOf[UnionOpDesc], name = "Union"),
    new Type(value = classOf[KeywordSearchOpDesc], name = "KeywordSearch"),
    new Type(value = classOf[SpecializedAggregateOpDesc], name = "Aggregate"),
    new Type(value = classOf[LinearRegressionOpDesc], name = "LinearRegression"),
    new Type(value = classOf[LineChartOpDesc], name = "LineChart"),
    new Type(value = classOf[BarChartOpDesc], name = "BarChart"),
    new Type(value = classOf[PieChartOpDesc], name = "PieChart"),
    new Type(value = classOf[WordCloudOpDesc], name = "WordCloud"),
    new Type(value = classOf[WordCloudV2OpDesc], name = "WordCloudV2"),
    new Type(value = classOf[HtmlVizOpDesc], name = "HTMLVisualizer"),
    new Type(value = classOf[UrlVizOpDesc], name = "URLVisualizer"),
    new Type(value = classOf[ScatterplotOpDesc], name = "Scatterplot"),
    new Type(value = classOf[PythonUDFOpDescV2], name = "PythonUDFV2"),
    new Type(value = classOf[PythonUDFSourceOpDescV2], name = "PythonUDFSourceV2"),
    new Type(value = classOf[DualInputPortsPythonUDFOpDescV2], name = "DualInputPortsPythonUDFV2"),
    new Type(value = classOf[MySQLSourceOpDesc], name = "MySQLSource"),
    new Type(value = classOf[PostgreSQLSourceOpDesc], name = "PostgreSQLSource"),
    new Type(value = classOf[AsterixDBSourceOpDesc], name = "AsterixDBSource"),
    new Type(value = classOf[TypeCastingOpDesc], name = "TypeCasting"),
    new Type(value = classOf[LimitOpDesc], name = "Limit"),
    new Type(value = classOf[RandomKSamplingOpDesc], name = "RandomKSampling"),
    new Type(value = classOf[ReservoirSamplingOpDesc], name = "ReservoirSampling"),
    new Type(value = classOf[HashJoinOpDesc[String]], name = "HashJoin"),
    new Type(value = classOf[DistinctOpDesc], name = "Distinct"),
    new Type(value = classOf[IntersectOpDesc], name = "Intersect"),
    new Type(value = classOf[SymmetricDifferenceOpDesc], name = "SymmetricDifference"),
    new Type(value = classOf[DifferenceOpDesc], name = "Difference"),
    new Type(value = classOf[IntervalJoinOpDesc], name = "IntervalJoin"),
    new Type(value = classOf[UnnestStringOpDesc], name = "UnnestString"),
    new Type(value = classOf[DictionaryMatcherOpDesc], name = "DictionaryMatcher"),
    new Type(value = classOf[SortPartitionsOpDesc], name = "SortPartitions"),
    new Type(value = classOf[CSVOldScanSourceOpDesc], name = "CSVOldFileScan"),
    new Type(value = classOf[RedditSearchSourceOpDesc], name = "RedditSearch"),
    new Type(value = classOf[PythonLambdaFunctionOpDesc], name = "PythonLambdaFunction"),
    new Type(value = classOf[PythonTableReducerOpDesc], name = "PythonTableReducer"),
    new Type(value = classOf[BulkDownloaderOpDesc], name = "BulkDownloader"),
    new Type(value = classOf[URLFetcherOpDesc], name = "URLFetcher"),
    new Type(value = classOf[CartesianProductOpDesc], name = "CartesianProduct"),
    new Type(value = classOf[FilledAreaPlotOpDesc], name = "FilledAreaPlot"),
    new Type(value = classOf[DotPlotOpDesc], name = "DotPlot"),
    new Type(value = classOf[BubbleChartOpDesc], name = "BubbleChart"),
    new Type(value = classOf[TimeSeriesOpDesc], name = "TimeSeries"),
    new Type(value = classOf[GanttChartOpDesc], name = "GanttChart"),
    new Type(value = classOf[ImageVisualizerOpDesc], name = "ImageVisualizer"),
    new Type(value = classOf[HierarchyChartOpDesc], name = "HierarchyChart"),
    new Type(value = classOf[DumbbellPlotOpDesc], name = "DumbbellPlot"),
    new Type(value = classOf[BoxPlotOpDesc], name = "BoxPlot")
  )
)
abstract class OperatorDescriptor extends Serializable {

  @JsonIgnore
  var context: WorkflowContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var operatorID: String = getClass.getSimpleName + "-" + UUID.randomUUID.toString

  @JsonProperty(PropertyNameConstants.OPERATOR_VERSION)
  var operatorVersion: String = getOperatorVersion()
  def operatorIdentifier: OperatorIdentity = OperatorIdentity(context.jobId, operatorID)

  def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    throw new UnsupportedOperationException(
      "operator " + operatorIdentifier + " is not migrated to new OpExec API"
    )
  }

  // a logical operator corresponds multiple physical operators (a small DAG)
  def operatorExecutorMultiLayer(operatorSchemaInfo: OperatorSchemaInfo): PhysicalPlan = {
    new PhysicalPlan(List(operatorExecutor(operatorSchemaInfo)), List())
  }

  def operatorInfo: OperatorInfo

  def getOutputSchema(schemas: Array[Schema]): Schema

  def getOperatorVersion(): String = {
    val path = "core/amber/src/main/scala/"
    val operatorPath = path + this.getClass.getPackage.getName.replace(".", "/")
    OPversion.getVersion(this.getClass.getSimpleName, operatorPath)
  }

  // override if the operator has multiple output ports, schema must be specified for each port
  def getOutputSchemas(schemas: Array[Schema]): Array[Schema] = {
    Array.fill(1)(getOutputSchema(schemas))
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that, "context")

  override def toString: String = ToStringBuilder.reflectionToString(this)

  def setContext(workflowContext: WorkflowContext): Unit = {
    this.context = workflowContext
  }

  def runtimeReconfiguration(
      newOpDesc: OperatorDescriptor,
      operatorSchemaInfo: OperatorSchemaInfo
  ): Try[(OpExecConfig, Option[StateTransferFunc])] = {
    throw new UnsupportedOperationException(
      "operator " + getClass.getSimpleName + " does not support reconfiguration"
    )
  }

}
