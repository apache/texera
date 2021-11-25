package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.amber.engine.common.Constants
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorInfo, PropertyNameConstants}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.aggregate.SpecializedAverageOpDesc
import edu.uci.ics.texera.workflow.operators.filter.SpecializedFilterOpDesc
import edu.uci.ics.texera.workflow.operators.hashJoin.HashJoinOpDesc
import edu.uci.ics.texera.workflow.operators.keywordSearch.KeywordSearchOpDesc
import edu.uci.ics.texera.workflow.operators.limit.LimitOpDesc
import edu.uci.ics.texera.workflow.operators.linearregression.LinearRegressionOpDesc
import edu.uci.ics.texera.workflow.operators.localscan.LocalCsvFileScanOpDesc
import edu.uci.ics.texera.workflow.operators.projection.ProjectionOpDesc
import edu.uci.ics.texera.workflow.operators.pythonUDF.PythonUDFOpDesc
import edu.uci.ics.texera.workflow.operators.randomksampling.RandomKSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
import edu.uci.ics.texera.workflow.operators.reservoirsampling.ReservoirSamplingOpDesc
import edu.uci.ics.texera.workflow.operators.sentiment.SentimentAnalysisOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import edu.uci.ics.texera.workflow.operators.source.mysql.MysqlSourceOpDesc
import edu.uci.ics.texera.workflow.operators.typecasting.TypeCastingOpDesc
import edu.uci.ics.texera.workflow.operators.union.UnionOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.barChart.BarChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.lineChart.LineChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.pieChart.PieChartOpDesc
import edu.uci.ics.texera.workflow.operators.visualization.wordCloud.WordCloudOpDesc
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import java.util.UUID

import edu.uci.ics.amber.engine.common.virtualidentity.OperatorIdentity
import edu.uci.ics.texera.workflow.operators.generated.GenerateDataSourceOpDesc
import edu.uci.ics.texera.workflow.operators.hashJoinSpecial.HashJoinSpecialOpDesc
import edu.uci.ics.texera.workflow.operators.hashJoinSpecial2.HashJoinSpecial2OpDesc
import edu.uci.ics.texera.workflow.operators.hashJoinTweets.HashJoinTweetsOpDesc
import edu.uci.ics.texera.workflow.operators.hdfsFileSink.HdfsFileSinkOpDesc
import edu.uci.ics.texera.workflow.operators.hdfsscan.HdfsScanOpDesc
import edu.uci.ics.texera.workflow.operators.sort.SortOpDesc
import edu.uci.ics.texera.workflow.operators.sortOneLayer.SortOneLayerOpDesc

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[GenerateDataSourceOpDesc], name = "GenerateData"),
    new Type(value = classOf[LocalCsvFileScanOpDesc], name = "LocalCsvFileScan"),
    new Type(value = classOf[SimpleSinkOpDesc], name = "SimpleSink"),
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
    new Type(value = classOf[SpecializedFilterOpDesc], name = "Filter"),
    new Type(value = classOf[SentimentAnalysisOpDesc], name = "SentimentAnalysis"),
    new Type(value = classOf[ProjectionOpDesc], name = "Projection"),
    new Type(value = classOf[UnionOpDesc], name = "Union"),
    new Type(value = classOf[KeywordSearchOpDesc], name = "KeywordSearch"),
    new Type(value = classOf[SpecializedAverageOpDesc], name = "Aggregate"),
    new Type(value = classOf[LinearRegressionOpDesc], name = "LinearRegression"),
    new Type(value = classOf[LineChartOpDesc], name = "LineChart"),
    new Type(value = classOf[BarChartOpDesc], name = "BarChart"),
    new Type(value = classOf[PieChartOpDesc], name = "PieChart"),
    new Type(value = classOf[WordCloudOpDesc], name = "WordCloud"),
    new Type(value = classOf[PythonUDFOpDesc], name = "PythonUDF"),
    new Type(value = classOf[MysqlSourceOpDesc], name = "MysqlSource"),
    new Type(value = classOf[TypeCastingOpDesc], name = "TypeCasting"),
    new Type(value = classOf[LimitOpDesc], name = "Limit"),
    new Type(value = classOf[RandomKSamplingOpDesc], name = "RandomKSampling"),
    new Type(value = classOf[ReservoirSamplingOpDesc], name = "ReservoirSampling"),
    new Type(value = classOf[HashJoinOpDesc[Constants.joinType]], name = "HashJoin"),
    new Type(value = classOf[HashJoinSpecialOpDesc[Constants.joinType]], name = "HashSpecialJoin"),
    new Type(
      value = classOf[HashJoinSpecial2OpDesc[Constants.joinType]],
      name = "HashSpecialJoin2"
    ),
    new Type(value = classOf[HdfsScanOpDesc], name = "HdfsScan"),
    new Type(value = classOf[SortOpDesc], name = "Sort"),
    new Type(value = classOf[SortOneLayerOpDesc], name = "SortOneLayer"),
    new Type(value = classOf[HdfsFileSinkOpDesc], name = "HdfsFileSink"),
    new Type(value = classOf[HashJoinTweetsOpDesc[Constants.joinType]], name = "HashJoinTweets")
  )
)
abstract class OperatorDescriptor extends Serializable {

  @JsonIgnore var context: WorkflowContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var operatorID: String = UUID.randomUUID.toString

  def operatorIdentifier: OperatorIdentity =
    OperatorIdentity(this.context.workflowID, this.operatorID)

  def operatorExecutor: OpExecConfig

  def operatorInfo: OperatorInfo

  def getOutputSchema(schemas: Array[Schema]): Schema

  def validate(): Array[ConstraintViolation] = {
    Array()
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

}
