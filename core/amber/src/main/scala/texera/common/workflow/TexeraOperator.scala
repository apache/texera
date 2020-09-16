package texera.common.workflow

import java.util.UUID

import Engine.Common.AmberTag.OperatorTag
import Engine.Operators.OperatorMetadata
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import texera.common.schema.{PropertyNameConstants, TexeraOperatorDescription}
import texera.common.{TexeraConstraintViolation, TexeraContext}
import texera.operators.count.TexeraCount
import texera.operators.filter.TexeraFilter
import texera.operators.hdfsscan.TexeraHdfsFileScan
import texera.operators.keyword.TexeraKeywordSearch
import texera.operators.localscan.TexeraLocalFileScan
import texera.operators.pythonUDF.TexeraPythonUDF
import texera.operators.regex.TexeraRegex
import texera.operators.sentiment.TexeraSentimentAnalysis
import texera.operators.sink.TexeraAdhocSink
import texera.operators.sleep.TexeraSleepOperator
import texera.operators.visualization.barChart.TexeraBarChart
import texera.operators.visualization.lineChart.TexeraLineChart

import scala.collection.{JavaConverters, mutable}

@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[TexeraLocalFileScan], name = "LocalFileScan"),
//  new Type(value = classOf[TexeraHdfsFileScan], name = "HdfsFileScan"),
    new Type(value = classOf[TexeraAdhocSink], name = "AdhocSink"),
    new Type(value = classOf[TexeraSleepOperator], name = "Sleep"),
    new Type(value = classOf[TexeraKeywordSearch], name = "KeywordSearch"),
    new Type(value = classOf[TexeraRegex], name = "Regex"),
    new Type(value = classOf[TexeraFilter], name = "Filter"),
    new Type(value = classOf[TexeraCount], name = "Count"),
    new Type(value = classOf[TexeraSentimentAnalysis], name = "SentimentAnalysis"),
    new Type(value = classOf[TexeraPythonUDF], name = "PythonUDF"),
    new Type(value = classOf[TexeraBarChart], name = "BarChart"),
    new Type(value = classOf[TexeraLineChart], name = "LineChart"),
  )
)
abstract class TexeraOperator {

  @JsonIgnore var context: TexeraContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID) var operatorID: String = UUID.randomUUID.toString

  def amberOperatorTag: OperatorTag = OperatorTag.apply(this.context.workflowID, this.operatorID)

  def amberOperator: OperatorMetadata

  def texeraOperatorDescription: TexeraOperatorDescription

  def validate(): Set[TexeraConstraintViolation] = {
    Set()
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

}
