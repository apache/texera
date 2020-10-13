package texera.common.operators

import java.util.UUID

import Engine.Common.AmberTag.OperatorIdentifier
import Engine.Operators.OpExecConfig
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import texera.common.metadata.{PropertyNameConstants, TexeraOperatorInfo}
import texera.common.tuple.schema.Schema
import texera.common.{TexeraConstraintViolation, TexeraContext}
import texera.operators.aggregate.AverageOpDesc
import texera.operators.filter.SpecializedFilterOpDesc
import texera.operators.localscan.LocalCsvFileScanOpDesc
import texera.operators.regex.RegexOpDesc
import texera.operators.sentiment.SentimentAnalysisOpDesc
import texera.operators.sink.SimpleSinkOpDesc


@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[LocalCsvFileScanOpDesc], name = "LocalFileScan"),
    new Type(value = classOf[SimpleSinkOpDesc], name = "AdhocSink"),
    new Type(value = classOf[RegexOpDesc], name = "Regex"),
    new Type(value = classOf[SpecializedFilterOpDesc], name = "Filter"),
    new Type(value = classOf[SentimentAnalysisOpDesc], name = "SentimentAnalysis"),
    new Type(value = classOf[AverageOpDesc], name = "Average"),
//    new Type(value = classOf[TexeraPythonUDFOpDesc], name = "PythonUDF"),
  )
)
abstract class TexeraOperatorDescriptor extends Serializable {

  @JsonIgnore var context: TexeraContext = _


  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var operatorID: String = UUID.randomUUID.toString

  def operatorIdentifier: OperatorIdentifier = OperatorIdentifier.apply(this.context.workflowID, this.operatorID)

  def texeraOperatorExecutor: OpExecConfig

  def texeraOperatorInfo: TexeraOperatorInfo

  def transformSchema(schemas: Schema*): Schema

  def validate(): Array[TexeraConstraintViolation] = {
    Array()
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

}
