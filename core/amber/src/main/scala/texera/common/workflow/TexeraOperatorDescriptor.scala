package texera.common.workflow

import java.util.UUID

import Engine.Common.AmberTag.OperatorTag
import Engine.Common.tuple.texera.schema.Schema
import Engine.Operators.OpExecConfig
import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}
import texera.common.schema.{PropertyNameConstants, TexeraOperatorDescription}
import texera.common.{TexeraConstraintViolation, TexeraContext}
import texera.operators.filter.TexeraFilterOpDesc
import texera.operators.localscan.TexeraLocalCsvFileScanOpDesc
import texera.operators.pythonUDF.TexeraPythonUDFOpDesc
import texera.operators.regex.TexeraRegexOpDesc
import texera.operators.sentiment.TexeraSentimentAnalysisOpDesc
import texera.operators.sink.TexeraSimpleSinkOpDesc


@JsonTypeInfo(
  use = JsonTypeInfo.Id.NAME,
  include = JsonTypeInfo.As.PROPERTY,
  property = "operatorType"
)
@JsonSubTypes(
  Array(
    new Type(value = classOf[TexeraLocalCsvFileScanOpDesc], name = "LocalFileScan"),
    new Type(value = classOf[TexeraSimpleSinkOpDesc], name = "AdhocSink"),
    new Type(value = classOf[TexeraRegexOpDesc], name = "Regex"),
    new Type(value = classOf[TexeraFilterOpDesc], name = "Filter"),
    new Type(value = classOf[TexeraSentimentAnalysisOpDesc], name = "SentimentAnalysis"),
//    new Type(value = classOf[TexeraPythonUDFOpDesc], name = "PythonUDF"),
  )
)
abstract class TexeraOperatorDescriptor {

  @JsonIgnore var context: TexeraContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID) var operatorID: String = UUID.randomUUID.toString

  def amberOperatorTag: OperatorTag = OperatorTag.apply(this.context.workflowID, this.operatorID)

  def amberOperator: OpExecConfig

  def texeraOperatorDescription: TexeraOperatorDescription

  def transformSchema(schemas: Schema*): Schema

  def validate(): Set[TexeraConstraintViolation] = {
    Set.empty
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

}
