package edu.uci.ics.texera.workflow.common.operators

import java.util.UUID

import com.fasterxml.jackson.annotation.JsonSubTypes.Type
import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonSubTypes, JsonTypeInfo}
import edu.uci.ics.amber.engine.common.ambertag.OperatorIdentifier
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{PropertyNameConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.{ConstraintViolation, WorkflowContext}
import edu.uci.ics.texera.workflow.operators.aggregate.AverageOpDesc
import edu.uci.ics.texera.workflow.operators.filter.SpecializedFilterOpDesc
import edu.uci.ics.texera.workflow.operators.localscan.LocalCsvFileScanOpDesc
import edu.uci.ics.texera.workflow.operators.regex.RegexOpDesc
import edu.uci.ics.texera.workflow.operators.sentiment.SentimentAnalysisOpDesc
import edu.uci.ics.texera.workflow.operators.sink.SimpleSinkOpDesc
import org.apache.commons.lang3.builder.{EqualsBuilder, HashCodeBuilder, ToStringBuilder}

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
    new Type(value = classOf[AverageOpDesc], name = "Average")
//    new Type(value = classOf[TexeraPythonUDFOpDesc], name = "PythonUDF"),
  )
)
abstract class OperatorDescriptor extends Serializable {

  @JsonIgnore var context: WorkflowContext = _

  @JsonProperty(PropertyNameConstants.OPERATOR_ID)
  var operatorID: String = UUID.randomUUID.toString

  def operatorIdentifier: OperatorIdentifier =
    OperatorIdentifier.apply(this.context.workflowID, this.operatorID)

  def operatorExecutor: OpExecConfig

  def operatorInfo: OperatorInfo

  def getOutputSchema(schemas: Schema*): Schema

  def validate(): Array[ConstraintViolation] = {
    Array()
  }

  override def hashCode: Int = HashCodeBuilder.reflectionHashCode(this)

  override def equals(that: Any): Boolean = EqualsBuilder.reflectionEquals(this, that)

  override def toString: String = ToStringBuilder.reflectionToString(this)

}
