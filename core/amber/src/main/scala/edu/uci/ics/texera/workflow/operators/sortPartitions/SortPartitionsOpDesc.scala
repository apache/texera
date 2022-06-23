package edu.uci.ics.texera.workflow.operators.sortPartitions

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

class SortPartitionsOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("Attribute to sort (must be numerical).")
  @AutofillAttributeName
  var sortAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Min")
  @JsonPropertyDescription("Minimum value of the domain of the attribute.")
  var domainMin: Long = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Max")
  @JsonPropertyDescription("Maximum value of the domain of the attribute.")
  var domainMax: Long = _

  @JsonIgnore
  var opExecConfig: SortPartitionsOpExecConfig = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    opExecConfig = new SortPartitionsOpExecConfig(
      this.operatorIdentifier,
      sortAttributeName,
      domainMin,
      domainMax,
      operatorSchemaInfo
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort Partitions",
      "Sort Partitions",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("")),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
