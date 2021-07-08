package edu.uci.ics.texera.workflow.operators.intervalJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}

class IntervalJoinOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input attr")
  @JsonPropertyDescription("Choose one attribute in the left table")
  @AutofillAttributeName
  var leftAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input attr")
  @JsonPropertyDescription("Choose one attribute in the right table")
  @AutofillAttributeNameOnPort1
  var rightAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Interval Constant")
  @JsonPropertyDescription(
    "The maximum length that the right input attribute can greater than the left Input attribute")
  var interval: Long = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Include Left Bound")
  @JsonPropertyDescription("Whether the condition holds when left input attr = right input attr")
  var leftBound: Boolean = true
  @JsonProperty(required = true)
  @JsonSchemaTitle("Include Right Bound")
  @JsonPropertyDescription(
    "Whether the condition holds when left input attr = right input attr + interval constant")
  var rightBound: Boolean = true

  @JsonProperty(required = false)
  @JsonSchemaTitle("Time interval type")
  @JsonPropertyDescription("Year, Month, Hour, etc.")
  var timeIntervalType: TimeIntervalType = TimeIntervalType.DAY
  @JsonIgnore
  var opExecConfig: IntervalJoinOpExecConfig = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    opExecConfig = new IntervalJoinOpExecConfig(
      operatorIdentifier,
      leftAttributeName,
      rightAttributeName,
      operatorSchemaInfo,
      interval,
      leftBound,
      rightBound,
      timeIntervalType
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Interval Join",
      "join two inputs with left table join key in the range of [right table join key, right table join key + constant value]",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("First Table"), InputPort("Second Table")),
      outputPorts = List(OutputPort())
    )
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    builder.add(schemas(0))
    if (rightAttributeName.equals(leftAttributeName)) {
      schemas(1).getAttributes
        .forEach(attr => {
          if (schemas(0).containsAttribute(attr.getName)) {
            // appending 1 to the output of Join schema in case of duplicate attributes in probe and build table
            builder.add(new Attribute(s"${attr.getName}#@1", attr.getType))
          } else {
            builder.add(attr)
          }
        })
    } else {
      schemas(1).getAttributes
        .forEach(attr => {
          if (schemas(0).containsAttribute(attr.getName)) {
            builder.add(new Attribute(s"${attr.getName}#@1", attr.getType))
          } else {
            builder.add(attr)
          }
        })
    }
    builder.build()
  }
}
