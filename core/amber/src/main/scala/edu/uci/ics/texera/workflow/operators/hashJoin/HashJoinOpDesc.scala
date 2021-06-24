package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema, OperatorSchemaInfo}

class HashJoinOpDesc[K] extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Small Input attr")
  @JsonPropertyDescription("Small Input Join Key")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Large input attr")
  @JsonPropertyDescription("Large Input Join Key")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonIgnore
  var opExecConfig: HashJoinOpExecConfig[K] = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OpExecConfig = {
    opExecConfig = new HashJoinOpExecConfig[K](
      operatorIdentifier,
      probeAttributeName,
      buildAttributeName,
      operatorSchemaInfo
    )
    opExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("small"), InputPort("large")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    builder.add(schemas(0)).removeIfExists(probeAttributeName)
    if (probeAttributeName.equals(buildAttributeName)) {
      schemas(1)
        .getAttributes()
        .forEach(attr => {
          if (
            schemas(0).containsAttribute(attr.getName()) && attr.getName() != probeAttributeName
          ) {
            // appending 1 to the output of Join schema in case of duplicate attributes in probe and build table
            builder.add(new Attribute(s"${attr.getName()}#@1", attr.getType()))
          } else {
            builder.add(attr)
          }
        })
    } else {
      schemas(1)
        .getAttributes()
        .forEach(attr => {
          if (schemas(0).containsAttribute(attr.getName())) {
            builder.add(new Attribute(s"${attr.getName()}#@1", attr.getType()))
          } else if (!attr.getName().equalsIgnoreCase(probeAttributeName)) {
            builder.add(attr)
          }
        })
    }
    builder.build()
  }
}
