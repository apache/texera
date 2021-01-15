package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.operators.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.operators.filter.FilterOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema

class HashJoinOpDesc[K] extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Larger table attribute")
  @JsonPropertyDescription("Join attribute name from the larger of the two tables")
  var probeAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Smaller table attribute")
  @JsonPropertyDescription("Join attribute name from the smaller of the two tables")
  var buildAttribute: String = _

  @JsonIgnore
  var hashJoinOpExecConfig: HashJoinOpExecConfig = _

  override def operatorExecutor: OpExecConfig = {
    hashJoinOpExecConfig = new HashJoinOpExecConfig(
      this.operatorIdentifier,
      _ => new HashJoinOpExec[K](this),
      probeAttribute,
      buildAttribute
    )
    hashJoinOpExecConfig
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Hash Join",
      operatorDescription = "Join two tables on specific columns",
      operatorGroupName = OperatorGroupConstants.JOIN_GROUP,
      numInputPorts = 2,
      numOutputPorts = 1
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (schemas.length != 2) {
      return Schema.newBuilder().build()
    }
    // Preconditions.checkArgument(schemas.length == 2)

    if (buildAttribute.equals(probeAttribute)) {
      Schema.newBuilder
        .add(schemas(0))
        .removeIfExists(buildAttribute)
        .add(schemas(1))
        .build()
    } else {
      Schema.newBuilder
        .add(schemas(0))
        .removeIfExists(buildAttribute)
        .add(schemas(1))
        .removeIfExists(buildAttribute)
        .build()
    }
  }
}
