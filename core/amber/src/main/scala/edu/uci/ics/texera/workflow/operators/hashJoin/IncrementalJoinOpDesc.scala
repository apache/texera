package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
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
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "leftAttributeName": {
      "const": {
        "$data": "rightAttributeName"
      }
    }
  }
}
""")
class IncrementalJoinOpDesc[K] extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var leftAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var rightAttributeName: String = _

  // incremental inner join can reuse some logic from hash join
  @JsonIgnore
  lazy val hashJoinOpDesc: HashJoinOpDesc[K] = {
    val op = new HashJoinOpDesc[K]
    op.buildAttributeName = leftAttributeName
    op.probeAttributeName = rightAttributeName
    op.joinType = JoinType.INNER
    op
  }

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    hashJoinOpDesc.setContext(this.context)
    val hashJoinOpExec = hashJoinOpDesc.operatorExecutor(operatorSchemaInfo)

    OpExecConfig
      .oneToOneLayer(
        operatorIdentifier,
        _ =>
          new IncrementalJoinOpExec[K](
            leftAttributeName,
            rightAttributeName,
            operatorSchemaInfo
          )
      )
      .copy(
        inputPorts = operatorInfo.inputPorts,
        outputPorts = operatorInfo.outputPorts,
        partitionRequirement = hashJoinOpExec.partitionRequirement,
        derivePartition = hashJoinOpExec.derivePartition
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Progressive Inner Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    hashJoinOpDesc.getOutputSchema(schemas)
  }
}
