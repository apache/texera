package edu.uci.ics.amber.operator.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import edu.uci.ics.amber.operator.hashJoin.HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import edu.uci.ics.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  AutofillAttributeNameOnPort1
}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

object HashJoinOpDesc {
  val HASH_JOIN_INTERNAL_KEY_NAME = "__internal__hashtable__key__"
}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "buildAttributeName": {
      "const": {
        "$data": "probeAttributeName"
      }
    }
  }
}
""")
class HashJoinOpDesc[K] extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Left Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Left Input")
  @AutofillAttributeName
  var buildAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Right Input Attribute")
  @JsonPropertyDescription("attribute to be joined on the Right Input")
  @AutofillAttributeNameOnPort1
  var probeAttributeName: String = _

  @JsonProperty(required = true, defaultValue = "inner")
  @JsonSchemaTitle("Join Type")
  @JsonPropertyDescription("select the join type to execute")
  var joinType: JoinType = JoinType.INNER

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {

    val buildInputPort = operatorInfo.inputPorts.head
    val buildOutputPort = OutputPort(PortIdentity(0, internal = true), blocking = true)

    val buildPhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "build"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "edu.uci.ics.amber.operator.hashJoin.HashJoinBuildOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(List(buildInputPort))
        .withOutputPorts(List(buildOutputPort))
        .withPartitionRequirement(List(Option(HashPartition(List(buildAttributeName)))))
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas =>
            Map(
              PortIdentity(internal = true) -> Schema
                .builder()
                .add(HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.ANY)
                .add(inputSchemas(operatorInfo.inputPorts.head.id))
                .build()
            )
          )
        )
        .withParallelizable(true)

    val probeBuildInputPort = InputPort(PortIdentity(0, internal = true))
    val probeDataInputPort =
      InputPort(operatorInfo.inputPorts(1).id, dependencies = List(probeBuildInputPort.id))
    val probeOutputPort = OutputPort(PortIdentity(0))

    val probePhysicalOp =
      PhysicalOp
        .oneToOnePhysicalOp(
          PhysicalOpIdentity(operatorIdentifier, "probe"),
          workflowId,
          executionId,
          OpExecWithClassName(
            "edu.uci.ics.amber.operator.hashJoin.HashJoinProbeOpExec",
            objectMapper.writeValueAsString(this)
          )
        )
        .withInputPorts(
          List(
            probeBuildInputPort,
            probeDataInputPort
          )
        )
        .withOutputPorts(List(probeOutputPort))
        .withPartitionRequirement(
          List(Option(OneToOnePartition()), Option(HashPartition(List(probeAttributeName))))
        )
        .withDerivePartition(_ => HashPartition(List(probeAttributeName)))
        .withParallelizable(true)
        .withPropagateSchema(
          SchemaPropagationFunc(inputSchemas => {
            val buildSchema = inputSchemas(PortIdentity(internal = true))
            val probeSchema = inputSchemas(PortIdentity(1))
            val builder = Schema.builder()
            builder.add(buildSchema)
            builder.removeIfExists(HASH_JOIN_INTERNAL_KEY_NAME)
            val leftAttributeNames = buildSchema.getAttributeNames
            val rightAttributeNames =
              probeSchema.getAttributeNames.filterNot(name => name == probeAttributeName)

            // Create a Map from rightTuple's fields, renaming conflicts
            rightAttributeNames
              .foreach { name =>
                var newName = name
                while (
                  leftAttributeNames.contains(newName) || rightAttributeNames
                    .filter(attrName => name != attrName)
                    .contains(newName)
                ) {
                  newName = s"$newName#@1"
                }
                builder.add(new Attribute(newName, probeSchema.getAttribute(name).getType))
              }
            val outputSchema = builder.build()
            Map(PortIdentity() -> outputSchema)
          })
        )

    PhysicalPlan(
      operators = Set(buildPhysicalOp, probePhysicalOp),
      links = Set(
        PhysicalLink(
          buildPhysicalOp.id,
          buildOutputPort.id,
          probePhysicalOp.id,
          probeBuildInputPort.id
        )
      )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(0), displayName = "left"),
        InputPort(PortIdentity(1), displayName = "right", dependencies = List(PortIdentity(0)))
      ),
      outputPorts = List(OutputPort())
    )
}
