package edu.uci.ics.amber.operator.hashJoin

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{HashPartition, OneToOnePartition, PhysicalOp, PhysicalPlan, SchemaPropagationFunc}
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, PhysicalOpIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort, PhysicalLink, PortIdentity}
import HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.common.annotations.{AutofillAttributeName, AutofillAttributeNameOnPort1}
import edu.uci.ics.amber.operator.common.metadata.{OperatorGroupConstants, OperatorInfo}

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
          OpExecInitInfo((_, _) => new HashJoinBuildOpExec[K](buildAttributeName))
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
          OpExecInitInfo((_, _) =>
            new HashJoinProbeOpExec[K](
              probeAttributeName,
              joinType
            )
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
          SchemaPropagationFunc(inputSchemas =>
            Map(
              PortIdentity() -> getOutputSchema(
                Array(inputSchemas(PortIdentity(internal = true)), inputSchemas(PortIdentity(1)))
              )
            )
          )
        )

    new PhysicalPlan(
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

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    val buildSchema = schemas(0)
    val probeSchema = schemas(1)
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
    builder.build()
  }
}
