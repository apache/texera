package edu.uci.ics.texera.workflow.operators.hashJoin

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.{NewOpExecConfig, WorkerLayer}
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
import edu.uci.ics.texera.workflow.common.workflow.{HashPartition, PartitionInfo}

import scala.collection.convert.ImplicitConversions.`collection AsScalaIterable`

class HashJoinOpDesc[K] extends OperatorDescriptor {

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

  override def newOperatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    val partitionRequirement = List(
      Option(HashPartition(List(operatorSchemaInfo.inputSchemas(0).getIndex(buildAttributeName)))),
      Option(HashPartition(List(operatorSchemaInfo.inputSchemas(1).getIndex(probeAttributeName))))
    )

    NewOpExecConfig
      .oneToOneLayer(
        operatorIdentifier,
        _ =>
          new HashJoinOpExec[K](
            buildAttributeName,
            probeAttributeName,
            joinType,
            operatorSchemaInfo
          )
      )
      .copy(
        inputPorts = operatorInfo.inputPorts,
        outputPorts = operatorInfo.outputPorts,
        partitionRequirement = partitionRequirement,
        blockingInputs = List(0),
        dependency = Map(1 -> 0)
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Hash Join",
      "join two inputs",
      OperatorGroupConstants.JOIN_GROUP,
      inputPorts = List(InputPort("left"), InputPort("right")),
      outputPorts = List(OutputPort())
    )

  // remove the probe attribute in the output
  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 2)
    val builder = Schema.newBuilder()
    val buildSchema = schemas(0)
    val probeSchema = schemas(1)
    builder.add(buildSchema).removeIfExists(probeAttributeName)
    if (probeAttributeName.equals(buildAttributeName)) {
      probeSchema.getAttributes.foreach(attr => {
        val attributeName = attr.getName
        if (buildSchema.containsAttribute(attributeName) && attributeName != probeAttributeName) {
          // appending 1 to the output of Join schema in case of duplicate attributes in probe and build table
          builder.add(new Attribute(s"$attributeName#@1", attr.getType))
        } else {
          builder.add(attr)
        }
      })

    } else {
      probeSchema.getAttributes
        .forEach(attr => {
          val attributeName = attr.getName
          if (buildSchema.containsAttribute(attributeName)) {
            builder.add(new Attribute(s"$attributeName#@1", attr.getType))
          } else if (!attributeName.equalsIgnoreCase(probeAttributeName)) {
            builder.add(attr)
          }
        })
    }
    builder.build()
  }
}
