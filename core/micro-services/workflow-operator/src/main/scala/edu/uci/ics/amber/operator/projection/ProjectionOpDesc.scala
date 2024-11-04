package edu.uci.ics.amber.operator.projection

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{Attribute, Schema}
import edu.uci.ics.amber.core.workflow.{BroadcastPartition, HashPartition, PartitionInfo, PhysicalOp, RangePartition, SchemaPropagationFunc, SinglePartition, UnknownPartition}
import edu.uci.ics.amber.core.workflow.PhysicalOp.oneToOnePhysicalOp
import edu.uci.ics.amber.operator.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.map.MapOpDesc
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

class ProjectionOpDesc extends MapOpDesc {

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Drop Option")
  @JsonPropertyDescription("check to drop the selected attributes")
  var isDrop: Boolean = false

  var attributes: List[AttributeUnit] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecInitInfo((_, _) => new ProjectionOpExec(attributes, isDrop))
    )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withDerivePartition(derivePartition())
      .withPropagateSchema(SchemaPropagationFunc(inputSchemas => {
        Map(
          operatorInfo.outputPorts.head.id -> getOutputSchema(
            Array(inputSchemas(operatorInfo.inputPorts.head.id))
          )
        )
      }))
  }

  def derivePartition()(partition: List[PartitionInfo]): PartitionInfo = {
    val inputPartitionInfo = partition.head

    val outputPartitionInfo = inputPartitionInfo match {
      case HashPartition(hashAttributeNames) =>
        if (hashAttributeNames.nonEmpty) HashPartition(hashAttributeNames) else UnknownPartition()
      case RangePartition(rangeAttributeNames, min, max) =>
        if (rangeAttributeNames.nonEmpty) RangePartition(rangeAttributeNames, min, max)
        else UnknownPartition()
      case SinglePartition()    => inputPartitionInfo
      case BroadcastPartition() => inputPartitionInfo
      case UnknownPartition()   => inputPartitionInfo
    }

    outputPartitionInfo
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps or drops the column",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    Preconditions.checkArgument(attributes.nonEmpty)
    if (!isDrop) {
      Schema
        .builder()
        .add(attributes.map { attribute =>
          val originalType = schemas.head.getAttribute(attribute.getOriginalAttribute).getType
          new Attribute(attribute.getAlias, originalType)
        })
        .build()
    } else {
      val outputSchemaBuilder = Schema.builder()
      val inputSchema = schemas(0)
      outputSchemaBuilder.add(inputSchema)
      for (attribute <- attributes) {
        outputSchemaBuilder.removeIfExists(attribute.getOriginalAttribute)
      }
      outputSchemaBuilder.build()

    }

  }
}
