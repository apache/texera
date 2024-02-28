package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.PhysicalOp.oneToOnePhysicalOp
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecInitInfo
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort}
import edu.uci.ics.texera.workflow.common.metadata._
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{
  BroadcastPartition,
  HashPartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}

class ProjectionOpDesc extends MapOpDesc {

  var attributes: List[AttributeUnit] = List()

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    oneToOnePhysicalOp(
      workflowId,
      executionId,
      operatorIdentifier,
      OpExecInitInfo((_, _, _) => new ProjectionOpExec(attributes))
    )
      .withInputPorts(operatorInfo.inputPorts, inputPortToSchemaMapping)
      .withOutputPorts(operatorInfo.outputPorts, outputPortToSchemaMapping)
      .withDerivePartition(derivePartition())
  }

  def derivePartition()(partition: List[PartitionInfo]): PartitionInfo = {
    val inputPartitionInfo = partition.head

    // mapping from original column index to new column index
    lazy val columnIndicesMapping: Map[Int, Int] = attributes.view
      .map(attr =>
        inputPortToSchemaMapping(operatorInfo.inputPorts.head.id)
          .getIndex(attr.getOriginalAttribute) -> attributes.indexOf(attr)
      )
      .toMap

    val outputPartitionInfo = inputPartitionInfo match {
      case HashPartition(hashAttributeNames) =>
        if (hashAttributeNames.nonEmpty) HashPartition(hashAttributeNames) else UnknownPartition()
      case RangePartition(rangeColumnIndices, min, max) =>
        val newIndices = rangeColumnIndices.flatMap(i => columnIndicesMapping.get(i))
        if (newIndices.nonEmpty) RangePartition(newIndices, min, max) else UnknownPartition()
      case SinglePartition()    => inputPartitionInfo
      case BroadcastPartition() => inputPartitionInfo
      case UnknownPartition()   => inputPartitionInfo
    }

    outputPartitionInfo
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps the column",
      OperatorGroupConstants.CLEANING_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    Preconditions.checkArgument(attributes.nonEmpty)

    Schema
      .builder()
      .add(attributes.map { attribute =>
        val originalType = schemas.head.getAttribute(attribute.getOriginalAttribute).getType
        new Attribute(attribute.getAlias, originalType)
      })
      .build()

  }
}
