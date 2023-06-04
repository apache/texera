package edu.uci.ics.texera.workflow.operators.projection

import com.google.common.base.Preconditions
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig.oneToOneLayer
import edu.uci.ics.texera.workflow.common.metadata._
import edu.uci.ics.texera.workflow.common.operators.map.MapOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.{
  HashPartition,
  PartitionInfo,
  RangePartition,
  SinglePartition,
  UnknownPartition
}

import scala.collection.JavaConverters._

class ProjectionOpDesc extends MapOpDesc {

  var attributes: List[AttributeUnit] = List()

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    oneToOneLayer(operatorIdentifier, _ => new ProjectionOpExec(attributes, operatorSchemaInfo))
      .copy(derivePartition = this.derivePartition(operatorSchemaInfo))
  }

  def derivePartition(schema: OperatorSchemaInfo)(partition: List[PartitionInfo]): PartitionInfo = {
    val inputPartitionInfo = partition.head

    // a mapping from original column index to new column index
    lazy val columnIndicesMapping = attributes.indices
      .map(i => (schema.inputSchemas(0).getIndex(attributes(i).getOriginalAttribute), i))
      .toMap

    val outputPartitionInfo = inputPartitionInfo match {
      case HashPartition(hashColumnIndices) =>
        val newIndices = hashColumnIndices.flatMap(i => columnIndicesMapping.get(i))
        if (newIndices.nonEmpty) HashPartition(newIndices) else UnknownPartition()
      case RangePartition(rangeColumnIndices, min, max) =>
        val newIndices = rangeColumnIndices.flatMap(i => columnIndicesMapping.get(i))
        if (newIndices.nonEmpty) RangePartition(newIndices, min, max) else UnknownPartition()
      case SinglePartition()  => inputPartitionInfo
      case UnknownPartition() => inputPartitionInfo
    }

    outputPartitionInfo
  }

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      "Projection",
      "Keeps the column",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort()),
      supportReconfiguration = false
    )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    Preconditions.checkArgument(attributes.nonEmpty)

    Schema.newBuilder
      .add(
        attributes
          .map(attribute =>
            new Attribute(
              attribute.getAlias(),
              schemas(0).getAttribute(attribute.getOriginalAttribute()).getType
            )
          )
          .asJava
      )
      .build()
  }
}
