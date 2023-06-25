package edu.uci.ics.texera.workflow.operators.sortPartitions

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.engine.architecture.deploysemantics.layer.OpExecConfig
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName
import edu.uci.ics.texera.workflow.common.metadata.{
  InputPort,
  OperatorGroupConstants,
  OperatorInfo,
  OutputPort
}
import edu.uci.ics.texera.workflow.common.operators.OperatorDescriptor
import edu.uci.ics.texera.workflow.common.tuple.schema.{OperatorSchemaInfo, Schema}
import edu.uci.ics.texera.workflow.common.workflow.RangePartition

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "sortAttributeName":{
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class SortPartitionsOpDesc extends OperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @JsonPropertyDescription("Attribute to sort (must be numerical).")
  @AutofillAttributeName
  var sortAttributeName: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Min")
  @JsonPropertyDescription("Minimum value of the domain of the attribute.")
  var domainMin: Long = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Domain Max")
  @JsonPropertyDescription("Maximum value of the domain of the attribute.")
  var domainMax: Long = _

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo) = {
    val partitionRequirement = List(
      Option(
        RangePartition(
          List(operatorSchemaInfo.inputSchemas(0).getIndex(sortAttributeName)),
          domainMin,
          domainMax
        )
      )
    )

    OpExecConfig
      .oneToOneLayer(
        operatorIdentifier,
        p =>
          new SortPartitionOpExec(
            sortAttributeName,
            operatorSchemaInfo,
            p._1,
            domainMin,
            domainMax,
            p._2.numWorkers
          )
      )
      .copy(
        partitionRequirement = partitionRequirement
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort Partitions",
      "Sort Partitions",
      OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort("")),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
