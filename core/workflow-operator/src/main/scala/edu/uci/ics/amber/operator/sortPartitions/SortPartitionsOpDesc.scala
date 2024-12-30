package edu.uci.ics.amber.operator.sortPartitions

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.workflow.{PhysicalOp, RangePartition}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "sortAttributeName":{
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class SortPartitionsOpDesc extends LogicalOp {

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

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp =
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo(opExecFunc =
          (idx, workerCount) =>
            new SortPartitionOpExec(
              sortAttributeName,
              idx,
              domainMin,
              domainMax,
              workerCount
            )
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPartitionRequirement(
        List(Option(RangePartition(List(sortAttributeName), domainMin, domainMax)))
      )

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Sort Partitions",
      "Sort Partitions",
      OperatorGroupConstants.SORT_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort(blocking = true))
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    schemas(0)
  }
}
