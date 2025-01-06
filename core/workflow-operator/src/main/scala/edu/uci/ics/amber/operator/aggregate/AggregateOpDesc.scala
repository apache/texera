package edu.uci.ics.amber.operator.aggregate

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.Schema
import edu.uci.ics.amber.core.virtualidentity.{
  ExecutionIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import edu.uci.ics.amber.core.workflow._
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeNameList
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.util.JSONUtils.objectMapper

import javax.validation.constraints.{NotNull, Size}

class AggregateOpDesc extends LogicalOp {
  @JsonProperty(value = "aggregations", required = true)
  @JsonPropertyDescription("multiple aggregation functions")
  @NotNull(message = "aggregation cannot be null")
  @Size(min = 1, message = "aggregations cannot be empty")
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = List()

  override def getPhysicalPlan(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    if (groupByKeys == null) groupByKeys = List()
    // TODO: this is supposed to be blocking but due to limitations of materialization naming on the logical operator
    // we are keeping it not annotated as blocking.
    val inputPort = InputPort(PortIdentity())
    val outputPort = OutputPort(PortIdentity(internal = true))
    val partialDesc = objectMapper.writeValueAsString(this)
    val localAggregations = List(aggregations: _*)
    val partialPhysicalOp = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, "localAgg"),
        workflowId,
        executionId,
        OpExecWithClassName("edu.uci.ics.amber.operator.aggregate.AggregateOpExec", partialDesc)
      )
      .withIsOneToManyOp(true)
      .withInputPorts(List(inputPort))
      .withOutputPorts(List(outputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val inputSchema = inputSchemas(operatorInfo.inputPorts.head.id)
          val outputSchema = Schema(
            groupByKeys.map(key => inputSchema.getAttribute(key)) ++
              localAggregations.map(agg =>
                agg.getAggregationAttribute(inputSchema.getAttribute(agg.attribute).getType)
              )
          )
          Map(PortIdentity(internal = true) -> outputSchema)
        })
      )

    val finalInputPort = InputPort(PortIdentity(0, internal = true))
    val finalOutputPort = OutputPort(PortIdentity(0), blocking = true)
    // change aggregations to final
    aggregations = aggregations.map(aggr => aggr.getFinal)
    val finalDesc = objectMapper.writeValueAsString(this)

    val finalPhysicalOp = PhysicalOp
      .oneToOnePhysicalOp(
        PhysicalOpIdentity(operatorIdentifier, "globalAgg"),
        workflowId,
        executionId,
        OpExecWithClassName("edu.uci.ics.amber.operator.aggregate.AggregateOpExec", finalDesc)
      )
      .withParallelizable(false)
      .withIsOneToManyOp(true)
      .withInputPorts(List(finalInputPort))
      .withOutputPorts(List(finalOutputPort))
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(operatorInfo.outputPorts.head.id -> inputSchemas(finalInputPort.id))
        )
      )
      .withPartitionRequirement(List(Option(HashPartition(groupByKeys))))
      .withDerivePartition(_ => HashPartition(groupByKeys))

    var plan = PhysicalPlan(
      operators = Set(partialPhysicalOp, finalPhysicalOp),
      links = Set(
        PhysicalLink(partialPhysicalOp.id, outputPort.id, finalPhysicalOp.id, finalInputPort.id)
      )
    )
    plan.operators.foreach(op => plan = plan.setOperator(op.withIsOneToManyOp(true)))
    plan
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "Calculate different types of aggregation values",
      OperatorGroupConstants.AGGREGATE_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )
}
