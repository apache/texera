package edu.uci.ics.texera.workflow.operators.aggregate

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.engine.common.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.engine.common.workflow.{InputPort, OutputPort, PortIdentity}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeNameList
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.aggregate.AggregateOpDesc
import edu.uci.ics.texera.workflow.common.tuple.schema.Schema
import edu.uci.ics.texera.workflow.common.workflow.PhysicalPlan

case class AveragePartialObj(sum: Double, count: Double) extends Serializable {}

class SpecializedAggregateOpDesc extends AggregateOpDesc {

  @JsonProperty(value = "aggregations", required = true)
  @JsonPropertyDescription("multiple aggregation functions")
  var aggregations: List[AggregationOperation] = List()

  @JsonProperty("groupByKeys")
  @JsonSchemaTitle("Group By Keys")
  @JsonPropertyDescription("group by columns")
  @AutofillAttributeNameList
  var groupByKeys: List[String] = _

  override def aggregateOperatorExecutor(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalPlan = {
    if (aggregations.isEmpty) {
      throw new UnsupportedOperationException("Aggregation Functions Cannot be Empty")
    }
    val inputSchema =
      operatorInfo.inputPorts.map(inputPort => inputPortToSchemaMapping(inputPort.id)).head
    val outputSchema =
      operatorInfo.outputPorts.map(outputPort => outputPortToSchemaMapping(outputPort.id)).head
    AggregateOpDesc.getPhysicalPlan(
      workflowId,
      executionId,
      operatorIdentifier,
      aggregations.map(agg => agg.getAggFunc(inputSchema)),
      groupByKeys,
      inputSchema,
      outputSchema
    )
  }

  private def getGroupByKeysSchema(schemas: Array[Schema]): Schema = {
    if (groupByKeys == null) {
      groupByKeys = List()
    }
    Schema
      .builder()
      .add(groupByKeys.map(key => schemas(0).getAttribute(key)): _*)
      .build()
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      "Aggregate",
      "Calculate different types of aggregation values",
      OperatorGroupConstants.AGGREGATE_GROUP,
      inputPorts = List(
        InputPort(PortIdentity(), displayName = "in")
      ),
      outputPorts = List(
        OutputPort(PortIdentity(), displayName = "out")
      )
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    if (
      aggregations.exists(agg => (agg.resultAttribute == null || agg.resultAttribute.trim.isEmpty))
    ) {
      return null
    }
    Schema
      .builder()
      .add(getGroupByKeysSchema(schemas).getAttributes)
      .add(aggregations.map(agg => agg.getAggregationAttribute(schemas(0))))
      .build()
  }

}
