package edu.uci.ics.amber.operator.unneststring

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.common.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.flatmap.FlatMapOpDesc
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

class UnnestStringOpDesc extends FlatMapOpDesc {
  @JsonProperty(value = "Delimiter", required = true, defaultValue = ",")
  @JsonPropertyDescription("string that separates the data")
  var delimiter: String = _

  @JsonProperty(value = "Attribute", required = true)
  @JsonPropertyDescription("column of the string to unnest")
  @AutofillAttributeName
  var attribute: String = _

  @JsonProperty(value = "Result attribute", required = true, defaultValue = "unnestResult")
  @JsonPropertyDescription("column name of the unnest result")
  var resultAttribute: String = _

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Unnest String",
      operatorDescription =
        "Unnest the string values in the column separated by a delimiter to multiple values",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new UnnestStringOpExec(attribute, delimiter))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas =>
          Map(
            operatorInfo.outputPorts.head.id -> getOutputSchema(
              operatorInfo.inputPorts.map(_.id).map(inputSchemas(_)).toArray
            )
          )
        )
      )
  }

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.forall(_ == schemas(0)))
    if (resultAttribute == null || resultAttribute.trim.isEmpty) return null
    Schema.builder().add(schemas(0)).add(resultAttribute, AttributeType.STRING).build()
  }
}
