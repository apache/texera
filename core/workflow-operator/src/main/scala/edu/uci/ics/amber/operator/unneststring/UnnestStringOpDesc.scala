package edu.uci.ics.amber.operator.unneststring

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import edu.uci.ics.amber.core.executor.OpExecWithClassName
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.flatmap.FlatMapOpDesc
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.util.JSONUtils.objectMapper
import edu.uci.ics.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.core.workflow.{InputPort, OutputPort}

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
        OpExecWithClassName(
          "edu.uci.ics.amber.operator.unneststring.UnnestStringOpExec",
          objectMapper.writeValueAsString(this)
        )
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(inputSchemas => {
          val outputSchema =
            if (resultAttribute == null || resultAttribute.trim.isEmpty) null
            else
              Schema
                .builder()
                .add(inputSchemas.values.head)
                .add(resultAttribute, AttributeType.STRING)
                .build()
          Map(operatorInfo.outputPorts.head.id -> outputSchema)
        })
      )
  }
}
