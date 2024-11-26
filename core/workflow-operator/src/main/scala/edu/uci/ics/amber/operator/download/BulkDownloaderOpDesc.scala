package edu.uci.ics.amber.operator.download

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.google.common.base.Preconditions
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{Attribute, AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.LogicalOp
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.{InputPort, OutputPort}

class BulkDownloaderOpDesc extends LogicalOp {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL Attribute")
  @JsonPropertyDescription(
    "Only accepts standard URL format"
  )
  @AutofillAttributeName
  var urlAttribute: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Result Attribute")
  @JsonPropertyDescription(
    "Attribute name for results(downloaded file paths)"
  )
  var resultAttribute: String = _

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .oneToOnePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) =>
          new BulkDownloaderOpExec(
            getContext,
            urlAttribute
          )
        )
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

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "Bulk Downloader",
      operatorDescription = "Download urls in a string column",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List(InputPort()),
      outputPorts = List(OutputPort())
    )

  override def getOutputSchema(schemas: Array[Schema]): Schema = {
    Preconditions.checkArgument(schemas.length == 1)
    val inputSchema = schemas(0)
    val outputSchemaBuilder = Schema.builder()
    // keep the same schema from input
    outputSchemaBuilder.add(inputSchema)
    if (resultAttribute == null || resultAttribute.isEmpty) {
      resultAttribute = urlAttribute + " result"
    }
    outputSchemaBuilder.add(
      new Attribute(
        resultAttribute,
        AttributeType.STRING
      )
    )
    outputSchemaBuilder.build()
  }
}
