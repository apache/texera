package edu.uci.ics.amber.operator.source.fetcher

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.core.executor.OpExecInitInfo
import edu.uci.ics.amber.core.tuple.{AttributeType, Schema}
import edu.uci.ics.amber.core.workflow.{PhysicalOp, SchemaPropagationFunc}
import edu.uci.ics.amber.operator.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import edu.uci.ics.amber.workflow.OutputPort

class URLFetcherOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription(
    "Only accepts standard URL format"
  )
  var url: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("Decoding")
  @JsonPropertyDescription(
    "The decoding method for the url content"
  )
  var decodingMethod: DecodingMethod = _

  def sourceSchema(): Schema = {
    Schema
      .builder()
      .add(
        "URL content",
        if (decodingMethod == DecodingMethod.UTF_8) {
          AttributeType.STRING
        } else {
          AttributeType.ANY
        }
      )
      .build()
  }

  override def getPhysicalOp(
      workflowId: WorkflowIdentity,
      executionId: ExecutionIdentity
  ): PhysicalOp = {
    PhysicalOp
      .sourcePhysicalOp(
        workflowId,
        executionId,
        operatorIdentifier,
        OpExecInitInfo((_, _) => new URLFetcherOpExec(url, decodingMethod))
      )
      .withInputPorts(operatorInfo.inputPorts)
      .withOutputPorts(operatorInfo.outputPorts)
      .withPropagateSchema(
        SchemaPropagationFunc(_ => Map(operatorInfo.outputPorts.head.id -> sourceSchema()))
      )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "URL fetcher",
      operatorDescription = "Fetch the content of a single url",
      operatorGroupName = OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )

}
