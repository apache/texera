package edu.uci.ics.texera.workflow.operators.download

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.texera.workflow.common.metadata.{InputPort, OperatorGroupConstants, OperatorInfo, OutputPort}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor
import edu.uci.ics.texera.workflow.common.operators.{OneToOneOpExecConfig, OperatorDescriptor}
import edu.uci.ics.texera.workflow.common.tuple.schema.{Attribute, AttributeType, OperatorSchemaInfo, Schema}

import scala.collection.immutable.List

class URLFetcherOpDesc extends SourceOperatorDescriptor {

  @JsonProperty(required = true)
  @JsonSchemaTitle("URL")
  @JsonPropertyDescription(
    "Should follow standard URL format: protocol+hostname+filename"
  )
  var url: String = _

  def sourceSchema(): Schema={
    Schema
      .newBuilder().add("URL content", AttributeType.BINARY).build()
  }

  override def operatorExecutor(operatorSchemaInfo: OperatorSchemaInfo): OneToOneOpExecConfig = {
    new OneToOneOpExecConfig(
      operatorIdentifier,
      _ =>
        new URLFetcherOpExec(
          url,
          operatorSchemaInfo
        )
    )
  }

  override def operatorInfo: OperatorInfo =
    OperatorInfo(
      userFriendlyName = "URL fetcher",
      operatorDescription = "Fetch the content of a single url",
      operatorGroupName = OperatorGroupConstants.UTILITY_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )


}
