package edu.uci.ics.texera.workflow.operators.source.apis.twitter

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaDescription, JsonSchemaTitle}
import edu.uci.ics.amber.engine.common.workflow.OutputPort
import edu.uci.ics.texera.workflow.common.metadata.{OperatorGroupConstants, OperatorInfo}
import edu.uci.ics.texera.workflow.common.operators.source.SourceOperatorDescriptor

abstract class TwitterSourceOpDesc extends SourceOperatorDescriptor {

  @JsonIgnore
  val APIName: Option[String] = None

  @JsonProperty(required = true)
  @JsonSchemaTitle("API Key")
  var apiKey: String = _

  @JsonProperty(required = true)
  @JsonSchemaTitle("API Secret Key")
  var apiSecretKey: String = _

  @JsonProperty(required = true, defaultValue = "false")
  @JsonSchemaTitle("Stop Upon Rate Limit")
  @JsonSchemaDescription("Stop when hitting rate limit?")
  var stopWhenRateLimited: Boolean = false

  override def operatorInfo: OperatorInfo = {
    OperatorInfo(
      userFriendlyName = s"Twitter ${APIName.get} API",
      operatorDescription = s"Retrieve data from Twitter ${APIName.get} API",
      OperatorGroupConstants.SOURCE_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  }

}
