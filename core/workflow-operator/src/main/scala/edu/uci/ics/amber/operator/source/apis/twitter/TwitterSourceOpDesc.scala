package edu.uci.ics.amber.operator.source.apis.twitter

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaDescription, JsonSchemaTitle}
import edu.uci.ics.amber.operator.metadata.OperatorInfo
import edu.uci.ics.amber.operator.metadata.OperatorGroupConstants
import edu.uci.ics.amber.operator.source.SourceOperatorDescriptor
import edu.uci.ics.amber.workflow.OutputPort

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
      OperatorGroupConstants.API_GROUP,
      inputPorts = List.empty,
      outputPorts = List(OutputPort())
    )
  }

}
