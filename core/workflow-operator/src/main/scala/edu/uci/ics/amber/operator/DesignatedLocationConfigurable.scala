package edu.uci.ics.amber.operator

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import edu.uci.ics.amber.operator.metadata.annotations.HideAnnotation
import edu.uci.ics.amber.core.workflow.{GoToSpecificNode, PhysicalOp}

/**
  * Provides configuration for users to designate a specific node location for executing operations.
  * When `autoSelectNodeAddress` is false and `nodeAddr` is not null, the specified node address will be used.
  */
trait DesignatedLocationConfigurable {

  @JsonProperty(defaultValue = "true")
  @JsonSchemaTitle("Auto Select Node Address")
  @JsonPropertyDescription(
    "Set to false to manually specify the node address instead of using the default RoundRobin strategy"
  )
  var autoSelectNodeAddress: Boolean = true

  @JsonProperty(required = false)
  @JsonSchemaTitle("Designated Node Address")
  @JsonPropertyDescription("The node address to use when autoSelectNodeAddress is disabled")
  @JsonSchemaInject(
    strings = Array(
      // This configuration hides `nodeAddr` when `autoSelectNodeAddress` equals "true".
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "autoSelectNodeAddress"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    )
  )
  var nodeAddr: String = _

  /**
    * Applies the manual location configuration to the given PhysicalOp.
    *
    * @param baseOp The base PhysicalOp object
    * @return The PhysicalOp object with the applied location preference if manual is enabled; otherwise, the original baseOp.
    */
  def configureLocationPreference(baseOp: PhysicalOp): PhysicalOp = {
    if (!autoSelectNodeAddress && nodeAddr != null) {
      baseOp.withLocationPreference(Some(GoToSpecificNode(nodeAddr)))
    } else {
      baseOp
    }
  }
}
