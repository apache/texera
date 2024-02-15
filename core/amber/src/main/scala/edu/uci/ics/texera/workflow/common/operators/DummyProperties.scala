package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.fasterxml.jackson.annotation.JsonProperty
class DummyProperties {
  @JsonProperty
  @JsonSchemaTitle("Dummy Property")
  //@JsonPropertyDescription("Dummy Property for incompatible property")
  var dummyProperty: String = ""

  @JsonProperty
  @JsonSchemaTitle("Dummy Value")
  //@JsonPropertyDescription("Value for the dummy property")
  var dummyValue: String = ""
}
