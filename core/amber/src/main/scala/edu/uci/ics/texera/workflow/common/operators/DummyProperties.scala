package edu.uci.ics.texera.workflow.common.operators

import com.fasterxml.jackson.annotation.JsonPropertyDescription
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.fasterxml.jackson.annotation.JsonProperty
class DummyProperties {
  @JsonProperty
  @JsonSchemaTitle("Dummy Attribute")
  //@JsonPropertyDescription("Dummy Attribute for incompatible attribute")
  var dummyAttribute: String = ""

  @JsonProperty
  @JsonSchemaTitle("Dummy Value")
  //@JsonPropertyDescription("Value for the dummy attribute")
  var dummyValue: String = ""
}
