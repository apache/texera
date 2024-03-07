package edu.uci.ics.texera.workflow.common.operators

import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import com.fasterxml.jackson.annotation.JsonProperty
class DummyProperties {
  @JsonProperty
  @JsonSchemaTitle("Dummy Property")
  var dummyProperty: String = ""

  @JsonProperty
  @JsonSchemaTitle("Dummy Value")
  var dummyValue: String = ""
}
