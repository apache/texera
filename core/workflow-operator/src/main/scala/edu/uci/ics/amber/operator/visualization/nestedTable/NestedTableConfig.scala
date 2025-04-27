package edu.uci.ics.amber.operator.visualization.nestedTable

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

class NestedTableConfig {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute group")
  var attributeGroup: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Original attribute Name")
  @AutofillAttributeName
  var originalName: String = ""

  @JsonProperty(value = "name", required = false)
  @JsonSchemaTitle("New Attribute Name")
  var newName: String = ""
}