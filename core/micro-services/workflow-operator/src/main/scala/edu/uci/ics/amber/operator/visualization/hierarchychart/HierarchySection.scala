package edu.uci.ics.amber.operator.visualization.hierarchychart

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.common.annotations.AutofillAttributeName

// This is a hack to get a order-preserve selection from the combobox
// TODO: remove it after we enabled a order-preserve combobox on the frontend.
class HierarchySection {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Name")
  @AutofillAttributeName
  var attributeName: String = ""
}
