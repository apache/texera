package edu.uci.ics.amber.operator.visualization.figureFactoryTable

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

class FigureFactoryTableConfig {
  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute Name")
  @AutofillAttributeName
  var attributeName: String = ""
}
