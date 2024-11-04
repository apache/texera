package edu.uci.ics.amber.operator.visualization.continuousErrorBands

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import edu.uci.ics.amber.operator.common.annotations.AutofillAttributeName
import edu.uci.ics.amber.operator.visualization.lineChart.LineConfig

class BandConfig extends LineConfig {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Axis Upper Bound")
  @JsonPropertyDescription("Represents upper bound error of y-values")
  @AutofillAttributeName
  var yUpper: String = ""

  @JsonProperty(required = true)
  @JsonSchemaTitle("Y-Axis Lower Bound")
  @JsonPropertyDescription("Represents lower bound error of y-values")
  @AutofillAttributeName
  var yLower: String = ""

  @JsonProperty(required = false)
  @JsonSchemaTitle("Fill Color")
  @JsonPropertyDescription("must be a valid CSS color or hex color string")
  var fillColor: String = ""
}
