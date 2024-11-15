package edu.uci.ics.amber.operator.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.operator.metadata.annotation.AutofillAttributeName

//type constraint: value can only be numeric
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "yValue": {
      "enum": ["integer", "long", "double"]
    },
    "xValue": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class LineConfig {

  @JsonProperty(value = "y", required = true)
  @JsonSchemaTitle("Y Value")
  @JsonPropertyDescription("value for y axis")
  @AutofillAttributeName
  var yValue: String = ""

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("X Value")
  @JsonPropertyDescription("value for x axis")
  @AutofillAttributeName
  var xValue: String = ""

  @JsonProperty(
    value = "mode",
    required = true,
    defaultValue = "line with dots"
  )
  @JsonSchemaTitle("Line Mode")
  var mode: LineMode = LineMode.LINE_WITH_DOTS

  @JsonProperty(value = "name", required = false)
  @JsonSchemaTitle("Line Name")
  var name: String = ""

  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("Line Color")
  @JsonPropertyDescription("must be a valid CSS color or hex color string")
  var color: String = ""

}
