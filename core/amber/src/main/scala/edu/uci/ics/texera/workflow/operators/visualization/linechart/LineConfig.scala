package edu.uci.ics.texera.workflow.operators.visualization.linechart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.{AutofillAttributeName, AutofillAttributeNameLambda}


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
  @JsonSchemaTitle("y Column")
  @JsonPropertyDescription("the y value")
  @AutofillAttributeName
  var yValue: String = ""

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("x Column")
  @JsonPropertyDescription("ths x value")
  @AutofillAttributeName
  var xValue: String = ""

  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("color Column")
  @JsonPropertyDescription("the color")
  var color: String = "None"

  @JsonProperty(
    value = "mode",
    required = true,
    defaultValue = "lines+markers"
  )
  @JsonSchemaTitle("mode Column")
  @JsonPropertyDescription("the mode")
  var mode: LineMode = LineMode.LINES_MARKERS

  @JsonProperty(value = "name", required = false)
  @JsonSchemaTitle("name line")
  @JsonPropertyDescription("the name value")
  var name: String = ""

}
