package edu.uci.ics.texera.workflow.operators.visualization.dumbbellPlot

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.texera.workflow.common.metadata.annotations.AutofillAttributeName

@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "dot": {
      "enum": ["integer", "long", "double"]
    }
  }
}
""")
class DumbbellDotConfig {

  @JsonProperty(value = "dot", required = true)
  @JsonSchemaTitle("Dot Column Value")
  @JsonPropertyDescription("value for dot axis")
  @AutofillAttributeName
  var dotValue: String = ""

}
