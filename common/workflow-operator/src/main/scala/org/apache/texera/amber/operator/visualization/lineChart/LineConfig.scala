/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.texera.amber.operator.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.operator.metadata.annotations.AutofillAttributeName

import javax.validation.constraints.NotNull

// Type constraint: both axes can only be numeric. The keys are the PROPERTY names —
// the `@JsonProperty` values, which is what the property editor looks up
// (`control.value[prop]`) — not the Scala field names; keyed by the latter the rule
// matched nothing and neither the UI nor the config generator enforced it.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "y": {
      "enum": ["integer", "long", "double"]
    },
    "x": {
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
  @NotNull(message = "Y Value cannot be empty")
  var yValue: EncodableString = ""

  @JsonProperty(value = "x", required = true)
  @JsonSchemaTitle("X Value")
  @JsonPropertyDescription("value for x axis")
  @AutofillAttributeName
  @NotNull(message = "X Value cannot be empty")
  var xValue: EncodableString = ""

  @JsonProperty(
    value = "mode",
    required = true,
    defaultValue = "line with dots"
  )
  @JsonSchemaTitle("Line Mode")
  @NotNull(message = "Line Mode cannot be empty")
  var mode: LineMode = LineMode.LINE_WITH_DOTS

  @JsonProperty(value = "name", required = false)
  @JsonSchemaTitle("Line Name")
  var name: EncodableString = ""

  // Mirrors plotly's own colour rule (_plotly_utils/basevalidators.py, ColorValidator):
  // it strips every space and lowercases, then accepts 3- or 6-digit hex, an
  // rgb/rgba/hsl/hsla/hsv/hsva call, a `var(--…)` theme variable, or a name from its
  // CSS list. Two consequences shape what this looks like. Letters are matched through
  // character classes because the browser compiles this with `new RegExp(pattern)`,
  // which takes no inline `(?i)`. And `\s*` sits between every element, not just
  // between tokens, because plotly strips spaces before matching — it really does
  // accept `#ff ffff` and `r g b(1,2,3)`, and rejecting those would make the box
  // stricter than the library it feeds. Empty stays legal: both code paths omit the
  // colour argument entirely, letting plotly pick from the template's colorway.
  //
  // Deliberately loose in one direction: the name branch is lexical, so a misspelling
  // still reaches plotly (matching exactly would mean copying its 148 CSS names in
  // here). It does reject what a free-text box let through before: `1`, `#12`, `#ggg`,
  // `#ffff`, `rgb(1,2)`, `rgb(-1,2,3)`. `examples` offers a legal sample to whatever
  // needs one, the verification config generator included.
  @JsonProperty(value = "color", required = false)
  @JsonSchemaTitle("Line Color")
  @JsonPropertyDescription("must be a valid CSS color or hex color string")
  @JsonSchemaInject(json = """
{
  "pattern": "^\\s*$|^\\s*#(?:\\s*[0-9a-fA-F]){3}(?:(?:\\s*[0-9a-fA-F]){3})?\\s*$|^\\s*(?:[rR]\\s*[gG]\\s*[bB]|[hH]\\s*[sS]\\s*[lL]|[hH]\\s*[sS]\\s*[vV])(?:\\s*[aA])?\\s*\\(\\s*(?:\\s*[0-9.])+(?:\\s*%)?(?:\\s*,(?:\\s*[0-9.])+(?:\\s*%)?){2,3}\\s*\\)\\s*$|^\\s*[vV]\\s*[aA]\\s*[rR]\\s*\\(\\s*-\\s*-[^)]*\\)\\s*$|^\\s*[a-zA-Z][a-zA-Z\\s]*$",
  "examples": ["red"]
}
""")
  var color: EncodableString = ""

}
