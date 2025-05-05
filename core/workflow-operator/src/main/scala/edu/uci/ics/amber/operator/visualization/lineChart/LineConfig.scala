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

package edu.uci.ics.amber.operator.visualization.lineChart

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{JsonSchemaInject, JsonSchemaTitle}
import edu.uci.ics.amber.operator.metadata.annotations.AutofillAttributeName

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
