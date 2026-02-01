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

package org.apache.texera.amber.operator.machineLearning.sklearnAdvanced.base

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations._
import org.apache.texera.amber.operator.metadata.annotations.{
  CommonOpDescAnnotation,
  HideAnnotation
}

class HyperParameters[T] {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Parameter")
  @JsonPropertyDescription("Choose the name of the parameter")
  var parameter: T = _

  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeName
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.`equals`),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 1)
    )
  )
  @JsonProperty(value = "attribute")
  var attribute: String = _

  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "parametersSource"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.`equals`),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "true")
    ),
    bools = Array(new JsonSchemaBool(path = HideAnnotation.hideOnNull, value = true))
  )
  @JsonProperty(value = "value")
  var value: String = _

  @JsonProperty(defaultValue = "false")
  @JsonSchemaTitle("Workflow")
  @JsonPropertyDescription("Parameter from workflow")
  var parametersSource: Boolean = false
}
