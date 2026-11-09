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

package org.apache.texera.amber.operator.udf

import com.fasterxml.jackson.annotation.JsonProperty
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle
import org.apache.texera.amber.core.tuple.Attribute

import javax.validation.Valid
import javax.validation.constraints.NotNull

/**
  * Serialized operator property shared by Python and R UDF UI parameters.
  *
  * `attribute` carries the inferred parameter name and type. `value` is user-entered text.
  * Language adapters own its encoding and native conversion; the wire model contains no language-specific types.
  */
class UiUDFParameter {

  @JsonProperty(required = true)
  @JsonSchemaTitle("Attribute")
  @Valid
  @NotNull(message = "Attribute is required")
  var attribute: Attribute = _

  @JsonProperty()
  @JsonSchemaTitle("Value")
  var value: String = ""

  /**
    * The Texera resource the value names — "model" or "dataset" — or empty for free text.
    * Inferred from the UDF's source (`value=Resource.MODEL`), like `attribute`. A resource value
    * is a version path, turned into the directory that version is mounted at when the run starts.
    */
  @JsonProperty()
  @JsonSchemaTitle("Input type")
  var inputType: String = ""
}
