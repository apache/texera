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

package org.apache.texera.amber.operator.udf.python

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.JsonSchemaTitle

/**
  * One row of the Python UDF's "Mounted dataset variables" property: it binds a dataset
  * version already mounted on the computing unit to a Python variable. At runtime the
  * variable holds the dataset's local (in-pod) mount path as a string.
  */
class DatasetVariableMapping {
  @JsonProperty(required = true, defaultValue = "")
  @JsonSchemaTitle("Variable name")
  @JsonPropertyDescription(
    "Name of the Python variable that will hold the dataset's local filesystem path"
  )
  var variableName: String = ""

  @JsonProperty(required = true, defaultValue = "")
  @JsonSchemaTitle("Mounted dataset")
  @JsonPropertyDescription("A dataset version mounted on this computing unit")
  var datasetPath: String = ""
}
