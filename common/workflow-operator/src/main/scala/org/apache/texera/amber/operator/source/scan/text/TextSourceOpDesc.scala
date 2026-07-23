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

package org.apache.texera.amber.operator.source.scan.text

import com.fasterxml.jackson.annotation.{JsonProperty, JsonPropertyDescription}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.operator.metadata.annotations.HideAnnotation
import org.apache.texera.amber.operator.source.scan.FileAttributeType

/**
  * TextSourceOpDesc is a trait holding commonly used properties and functions used for variations of text input processing
  * Create new, identical limit and offset fields with additional annotations to make hideable binary attributes
  * and strings that are in SingleTuple mode will always read the entire input, so limit / offset are disabled in these cases
  */
trait TextSourceOpDesc {
  @JsonProperty(defaultValue = "string", required = true)
  @JsonSchemaTitle("Attribute Type")
  @JsonPropertyDescription(
    "This specifies how to read this text.\n\n" +
      "Per-line types (e.g. string, integer, long, double, boolean, timestamp) emit one output tuple per line.\n\n" +
      "Single-tuple types ('single string' / 'binary' / 'large binary') output one tuple total."
  )
  var attributeType: FileAttributeType = FileAttributeType.STRING

  @JsonProperty(defaultValue = "line", required = true)
  @JsonSchemaTitle("Attribute Name")
  @JsonDeserialize(contentAs = classOf[java.lang.String])
  var attributeName: String = "line"

  @JsonSchemaTitle("Limit (lines)")
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonPropertyDescription(
    "Maximum number of lines to output. Leave empty to read all lines. " +
      "(Not used when reading the whole file as one tuple.)"
  )
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanLimit: Option[Int] = None

  @JsonSchemaTitle("Offset (lines)")
  @JsonPropertyDescription(
    "Number of lines to skip from the start before reading. " +
      "(Not used when reading the whole file as one tuple.)"
  )
  @JsonDeserialize(contentAs = classOf[Int])
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "attributeType"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.regex),
      new JsonSchemaString(
        path = HideAnnotation.hideExpectedValue,
        value = "^binary$|^single string$"
      )
    )
  )
  var fileScanOffset: Option[Int] = None
}
