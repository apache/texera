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

package org.apache.texera.amber.operator.sklearn

import com.fasterxml.jackson.annotation.{JsonIgnore, JsonProperty, JsonPropertyDescription}
import com.kjetland.jackson.jsonSchema.annotations.{
  JsonSchemaInject,
  JsonSchemaInt,
  JsonSchemaString,
  JsonSchemaTitle
}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.pybuilder.PyStringTypes.EncodableString
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  CommonOpDescAnnotation,
  HideAnnotation
}

abstract class SklearnModelOpDesc extends PythonOperatorDescriptor {

  @JsonSchemaTitle("Target Attribute")
  @JsonPropertyDescription("Attribute in your dataset corresponding to target.")
  @JsonProperty(required = true)
  @AutofillAttributeName
  var target: EncodableString = _

  @JsonSchemaTitle("Count Vectorizer")
  @JsonPropertyDescription("Convert a collection of text documents to a matrix of token counts.")
  @JsonProperty(defaultValue = "false")
  var countVectorizer: Boolean = false

  @JsonSchemaTitle("Text Attribute")
  @JsonPropertyDescription("Attribute in your dataset with text to vectorize.")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeName
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 0)
    )
  )
  var text: EncodableString = _

  @JsonSchemaTitle("Tfidf Transformer")
  @JsonPropertyDescription("Transform a count matrix to a normalized tf or tf-idf representation.")
  @JsonProperty(defaultValue = "false")
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    )
  )
  var tfidfTransformer: Boolean = false

  @JsonIgnore
  def getImportStatements: String

  @JsonIgnore
  def getUserFriendlyModelName: String

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    Map(
      operatorInfo.outputPorts.head.id -> Schema()
        .add("model_name", AttributeType.STRING)
        .add("model", AttributeType.BINARY)
    )
  }
}
