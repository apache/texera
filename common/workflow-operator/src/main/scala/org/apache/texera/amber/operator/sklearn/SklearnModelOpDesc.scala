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

import com.fasterxml.jackson.annotation.{
  JsonFormat,
  JsonIgnore,
  JsonProperty,
  JsonPropertyDescription
}
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
  @JsonPropertyDescription("Attributes in your dataset with text to vectorize.")
  // A workflow written before this field took several columns holds a bare string
  // here, which reads as a list of one.
  @JsonFormat(`with` = Array(JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY))
  @JsonSchemaInject(
    strings = Array(
      new JsonSchemaString(
        path = CommonOpDescAnnotation.autofill,
        value = CommonOpDescAnnotation.attributeNameList
      ),
      new JsonSchemaString(path = HideAnnotation.hideTarget, value = "countVectorizer"),
      new JsonSchemaString(path = HideAnnotation.hideType, value = HideAnnotation.Type.equals),
      new JsonSchemaString(path = HideAnnotation.hideExpectedValue, value = "false")
    ),
    ints = Array(
      new JsonSchemaInt(path = CommonOpDescAnnotation.autofillAttributeOnPort, value = 0)
    )
  )
  var text: List[EncodableString] = List()

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

  /** The pipeline step that turns the named text columns into features, empty when
    * Count Vectorizer is off.
    *
    * One `CountVectorizer` per column rather than one over all of them:
    * `CountVectorizer` takes a flat sequence of documents, so handing it several
    * columns reads them as one document each rather than as the rows. A
    * `ColumnTransformer` gives each its own and concatenates the results, keeping
    * the column a word came from distinguishable through the feature's prefix.
    *
    * The steps are named by position, not after the column, so that a column whose
    * name carries a double underscore cannot collide with the separator
    * `get_feature_names_out` puts between step and feature.
    *
    * `renderColumn` writes one column name in the caller's dialect: an encoded
    * expression for the operator, a literal for the standalone script.
    */
  @JsonIgnore
  protected def vectorizerStage(renderColumn: EncodableString => String): String =
    if (!countVectorizer) ""
    else
      text.zipWithIndex
        .map { case (column, i) => s"""("text$i", CountVectorizer(), ${renderColumn(column)})""" }
        .mkString("ColumnTransformer([", ", ", "]),")

  @JsonIgnore
  def getImportStatements: String

  @JsonIgnore
  def getUserFriendlyModelName: String

  /** An estimator that cannot be fitted on the sparse matrix `CountVectorizer`
    * produces names here what to reach for instead, and turning the switch on
    * stops the workflow at compile time rather than inside scikit-learn.
    *
    * The switch is declared on this base, so every estimator in both families
    * inherits it whether or not its own can use it.
    */
  @JsonIgnore
  protected def countVectorizerAlternatives: Option[String] = None

  override def getOutputSchemas(
      inputSchemas: Map[PortIdentity, Schema]
  ): Map[PortIdentity, Schema] = {
    if (countVectorizer) {
      countVectorizerAlternatives.foreach { alternatives =>
        throw new RuntimeException(
          s"$getUserFriendlyModelName cannot be trained on the sparse matrix Count Vectorizer" +
            s" produces. Turn Count Vectorizer off, or use $alternatives."
        )
      }
      // The generated code drops the target before the text pipeline reads its
      // columns, so naming it here asks the pipeline for a column that is no
      // longer there. Refused rather than vectorized: the label is the answer,
      // and a model given it as a feature reads that answer off its own input.
      if (text.contains(target)) {
        throw new RuntimeException(
          s""""$target" is the Target Attribute, so it cannot also be a Text Attribute.""" +
            " Remove it from Text Attribute, or fit against a different column."
        )
      }
    }
    Map(
      operatorInfo.outputPorts.head.id -> Schema()
        .add("model_name", AttributeType.STRING)
        .add("model", AttributeType.BINARY)
    )
  }
}
