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
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.PythonTemplateBuilderStringContext
import org.apache.texera.amber.pybuilder.PythonTemplateBuilder.pyStringLiteral
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.PythonOperatorDescriptor
import org.apache.texera.amber.operator.metadata.annotations.{
  AutofillAttributeName,
  CommonOpDescAnnotation,
  HideAnnotation,
  SampleColumn
}

// `text` names the columns Count Vectorizer tokenizes, so they are string columns
// and at least one is required only when that switch is on. Conditional rather than
// plain required, so a freshly dropped operator is not flagged for a field it has
// no use for.
@JsonSchemaInject(json = """
{
  "attributeTypeRules": {
    "text": {
      "enum": ["string"]
    }
  },
  "allOf": [
    {
      "if": {
        "properties": {
          "countVectorizer": { "const": true }
        }
      },
      "then": {
        "required": ["text"],
        "properties": {
          "text": { "minItems": 1 }
        }
      }
    }
  ]
}
""")
abstract class SklearnModelOpDesc extends PythonOperatorDescriptor with SklearnFittableColumns {

  // The label the estimator fits against. Test-only steering: without it the
  // first column wins, which on a feature/label table is a feature.
  @SampleColumn("species")
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

  /** [[SklearnFittableColumns.narrowToFittableColumns]], except under the text
    * pipeline: there the `ColumnTransformer` names the columns it reads, and they
    * are the ones the narrowing would drop.
    */
  @JsonIgnore
  protected def dropNonFeatureColumns(frame: String, indent: String): String =
    if (countVectorizer) "" else narrowToFittableColumns(frame, indent)

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

  // Tree-based estimators send the missing values of a split down one branch, so a
  // blank feature is a signal they can fit on, and the dummy estimator never reads a
  // feature at all. Every other estimator here computes over the feature matrix, where
  // one NaN spreads through the arithmetic, and scikit-learn refuses the fit rather
  // than return a meaningless model.
  @JsonIgnore
  def handlesMissingValues: Boolean = false

  // A blank target is refused by every estimator, and CountVectorizer calls .lower()
  // on each document, so the target and every vectorized column are dropped whatever
  // the estimator does.
  @JsonIgnore
  protected def dropMissingRows: String =
    if (countVectorizer)
      (text :+ target).map(c => pyb"$c".toString).mkString("table.dropna(subset=[", ", ", "])")
    else if (handlesMissingValues) pyb"table.dropna(subset=[$target])".toString
    else "table.dropna()"

  /** [[dropMissingRows]] for the standalone path, which names its own frame and
    * has no `self` to decode a column name through. Stated separately rather than
    * shared through a renderer: `pyb` encodes by the STATIC type of what it
    * interpolates, so a column handed through a `String => String` loses the
    * annotation that makes it encode and reaches the script raw.
    */
  @JsonIgnore
  protected def dropMissingRowsStandalone(frame: String): String = {
    val subset =
      if (countVectorizer) (text :+ target).map(c => pyStringLiteral(c).toString)
      else if (handlesMissingValues) Seq(pyStringLiteral(target).toString)
      else Seq.empty
    if (subset.isEmpty) s"$frame.dropna()"
    else subset.mkString(s"$frame.dropna(subset=[", ", ", "])")
  }

  // Rows the estimator keeps are still rows the user did not know were incomplete,
  // so say how many reached the fit. Empty for the estimators that dropped them all.
  @JsonIgnore
  protected def reportMissingKept: String =
    if (handlesMissingValues && !countVectorizer)
      """       |        rows_with_gaps = int(X.isna().any(axis=1).sum())
        |       |        if rows_with_gaps:
        |       |            print("Kept", rows_with_gaps, "rows with missing values, which this model fits without dropping")""".stripMargin
    else ""

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
