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

package org.apache.texera.amber.operator.sklearn.training

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
  * Covers the vectorizer branches of the shared training codegen template in
  * SklearnTrainingOpDesc. The template is exercised through a representative
  * concrete subclass (SklearnTrainingKNNOpDesc) so the generated pipeline ends
  * in a real estimator, matching how the 26 training operators use the base.
  *
  * The template distinguishes all four flag combinations: countVectorizer
  * gates the text-column selection and the CountVectorizer() stage, while
  * tfidfTransformer independently prepends a TfidfTransformer() stage (the UI
  * hides it when countVectorizer is off, but codegen still honors it).
  */
class SklearnTrainingOpDescCodegenSpec extends AnyFlatSpec with Matchers {

  // UI-provided attribute names are EncodableStrings: the encoded template
  // renders them as a runtime base64-decode expression, not a Python literal.
  private def decodeExpr(value: String): String =
    PythonTemplateBuilder.wrapWithPythonDecoderExpr(
      Base64.getEncoder.encodeToString(value.getBytes(StandardCharsets.UTF_8))
    )

  // Collapse space runs so assertions on the make_pipeline call are not
  // coupled to the exact spacing the empty branches leave behind.
  private def normalized(code: String): String = code.replaceAll(" +", " ")

  private def descriptor(
      countVectorizer: Boolean = false,
      tfidfTransformer: Boolean = false
  ): SklearnTrainingKNNOpDesc = {
    val d = new SklearnTrainingKNNOpDesc
    d.target = "label"
    d.text = List("docs")
    d.countVectorizer = countVectorizer
    d.tfidfTransformer = tfidfTransformer
    d
  }

  "SklearnTrainingOpDesc.generatePythonCode" should
    "generate a plain feature pipeline when both vectorizer flags are off" in {
    val code = descriptor().generatePythonCode()
    code should include("from sklearn.neighbors import KNeighborsClassifier")
    code should include(s"Y = table[${decodeExpr("label")}]")
    code should include(s"X = table.drop(${decodeExpr("label")}, axis=1)")
    // Feature-column path: X is kept whole, the text attribute is never read.
    code should not include "ColumnTransformer("
    code should not include decodeExpr("docs")
    normalized(code) should include("make_pipeline( KNeighborsClassifier()).fit(X, Y)")
    code should not include "CountVectorizer()"
    code should not include "TfidfTransformer()"
  }

  it should "select the text column and prepend CountVectorizer when countVectorizer is on" in {
    val code = descriptor(countVectorizer = true).generatePythonCode()
    // ColumnTransformer selects the columns itself, so X stays the whole frame.
    normalized(code) should include(
      s"""make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
        "docs"
      )})]), KNeighborsClassifier()).fit(X, Y)"""
    )
    code should not include "TfidfTransformer()"
  }

  // One CountVectorizer per column: it reads a flat sequence of documents, so
  // several columns handed to one would be read as a document each. The steps are
  // named by position, keeping a column named with a double underscore away from
  // the separator get_feature_names_out uses.
  it should "give each named column its own CountVectorizer" in {
    val d = descriptor(countVectorizer = true)
    d.text = List("title", "body")
    normalized(d.generatePythonCode()) should include(
      s"""make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
        "title"
      )}), ("text1", CountVectorizer(), ${decodeExpr(
        "body"
      )})]), KNeighborsClassifier()).fit(X, Y)"""
    )
  }

  it should "chain CountVectorizer before TfidfTransformer when both flags are on" in {
    val code =
      descriptor(countVectorizer = true, tfidfTransformer = true).generatePythonCode()
    normalized(code) should include(
      s"""make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
        "docs"
      )})]), TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"""
    )
  }

  it should "prepend only TfidfTransformer and keep all features when tfidfTransformer is on alone" in {
    val code = descriptor(tfidfTransformer = true).generatePythonCode()
    // Without countVectorizer there is no text-column selection.
    code should not include "ColumnTransformer("
    code should not include decodeExpr("docs")
    normalized(code) should include(
      "make_pipeline( TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"
    )
    code should not include "CountVectorizer()"
  }
}
