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

import org.apache.texera.amber.pybuilder.PythonTemplateBuilder
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

/**
  * Covers the vectorizer branches of the shared train-and-evaluate codegen
  * template in SklearnClassifierOpDesc. The abstract base is exercised through
  * a representative concrete subclass (SklearnKNNOpDesc), matching how the 25
  * classifier operators use it.
  *
  * The template distinguishes all four flag combinations: countVectorizer
  * gates the text-column selection and the CountVectorizer() stage, while
  * tfidfTransformer independently prepends a TfidfTransformer() stage (the UI
  * hides it when countVectorizer is off, but codegen still honors it).
  */
class SklearnClassifierOpDescCodegenSpec extends AnyFlatSpec with Matchers {

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
  ): SklearnKNNOpDesc = {
    val d = new SklearnKNNOpDesc
    d.target = "label"
    d.text = "docs"
    d.countVectorizer = countVectorizer
    d.tfidfTransformer = tfidfTransformer
    d
  }

  "SklearnClassifierOpDesc.generatePythonCode" should
    "generate a plain feature pipeline when both vectorizer flags are off" in {
    val code = descriptor().generatePythonCode()
    code should include("from sklearn.neighbors import KNeighborsClassifier")
    code should include(s"Y = table[${decodeExpr("label")}]")
    code should include(s"X = table.drop(${decodeExpr("label")}, axis=1)")
    // Feature-column path: X is kept whole, the text attribute is never read.
    code should include("X = X\n")
    code should not include decodeExpr("docs")
    normalized(code) should include(
      "self.model = make_pipeline( KNeighborsClassifier()).fit(X, Y)"
    )
    code should include("predictions = self.model.predict(X)")
    code should not include "CountVectorizer()"
    code should not include "TfidfTransformer()"
  }

  it should "select the text column and prepend CountVectorizer when countVectorizer is on" in {
    val code = descriptor(countVectorizer = true).generatePythonCode()
    code should include(s"X = X[${decodeExpr("docs")}]")
    code should not include "X = X\n"
    normalized(code) should include(
      "self.model = make_pipeline(CountVectorizer(), KNeighborsClassifier()).fit(X, Y)"
    )
    code should not include "TfidfTransformer()"
  }

  it should "chain CountVectorizer before TfidfTransformer when both flags are on" in {
    val code =
      descriptor(countVectorizer = true, tfidfTransformer = true).generatePythonCode()
    code should include(s"X = X[${decodeExpr("docs")}]")
    normalized(code) should include(
      "self.model = make_pipeline(CountVectorizer(), TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"
    )
  }

  it should "prepend only TfidfTransformer and keep all features when tfidfTransformer is on alone" in {
    val code = descriptor(tfidfTransformer = true).generatePythonCode()
    // Without countVectorizer there is no text-column selection.
    code should include("X = X\n")
    code should not include decodeExpr("docs")
    normalized(code) should include(
      "self.model = make_pipeline( TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"
    )
    code should not include "CountVectorizer()"
  }
}
