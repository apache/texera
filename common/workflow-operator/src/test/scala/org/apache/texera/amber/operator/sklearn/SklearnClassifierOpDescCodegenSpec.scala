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
    d.text = List("docs")
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
    // Feature-column path: every column an estimator can fit is kept, the rest are
    // named on the console, and the text attribute is never read.
    code should not include "ColumnTransformer("
    code should include("""_fittable = X.select_dtypes(include=["number", "bool"])""")
    code should include("""print("Ignoring columns an estimator cannot fit:", _ignored)""")
    code should include("X = _fittable")
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
    // ColumnTransformer selects the columns itself, so X stays the whole frame, and
    // narrowing it to the fittable columns would drop the text ones it reads.
    code should not include "_fittable"
    normalized(code) should include(
      s"""self.model = make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
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
      s"""self.model = make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
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
      s"""self.model = make_pipeline(ColumnTransformer([("text0", CountVectorizer(), ${decodeExpr(
        "docs"
      )})]), TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"""
    )
  }

  it should "prepend only TfidfTransformer and keep all features when tfidfTransformer is on alone" in {
    val code = descriptor(tfidfTransformer = true).generatePythonCode()
    // Without countVectorizer there is no text-column selection.
    code should not include "ColumnTransformer("
    code should include("X = _fittable")
    code should not include decodeExpr("docs")
    normalized(code) should include(
      "self.model = make_pipeline( TfidfTransformer(), KNeighborsClassifier()).fit(X, Y)"
    )
    code should not include "CountVectorizer()"
  }

  // --- the base's own model-identity defaults --------------------------------

  // Declared inside the spec class on purpose: PythonClassgraphScanner drops
  // non-static enclosed classes, which is what keeps SklearnOpDescRegistrySpec's
  // and PythonCodeRawInvalidTextSpec's classpath scans from treating this stub as
  // a shipped operator. A top-level subclass here would break both suites.
  private class BareClassifier extends SklearnClassifierOpDesc

  // Overrides only the model name, so the two hooks hold different values. That
  // is what separates the base's own getImportStatements body from a body that
  // merely forwards to the other hook — on BareClassifier alone both return "",
  // which makes an exchange between them invisible.
  private class NamedOnlyClassifier extends SklearnClassifierOpDesc {
    override def getUserFriendlyModelName = "ProbeModel"
  }

  "SklearnClassifierOpDesc" should "leave both model-identity hooks blank as base placeholders" in {
    // NOT a claim that "" is the intended design. SklearnModelOpDesc declares both
    // hooks abstract, and all of the shipped classifiers override them; these two
    // bodies are placeholders that satisfy the abstract contract for the family.
    // What is pinned is therefore what a subclass that forgets to override
    // actually ships — a nameless operator whose generated pipeline stage comes
    // out empty — not a default anyone should rely on. Leaving these two hooks
    // abstract on SklearnClassifierOpDesc is filed as a follow-up rather than
    // asserted here.
    val bare = new BareClassifier
    bare.getImportStatements shouldBe ""
    bare.getUserFriendlyModelName shouldBe ""

    // The import hook is its own constant, not an alias for the name hook.
    new NamedOnlyClassifier().getImportStatements shouldBe ""
  }
}
