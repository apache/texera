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

import org.scalatest.flatspec.AnyFlatSpec

/** Covers the vectorizer branches of SklearnClassifierOpDesc and its empty placeholder hooks. */
class SklearnClassifierOpDescCodegenSpec
    extends AnyFlatSpec
    with SklearnVectorizerCodegenBehaviors {

  vectorizerCodegenBehavior(
    "SklearnClassifierOpDesc.generatePythonCode",
    pipelinePrefix = "self.model = ",
    makeDescriptor = { (cv, tfidf, cols) =>
      val d = new SklearnKNNOpDesc
      d.target = "label"
      d.text = cols
      d.countVectorizer = cv
      d.tfidfTransformer = tfidf
      d
    }
  )

  // Declared inside the spec class on purpose: PythonClassgraphScanner drops
  // non-static enclosed classes, which keeps SklearnOpDescRegistrySpec and
  // PythonCodeRawInvalidTextSpec from treating this stub as a shipped operator.
  private class BareClassifier extends SklearnClassifierOpDesc

  private class NamedOnlyClassifier extends SklearnClassifierOpDesc {
    override def getUserFriendlyModelName = "ProbeModel"
  }

  "SklearnClassifierOpDesc" should "leave both model-identity hooks blank as base placeholders" in {
    val bare = new BareClassifier
    bare.getImportStatements shouldBe ""
    bare.getUserFriendlyModelName shouldBe ""

    // The import hook is its own constant, not an alias for the name hook.
    new NamedOnlyClassifier().getImportStatements shouldBe ""
  }
}
