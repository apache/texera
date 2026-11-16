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

class SklearnGaussianNaiveBayesOpDescSpec extends AnyFlatSpec with SklearnModelOpDescBehaviors {

  sklearnClassifier(
    new SklearnGaussianNaiveBayesOpDesc,
    "SklearnGaussianNaiveBayes",
    "Gaussian Naive Bayes",
    "from sklearn.naive_bayes import GaussianNB",
    estimatorTestName = "import and instantiate the GaussianNB estimator"
  )

  "SklearnGaussianNaiveBayesOpDesc.getOutputSchemas" should
    "reject Count Vectorizer, naming the estimator and what accepts it instead" in {
    // GaussianNB is fitted on a mean and a variance per feature, which a sparse
    // matrix cannot supply without being densified, so this used to end the run
    // from inside scikit-learn. It stops at compile time now.
    val d = new SklearnGaussianNaiveBayesOpDesc
    d.target = "y"
    d.text = List("note")
    d.countVectorizer = true
    val thrown = intercept[RuntimeException](d.getOutputSchemas(Map.empty))
    thrown.getMessage should include("Gaussian Naive Bayes")
    thrown.getMessage should include("Count Vectorizer")
    thrown.getMessage should include("Multinomial, Bernoulli or Complement Naive Bayes")
  }
}
