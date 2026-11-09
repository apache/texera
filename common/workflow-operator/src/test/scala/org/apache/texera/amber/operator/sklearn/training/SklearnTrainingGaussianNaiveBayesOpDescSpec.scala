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

import org.apache.texera.amber.operator.sklearn.SklearnModelOpDescBehaviors
import org.scalatest.flatspec.AnyFlatSpec

class SklearnTrainingGaussianNaiveBayesOpDescSpec
    extends AnyFlatSpec
    with SklearnModelOpDescBehaviors {

  sklearnTrainer(
    new SklearnTrainingGaussianNaiveBayesOpDesc,
    "SklearnTrainingGaussianNaiveBayes",
    "Training: Gaussian Naive Bayes",
    "from sklearn.naive_bayes import GaussianNB"
  )

  "SklearnTrainingGaussianNaiveBayesOpDesc.getOutputSchemas" should
    "reject Count Vectorizer the same way its classifier counterpart does" in {
    val d = new SklearnTrainingGaussianNaiveBayesOpDesc
    d.target = "y"
    d.text = List("note")
    d.countVectorizer = true
    val thrown = intercept[RuntimeException](d.getOutputSchemas(Map.empty))
    thrown.getMessage should include("Training: Gaussian Naive Bayes")
    thrown.getMessage should include("Multinomial, Bernoulli or Complement Naive Bayes")
  }
}
