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

class SklearnTrainingBernoulliNaiveBayesOpDescSpec
    extends AnyFlatSpec
    with SklearnModelOpDescBehaviors {

  sklearnTrainer(
    new SklearnTrainingBernoulliNaiveBayesOpDesc,
    "SklearnTrainingBernoulliNaiveBayes",
    "Training: Bernoulli Naive Bayes",
    "from sklearn.naive_bayes import BernoulliNB"
  )

  // Every column but the target is a feature here, so a row missing any value is
  // one the estimator cannot be fitted on.
  "SklearnTrainingBernoulliNaiveBayesOpDesc.generatePythonCode" should
    "drop rows with missing values before fitting" in {
    val d = new SklearnTrainingBernoulliNaiveBayesOpDesc
    d.target = "y"
    d.generatePythonCode() should include("table.dropna()")
  }

  // With Count Vectorizer on, only the text and target columns are read, so a
  // blank in any other column must not cost the row.
  it should "drop on the text and target columns only when vectorizing text" in {
    val d = new SklearnTrainingBernoulliNaiveBayesOpDesc
    d.target = "y"
    d.countVectorizer = true
    d.text = List("note")
    d.generatePythonCode() should include("table.dropna(subset=[")
  }
}
