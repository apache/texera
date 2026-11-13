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

class SklearnBernoulliNaiveBayesOpDescSpec extends AnyFlatSpec with SklearnModelOpDescBehaviors {

  sklearnClassifier(
    new SklearnBernoulliNaiveBayesOpDesc,
    "SklearnBernoulliNaiveBayes",
    "Bernoulli Naive Bayes",
    "from sklearn.naive_bayes import BernoulliNB",
    estimatorTestName = "import and instantiate the BernoulliNB estimator"
  )

  // The same table statement serves both ports, so training and scoring skip a
  // row with a missing value alike.
  "SklearnBernoulliNaiveBayesOpDesc.generatePythonCode" should
    "drop rows with missing values before fitting and before scoring" in {
    val d = new SklearnBernoulliNaiveBayesOpDesc
    d.target = "y"
    d.generatePythonCode() should include("table.dropna()")
  }

  // Dropping a row changes the data the model was asked to learn from, so the count
  // is reported next to the metrics rather than left for the user to notice.
  it should "say how many rows it dropped" in {
    val d = new SklearnBernoulliNaiveBayesOpDesc
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("rows_read = len(table)")
    code should include("\"Skipped\"")
  }

  // The operator runs once per port and counts the rows it dropped each time, so
  // the script says it for the table it scores as well as the one it fits.
  "SklearnBernoulliNaiveBayesOpDesc.generateStandaloneCode" should
    "count the rows it dropped on both tables" in {
    val d = new SklearnBernoulliNaiveBayesOpDesc
    d.target = "y"
    val code = d.generateStandaloneCode()
    code should include(
      """print("Skipped", len(in1df) - len(_train), "of", len(in1df), "rows with missing values")"""
    )
    code should include(
      """print("Skipped", len(in2df) - len(_test), "of", len(in2df), "rows with missing values")"""
    )
  }
}
