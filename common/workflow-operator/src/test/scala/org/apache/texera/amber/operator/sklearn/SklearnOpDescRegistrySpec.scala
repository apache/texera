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

import org.apache.texera.amber.operator.sklearn.training._
import org.apache.texera.amber.pybuilder.PythonReflectionUtils
import org.scalatest.flatspec.AnyFlatSpec

/**
  * Pins the wiring (Python import statement + user-friendly model name) for
  * every concrete `SklearnClassifierOpDesc` and `SklearnTrainingOpDesc`. A
  * typo in either string would silently misroute downstream UI labels and
  * cause breakage in the generated Python pipeline.
  */
class SklearnOpDescRegistrySpec extends AnyFlatSpec {

  // ---------------------------------------------------------------------------
  // Paired Classifier + Training registry (25 shared models + 1 training-only)
  // ---------------------------------------------------------------------------

  private val pairedModels: List[(SklearnClassifierOpDesc, SklearnTrainingOpDesc, String, String)] =
    List(
      (
        new SklearnAdaptiveBoostingOpDesc(),
        new SklearnTrainingAdaptiveBoostingOpDesc(),
        "from sklearn.ensemble import AdaBoostClassifier",
        "Adaptive Boosting"
      ),
      (
        new SklearnBaggingOpDesc(),
        new SklearnTrainingBaggingOpDesc(),
        "from sklearn.ensemble import BaggingClassifier",
        "Bagging"
      ),
      (
        new SklearnBernoulliNaiveBayesOpDesc(),
        new SklearnTrainingBernoulliNaiveBayesOpDesc(),
        "from sklearn.naive_bayes import BernoulliNB",
        "Bernoulli Naive Bayes"
      ),
      (
        new SklearnComplementNaiveBayesOpDesc(),
        new SklearnTrainingComplementNaiveBayesOpDesc(),
        "from sklearn.naive_bayes import ComplementNB",
        "Complement Naive Bayes"
      ),
      (
        new SklearnDummyClassifierOpDesc(),
        new SklearnTrainingDummyClassifierOpDesc(),
        "from sklearn.dummy import DummyClassifier",
        "Dummy Classifier"
      ),
      (
        new SklearnDecisionTreeOpDesc(),
        new SklearnTrainingDecisionTreeOpDesc(),
        "from sklearn.tree import DecisionTreeClassifier",
        "Decision Tree"
      ),
      (
        new SklearnExtraTreeOpDesc(),
        new SklearnTrainingExtraTreeOpDesc(),
        "from sklearn.tree import ExtraTreeClassifier",
        "Extra Tree"
      ),
      (
        new SklearnExtraTreesOpDesc(),
        new SklearnTrainingExtraTreesOpDesc(),
        "from sklearn.ensemble import ExtraTreesClassifier",
        "Extra Trees"
      ),
      (
        new SklearnGaussianNaiveBayesOpDesc(),
        new SklearnTrainingGaussianNaiveBayesOpDesc(),
        "from sklearn.naive_bayes import GaussianNB",
        "Gaussian Naive Bayes"
      ),
      (
        new SklearnGradientBoostingOpDesc(),
        new SklearnTrainingGradientBoostingOpDesc(),
        "from sklearn.ensemble import GradientBoostingClassifier",
        "Gradient Boosting"
      ),
      (
        new SklearnKNNOpDesc(),
        new SklearnTrainingKNNOpDesc(),
        "from sklearn.neighbors import KNeighborsClassifier",
        "K-nearest Neighbors"
      ),
      (
        new SklearnLinearSVMOpDesc(),
        new SklearnTrainingLinearSVMOpDesc(),
        "from sklearn.svm import LinearSVC",
        "Linear Support Vector Machine"
      ),
      (
        new SklearnLogisticRegressionCVOpDesc(),
        new SklearnTrainingLogisticRegressionCVOpDesc(),
        "from sklearn.linear_model import LogisticRegressionCV",
        "Logistic Regression Cross Validation"
      ),
      (
        new SklearnLogisticRegressionOpDesc(),
        new SklearnTrainingLogisticRegressionOpDesc(),
        "from sklearn.linear_model import LogisticRegression",
        "Logistic Regression"
      ),
      (
        new SklearnMultiLayerPerceptronOpDesc(),
        new SklearnTrainingMultiLayerPerceptronOpDesc(),
        "from sklearn.neural_network import MLPClassifier",
        "Multi-layer Perceptron"
      ),
      (
        new SklearnMultinomialNaiveBayesOpDesc(),
        new SklearnTrainingMultinomialNaiveBayesOpDesc(),
        "from sklearn.naive_bayes import MultinomialNB",
        "Multinomial Naive Bayes"
      ),
      (
        new SklearnNearestCentroidOpDesc(),
        new SklearnTrainingNearestCentroidOpDesc(),
        "from sklearn.neighbors import NearestCentroid",
        "Nearest Centroid"
      ),
      (
        new SklearnPassiveAggressiveOpDesc(),
        new SklearnTrainingPassiveAggressiveOpDesc(),
        "from sklearn.linear_model import PassiveAggressiveClassifier",
        "Passive Aggressive"
      ),
      (
        new SklearnPerceptronOpDesc(),
        new SklearnTrainingPerceptronOpDesc(),
        "from sklearn.linear_model import Perceptron",
        "Linear Perceptron"
      ),
      (
        new SklearnProbabilityCalibrationOpDesc(),
        new SklearnTrainingProbabilityCalibrationOpDesc(),
        "from sklearn.calibration import CalibratedClassifierCV",
        "Probability Calibration"
      ),
      (
        new SklearnRandomForestOpDesc(),
        new SklearnTrainingRandomForestOpDesc(),
        "from sklearn.ensemble import RandomForestClassifier",
        "Random Forest"
      ),
      (
        new SklearnRidgeCVOpDesc(),
        new SklearnTrainingRidgeCVOpDesc(),
        "from sklearn.linear_model import RidgeClassifierCV",
        "Ridge Regression Cross Validation"
      ),
      (
        new SklearnRidgeOpDesc(),
        new SklearnTrainingRidgeOpDesc(),
        "from sklearn.linear_model import RidgeClassifier",
        "Ridge Regression"
      ),
      (
        new SklearnSDGOpDesc(),
        new SklearnTrainingSDGOpDesc(),
        "from sklearn.linear_model import SGDClassifier",
        "Stochastic Gradient Descent"
      ),
      (
        new SklearnSVMOpDesc(),
        new SklearnTrainingSVMOpDesc(),
        "from sklearn.svm import SVC",
        "Support Vector Machine"
      )
    )

  private val classifierEntries: List[(SklearnClassifierOpDesc, String, String)] =
    pairedModels.map { case (classifier, _, imp, name) => (classifier, imp, name) }

  private val trainingEntries: List[(SklearnTrainingOpDesc, String, String)] =
    pairedModels.map {
      case (_, trainer, imp, name) => (trainer, imp, s"Training: $name")
    } :+ (
      new SklearnTrainingLinearRegressionOpDesc(),
      "from sklearn.linear_model import LinearRegression",
      "Training: Linear Regression"
  )

  private def registerWiringSpecs[T](entries: Seq[(T, String, String)])(
      importsOf: T => String,
      nameOf: T => String
  ): Unit =
    entries.foreach {
      case (desc, expectedImport, expectedName) =>
        val cls = desc.getClass.getSimpleName
        cls should s"return import statement '$expectedImport'" in {
          assert(importsOf(desc) == expectedImport)
        }
        it should s"return user-friendly model name '$expectedName'" in {
          assert(nameOf(desc) == expectedName)
        }
    }

  registerWiringSpecs(classifierEntries)(_.getImportStatements, _.getUserFriendlyModelName)

  "SklearnClassifierOpDesc" should "embed the import statement into generatePythonCode for a concrete subclass" in {
    val desc = new SklearnLogisticRegressionOpDesc()
    desc.target = "y"
    desc.countVectorizer = false
    // `tfidfTransformer` is defined on the shared base class, defaults to false.
    val code = desc.generatePythonCode()
    assert(code.contains("from sklearn.linear_model import LogisticRegression"))
    // Classifier OpDescs emit a UDFTableOperator pipeline.
    assert(code.contains("ProcessTableOperator"))
  }
  // NOTE: the abstract base class's empty-string defaults are NOT tested here.
  // Instantiating `SklearnClassifierOpDesc` from this spec (e.g. via
  // `new SklearnClassifierOpDesc {}`) creates an anonymous test-package class
  // under `org.apache.texera.amber.operator.sklearn`, which the
  // PythonCodeRawInvalidTextSpec classpath scan then picks up as a descriptor
  // candidate and fails on (anonymous classes have no accessible no-arg
  // constructor). Every concrete subclass below overrides both methods, so
  // the base default is never observable in production anyway.

  registerWiringSpecs(trainingEntries)(_.getImportStatements, _.getUserFriendlyModelName)

  "SklearnTrainingOpDesc default" should "use empty defaults until a subclass overrides" in {
    val base = new SklearnTrainingOpDesc()
    assert(base.getImportStatements == "")
    assert(base.getUserFriendlyModelName == "")
  }

  it should "embed the import statement into generatePythonCode for a concrete subclass" in {
    val desc = new SklearnTrainingLogisticRegressionOpDesc()
    desc.target = "y"
    desc.countVectorizer = false
    desc.tfidfTransformer = false
    val code = desc.generatePythonCode()
    assert(code.contains("from sklearn.linear_model import LogisticRegression"))
    assert(code.contains("ProcessTableOperator"))
  }

  // ---------------------------------------------------------------------------
  // Completeness — guard against a new subclass silently bypassing this spec
  // ---------------------------------------------------------------------------
  //
  // Reuse the same classpath scanner that PythonCodeRawInvalidTextSpec uses,
  // so the two suites agree on what counts as a "concrete" descriptor.

  private def scanConcrete[T](base: Class[T], pkg: String): Set[Class[_]] =
    PythonReflectionUtils
      .scanCandidates(
        base = base,
        acceptPackages = Seq(pkg),
        classLoader = Thread.currentThread().getContextClassLoader
      )
      .toSet

  private def assertRegistryCoversConcrete[T](
      entryName: String,
      base: Class[T],
      pkg: String,
      entries: Seq[(T, String, String)],
      excluded: Set[Class[_]] = Set.empty
  ): Unit = {
    val scanned = scanConcrete(base, pkg) -- excluded
    val tested = entries.map(_._1.getClass).toSet[Class[_]]
    val missing = scanned.diff(tested)
    val extra = tested.diff(scanned)
    assert(
      missing.isEmpty && extra.isEmpty,
      s"$entryName drift — missing on classpath: ${missing
        .map(_.getName)}, no longer concrete: ${extra.map(_.getName)}"
    )
  }

  "classifierEntries" should
    "cover every concrete SklearnClassifierOpDesc subclass on the classpath" in {
    assertRegistryCoversConcrete(
      "classifierEntries",
      classOf[SklearnClassifierOpDesc],
      "org.apache.texera.amber.operator.sklearn",
      classifierEntries
    )
  }

  "trainingEntries" should
    "cover every concrete SklearnTrainingOpDesc subclass on the classpath" in {
    // SklearnTrainingOpDesc is itself concrete (used as a default fallback),
    // so the scan picks it up alongside the real subclasses. Exclude it from
    // the "concrete subclasses" comparison since it is not part of the
    // registry being pinned.
    assertRegistryCoversConcrete(
      "trainingEntries",
      classOf[SklearnTrainingOpDesc],
      "org.apache.texera.amber.operator.sklearn.training",
      trainingEntries,
      excluded = Set(classOf[SklearnTrainingOpDesc])
    )
  }
}
