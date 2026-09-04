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

package org.apache.texera.amber.operator.sklearn.testing

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class SklearnTestingOpDescSpec extends AnyFlatSpec with Matchers {

  "SklearnTestingOpDesc.operatorInfo" should
    "advertise the name, Sklearn group, and a model/data 2-in 1-out shape" in {
    val info = (new SklearnTestingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sklearn Testing"
    info.operatorDescription shouldBe "It will generate scorers for Sklearn model"
    info.operatorGroupName shouldBe OperatorGroupConstants.SKLEARN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("model", "data")
    info.outputPorts should have length 1
  }

  "SklearnTestingOpDesc" should "default isRegression false and the attribute fields to null" in {
    val d = new SklearnTestingOpDesc
    d.isRegression shouldBe false
    d.model shouldBe null
    d.target shouldBe null
  }

  "SklearnTestingOpDesc.getOutputSchemas" should
    "append the classification metric columns for the default (non-regression) case" in {
    val d = new SklearnTestingOpDesc
    val input = Schema().add("x", AttributeType.STRING)
    val schema =
      d.getOutputSchemas(Map(PortIdentity() -> input))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("x").getType shouldBe AttributeType.STRING
    schema.getAttribute("accuracy").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("f1").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("precision").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("recall").getType shouldBe AttributeType.DOUBLE
  }

  it should "append the regression metric columns when isRegression is true" in {
    val d = new SklearnTestingOpDesc
    d.isRegression = true
    val input = Schema().add("x", AttributeType.STRING)
    val schema =
      d.getOutputSchemas(Map(PortIdentity() -> input))(d.operatorInfo.outputPorts.head.id)
    schema.getAttribute("R2").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("RMSE").getType shouldBe AttributeType.DOUBLE
    schema.getAttribute("MAE").getType shouldBe AttributeType.DOUBLE
  }

  // The scorer reads every column but the target, so it has to leave out what an
  // estimator cannot fit for the same reason the fitting operators do, and leave
  // out the same columns: a model fitted without them refuses a frame naming them.
  "SklearnTestingOpDesc" should "narrow the features to the columns an estimator can fit" in {
    val d = new SklearnTestingOpDesc
    d.model = "model"
    d.target = "y"
    Seq(d.generatePythonCode(), d.generateStandaloneCode()).foreach { code =>
      code should include("""_fittable = X.select_dtypes(include=["number", "bool"])""")
      code should include("""print("Ignoring columns an estimator cannot fit:", _ignored)""")
      code should include("X = _fittable")
    }
  }

  "SklearnTestingOpDesc.generatePythonCode" should "emit the scorer tuple operator" in {
    val d = new SklearnTestingOpDesc
    d.model = "model"
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("class ProcessTupleOperator(UDFOperatorV2)")
    code should include("from sklearn.metrics import")
    code should include(".predict(")
  }

  // The scores are computed over the rows the model can be applied to, the way
  // COUNT and MIN are computed over the rows that have a value.
  it should "drop rows with missing values before scoring" in {
    val d = new SklearnTestingOpDesc
    d.model = "model"
    d.target = "y"
    d.generatePythonCode() should include("Table(self.data).dropna()")
  }

  // The scorer reads every column but the target, so it has to leave out what an
  // estimator cannot fit for the same reason the fitting operators do, and leave
  // out the same columns: a model fitted without them refuses a frame naming them.
  it should "narrow the features to the columns an estimator can fit" in {
    val d = new SklearnTestingOpDesc
    d.model = "model"
    d.target = "y"
    val code = d.generatePythonCode()
    code should include("""_fittable = X.select_dtypes(include=["number", "bool"])""")
    code should include("""print("Ignoring columns an estimator cannot fit:", _ignored)""")
    code should include("X = _fittable")
  }

  "SklearnTestingOpDesc" should
    "round-trip its config fields through the polymorphic base" in {
    val d = new SklearnTestingOpDesc
    d.isRegression = true
    d.model = "m"
    d.target = "t"
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"SklearnTesting\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SklearnTestingOpDesc]
    val r = restored.asInstanceOf[SklearnTestingOpDesc]
    r.isRegression shouldBe true
    r.model shouldBe "m"
    r.target shouldBe "t"
  }

  // The parity this operator cannot get from the verification runner: a fixture
  // written from the JVM cannot carry a fitted model on an input port, so the
  // two paths are never run side by side. The executor drops the rows holding a
  // missing value before it scores, and the property that follows is testable
  // here on its own: a row it would drop must not move the score.
  it should "score a table with a missing row the way it scores that table without it" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas, sklearn")) cancel(s"'$python' cannot import pandas and sklearn")

    val op = new SklearnTestingOpDesc
    op.model = "model"
    op.target = "target"
    // The generated block reads in1df / in2df and writes out1df, so it runs as
    // the body of the loop below.
    val body = op.generateStandaloneCode().linesIterator.map("    " + _).mkString("\n")

    val driver =
      s"""import pandas as pd
         |from sklearn.tree import DecisionTreeClassifier
         |
         |train = pd.DataFrame({"f1": [0, 1, 0, 1], "f2": [0, 0, 1, 1], "target": [0, 1, 1, 0]})
         |fitted = DecisionTreeClassifier(random_state=0).fit(train[["f1", "f2"]], train["target"])
         |
         |# The same rows twice, except that one carries a hole the executor would
         |# drop. Reading the same score from both is the parity.
         |#
         |# That row's label is one the model was never trained on, so scoring it
         |# is wrong whatever the estimator answers. Without it the score has to
         |# move, which is what makes this test able to fail: a decision tree
         |# predicts through a missing feature rather than refusing it, so a row
         |# with a plausible label would have agreed by luck.
         |with_hole = pd.DataFrame(
         |    {"f1": [0.0, 1.0, 0.0, None], "f2": [0.0, 0.0, 1.0, 1.0], "target": [0, 1, 1, 2]}
         |)
         |without = with_hole.dropna().reset_index(drop=True)
         |
         |scores = []
         |for frame in (with_hole, without):
         |    in1df = pd.DataFrame({"model": [fitted]})
         |    in2df = frame
         |$body
         |    scores.append(round(float(out1df["accuracy"].iloc[0]), 10))
         |
         |print(scores[0])
         |print(scores[1])
         |""".stripMargin

    val script = Files.createTempFile("sklearn-testing-parity-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(180, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") { process.exitValue() shouldBe 0 }

    val answers = out.trim.linesIterator.filter(_.matches("""-?\d+\.\d+""")).toSeq
    withClue(s"python said:\n$out") {
      answers should have length 2
      answers.head shouldBe answers(1)
    }
  }

  private def resolvePython(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def runnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(runnable)
  }

  private def canImport(python: String, modules: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", s"import $modules").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(120, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
