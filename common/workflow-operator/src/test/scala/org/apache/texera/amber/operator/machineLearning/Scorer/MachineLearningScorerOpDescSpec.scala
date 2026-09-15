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

package org.apache.texera.amber.operator.machineLearning.Scorer

import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.util.Try

class MachineLearningScorerOpDescSpec extends AnyFlatSpec with Matchers {

  /** An EncodableString field renders as a runtime decode site in the emitted code. */
  private val decodeSite = "self.decode_python_template"

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  /** The one emitted line that assigns metric_list, isolated from the template body. */
  private def metricListLine(code: String): String =
    code.linesIterator
      .find(_.contains("metric_list = ["))
      .getOrElse(fail(s"no metric_list assignment in:\n$code"))

  "MachineLearningScorerOpDesc.operatorInfo" should
    "advertise the name and Machine Learning General group" in {
    val info = (new MachineLearningScorerOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Machine Learning Scorer"
    info.operatorDescription shouldBe "Scorer for machine learning models"
    info.operatorGroupName shouldBe OperatorGroupConstants.MACHINE_LEARNING_GENERAL_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "MachineLearningScorerOpDesc" should "default isRegression false and the column fields to empty" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression shouldBe false
    d.actualValueColumn shouldBe ""
    d.predictValueColumn shouldBe ""
    d.classificationMetrics shouldBe empty
    d.regressionMetrics shouldBe empty
  }

  "MachineLearningScorerOpDesc.getOutputSchemas" should
    "include a Class column for classification with no metrics" in {
    val d = new MachineLearningScorerOpDesc
    d.getOutputSchemas(Map.empty) shouldBe Map(
      d.operatorInfo.outputPorts.head.id -> Schema(
        List(new Attribute("Class", AttributeType.STRING))
      )
    )
  }

  it should "append one DOUBLE column per selected classification metric, after Class" in {
    // The metric columns carry numeric scores, so their type is the one schema
    // decision this method makes. With empty metric lists the foldLeft body never
    // runs and that type stays unpinned -- a scorer could advertise Accuracy as
    // STRING and the suite would not notice.
    val d = new MachineLearningScorerOpDesc
    d.classificationMetrics =
      List(classificationMetricsFnc.accuracy, classificationMetricsFnc.f1Score)
    val port = d.operatorInfo.outputPorts.head.id
    d.getOutputSchemas(Map.empty)(port).getAttributes.map(a => (a.getName, a.getType)) shouldBe
      List(
        ("Class", AttributeType.STRING),
        ("Accuracy", AttributeType.DOUBLE),
        ("F1 Score", AttributeType.DOUBLE)
      )
  }

  it should "produce an empty schema for regression with no metrics" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    val out = d.getOutputSchemas(Map.empty)
    out.keySet shouldBe Set(d.operatorInfo.outputPorts.head.id)
    out(d.operatorInfo.outputPorts.head.id).getAttributes shouldBe empty
  }

  it should "emit one DOUBLE column per selected regression metric and no Class column" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.regressionMetrics = List(regressionMetricsFnc.mse, regressionMetricsFnc.r2)
    // the classification list must be ignored entirely once the task is regression
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    val port = d.operatorInfo.outputPorts.head.id
    d.getOutputSchemas(Map.empty)(port).getAttributes.map(a => (a.getName, a.getType)) shouldBe
      List(("MSE", AttributeType.DOUBLE), ("R2", AttributeType.DOUBLE))
  }

  /** An input table holding one column of each type the scored-column check reasons about. */
  private def inputSchemas(d: MachineLearningScorerOpDesc): Map[PortIdentity, Schema] =
    Map(
      d.operatorInfo.inputPorts.head.id -> Schema(
        List(
          new Attribute("y_int", AttributeType.INTEGER),
          new Attribute("pred_long", AttributeType.LONG),
          new Attribute("label_str", AttributeType.STRING),
          new Attribute("pred_str", AttributeType.STRING)
        )
      )
    )

  private def scorer(
      regression: Boolean,
      actual: String,
      predict: String
  ): MachineLearningScorerOpDesc = {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = regression
    d.actualValueColumn = actual
    d.predictValueColumn = predict
    d
  }

  it should "accept a classification pair whose types are both numeric" in {
    // INTEGER against LONG is one label domain read through two widths, which is
    // what an upstream predictor emitting a wider column looks like.
    val d = scorer(regression = false, "y_int", "pred_long")
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    noException should be thrownBy d.getOutputSchemas(inputSchemas(d))
  }

  it should "reject a classification pair that mixes a number with a string" in {
    val d = scorer(regression = false, "y_int", "pred_str")
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    the[RuntimeException] thrownBy d.getOutputSchemas(inputSchemas(d)) should have message
      "Actual Value 'y_int' (integer) and Predicted Value 'pred_str' (string) hold different " +
        "kinds of label, so a classification metric cannot compare them"
  }

  it should "reject a non-numeric column once the task is regression" in {
    val d = scorer(regression = true, "label_str", "pred_str")
    d.regressionMetrics = List(regressionMetricsFnc.mse)
    the[RuntimeException] thrownBy d.getOutputSchemas(inputSchemas(d)) should have message
      "A regression metric needs a numeric Actual Value column, but 'label_str' is string"
  }

  it should "reject a column the input table does not hold" in {
    val d = scorer(regression = false, "y_int", "absent")
    the[RuntimeException] thrownBy d.getOutputSchemas(inputSchemas(d)) should have message
      "Predicted Value column 'absent' is not in the input table"
  }

  it should "say nothing about the pair while a column is still unpicked" in {
    // Half-filled is the state every operator passes through on the way to a
    // valid one; the empty field already carries its own required marker.
    val d = scorer(regression = false, "y_int", "")
    noException should be thrownBy d.getOutputSchemas(inputSchemas(d))
  }

  "MachineLearningScorerOpDesc.generatePythonCode" should "emit the scorer table operator" in {
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    val code = d.generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("from sklearn.metrics import")
    // actualValueColumn/predictValueColumn are EncodableString: base64-encoded into
    // the emitted code. Assert WHICH variable each payload is bound to, not just that
    // both payloads appear somewhere: precision_score/recall_score are asymmetric in
    // (y_true, y_pred), so a swap would silently report recall as precision.
    code should include(s"y_true = table[$decodeSite('${b64("y")}')]")
    code should include(s"y_pred = table[$decodeSite('${b64("yhat")}')]")
    // isRegression defaults to false, so the emitted branch guard must read False;
    // without this the `else "False"` arm of the flag is unpinned in both tests.
    code should include("if False:")
  }

  it should "drop the rows missing either scored column, on both paths" in {
    // The metrics refuse an empty cell instead of passing over it, so the row has
    // to go before it reaches them. Both generators emit the same drop, and the
    // subset names both columns: dropping on either one alone would leave the two
    // series misaligned.
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.generatePythonCode() should include(
      s"table = table.dropna(subset=[$decodeSite('${b64("y")}'), $decodeSite('${b64("yhat")}')])"
    )
    // Each binds the drop to a name of its own rather than writing it back: the
    // frame this operator was handed can feed another branch of the plan.
    d.generateStandaloneCode() should include("""scored_df = in1df.dropna(subset=["y", "yhat"])""")
  }

  it should "splice the selected metrics verbatim into a proper metric_list" in {
    // The metric fragment must be spliced verbatim, not re-encoded as one quoted
    // value (which would collapse the whole list into a single malformed element).
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.classificationMetrics =
      List(classificationMetricsFnc.accuracy, classificationMetricsFnc.f1Score)
    val code = d.generatePythonCode()
    code should include("metric_list = ['Accuracy','F1 Score']")
    // The metric names must NOT be base64-re-encoded through the template builder.
    val encoded =
      Base64.getEncoder.encodeToString("'Accuracy','F1 Score'".getBytes(StandardCharsets.UTF_8))
    code should not include encoded
  }

  it should "select the regression metrics and the regression branch when isRegression is set" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.regressionMetrics = List(regressionMetricsFnc.mse, regressionMetricsFnc.r2)
    // the classification list must be ignored entirely once the task is regression
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)
    val code = d.generatePythonCode()

    metricListLine(code) should include("['MSE','R2']")
    // isRegression also picks the branch process_table takes at runtime
    code should include("if True:")
    code should not include "if False:"
  }

  it should "reject an unrecognized entry in the metric list loudly" in {
    // A saved workflow can carry a null element in the metric array. Emitting
    // `metric_list = ['']` for it would generate silently broken Python, so the
    // descriptor must fail while the workflow is still being compiled.
    val node = objectMapper
      .readTree(objectMapper.writeValueAsString(new MachineLearningScorerOpDesc))
      .asInstanceOf[ObjectNode]
    node.putArray("classificationFlag").addNull()
    val d =
      objectMapper.treeToValue(node, classOf[LogicalOp]).asInstanceOf[MachineLearningScorerOpDesc]
    d.classificationMetrics shouldBe List(null)

    val ex = intercept[IllegalArgumentException](d.generatePythonCode())
    ex.getMessage shouldBe "Unknown metric type"
  }

  "MachineLearningScorerOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new MachineLearningScorerOpDesc
    d.isRegression = true
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[MachineLearningScorerOpDesc]
    val s = restored.asInstanceOf[MachineLearningScorerOpDesc]
    s.isRegression shouldBe true
    s.actualValueColumn shouldBe "y"
    s.predictValueColumn shouldBe "yhat"
  }

  it should "refuse a table the drop leaves empty, rather than scoring nothing" in {
    // The metrics answer an emptied table badly and each in its own way, so the
    // operator has to say what happened before they are reached.
    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.generatePythonCode() should include("if table.empty:")
    // The standalone path drops the same rows, so it has to answer the same way.
    d.generateStandaloneCode() should include("if scored_df.empty:")
    Seq(d.generatePythonCode(), d.generateStandaloneCode()).foreach { code =>
      code should include("No rows left to score")
    }
  }

  // Python executable resolution, following FilledAreaPlotOpDescSpec:
  // udf.conf python.path (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePythonExecutable(): Option[String] = {
    def fromConfig: Option[String] = {
      val configOpt =
        Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
          .orElse(Try(ConfigFactory.load()).toOption)
      configOpt
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)
    }

    def isRunnable(exe: String): Boolean = {
      val pTry = Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start())
      pTry.toOption.exists { p =>
        val finished = p.waitFor(5, TimeUnit.SECONDS)
        if (!finished) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
    }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private def canImportPandasAndSklearn(python: String): Boolean = {
    val pTry = Try(
      new ProcessBuilder(python, "-c", "import pandas, sklearn").redirectErrorStream(true).start()
    )
    pTry.toOption.exists { p =>
      val finished = p.waitFor(60, TimeUnit.SECONDS)
      if (!finished) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
  }

  // Driver executed by the runtime test below. It stubs only the pytexera import seam;
  // the generated module runs unmodified, against the real scikit-learn metrics.
  private val runtimeDriverScript: String =
    """import base64
      |import sys
      |import types
      |from typing import Iterator, Optional
      |
      |import numpy as np
      |import pandas as pd
      |
      |class UDFTableOperator:
      |    def decode_python_template(self, data):
      |        return base64.b64decode(data).decode("utf-8")
      |
      |stub = types.ModuleType("pytexera")
      |stub.UDFTableOperator = UDFTableOperator
      |stub.overrides = lambda fn: fn
      |stub.Table = pd.DataFrame
      |stub.TableLike = object
      |stub.Iterator = Iterator
      |stub.Optional = Optional
      |sys.modules["pytexera"] = stub
      |
      |ns = {"__name__": "generated_scorer"}
      |with open(sys.argv[1]) as f:
      |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
      |op = ns["ProcessTableOperator"]()
      |
      |nan = float("nan")
      |cases = [
      |    ("filled", [(1, 1), (0, 0), (1, 1)]),
      |    ("some_blank", [(nan, 0), (0, nan), (1, 1), (1, 0), (1, 1)]),
      |    ("all_blank", [(nan, 0), (0, nan), (nan, nan)]),
      |]
      |
      |for cid, rows in cases:
      |    frame = pd.DataFrame(rows, columns=["y", "yhat"])
      |    try:
      |        scored = list(op.process_table(frame, 0))[0]
      |        print("CASE %s ACCURACY %s" % (cid, scored["Accuracy"][0]))
      |    except ValueError as e:
      |        print("CASE %s REFUSED %s" % (cid, e))
      |""".stripMargin

  it should "score the rows it can and refuse a table that keeps none of them" in {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndSklearn(python)) {
      cancel(s"'$python' cannot import pandas and sklearn; skipping runtime verification")
    }

    val d = new MachineLearningScorerOpDesc
    d.actualValueColumn = "y"
    d.predictValueColumn = "yhat"
    d.classificationMetrics = List(classificationMetricsFnc.accuracy)

    val moduleFile = Files.createTempFile("scorer_op_", ".py")
    val driverFile = Files.createTempFile("scorer_driver_", ".py")
    try {
      Files.write(moduleFile, d.generatePythonCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, runtimeDriverScript.getBytes(StandardCharsets.UTF_8))

      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      val finished = process.waitFor(120, TimeUnit.SECONDS)
      if (!finished) {
        process.destroyForcibly()
        fail("Scoring driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
        // Every row usable: all three agree, so the score is 1.
        output should include("CASE filled ACCURACY 1.0")
        // Two rows dropped, three scored, two of those correct.
        output should include("CASE some_blank ACCURACY 0.6667")
        // Nothing survives the drop, and the operator says so rather than
        // handing back the NaN the classification metrics would produce.
        output should include("CASE all_blank REFUSED No rows left to score")
      }
    } finally {
      Files.deleteIfExists(moduleFile)
      Files.deleteIfExists(driverFile)
    }
  }
}
