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

package org.apache.texera.amber.operator.binning

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.util.Try

class BinningOpDescSpec extends AnyFlatSpec with Matchers {

  private val inputSchema = new Schema(
    new Attribute("id", AttributeType.INTEGER),
    new Attribute("age", AttributeType.DOUBLE)
  )

  private def desc(m: BinningMethod = BinningMethod.EQUAL_WIDTH, n: Int = 4): BinningOpDesc = {
    val d = new BinningOpDesc
    d.attribute = "age"
    d.method = m
    d.bins = n
    d
  }

  "BinningOpDesc.operatorInfo" should "advertise the name and the Cleaning group" in {
    val info = (new BinningOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Binning"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  // A quantile cut is made at the column's own quantiles, which the last row can
  // still move, so nothing may be emitted until the input ends.
  it should "declare its output port blocking" in {
    (new BinningOpDesc).operatorInfo.outputPorts.head.blocking shouldBe true
  }

  "The output schema" should "append one STRING column named after the source" in {
    val schema = desc().getOutputSchemas(Map(PortIdentity() -> inputSchema))(PortIdentity())
    schema.getAttributeNames shouldBe List("id", "age", "age_bin")
    schema.getAttribute("age_bin").getType shouldBe AttributeType.STRING
  }

  it should "refuse a derived name the input already carries" in {
    val clashing = inputSchema.add("age_bin", AttributeType.STRING)
    a[RuntimeException] should be thrownBy
      desc().getOutputSchemas(Map(PortIdentity() -> clashing))
  }

  "The generated code" should "cut on the count of bins asked for" in {
    desc(BinningMethod.EQUAL_WIDTH, 7).generateStandaloneCode() should include("bins=7")
  }

  // A quantile cut can put two edges in one place where the values repeat, and
  // dropping the duplicate yields fewer bins rather than raising.
  it should "ask the quantile cut to drop a duplicate edge" in {
    val code = desc(BinningMethod.EQUAL_FREQUENCY).generateStandaloneCode()
    code should include("pd.qcut(")
    code should include("""duplicates="drop"""")
    code should include("q=4")
  }

  it should "keep an empty cell empty rather than rendering it as text" in {
    desc().generateStandaloneCode() should include("where(lambda s: s.notna(), None)")
  }

  it should "hold the source and the derived name as escaped literals" in {
    val d = desc()
    d.attribute = "a\"b"
    val code = d.generateStandaloneCode()
    code should include("""in1df.copy()""")
    code should include("""out1df["a\"b"]""")
    code should include("""out1df["a\"b_bin"]""")
  }

  // Edges cannot be found in a column that holds nothing, and an equal-width cut
  // raises rather than handing back empty bins. Both renderings skip the cut, which
  // leaves the holes for the suffix to turn into the empty labels they already are.
  it should "leave a column with nothing in it uncut" in {
    val guard = "if _binned.notna().any():"
    desc().generateStandaloneCode() should include(guard)
    desc().generatePythonCode() should include(guard)
  }

  // Both paths make the same call, on a series both name the same, so a difference
  // between them is a difference in this operator rather than in pandas.
  "The two renderings" should "make the same pandas call" in {
    val d = desc(BinningMethod.EQUAL_FREQUENCY, 5)
    val call = """pd.qcut(_binned, q=5, duplicates="drop")"""
    d.generateStandaloneCode() should include(call)
    d.generatePythonCode() should include(call)
  }

  "The platform code" should "be a table operator, since a quantile needs every row" in {
    val code = desc().generatePythonCode()
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("def process_table(")
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

  private def canImportPandas(python: String): Boolean = {
    val pTry = Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    )
    pTry.toOption.exists { p =>
      val finished = p.waitFor(60, TimeUnit.SECONDS)
      if (!finished) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
  }

  // Runs the exported code as written over three frames, and reports the length of
  // the bin column and how much of it was filled. The exported code is plain pandas,
  // so nothing about the operator has to be stubbed for it to run.
  private val runtimeDriverScript: String =
    """import sys
      |import pandas as pd
      |
      |code = open(sys.argv[1]).read()
      |cases = {
      |    "allnull": pd.DataFrame({"age": pd.Series([None, None], dtype="float64")}),
      |    "empty": pd.DataFrame({"age": pd.Series([], dtype="float64")}),
      |    "mixed": pd.DataFrame({"age": pd.Series([1.0, 2.0, None])}),
      |}
      |for cid, frame in cases.items():
      |    namespace = {"pd": pd, "in1df": frame}
      |    try:
      |        exec(code, namespace)
      |        labels = namespace["out1df"]["age_bin"]
      |        print("CASE %s OK:%d:%d" % (cid, len(labels), labels.notna().sum()))
      |    except Exception as error:
      |        print("CASE %s %s" % (cid, type(error).__name__))
      |""".stripMargin

  // An all-empty column used to raise on an equal-width cut, and so did an empty
  // table, both before the suffix that keeps a hole a hole could run.
  for (method <- BinningMethod.values) {
    it should s"bin a column with nothing in it rather than raising, cutting by $method" in {
      val python = resolvePythonExecutable().getOrElse(
        cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
      )
      if (!canImportPandas(python)) {
        cancel(s"'$python' cannot import pandas; skipping runtime verification")
      }

      val moduleFile = Files.createTempFile("binning_op_", ".py")
      val driverFile = Files.createTempFile("binning_driver_", ".py")
      try {
        val code = desc(method).generateStandaloneCode()
        Files.write(moduleFile, code.getBytes(StandardCharsets.UTF_8))
        Files.write(driverFile, runtimeDriverScript.getBytes(StandardCharsets.UTF_8))

        val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
          .redirectErrorStream(true)
          .start()
        val finished = process.waitFor(120, TimeUnit.SECONDS)
        if (!finished) {
          process.destroyForcibly()
          fail("Runtime verification driver timed out after 120s")
        }
        val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
        withClue(s"Exported code:\n$code\nDriver output:\n$output\n") {
          process.exitValue() shouldBe 0
          val verdicts = "CASE (\\S+) (\\S+)".r
            .findAllMatchIn(output)
            .map(m => m.group(1) -> m.group(2))
            .toMap
          verdicts shouldBe Map(
            "allnull" -> "OK:2:0", // two rows, neither of them in a bin
            "empty" -> "OK:0:0", // no rows to put in one
            "mixed" -> "OK:3:2" // the hole is the only row left unlabelled
          )
        }
      } finally {
        Try(Files.deleteIfExists(moduleFile))
        Try(Files.deleteIfExists(driverFile))
        ()
      }
    }
  }
}
