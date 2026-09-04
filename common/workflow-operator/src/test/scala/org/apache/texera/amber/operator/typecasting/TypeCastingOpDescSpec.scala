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

package org.apache.texera.amber.operator.typecasting

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, AttributeTypeUtils, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
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

class TypeCastingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def castUnit(attr: String, to: AttributeType): TypeCastingUnit = {
    val u = new TypeCastingUnit()
    u.attribute = attr
    u.resultType = to
    u
  }

  "TypeCastingOpDesc.operatorInfo" should "advertise the name and Cleaning group" in {
    val info = (new TypeCastingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Type Casting"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
  }

  "TypeCastingOpDesc.getPhysicalOp" should "wire TypeCastingOpExec and carry port identities" in {
    val op = new TypeCastingOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.typecasting.TypeCastingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "TypeCastingOpDesc schema propagation" should
    "leave the schema unchanged when there are no casting units" in {
    val op = new TypeCastingOpDesc
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(op.operatorInfo.outputPorts.head.id -> input)
  }

  it should "change the target column's type for a casting unit" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val input = Schema().add(new Attribute("n", AttributeType.INTEGER))
    val out = op.getExternalOutputSchemas(Map(op.operatorInfo.inputPorts.head.id -> input))
    out shouldBe Map(
      op.operatorInfo.outputPorts.head.id -> Schema().add(new Attribute("n", AttributeType.STRING))
    )
  }

  "TypeCastingOpDesc" should "round-trip its casting units through the polymorphic base" in {
    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("n", AttributeType.STRING))
    val restored =
      objectMapper.readValue(objectMapper.writeValueAsString(op), classOf[LogicalOp])
    restored shouldBe a[TypeCastingOpDesc]
    val tc = restored.asInstanceOf[TypeCastingOpDesc]
    tc.typeCastingUnits should have size 1
    tc.typeCastingUnits.head.attribute shouldBe "n"
    tc.typeCastingUnits.head.resultType shouldBe AttributeType.STRING
  }

  // The values a cast reads differently on the two sides. Python's own `bool`
  // answers true for every non-empty string, so "false" and "0" are where the
  // script used to disagree with the run it came from; text that is neither a
  // boolean nor a number, and an empty cell, are the two ends of the range.
  private val boolCases = Seq("true", "false", "0", "1", "not a boolean", null)

  /** What the engine answers, as the string the Python side prints back: the
    * literal, or `error` for a value `parseField` refuses.
    */
  private def engineAnswer(value: String, to: AttributeType): String =
    Try(AttributeTypeUtils.parseField(value, to))
      .map(v => if (v == null) "null" else v.toString)
      .getOrElse("error")

  it should "cast to boolean the way AttributeTypeUtils does" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new TypeCastingOpDesc
    op.typeCastingUnits = List(castUnit("v", AttributeType.BOOLEAN))

    // The generated block reads `in1df` and writes `out1df`, so it becomes the
    // body of a function the driver calls once per value. One row at a time,
    // because a value the cast refuses would otherwise end the comparison at
    // the first one.
    val body = op.generateStandaloneCode().linesIterator.map("    " + _).mkString("\n")
    val values = boolCases
      .map(v => if (v == null) "None" else "\"" + v + "\"")
      .mkString("[", ", ", "]")
    val driver =
      s"""import pandas as pd
         |
         |${op.standaloneHelpers().mkString("\n\n")}
         |
         |
         |def cast(in1df):
         |$body
         |    return out1df
         |
         |
         |for value in $values:
         |    try:
         |        answer = cast(pd.DataFrame({"v": [value]}))["v"].iloc[0]
         |        print("null" if pd.isna(answer) else str(answer).lower())
         |    except Exception:
         |        print("error")
         |""".stripMargin

    val script = Files.createTempFile("typecast-bool-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process =
      new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n${Source.fromFile(script.toFile).mkString}") {
      process.exitValue() shouldBe 0
    }

    val fromScript = out.trim.linesIterator.toSeq
    val fromEngine = boolCases.map { v =>
      if (v == null) "null" else engineAnswer(v, AttributeType.BOOLEAN).toLowerCase
    }
    withClue(s"cases=${boolCases.mkString(", ")}\nscript said $fromScript\n") {
      fromScript shouldBe fromEngine
    }
    // The pairs that made the review: "false" is not true, and "0" is not true.
    fromEngine shouldBe Seq("true", "false", "false", "true", "error", "null")
  }

  // Python resolution follows FilledAreaPlotOpDescSpec: udf.conf python.path
  // (UDF_PYTHON_PATH), then python3 / python / py.
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

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption
      .exists { p =>
        if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
}
