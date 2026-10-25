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

package org.apache.texera.amber.operator.substringSearch

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.util.Try

class SubstringSearchOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, sub: String, caseSensitive: Boolean): SubstringSearchOpDesc = {
    val d = new SubstringSearchOpDesc
    d.attribute = attr
    d.substring = sub
    d.isCaseSensitive = caseSensitive
    d
  }

  "SubstringSearchOpDesc.operatorInfo" should
    "advertise the name, description, Search group, and reconfiguration support" in {
    val info = (new SubstringSearchOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Substring Search"
    info.operatorDescription shouldBe "Search for Substring(s) in a string column"
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "SubstringSearchOpDesc.isCaseSensitive" should "default to false" in {
    (new SubstringSearchOpDesc).isCaseSensitive shouldBe false
  }

  "SubstringSearchOpDesc.getPhysicalOp" should
    "wire the SubstringSearchOpExec class name and carry forward the operatorInfo port identities" in {
    val op = newDesc("col", "ub", caseSensitive = false)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.substringSearch.SubstringSearchOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "SubstringSearchOpDesc JSON round-trip" should
    "preserve attribute, substring, and isCaseSensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("body", "lo", caseSensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SubstringSearchOpDesc]
    val ss = restored.asInstanceOf[SubstringSearchOpDesc]
    ss.attribute shouldBe "body"
    ss.substring shouldBe "lo"
    ss.isCaseSensitive shouldBe true
  }

  // Without Case Sensitive the engine lowers both sides with toLowerCase, where
  // pandas' case=False compared upper(), which folds "ß" to "SS" and "ς" to
  // "Σ". Each search runs through SubstringSearchOpExec and through the
  // export, and the rows each keeps are named by their input position.
  "SubstringSearchOpDesc.generateStandaloneCode" should
    "keep the rows the engine keeps on non-ASCII text" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val cases = Seq(
      ("ss", false, Seq("ß", "SS", "Strasse")),
      ("ς", false, Seq("σ", "Σ", "ς")),
      ("σ", false, Seq("Σ", "ΟΔΟΣ", "σ")),
      ("i̇", false, Seq("İ", "i", "I")),
      ("ﬀ", false, Seq("FF", "ﬀ", "ff")),
      ("é", false, Seq("É", "é", "e")),
      ("K", false, Seq("K", "k")),
      ("é", true, Seq("É", "é"))
    )
    val schema = Schema().add("v", AttributeType.STRING).add("i", AttributeType.INTEGER)
    val runs = cases.map {
      case (substring, caseSensitive, values) =>
        val desc = newDesc("v", substring, caseSensitive)
        val exec = new SubstringSearchOpExec(objectMapper.writeValueAsString(desc))
        exec.open()
        val kept = values.zipWithIndex.flatMap {
          case (v, i) => exec.processTuple(Tuple(schema, Array[Any](v, i)), 0)
        }
        exec.close()
        val label = s"'$substring' caseSensitive=$caseSensitive"
        val engine = s"$label: ${kept.map(_.getFields(1)).mkString(" ")}"
        val code = (desc.standaloneHelpers() :+
          desc.generateStandaloneCode(Map(PortIdentity() -> schema))).mkString("\n")
        val script =
          s"""in1df = pd.DataFrame({"v": pd.Series(${objectMapper.writeValueAsString(
            values
          )}, dtype=object)})
             |in1df["i"] = range(len(in1df))
             |ns = {"pd": pd, "in1df": in1df}
             |exec(${objectMapper.writeValueAsString(code)}, ns)
             |print(${objectMapper.writeValueAsString(
            label + ":"
          )}, " ".join(str(i) for i in ns["out1df"]["i"]))""".stripMargin
        (engine, script)
    }

    val driver = ("import pandas as pd" +: runs.map(_._2)).mkString("\n")
    val file = Files.createTempFile("substring-engine-parity-", ".py")
    file.toFile.deleteOnExit()
    Files.write(file, driver.getBytes(StandardCharsets.UTF_8))
    val builder = new ProcessBuilder(python, file.toString).redirectErrorStream(true)
    builder.environment().put("PYTHONIOENCODING", "utf-8")
    val process = builder.start()
    val out = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\n") {
      process.exitValue() shouldBe 0
      out.linesIterator.map(_.trim).toList shouldBe runs.map(_._1.trim).toList
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

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
