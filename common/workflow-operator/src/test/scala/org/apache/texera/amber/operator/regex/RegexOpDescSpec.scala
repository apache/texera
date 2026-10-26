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

package org.apache.texera.amber.operator.regex

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

class RegexOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, re: String, caseInsensitive: Boolean): RegexOpDesc = {
    val d = new RegexOpDesc
    d.attribute = attr
    d.regex = re
    d.caseInsensitive = caseInsensitive
    d
  }

  "RegexOpDesc.operatorInfo" should
    "advertise the name, Search group, and reconfiguration support" in {
    val info = (new RegexOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Regular Expression"
    info.operatorDescription.toLowerCase should include("regular expression")
    info.operatorGroupName shouldBe OperatorGroupConstants.SEARCH_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.supportReconfiguration shouldBe true
  }

  "RegexOpDesc.getPhysicalOp" should "wire the RegexOpExec class name and carry ports" in {
    val op = newDesc("col", ".*foo.*", caseInsensitive = false)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.regex.RegexOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    // Pin the actual port identities (not just counts).
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "RegexOpDesc JSON round-trip" should
    "preserve attribute, regex, and caseInsensitive via the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("url", "^https?://", caseInsensitive = true))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[RegexOpDesc]
    val re = restored.asInstanceOf[RegexOpDesc]
    re.attribute shouldBe "url"
    re.regex shouldBe "^https?://"
    re.caseInsensitive shouldBe true
  }

  "RegexOpDesc.unicodeBoundaries" should
    "scope \\b and \\B to Unicode outside a class and leave the rest alone" in {
    RegexOpDesc.unicodeBoundaries("""\bcafé\B""") shouldBe """(?u:\b)café(?u:\B)"""
    RegexOpDesc.unicodeBoundaries("""\\b""") shouldBe """\\b"""
    RegexOpDesc.unicodeBoundaries("""[\b]\b""") shouldBe """[\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""[]\b]\b""") shouldBe """[]\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""[^]\b]\b""") shouldBe """[^]\b](?u:\b)"""
    RegexOpDesc.unicodeBoundaries("""\[\b""") shouldBe """\[(?u:\b)"""
  }

  "RegexOpDesc.unicodeCase" should
    "scope a leading u around the rest and keep each shorthand ASCII" in {
    RegexOpDesc.unicodeCase("""\w[é\d]""") shouldBe """\w[é\d]"""
    RegexOpDesc.unicodeCase("(?iu)é") shouldBe "(?i)(?u:é)"
    RegexOpDesc.unicodeCase("""(?u)\w[\s][é\d][^\W][^é\S][\w^]""") shouldBe
      """(?u:(?a:\w)(?a:[\s])(?:[é]|(?a:[\d]))(?a:[^\W])(?:(?![é])(?a:[^\S]))(?:[\^]|(?a:[\w])))"""
    RegexOpDesc.unicodeCase("""x(?iu:é)\d\\w""") shouldBe """x(?iu:é)(?a:\d)\\w"""
    RegexOpDesc.unicodeCase("(?iux)é # note") shouldBe "(?ix)(?u:é # note\n)"
  }

  // Pattern without UNICODE_CASE ignores case for ASCII letters only, and its
  // \w, \d and \s are ASCII, while Java 17's \b takes any letter or digit as
  // part of a word. Each pattern runs through RegexOpExec and through the
  // export, and the rows each keeps are named by their input position.
  "RegexOpDesc.generateStandaloneCode" should
    "keep the rows the engine keeps on non-ASCII text" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val cases = Seq(
      ("é", true, Seq("É", "é", "e")),
      ("σ", true, Seq("Σ", "σ", "ς")),
      ("k", true, Seq("K", "k", "\u212a")),
      ("ǅ", true, Seq("ǆ", "Ǆ", "ǅ")),
      ("""\w+""", false, Seq("é", "e", "\u0663", "_")),
      ("""\d""", false, Seq("\u0663", "3", "x")),
      ("""\s""", false, Seq("\u00a0", " ", "\u2003")),
      ("""^\w$""", false, Seq("ß", "s")),
      ("""\bcafé\b""", false, Seq("cafés", "café", "un café noir")),
      ("""\Bé""", false, Seq("café", "é")),
      ("""\\b""", false, Seq("""\b""", "b")),
      // An inline u folds case by Unicode and leaves \w, \d and \s ASCII.
      ("(?iu)é", false, Seq("é", "É", "x", null)),
      ("(?u)é", true, Seq("É", "é", "x")),
      ("""(?iu)\w""", false, Seq("é", "e", "\u212a")),
      ("""(?iu)^[é\w]+$""", false, Seq("Éa", "e", "!")),
      ("""(?iu)[^é\W]""", false, Seq("É", "é", "e")),
      ("""x(?iu:é)\d""", false, Seq("xÉ3", "xÉ\u0663", "XÉ3")),
      ("(?iux)é # note", false, Seq("É", "x"))
    )
    val schema = Schema().add("v", AttributeType.STRING).add("i", AttributeType.INTEGER)
    val runs = cases.map {
      case (pattern, caseInsensitive, values) =>
        val desc = newDesc("v", pattern, caseInsensitive)
        val exec = new RegexOpExec(objectMapper.writeValueAsString(desc))
        exec.open()
        val kept = values.zipWithIndex.flatMap {
          case (v, i) => exec.processTuple(Tuple(schema, Array[Any](v, i)), 0)
        }
        exec.close()
        val label = s"'$pattern' caseInsensitive=$caseInsensitive"
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

    // The values arrive as JSON arrays, so a missing one reads as null.
    val driver = (Seq("import pandas as pd", "null = None") ++ runs.map(_._2)).mkString("\n")
    val file = Files.createTempFile("regex-engine-parity-", ".py")
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
