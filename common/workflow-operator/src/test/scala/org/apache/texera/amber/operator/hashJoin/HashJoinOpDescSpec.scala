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

package org.apache.texera.amber.operator.hashJoin

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
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class HashJoinOpDescSpec extends AnyFlatSpec with Matchers {

  private def leftRight(): (Schema, Schema) =
    (
      Schema()
        .add(new Attribute("a", AttributeType.STRING))
        .add(new Attribute("k", AttributeType.LONG)),
      Schema()
        .add(new Attribute("b", AttributeType.STRING))
        .add(new Attribute("k", AttributeType.LONG))
    )

  "HashJoinOpDesc.operatorInfo" should
    "advertise the Hash Join in the Join group with a left/right 2-in 1-out shape" in {
    val info = (new HashJoinOpDesc[String]).operatorInfo
    info.userFriendlyName shouldBe "Hash Join"
    info.operatorDescription shouldBe "join two inputs"
    info.operatorGroupName shouldBe OperatorGroupConstants.JOIN_GROUP
    info.inputPorts.map(_.displayName) shouldBe List("left", "right")
    info.outputPorts should have length 1
  }

  "HashJoinOpDesc" should "default the join keys to null and the join type to inner" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName shouldBe null
    d.probeAttributeName shouldBe null
    d.joinType shouldBe JoinType.INNER
  }

  "HashJoinOpDesc.getExternalOutputSchemas" should
    "drop the probe key and keep the build key when join columns share a name" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName = "k"
    d.probeAttributeName = "k"
    val (left, right) = leftRight()
    val out = d.getExternalOutputSchemas(Map(PortIdentity() -> left, PortIdentity(1) -> right))
    out(d.operatorInfo.outputPorts.head.id).getAttributeNames shouldBe List("a", "k", "b")
  }

  it should "rename a retained right-side column that clashes with a left-side name" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName = "k"
    d.probeAttributeName = "b"
    val (left, right) = leftRight()
    val out = d.getExternalOutputSchemas(Map(PortIdentity() -> left, PortIdentity(1) -> right))
    out(d.operatorInfo.outputPorts.head.id).getAttributeNames shouldBe List("a", "k", "k#@1")
  }

  "HashJoinOpDesc.generateStandaloneCode" should
    "drop the probe key under the name the merge left it with" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName = "id"
    d.probeAttributeName = "key"
    // "key" is renamed only when the left frame has a column of that name, and
    // the frame is not known until the script runs, so the name the rename
    // settled on is what gets dropped.
    d.generateStandaloneCode() should include("""_probe_key = _rename.get("key", "key")""")
    d.generateStandaloneCode() should include("drop(columns=[_probe_key])")
  }

  it should "keep the shared key when both sides name it the same" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName = "k"
    d.probeAttributeName = "k"
    d.generateStandaloneCode() should not include "drop(columns="
  }

  /** The bug this pins: with a left `key` payload and a right `key` join column,
    * dropping the bare `key` took the payload and kept the right key. Run the
    * generated pandas and hold its columns against the schema the operator
    * itself promises for the same two inputs.
    */
  it should "return the columns getExternalOutputSchemas promises" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val left = Schema()
      .add(new Attribute("id", AttributeType.INTEGER))
      .add(new Attribute("key", AttributeType.STRING))
    val right = Schema()
      .add(new Attribute("key", AttributeType.INTEGER))
      .add(new Attribute("value", AttributeType.INTEGER))

    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "id"
    d.probeAttributeName = "key"
    val promised = d
      .getExternalOutputSchemas(Map(PortIdentity() -> left, PortIdentity(1) -> right))(
        d.operatorInfo.outputPorts.head.id
      )
      .getAttributeNames

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"id": [1], "key": ["payload"]})
         |in2df = pd.DataFrame({"key": [1], "value": [9]})
         |${d.generateStandaloneCode()}
         |print(",".join(map(str, out1df.columns)))
         |print(",".join(map(str, out1df.iloc[0].tolist())))
         |""".stripMargin

    val script = Files.createTempFile("hashjoin-columns-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      lines.head.split(",").toList shouldBe promised
      // The payload the drop used to take, not the right key it used to keep.
      lines(1) shouldBe "1,payload,9"
    }
  }

  // A right column meeting an already-suffixed one lands on "#@1#@1". pandas'
  // `suffixes` refused the duplicate it made and ended the run.
  it should "rename a right column that collides twice, the way the engine does" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "k"
    d.probeAttributeName = "k"

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"k": [1], "x": [10]})
         |in2df = pd.DataFrame({"k": [1], "x": [20], "x#@1": [30]})
         |${d.generateStandaloneCode()}
         |print(",".join(map(str, out1df.columns)))
         |""".stripMargin

    val script = Files.createTempFile("hashjoin-chained-rename-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      // Left names first and untouched, then the right ones in their own order:
      // "k" and "x" each lost a collision, and the right's own "x#@1" was free.
      out.trim shouldBe "k,x,k#@1,x#@1#@1,x#@1"
    }
  }

  // A hole costs a pandas integer column its type; the engine keeps INTEGER.
  it should "keep a declared integer column integral across an outer join" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val left = Schema()
      .add(new Attribute("k", AttributeType.INTEGER))
      .add(new Attribute("x", AttributeType.INTEGER))
    val right = Schema()
      .add(new Attribute("kk", AttributeType.INTEGER))
      .add(new Attribute("y", AttributeType.INTEGER))

    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "k"
    d.probeAttributeName = "kk"
    d.joinType = JoinType.FULL_OUTER
    val block =
      d.generateStandaloneCode(Map(PortIdentity() -> left, PortIdentity(1) -> right))

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"k": [1, 2], "x": [10, 20]})
         |in2df = pd.DataFrame({"kk": [1, 3], "y": [30, 40]})
         |$block
         |print(",".join(f"{c}:{out1df[c].dtype}" for c in out1df.columns))
         |""".stripMargin

    val script = Files.createTempFile("hashjoin-outer-int-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "k:Int64,x:Int64,y:Int64"
    }
  }

  // Without a schema nothing says which columns were integers.
  it should "leave the widening alone when no schema is given" in {
    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "k"
    d.probeAttributeName = "kk"
    d.joinType = JoinType.FULL_OUTER
    d.generateStandaloneCode() should not include "astype(\"Int64\")"
  }

  "HashJoinOpDesc" should "round-trip its config fields through the polymorphic base" in {
    val d = new HashJoinOpDesc[String]
    d.buildAttributeName = "lk"
    d.probeAttributeName = "rk"
    d.joinType = JoinType.LEFT_OUTER
    val json = objectMapper.writeValueAsString(d)
    json should include("\"operatorType\":\"HashJoin\"")
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[HashJoinOpDesc[_]]
    val r = restored.asInstanceOf[HashJoinOpDesc[String]]
    r.buildAttributeName shouldBe "lk"
    r.probeAttributeName shouldBe "rk"
    r.joinType shouldBe JoinType.LEFT_OUTER
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
