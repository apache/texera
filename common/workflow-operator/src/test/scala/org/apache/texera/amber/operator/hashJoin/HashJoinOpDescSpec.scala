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

  // A key that is missing and a key holding a NaN are two different keys to the
  // engine, so only the missing one matches a missing right key. The two are
  // only distinct in a nullable dtype, which is what an Arrow file is read into.
  "HashJoinOpDesc.generateStandaloneCode" should "match a missing key but not a NaN one" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "k"
    d.probeAttributeName = "k"
    d.joinType = JoinType.LEFT_OUTER

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |def col(vals, mask):
         |    return pd.arrays.FloatingArray(np.array(vals), np.array(mask))
         |
         |in1df = pd.DataFrame({"k": col([0.0, np.nan], [True, False]), "lv": [1, 2]})
         |in2df = pd.DataFrame({"k": col([0.0], [True]), "rv": [9]})
         |${d.generateStandaloneCode()}
         |print(len(out1df), int(out1df["rv"].notna().sum()))
         |""".stripMargin

    val script = Files.createTempFile("hashjoin-null-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      // Both left rows come back, and exactly one of them found a partner.
      out.trim shouldBe "2 1"
    }
  }

  // Without a schema nothing says which columns were integers.
  it should "leave the widening alone when no schema is given" in {
    val d = new HashJoinOpDesc[Integer]
    d.buildAttributeName = "k"
    d.probeAttributeName = "kk"
    d.joinType = JoinType.FULL_OUTER
    d.generateStandaloneCode() should not include "astype("
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
      new ProcessBuilder(python, "-c", "import pandas, numpy").redirectErrorStream(true).start()
    ).toOption
      .exists { p =>
        if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
}
