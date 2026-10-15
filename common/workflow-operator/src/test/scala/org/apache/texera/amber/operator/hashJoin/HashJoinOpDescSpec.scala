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
import org.apache.texera.amber.core.tuple.{
  Attribute,
  AttributeType,
  Schema,
  SchemaEnforceable,
  Tuple
}
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
import scala.io.Source
import scala.util.Try

class HashJoinOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

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
  "HashJoinOpDesc.generateStandaloneCode" should "match a missing key but not a NaN one" taggedAs NeedsPythonPackages in {
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

  // The probe looks each key up in a HashMap under Scala's ==: a NaN finds
  // nothing, not even another NaN, while 0.0 meets -0.0 and a null meets a
  // null. Each join type keeps or drops the unmatched NaN rows by its own rule.
  for (joinType <- JoinType.values) {
    it should s"pair NaN, signed-zero and null keys as the engine does in a $joinType join" taggedAs NeedsPythonPackages in {
      val python = resolvePython().getOrElse(cancel("No runnable python executable"))
      if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

      val d = new HashJoinOpDesc[java.lang.Double]
      d.buildAttributeName = "k"
      d.probeAttributeName = "k2"
      d.joinType = joinType
      val left = Schema().add("k", AttributeType.DOUBLE).add("lv", AttributeType.STRING)
      val right = Schema().add("k2", AttributeType.DOUBLE).add("rv", AttributeType.STRING)
      val leftRows: Seq[(java.lang.Double, String)] =
        Seq((Double.NaN, "L-nan"), (0.0, "L+0"), (1.0, "L1"), (null, "L-null"))
      val rightRows: Seq[(java.lang.Double, String)] =
        Seq((Double.NaN, "R-nan"), (-0.0, "R-0"), (1.0, "R1"), (null, "R-null"))

      val outSchema = d
        .getExternalOutputSchemas(Map(PortIdentity() -> left, PortIdentity(1) -> right))
        .values
        .head
      val build = new HashJoinBuildOpExec[java.lang.Double](objectMapper.writeValueAsString(d))
      build.open()
      leftRows.foreach { case (k, v) => build.processTuple(Tuple(left, Array[Any](k, v)), 0) }
      val probe = new HashJoinProbeOpExec[java.lang.Double](objectMapper.writeValueAsString(d))
      probe.open()
      val internal =
        Schema().add(HashJoinOpDesc.HASH_JOIN_INTERNAL_KEY_NAME, AttributeType.ANY).add(left)
      build
        .onFinish(0)
        .foreach(t =>
          probe.processTuple(t.asInstanceOf[SchemaEnforceable].enforceSchema(internal), 0)
        )
      probe.onFinish(0)
      val probed = rightRows.flatMap {
        case (k, v) => probe.processTuple(Tuple(right, Array[Any](k, v)), 1)
      } ++ probe.onFinish(1)
      val engine = probed
        .map(_.asInstanceOf[SchemaEnforceable].enforceSchema(outSchema).getFields.map {
          case null                           => "None"
          case d: java.lang.Double if d.isNaN => "nan"
          case other                          => other.toString
        })
        .map(_.mkString(" "))
        .toList
        .sorted

      def frame(rows: Seq[(java.lang.Double, String)], key: String, value: String): String = {
        val keys =
          rows.map(_._1).map(k => if (k == null) "None" else if (k.isNaN) "np.nan" else k.toString)
        val values = rows.map(r => s""""${r._2}"""")
        s"""pd.DataFrame({
           |    "$key": col([${keys.mkString(", ")}]),
           |    "$value": [${values.mkString(", ")}],
           |})""".stripMargin
      }
      val driver =
        s"""import pandas as pd, numpy as np
           |
           |def col(vals):
           |    return pd.arrays.FloatingArray(
           |        np.array([0.0 if v is None else v for v in vals]),
           |        np.array([v is None for v in vals]),
           |    )
           |
           |in1df = ${frame(leftRows, "k", "lv")}
           |in2df = ${frame(rightRows, "k2", "rv")}
           |${d.generateStandaloneCode(Map(PortIdentity() -> left, PortIdentity(1) -> right))}
           |def show(v):
           |    if isinstance(v, float):
           |        return "nan" if v != v else repr(float(v))
           |    return str(v)
           |rows = []
           |for values, missing in zip(out1df.itertuples(index=False), out1df.isna().to_numpy()):
           |    rows.append(" ".join("None" if m else show(v) for v, m in zip(values, missing)))
           |print("\\n".join(sorted(rows)))
           |""".stripMargin

      val script = Files.createTempFile("hashjoin-nan-zero-", ".py")
      script.toFile.deleteOnExit()
      Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
      val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
      val out = Source.fromInputStream(process.getInputStream).mkString
      process.waitFor(120, TimeUnit.SECONDS)
      withClue(s"python said:\n$out\nscript:\n$driver") {
        process.exitValue() shouldBe 0
        out.trim.linesIterator.toList shouldBe engine
      }
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
