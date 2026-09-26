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

package org.apache.texera.amber.operator.intervalJoin

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, PortIdentity}
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

class IntervalJoinOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // Set the join keys + a concrete (non-null) Option for timeIntervalType before any
  // path that serializes `this` (getPhysicalOp / round-trip).
  private def configured(): IntervalJoinOpDesc = {
    val d = new IntervalJoinOpDesc
    d.leftAttributeName = "lk"
    d.rightAttributeName = "rk"
    d.timeIntervalType = None
    d
  }

  "IntervalJoinOpDesc.operatorInfo" should
    "advertise two ordered inputs (left then right) in the Join group" in {
    val info = (new IntervalJoinOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Interval Join"
    info.operatorGroupName shouldBe OperatorGroupConstants.JOIN_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.id shouldBe PortIdentity()
    info.inputPorts.head.displayName shouldBe "left table"
    info.inputPorts.last.id shouldBe PortIdentity(1)
    info.inputPorts.last.displayName shouldBe "right table"
    info.inputPorts.last.dependencies shouldBe List(PortIdentity(0))
    info.outputPorts should have length 1
  }

  "IntervalJoinOpDesc" should
    "default the join-key attributes to null and the bounds/constant to their defaults" in {
    val d = new IntervalJoinOpDesc
    d.leftAttributeName shouldBe null
    d.rightAttributeName shouldBe null
    d.constant shouldBe 10L
    d.includeLeftBound shouldBe true
    d.includeRightBound shouldBe true
  }

  "IntervalJoinOpDesc.getPhysicalOp" should
    "wire IntervalJoinOpExec, carry port identities, and require HashPartition on each join key" in {
    val op = configured()
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.intervalJoin.IntervalJoinOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    physical.partitionRequirement shouldBe List(
      Option(HashPartition(List("lk"))),
      Option(HashPartition(List("rk")))
    )
  }

  "IntervalJoinOpDesc schema propagation" should
    "merge the left and right schemas, suffixing a conflicting attribute with #@1" in {
    val op = configured()
    val physical = op.getPhysicalOp(workflowId, executionId)
    val leftSchema = Schema()
      .add(new Attribute("a", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val rightSchema = Schema()
      .add(new Attribute("b", AttributeType.STRING))
      .add(new Attribute("k", AttributeType.LONG))
    val out = physical.propagateSchema.func(
      Map(PortIdentity() -> leftSchema, PortIdentity(1) -> rightSchema)
    )
    out.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    out(op.operatorInfo.outputPorts.head.id).getAttributes.map(_.getName) shouldBe
      List("a", "k", "b", "k#@1")
  }

  "IntervalJoinOpDesc.generateStandaloneCode" should
    "read the right key under the name the merge left it with" in {
    // "rk" is only suffixed when the left frame has a column of that name, and
    // the frame is not known until the script runs, so the choice is in Python.
    configured().generateStandaloneCode() should include(
      """_iv_r = _pairs["rk" if "rk" not in in1df.columns else "rk" + "#@1"]"""
    )
  }

  /** The bug this pins: the keys used to be copied into `_iv_l` and `_iv_r`
    * columns, so an input column already carrying one of those names was
    * overwritten and then dropped. Run the generated pandas and hold its
    * columns against the schema the operator promises for the same two inputs.
    */
  it should "keep an input column that carries a temporary's name" in {
    val python = resolvePython().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val left = Schema()
      .add(new Attribute("x", AttributeType.INTEGER))
      .add(new Attribute("_iv_l", AttributeType.STRING))
    val right = Schema().add(new Attribute("r", AttributeType.INTEGER))

    val d = new IntervalJoinOpDesc
    d.leftAttributeName = "x"
    d.rightAttributeName = "r"
    d.constant = 3L
    d.timeIntervalType = None
    val promised = d
      .getPhysicalOp(workflowId, executionId)
      .propagateSchema
      .func(Map(PortIdentity() -> left, PortIdentity(1) -> right))(
        d.operatorInfo.outputPorts.head.id
      )
      .getAttributeNames

    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"x": [2], "_iv_l": ["payload"]})
         |in2df = pd.DataFrame({"r": [1]})
         |${d.generateStandaloneCode()}
         |print(",".join(map(str, out1df.columns)))
         |print(",".join(map(str, out1df.iloc[0].tolist())))
         |""".stripMargin

    val script = Files.createTempFile("intervaljoin-columns-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      lines.head.split(",").toList shouldBe promised
      // The payload the temporary used to overwrite, and the row still joins.
      lines(1) shouldBe "2,payload,1"
    }
  }

  "IntervalJoinOpDesc" should "round-trip its fields through the polymorphic base" in {
    val d = configured()
    d.constant = 42L
    d.includeLeftBound = false
    d.includeRightBound = false
    val restored = objectMapper.readValue(objectMapper.writeValueAsString(d), classOf[LogicalOp])
    restored shouldBe a[IntervalJoinOpDesc]
    val ij = restored.asInstanceOf[IntervalJoinOpDesc]
    ij.leftAttributeName shouldBe "lk"
    ij.rightAttributeName shouldBe "rk"
    ij.constant shouldBe 42L
    ij.includeLeftBound shouldBe false
    ij.includeRightBound shouldBe false
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
