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

package org.apache.texera.amber.operator.distinct

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, SinglePartition, UnknownPartition}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.operator.tags.IntegrationTest
import org.scalatest.Tag
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class DistinctOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  // ---------------------------------------------------------------------------
  // operatorInfo — descriptor metadata
  // ---------------------------------------------------------------------------

  "DistinctOpDesc.operatorInfo" should
    "advertise the user-friendly name and Cleaning group" in {
    val info = (new DistinctOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Distinct"
    info.operatorGroupName shouldBe OperatorGroupConstants.CLEANING_GROUP
    info.operatorDescription.toLowerCase should include("duplicate")
  }

  it should "expose one input port and one blocking output port" in {
    val info = (new DistinctOpDesc).operatorInfo
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring to DistinctOpExec + partitioning contract
  // ---------------------------------------------------------------------------

  "DistinctOpDesc.getPhysicalOp" should
    "wire the DistinctOpExec class name into the OpExecInitInfo" in {
    val op = new DistinctOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.distinct.DistinctOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "require HashPartition on the single input port" in {
    val physical = (new DistinctOpDesc).getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(Option(HashPartition()))
  }

  it should "always derive HashPartition for the output regardless of input partitions" in {
    // Distinct's dedup semantics depend on hash-alignment, so the
    // derived output partition stays hash even when upstream inputs
    // report differing partition kinds.
    val physical = (new DistinctOpDesc).getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition())) shouldBe HashPartition()
    physical.derivePartition(List(UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(List(HashPartition(List("col-a")))) shouldBe HashPartition()
  }

  it should "preserve the one input / one blocking output port shape from operatorInfo" in {
    val op = new DistinctOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.inputPorts should have size 1
    physical.outputPorts should have size 1
    // PhysicalOp.outputPorts is a Map[PortIdentity, (OutputPort, …, …)],
    // so the blocking flag is on the first tuple element of the value.
    val (outputPort, _, _) = physical.outputPorts.values.head
    outputPort.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "DistinctOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    // `LogicalOp` initializes `operatorId` from `UUID.randomUUID()` in
    // its constructor body, so two `new DistinctOpDesc` allocations
    // must hold different identifiers. A regression to a static /
    // shared id would surface here as the two ids being equal.
    val a = new DistinctOpDesc
    val b = new DistinctOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }

  // The engine's LinkedHashSet compares rows with Scala's ==, under which NaN
  // equals nothing, and hashes them by each double's bits, which tell 0.0 from
  // -0.0. A repeated finite value and a repeated null are the controls, and the
  // rows are compared in order, since the first of each is the one kept.
  "DistinctOpDesc.generateStandaloneCode" should
    "keep the rows the engine keeps for NaN and signed zero" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val values: Seq[java.lang.Double] =
      Seq(1.0, Double.NaN, 0.0, -0.0, null, 1.0, Double.NaN, null, -0.0)
    val schema = Schema().add("v", AttributeType.DOUBLE)

    val exec = new DistinctOpExec
    exec.open()
    values.foreach(v => exec.processTuple(Tuple(schema, Array[Any](v)), 0))
    val engine = exec
      .onFinish(0)
      .map(_.getFields.head match {
        case null                           => "None"
        case d: java.lang.Double if d.isNaN => "nan"
        case d                              => d.toString
      })
      .toList
    exec.close()

    val desc = new DistinctOpDesc
    val pyValues =
      values.map(v => if (v == null) "None" else if (v.isNaN) "np.nan" else v.toString)
    val driver =
      s"""import pandas as pd, numpy as np
         |vals = [${pyValues.mkString(", ")}]
         |mask = np.array([v is None for v in vals])
         |in1df = pd.DataFrame({"v": pd.arrays.FloatingArray(
         |    np.array([0.0 if v is None else v for v in vals]), mask)})
         |${(desc.standaloneHelpers() :+ desc.generateStandaloneCode()).mkString("\n")}
         |for v in out1df["v"]:
         |    print("None" if v is pd.NA else repr(float(v)))
         |""".stripMargin
    val script = Files.createTempFile("distinct-nan-zero-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream, "UTF-8").mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.linesIterator.toList shouldBe engine
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
      new ProcessBuilder(python, "-c", "import pandas, numpy").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
