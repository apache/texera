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

package org.apache.texera.amber.operator.difference

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{
  HashPartition,
  PortIdentity,
  SinglePartition,
  UnknownPartition
}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class DifferenceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val schemaA: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))
  private val schemaB: Schema =
    Schema().add(new Attribute("col", AttributeType.STRING))
  private val schemaDifferent: Schema =
    Schema().add(new Attribute("other", AttributeType.INTEGER))

  // ---------------------------------------------------------------------------
  // operatorInfo
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc.operatorInfo" should "advertise the Set group + difference description" in {
    val info = (new DifferenceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Difference"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.operatorDescription.toLowerCase should include("difference")
  }

  it should
    "expose two input ports (left at PortIdentity 0, right at PortIdentity 1) and one blocking output" in {
    val info = (new DifferenceOpDesc).operatorInfo
    info.inputPorts should have length 2
    info.inputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1))
    info.inputPorts.map(_.displayName) shouldBe List("left", "right")
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  // ---------------------------------------------------------------------------
  // getPhysicalOp — wiring + partitioning + schema propagation
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc.getPhysicalOp" should
    "wire the DifferenceOpExec class name into the OpExecInitInfo" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.difference.DifferenceOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  it should "require HashPartition on BOTH input ports" in {
    // Set-difference semantics require both inputs to be hash-aligned so
    // matching keys can be compared on the same worker.
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(
      Option(HashPartition()),
      Option(HashPartition())
    )
  }

  it should "derive HashPartition for the output regardless of input partition kinds" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition(), UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(
      List(HashPartition(List("a")), HashPartition(List("b")))
    ) shouldBe HashPartition()
  }

  // ---------------------------------------------------------------------------
  // Schema propagation
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc schema propagation" should
    "produce a single output schema equal to the (shared) input schema" in {
    // When both inputs report the same schema, propagation succeeds and
    // every output port receives that schema.
    val op = new DifferenceOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    val propagateFn = physical.propagateSchema
    val inputs = Map(PortIdentity() -> schemaA, PortIdentity(1) -> schemaB)
    val outputs = propagateFn.func(inputs)
    outputs.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    outputs.values.toSet shouldBe Set(schemaA)
  }

  it should
    "throw IllegalArgumentException when the two inputs do not share one schema" in {
    val physical = (new DifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    val propagateFn = physical.propagateSchema
    val mismatched =
      Map(PortIdentity() -> schemaA, PortIdentity(1) -> schemaDifferent)
    intercept[IllegalArgumentException] {
      propagateFn.func(mismatched)
    }
  }

  // ---------------------------------------------------------------------------
  // Independent instances
  // ---------------------------------------------------------------------------

  "DifferenceOpDesc" should
    "assign a fresh operatorIdentifier per instance (UUID-based id is not shared)" in {
    // `LogicalOp` initializes `operatorId` from `UUID.randomUUID()` in
    // its constructor body, so two `new DifferenceOpDesc` allocations
    // must hold different identifiers. A regression to a static /
    // shared id would surface here as the two ids being equal.
    val a = new DifferenceOpDesc
    val b = new DifferenceOpDesc
    a.operatorIdentifier should not equal b.operatorIdentifier
  }

  // A missing value and a stored NaN are two different values to the engine, so
  // a left holding both against a right holding only the missing one keeps the
  // NaN row. The two are only distinct in a nullable dtype, which is what an
  // Arrow file is read into.
  "DifferenceOpDesc.generateStandaloneCode" should
    "keep the NaN row that a null row does not cancel" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |def col(vals, mask):
         |    return pd.arrays.FloatingArray(np.array(vals), np.array(mask))
         |
         |in1df = pd.DataFrame({"v": col([0.0, np.nan], [True, False])})
         |in2df = pd.DataFrame({"v": col([0.0], [True])})
         |${(new DifferenceOpDesc).generateStandaloneCode()}
         |print(len(out1df), [("NULL" if x is pd.NA else repr(float(x))) for x in out1df["v"]])
         |""".stripMargin

    val script = Files.createTempFile("difference-null-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "1 ['nan']"
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
