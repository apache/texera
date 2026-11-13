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

package org.apache.texera.amber.operator.set.symmetricDifference

import org.apache.texera.amber.util.PythonTestSupport.{
  NeedsPythonPackages,
  canImport,
  configuredPython
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.{HashPartition, PortIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.testsupport.setop.SetRows
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source

class SymmetricDifferenceOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val schemaA = Schema().add(new Attribute("a", AttributeType.STRING))
  private val schemaDifferent = Schema().add(new Attribute("b", AttributeType.INTEGER))

  "SymmetricDifferenceOpDesc.operatorInfo" should
    "advertise the name, Set group, two inputs, and a single blocking output" in {
    val info = (new SymmetricDifferenceOpDesc).operatorInfo
    info.userFriendlyName shouldBe "SymmetricDifference"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.inputPorts.map(_.id) shouldBe List(PortIdentity(0), PortIdentity(1))
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "SymmetricDifferenceOpDesc.getPhysicalOp" should
    "wire SymmetricDifferenceOpExec, carry port identities, and require HashPartition on both inputs" in {
    val op = new SymmetricDifferenceOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.set.symmetricDifference.SymmetricDifferenceOpExec"
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    physical.partitionRequirement shouldBe List(Option(HashPartition()), Option(HashPartition()))
  }

  "SymmetricDifferenceOpDesc schema propagation" should
    "pass the shared input schema through to the output port" in {
    val op = new SymmetricDifferenceOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    val inputs = Map(PortIdentity(0) -> schemaA, PortIdentity(1) -> schemaA)
    val outputs = physical.propagateSchema.func(inputs)
    outputs.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    outputs.values.toSet shouldBe Set(schemaA)
  }

  it should "reject inputs whose schemas differ" in {
    val physical = (new SymmetricDifferenceOpDesc).getPhysicalOp(workflowId, executionId)
    val mismatched = Map(PortIdentity(0) -> schemaA, PortIdentity(1) -> schemaDifferent)
    intercept[IllegalArgumentException] {
      physical.propagateSchema.func(mismatched)
    }
  }

  "SymmetricDifferenceOpDesc" should
    "round-trip through the polymorphic base (pins the SymmetricDifference discriminator)" in {
    // The operator has no config fields, so this pins the @JsonSubTypes
    // discriminator + type resolution, on par with the other specs in this PR.
    val json = objectMapper.writeValueAsString(new SymmetricDifferenceOpDesc)
    objectMapper.readValue(json, classOf[LogicalOp]) shouldBe a[SymmetricDifferenceOpDesc]
  }

  // A missing value and a stored NaN are two values to the engine's HashSet, so
  // a left holding both against a right holding only the missing one leaves the
  // NaN row alone in exactly one input. They are only distinct in a nullable
  // dtype, which is what an Arrow file is read into.
  "SymmetricDifferenceOpDesc.generateStandaloneCode" should
    "keep the NaN row that a null row does not cancel" taggedAs NeedsPythonPackages in {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas", "numpy")) cancel(s"'$python' cannot import pandas")

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |def col(vals, mask):
         |    return pd.arrays.FloatingArray(np.array(vals), np.array(mask))
         |
         |in1df = pd.DataFrame({"v": col([0.0, np.nan], [True, False])})
         |in2df = pd.DataFrame({"v": col([0.0], [True])})
         |${exported()}
         |print(len(out1df), [("NULL" if x is pd.NA else repr(float(x))) for x in out1df["v"]])
         |""".stripMargin

    val script = Files.createTempFile("symmetric-difference-null-nan-", ".py")
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

  // The engine's HashSet compares rows with Scala's ==, under which NaN equals
  // nothing, so every NaN row is in exactly one input, and hashes them by each
  // double's bits, so 0.0 and -0.0 are each in one. A finite value and a null
  // on both sides are the controls.
  it should "match rows by the engine's equality for NaN and signed zero" taggedAs NeedsPythonPackages in {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas", "numpy")) cancel(s"'$python' cannot import pandas")

    val exec = new SymmetricDifferenceOpExec
    exec.open()
    SetRows.left.foreach(t => exec.processTuple(t, 0))
    SetRows.right.foreach(t => exec.processTuple(t, 1))
    exec.onFinish(0)
    val engine = SetRows.rendered(exec.onFinish(1))

    val out = SetRows.runExport(python, exported(), "symmetric-difference-nan-zero-")
    withClue(s"script:\n${exported()}")(out shouldBe engine)
  }

  private def exported(): String = {
    val desc = new SymmetricDifferenceOpDesc
    (desc.standaloneHelpers() :+ desc.generateStandaloneCode()).mkString("\n")
  }
}
