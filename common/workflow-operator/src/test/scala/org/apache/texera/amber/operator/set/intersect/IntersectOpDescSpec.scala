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

package org.apache.texera.amber.operator.set.intersect

import org.apache.texera.amber.util.PythonTestSupport.{
  NeedsPythonPackages,
  canImport,
  configuredPython
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.tuple.{AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.workflow.{
  HashPartition,
  PortIdentity,
  SinglePartition,
  UnknownPartition
}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.testsupport.setop.SetRows
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source

class IntersectOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "IntersectOpDesc.operatorInfo" should "advertise the user-friendly name and Set group" in {
    val info = (new IntersectOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Intersect"
    info.operatorGroupName shouldBe OperatorGroupConstants.SET_GROUP
    info.operatorDescription should include("intersect")
  }

  it should "expose two input ports (PortIdentity 0 and 1) and one blocking output" in {
    val info = (new IntersectOpDesc).operatorInfo
    info.inputPorts should have length 2
    info.inputPorts.map(_.id.id) shouldBe List(0, 1)
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "IntersectOpDesc.getPhysicalOp" should "require HashPartition on both input ports" in {
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.partitionRequirement shouldBe List(
      Option(HashPartition()),
      Option(HashPartition())
    )
  }

  it should "always derive HashPartition for the output regardless of input partitions" in {
    // The Intersect set semantics require both inputs to be hash-aligned, so
    // the derived output partition must remain hash even when the upstream
    // inputs report differing partition kinds.
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.derivePartition(List(SinglePartition(), UnknownPartition())) shouldBe HashPartition()
    physical.derivePartition(
      List(HashPartition(List("a")), HashPartition(List("b")))
    ) shouldBe HashPartition()
  }

  it should "wire the IntersectOpExec class name into the OpExecInitInfo" in {
    // Pattern-match on OpExecWithClassName instead of substring-matching the
    // toString output, which is brittle to scalapb formatting changes.
    val op = new IntersectOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, _) =>
        className shouldBe "org.apache.texera.amber.operator.set.intersect.IntersectOpExec"
      case other =>
        fail(s"expected OpExecWithClassName, got $other")
    }
  }

  // A missing value and a stored NaN are two values to the engine's HashSet, so
  // a left holding both against a right holding only the missing one keeps the
  // missing row alone. They are only distinct in a nullable dtype, which is what
  // an Arrow file is read into.
  "IntersectOpDesc.generateStandaloneCode" should
    "keep the null row a null row matches and not the NaN row" taggedAs NeedsPythonPackages in {
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

    val script = Files.createTempFile("intersect-null-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "1 ['NULL']"
    }
  }

  // The engine's HashSet compares rows with Scala's ==, under which NaN equals
  // nothing, and hashes them by each double's bits, which tell 0.0 from -0.0.
  // A finite value and a null on both sides are the controls.
  it should "match rows by the engine's equality for NaN and signed zero" taggedAs NeedsPythonPackages in {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas", "numpy")) cancel(s"'$python' cannot import pandas")

    val exec = new IntersectOpExec
    exec.open()
    SetRows.left.foreach(t => exec.processTuple(t, 0))
    SetRows.right.foreach(t => exec.processTuple(t, 1))
    exec.onFinish(0)
    val engine = SetRows.rendered(exec.onFinish(1))

    val out = SetRows.runExport(python, exported(), "intersect-nan-zero-")
    withClue(s"script:\n${exported()}")(out shouldBe engine)
  }

  // The engine's Tuple equality includes the schema, so x:DOUBLE=1 on the left and
  // y:DOUBLE=1 on the right share no row, though their values are equal.
  it should "keep no row when the two inputs are declared differently" taggedAs NeedsPythonPackages in {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas", "numpy")) cancel(s"'$python' cannot import pandas")

    val leftSchema = Schema().add("x", AttributeType.DOUBLE)
    val rightSchema = Schema().add("y", AttributeType.DOUBLE)
    val exec = new IntersectOpExec
    exec.open()
    exec.processTuple(Tuple(leftSchema, Array[Any](1.0)), 0)
    exec.processTuple(Tuple(rightSchema, Array[Any](1.0)), 1)
    exec.onFinish(0)
    exec.onFinish(1).toList shouldBe empty

    val desc = new IntersectOpDesc
    val code =
      desc.generateStandaloneCode(Map(PortIdentity() -> leftSchema, PortIdentity(1) -> rightSchema))
    val driver =
      s"""import pandas as pd
         |
         |in1df = pd.DataFrame({"x": [1.0]})
         |in2df = pd.DataFrame({"y": [1.0]})
         |${(desc.standaloneHelpers() :+ code).mkString("\n")}
         |print(len(out1df), list(out1df.columns))
         |""".stripMargin

    val script = Files.createTempFile("intersect-schemas-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "0 ['x']"
    }
  }

  private def exported(): String = {
    val desc = new IntersectOpDesc
    (desc.standaloneHelpers() :+ desc.generateStandaloneCode()).mkString("\n")
  }
}
