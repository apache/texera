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

package org.apache.texera.amber.operator.intersect

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
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

class IntersectOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

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
        className shouldBe "org.apache.texera.amber.operator.intersect.IntersectOpExec"
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
         |${(new IntersectOpDesc).generateStandaloneCode()}
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
