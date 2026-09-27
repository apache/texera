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

package org.apache.texera.amber.operator.sortPartitions

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.RangePartition
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

class SortPartitionsOpDescSpec extends AnyFlatSpec with Matchers {

  private val NeedsPythonPackages = Tag(classOf[IntegrationTest].getName)

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private def newDesc(attr: String, min: Long, max: Long): SortPartitionsOpDesc = {
    val d = new SortPartitionsOpDesc
    d.sortAttributeName = attr
    d.domainMin = min
    d.domainMax = max
    d
  }

  "SortPartitionsOpDesc.operatorInfo" should
    "advertise the name, Sort group, and a single blocking output" in {
    val info = (new SortPartitionsOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Sort Partitions"
    info.operatorGroupName shouldBe OperatorGroupConstants.SORT_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    info.outputPorts.head.blocking shouldBe true
  }

  "SortPartitionsOpDesc.getPhysicalOp" should
    "wire SortPartitionsOpExec and require a RangePartition over the sort attribute/domain" in {
    val op = newDesc("score", 0L, 100L)
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.sortPartitions.SortPartitionsOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
    physical.partitionRequirement shouldBe List(
      Option(RangePartition(List("score"), 0L, 100L))
    )
  }

  "SortPartitionsOpDesc" should "round-trip its attribute/domain fields through the polymorphic base" in {
    val json = objectMapper.writeValueAsString(newDesc("age", 1L, 99L))
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[SortPartitionsOpDesc]
    val sp = restored.asInstanceOf[SortPartitionsOpDesc]
    sp.sortAttributeName shouldBe "age"
    sp.domainMin shouldBe 1L
    sp.domainMax shouldBe 99L
  }

  // The engine puts a null first and a NaN after positive infinity. Only a
  // column read into a nullable dtype can hold both, which is what an Arrow
  // file is read into, so that is where this is run.
  "SortPartitionsOpDesc.generateStandaloneCode" should
    "put a null first and a NaN after infinity" taggedAs NeedsPythonPackages in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |# 3.0, a null, a stored NaN, 1.0, +inf
         |in1df = pd.DataFrame({"v": pd.arrays.FloatingArray(
         |    np.array([3.0, 0.0, np.nan, 1.0, np.inf]),
         |    np.array([False, True, False, False, False]),
         |)})
         |${newDesc("v", 0L, 10L).generateStandaloneCode()}
         |print([("NULL" if x is pd.NA else repr(float(x))) for x in out1df["v"]])
         |""".stripMargin

    val script = Files.createTempFile("sort-partitions-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      out.trim shouldBe "['NULL', '1.0', '3.0', 'inf', 'nan']"
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
