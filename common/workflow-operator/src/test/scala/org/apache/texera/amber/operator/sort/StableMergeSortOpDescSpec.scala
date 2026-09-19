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

package org.apache.texera.amber.operator.sort

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.LogicalOp
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.util.Try

class StableMergeSortOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "StableMergeSortOpDesc.operatorInfo" should
    "advertise the name, Sort group, and a single blocking output" in {
    val info = (new StableMergeSortOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Stable Merge Sort"
    info.operatorGroupName shouldBe OperatorGroupConstants.SORT_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    // A stable sort must observe all rows before emitting, so the output blocks.
    info.outputPorts.head.blocking shouldBe true
  }

  "StableMergeSortOpDesc.getPhysicalOp" should
    "be a non-parallelizable many-to-one op wiring StableMergeSortOpExec" in {
    val op = new StableMergeSortOpDesc
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.sort.StableMergeSortOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "StableMergeSortOpDesc" should
    "deserialize its sort keys (List of SortCriteriaUnit) through the polymorphic base" in {
    val json =
      """{"operatorType":"StableMergeSort","keys":[{"attribute":"age","sortPreference":"DESC"}]}"""
    val desc = objectMapper.readValue(json, classOf[LogicalOp]).asInstanceOf[StableMergeSortOpDesc]
    desc.keys should have size 1
    desc.keys.head.attributeName shouldBe "age"
    desc.keys.head.sortPreference shouldBe SortPreference.DESC
  }

  // A null goes last whichever way the key points; a NaN compares above every
  // number, so it goes last ascending and first descending. Only a column read
  // into a nullable dtype can hold both, which is where this is run.
  "StableMergeSortOpDesc.generateStandaloneCode" should
    "put a null last both ways and a NaN where the key points" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    def blockFor(pref: SortPreference): String = {
      val unit = new SortCriteriaUnit
      unit.attributeName = "v"
      unit.sortPreference = pref
      val d = new StableMergeSortOpDesc
      d.keys = ListBuffer(unit)
      d.generateStandaloneCode().linesIterator.map("    " + _).mkString("\n")
    }

    val driver =
      s"""import pandas as pd, numpy as np
         |
         |def frame():
         |    # 3.0, a null, a stored NaN, 1.0, +inf
         |    col = pd.arrays.FloatingArray(
         |        np.array([3.0, 0.0, np.nan, 1.0, np.inf]),
         |        np.array([False, True, False, False, False]),
         |    )
         |    return pd.DataFrame({"v": col})
         |
         |def show(df):
         |    return [("NULL" if x is pd.NA else repr(float(x))) for x in df["v"]]
         |
         |if True:
         |    in1df = frame()
         |${blockFor(SortPreference.ASC)}
         |    print("asc", show(out1df))
         |if True:
         |    in1df = frame()
         |${blockFor(SortPreference.DESC)}
         |    print("desc", show(out1df))
         |""".stripMargin

    val script = Files.createTempFile("sort-nan-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      lines.head shouldBe "asc ['1.0', '3.0', 'inf', 'nan', 'NULL']"
      lines(1) shouldBe "desc ['nan', 'inf', '3.0', '1.0', 'NULL']"
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
