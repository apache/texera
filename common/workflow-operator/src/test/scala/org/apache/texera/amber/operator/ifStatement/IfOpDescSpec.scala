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

package org.apache.texera.amber.operator.ifStatement

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.core.workflow.PortIdentity
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class IfOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  "IfOpDesc.operatorInfo" should
    "advertise two inputs (Condition + data) and two outputs (False/True) in the Control group" in {
    val info = (new IfOpDesc).operatorInfo
    info.userFriendlyName shouldBe "If"
    info.operatorGroupName shouldBe OperatorGroupConstants.CONTROL_GROUP
    info.inputPorts should have length 2
    info.inputPorts.head.id shouldBe PortIdentity()
    info.inputPorts.head.displayName shouldBe "Condition"
    info.inputPorts.last.id shouldBe PortIdentity(1)
    info.outputPorts.map(_.id) shouldBe List(PortIdentity(), PortIdentity(1))
    info.outputPorts.head.displayName shouldBe "False"
    info.outputPorts.last.displayName shouldBe "True"
  }

  "IfOpDesc.conditionName" should "default to null" in {
    (new IfOpDesc).conditionName shouldBe null
  }

  "IfOpDesc.getPhysicalOp" should
    "wire IfOpExec, be non-parallelizable, and carry the port identities" in {
    val op = new IfOpDesc
    op.conditionName = "ready"
    val physical = op.getPhysicalOp(workflowId, executionId)
    physical.parallelizable shouldBe false
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.ifStatement.IfOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe op.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe op.operatorInfo.outputPorts.map(_.id).toSet
  }

  "IfOpDesc schema propagation" should
    "route the data input's schema (inputPorts.last) to BOTH outputs, dropping the condition schema" in {
    val physical = (new IfOpDesc).getPhysicalOp(workflowId, executionId)
    val condSchema = Schema().add(new Attribute("cond", AttributeType.BOOLEAN))
    val dataSchema = Schema().add(new Attribute("payload", AttributeType.STRING))
    val out = physical.propagateSchema.func(
      Map(PortIdentity() -> condSchema, PortIdentity(1) -> dataSchema)
    )
    out shouldBe Map(PortIdentity() -> dataSchema, PortIdentity(1) -> dataSchema)
  }

  "IfOpDesc.generateStandaloneCode" should
    "name the switch after the condition so two Ifs in one script do not share it" in {
    val op = new IfOpDesc
    op.conditionName = "ready"
    op.generateStandaloneCode() should include("\"_texera_if_ready\"")
  }

  // The engine picks the route from a State message, which the verification harness
  // has no channel for, so the exported block is where both routes can be run. False
  // is the half a fixture cannot reach: with no State the engine keeps its default,
  // and that default is True.
  it should "send the rows to True by default and to False when the switch is off" in {
    val python = resolvePythonExecutable().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val op = new IfOpDesc
    op.conditionName = "ready"
    val body = op.generateStandaloneCode().linesIterator.map("    " + _).mkString("\n")

    val driver =
      s"""import pandas as pd
         |
         |for _switch in (None, True, False):
         |    in2df = pd.DataFrame({"id": [1, 2, 3]})
         |    if _switch is None:
         |        globals().pop("_texera_if_ready", None)
         |    else:
         |        globals()["_texera_if_ready"] = _switch
         |$body
         |    print(_switch, list(out1df["id"]), list(out2df["id"]))
         |""".stripMargin

    val script = Files.createTempFile("if-standalone-", ".py")
    script.toFile.deleteOnExit()
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(120, TimeUnit.SECONDS)

    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() shouldBe 0
      val lines = out.trim.linesIterator.toSeq
      // out1df is the False port, out2df the True port. Each route has to be
      // exclusive: the rows leave by one and the other comes back empty.
      lines should contain("None [] [1, 2, 3]")
      lines should contain("True [] [1, 2, 3]")
      lines should contain("False [1, 2, 3] []")
    }
  }

  private def resolvePythonExecutable(): Option[String] = {
    def fromConfig: Option[String] =
      Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
        .orElse(Try(ConfigFactory.load()).toOption)
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)

    def isRunnable(exe: String): Boolean =
      Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start()).toOption
        .exists { p =>
          if (!p.waitFor(5, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
          else p.exitValue() == 0
        }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private def canImportPandas(python: String): Boolean =
    Try(
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
