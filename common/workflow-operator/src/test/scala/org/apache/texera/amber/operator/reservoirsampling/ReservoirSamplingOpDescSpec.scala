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

package org.apache.texera.amber.operator.reservoirsampling

import com.typesafe.config.ConfigFactory
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.{LogicalOp, SamplingHelpers}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import scala.io.Source
import scala.util.Try

class ReservoirSamplingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val WireKey = "number of item sampled in reservoir sampling"

  "ReservoirSamplingOpDesc.operatorInfo" should
    "advertise the name, Utility group, and (intentionally) NOT support reconfiguration" in {
    val info = (new ReservoirSamplingOpDesc).operatorInfo
    info.userFriendlyName shouldBe "Reservoir Sampling"
    info.operatorDescription shouldBe "Reservoir Sampling with k items being kept randomly"
    info.operatorGroupName shouldBe OperatorGroupConstants.UTILITY_GROUP
    info.inputPorts should have length 1
    info.outputPorts should have length 1
    // ReservoirSampling does not opt into reconfiguration (unlike RandomKSampling),
    // so it inherits the OperatorInfo default of false.
    info.supportReconfiguration shouldBe false
  }

  "ReservoirSamplingOpDesc" should "serialize k under its wire-key and round-trip it" in {
    val d = new ReservoirSamplingOpDesc
    d.k = 100
    val json = objectMapper.writeValueAsString(d)
    val tree = objectMapper.readTree(json)
    tree.has(WireKey) shouldBe true
    tree.get(WireKey).asInt shouldBe 100
    val restored = objectMapper.readValue(json, classOf[LogicalOp])
    restored shouldBe a[ReservoirSamplingOpDesc]
    restored.asInstanceOf[ReservoirSamplingOpDesc].k shouldBe 100
  }

  // Algorithm R: fill the reservoir with the first k rows, then replace a
  // uniformly-drawn slot for each later row. Pin the whole snippet — the
  // generated Python is indentation-sensitive.
  "ReservoirSamplingOpDesc.generateStandaloneCode" should
    "emit a seeded Algorithm R reservoir over the input rows" in {
    val d = new ReservoirSamplingOpDesc
    d.k = 3
    d.generateStandaloneCode() shouldBe
      """_texera_rs_rng = _TexeraJavaRandom(1)
        |_texera_rs_k = 3
        |_texera_rs_reservoir = []
        |for _texera_rs_n, _texera_rs_row in enumerate(in1df.itertuples(index=False, name=None)):
        |    if _texera_rs_n < _texera_rs_k:
        |        _texera_rs_reservoir.append(_texera_rs_row)
        |    else:
        |        _texera_rs_i = _texera_rs_rng.next_int(_texera_rs_n)
        |        if _texera_rs_i < _texera_rs_k:
        |            _texera_rs_reservoir[_texera_rs_i] = _texera_rs_row
        |out1df = pd.DataFrame(_texera_rs_reservoir, columns=list(in1df.columns)).reset_index(drop=True)""".stripMargin
  }

  // A reservoir of zero ends the engine's run on the first row: the executor
  // skips the fill branch and hands nextInt a bound of zero, which Java refuses.
  // The script has to refuse it there too. Answering with an empty table would
  // report a result the run never produced.
  it should "fail on the first row when the reservoir holds nothing" in {
    val python = resolvePython().getOrElse(cancel("No runnable python executable"))
    if (!canImportPandas(python)) cancel(s"'$python' cannot import pandas")

    val d = new ReservoirSamplingOpDesc
    d.k = 0
    val script = Files.createTempFile("reservoir-zero-", ".py")
    script.toFile.deleteOnExit()
    val driver =
      s"""import pandas as pd
         |${SamplingHelpers.JavaRandom}
         |in1df = pd.DataFrame({"id": [1, 2, 3]})
         |${d.generateStandaloneCode()}
         |""".stripMargin
    Files.write(script, driver.getBytes(StandardCharsets.UTF_8))

    val process = new ProcessBuilder(python, script.toString).redirectErrorStream(true).start()
    val out = Source.fromInputStream(process.getInputStream).mkString
    process.waitFor(60, TimeUnit.SECONDS)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      process.exitValue() should not be 0
      out should include("bound must be positive")
    }
  }

  "ReservoirSamplingOpDesc.getPhysicalOp" should
    "wire the ReservoirSamplingOpExec class name and carry ports" in {
    val d = new ReservoirSamplingOpDesc
    d.k = 10
    val physical = d.getPhysicalOp(workflowId, executionId)
    physical.opExecInitInfo match {
      case OpExecWithClassName(className, descString) =>
        className shouldBe "org.apache.texera.amber.operator.reservoirsampling.ReservoirSamplingOpExec"
        descString should not be empty
      case other => fail(s"expected OpExecWithClassName, got $other")
    }
    physical.inputPorts.keySet shouldBe d.operatorInfo.inputPorts.map(_.id).toSet
    physical.outputPorts.keySet shouldBe d.operatorInfo.outputPorts.map(_.id).toSet
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
      new ProcessBuilder(python, "-c", "import pandas").redirectErrorStream(true).start()
    ).toOption.exists { p =>
      if (!p.waitFor(60, TimeUnit.SECONDS)) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
}
