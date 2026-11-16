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

import org.apache.texera.amber.util.PythonTestSupport.{
  NeedsPythonPackages,
  canImport,
  configuredPython
}
import org.apache.texera.amber.core.executor.OpExecWithClassName
import org.apache.texera.amber.core.tuple.{Attribute, AttributeType, Schema, Tuple}
import org.apache.texera.amber.core.virtualidentity.{ExecutionIdentity, WorkflowIdentity}
import org.apache.texera.amber.operator.{LogicalOp, SamplingHelpers}
import org.apache.texera.amber.operator.metadata.OperatorGroupConstants
import org.apache.texera.amber.util.JSONUtils.objectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

class ReservoirSamplingOpDescSpec extends AnyFlatSpec with Matchers {

  private val workflowId = WorkflowIdentity(1L)
  private val executionId = ExecutionIdentity(1L)

  private val WireKey = "number of item sampled in reservoir sampling"

  private def desc(k: Int): ReservoirSamplingOpDesc = {
    val d = new ReservoirSamplingOpDesc
    d.k = k
    d
  }

  // Runs each (descriptor, input ids) case the way an exported script does: the Java random
  // helper, an `in1df` of the ids, then the generated snippet, which seeds its own generator.
  // Each case prints its sampled ids on an "IDS <case>:" line. Returns the exit code, the
  // output and the script.
  private def runExported(
      cases: Seq[(ReservoirSamplingOpDesc, Seq[Int])]
  ): (Int, String, String) = {
    val python = configuredPython().getOrElse(cancel("No runnable python executable"))
    if (!canImport(python, "pandas")) cancel(s"'$python' cannot import pandas")

    val body = cases.zipWithIndex.map {
      case ((d, ids), c) =>
        s"""in1df = pd.DataFrame({"id": pd.Series([${ids.mkString(", ")}], dtype="int64")})
           |${d.generateStandaloneCode()}
           |print("IDS $c:", ",".join(str(i) for i in out1df["id"]))""".stripMargin
    }
    val driver =
      s"""import pandas as pd
         |${SamplingHelpers.JavaRandom}
         |${body.mkString("\n")}
         |""".stripMargin
    val (exit, out) = runPython(python, driver)
    (exit, out, driver)
  }

  // The output goes to a file rather than a pipe, so the timeout holds even when the script
  // hangs; a script still running then is killed and the test fails with what it printed.
  private def runPython(
      python: String,
      driver: String,
      timeoutSeconds: Long = 60
  ): (Int, String) = {
    val script = Files.createTempFile("reservoir-export-", ".py")
    val output = Files.createTempFile("reservoir-export-", ".log")
    try {
      Files.write(script, driver.getBytes(StandardCharsets.UTF_8))
      val process = new ProcessBuilder(python, script.toString)
        .redirectErrorStream(true)
        .redirectOutput(output.toFile)
        .start()
      val finished = process.waitFor(timeoutSeconds, TimeUnit.SECONDS)
      if (!finished) {
        process.destroyForcibly()
        process.waitFor(5, TimeUnit.SECONDS)
      }
      val out = new String(Files.readAllBytes(output), StandardCharsets.UTF_8)
      if (!finished)
        fail(s"python did not finish within $timeoutSeconds s:\n$out\nscript:\n$driver")
      (process.exitValue(), out)
    } finally {
      Files.deleteIfExists(script)
      Files.deleteIfExists(output)
    }
  }

  private def printedIds(out: String, c: Int): List[Int] =
    out.linesIterator
      .collectFirst {
        case line if line.startsWith(s"IDS $c:") => line.stripPrefix(s"IDS $c:").trim
      }
      .map(ids => if (ids.isEmpty) Nil else ids.split(",").map(_.toInt).toList)
      .getOrElse(fail(s"case $c printed no ids:\n$out"))

  // The ids the engine keeps on one worker. The script states the one seed a single process can,
  // the generator seeded with a worker count of 1, so this is the run it has to agree with.
  private def engineIds(d: ReservoirSamplingOpDesc, ids: Seq[Int]): List[Int] = {
    val schema = Schema().add(new Attribute("id", AttributeType.INTEGER))
    val exec = new ReservoirSamplingOpExec(objectMapper.writeValueAsString(d), 0, 1)
    exec.open()
    ids.foreach { id =>
      exec.processTuple(
        Tuple
          .builder(schema)
          .add(new Attribute("id", AttributeType.INTEGER), Integer.valueOf(id))
          .build(),
        0
      )
    }
    exec.onFinish(0).map(_.asInstanceOf[Tuple].getField[Integer]("id").intValue).toList
  }

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

  // Algorithm R: fill the reservoir with the first k rows, then the row at 0-based
  // position n draws a slot from n + 1 and replaces it if the slot is below k. A
  // sample size below 1 is refused once there is a row. Pin the whole snippet — the
  // generated Python is indentation-sensitive.
  "ReservoirSamplingOpDesc.generateStandaloneCode" should
    "emit a seeded Algorithm R reservoir over the input rows" in {
    desc(3).generateStandaloneCode() shouldBe
      """_texera_rs_rng = _TexeraJavaRandom(1)
        |_texera_rs_k = 3
        |if _texera_rs_k < 1 and len(in1df) > 0:
        |    raise ValueError(f"Reservoir Sampling needs a sample size of at least 1, got {_texera_rs_k}")
        |_texera_rs_reservoir = []
        |for _texera_rs_n in range(len(in1df)):
        |    if _texera_rs_n < _texera_rs_k:
        |        _texera_rs_reservoir.append(_texera_rs_n)
        |    else:
        |        _texera_rs_i = _texera_rs_rng.next_int(_texera_rs_n + 1)
        |        if _texera_rs_i < _texera_rs_k:
        |            _texera_rs_reservoir[_texera_rs_i] = _texera_rs_n
        |out1df = in1df.iloc[_texera_rs_reservoir].reset_index(drop=True)""".stripMargin
  }

  // The engine refuses a sample size below 1 at the first row, so the script refuses
  // it there too. Answering with an empty table would report a result the run never
  // produced.
  it should "fail on the first row when the sample size is below 1, saying why" taggedAs NeedsPythonPackages in {
    for (k <- Seq(0, -1)) {
      val (exit, out, driver) = runExported(Seq(desc(k) -> Seq(1, 2, 3)))
      withClue(s"k = $k, python said:\n$out\nscript:\n$driver") {
        exit should not be 0
        out should include(s"needs a sample size of at least 1, got $k")
      }
    }
  }

  // Guard: the refusal waits for a row, as the engine's does. Over an empty table the
  // engine finishes with nothing, so the script must answer with an empty table.
  it should "return an empty table for a sample size of 0 over an empty table" taggedAs NeedsPythonPackages in {
    val (exit, out, driver) = runExported(Seq(desc(0) -> Seq.empty))
    withClue(s"python said:\n$out\nscript:\n$driver") {
      exit shouldBe 0
      printedIds(out, 0) shouldBe empty
      engineIds(desc(0), Seq.empty) shouldBe empty
    }
  }

  // Guard: the engine and the script draw from one generator sequence, so they must keep
  // the same rows in the same order. Fixing the bound on one side only breaks this.
  it should "keep the same rows as the engine" taggedAs NeedsPythonPackages in {
    val cases = Seq(
      desc(1) -> (0 until 2),
      desc(3) -> (0 until 3),
      desc(5) -> (0 until 200),
      desc(7) -> (0 until 1000)
    )
    val (exit, out, driver) = runExported(cases)
    withClue(s"python said:\n$out\nscript:\n$driver") {
      exit shouldBe 0
      cases.zipWithIndex.foreach {
        case ((d, ids), c) =>
          withClue(s"case $c (k = ${d.k}, ${ids.size} rows): ") {
            printedIds(out, c) shouldBe engineIds(d, ids)
          }
      }
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
}
