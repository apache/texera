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

package org.apache.texera.amber.operator.visualization.treeplot

import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import java.util.concurrent.TimeUnit
import scala.util.Try

class TreePlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: TreePlotOpDesc = _

  before {
    opDesc = new TreePlotOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  // The not-blank assert lives directly in generatePythonCode().
  it should "throw AssertionError naming the Edge List Column when it is left empty" in {
    val ex = intercept[AssertionError](opDesc.generatePythonCode())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("edge")
  }

  it should "generate python code carrying the configured edge list column" in {
    opDesc.edgeListColumn = "edge_pairs"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "edge_pairs"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
    code should include("self.build_tree_layout(edges)")
  }

  // igraph is GPL v2, Category X under the ASF 3rd party license policy, so the
  // layout has to stay something the repository can actually ship.
  it should "not reach for igraph" in {
    opDesc.edgeListColumn = "edge_pairs"
    opDesc.generatePythonCode().toLowerCase should not include "igraph"
  }

  // Python executable resolution, following FilledAreaPlotOpDescSpec:
  // udf.conf python.path (UDF_PYTHON_PATH), then python3 / python / py.
  private def resolvePythonExecutable(): Option[String] = {
    def fromConfig: Option[String] = {
      val configOpt =
        Try(ConfigFactory.parseResources("udf.conf").resolve()).toOption
          .orElse(Try(ConfigFactory.load()).toOption)
      configOpt
        .flatMap(c => Try(c.getConfig("python").getString("path")).toOption)
        .map(_.trim)
        .filter(_.nonEmpty)
    }

    def isRunnable(exe: String): Boolean = {
      val pTry = Try(new ProcessBuilder(exe, "--version").redirectErrorStream(true).start())
      pTry.toOption.exists { p =>
        val finished = p.waitFor(5, TimeUnit.SECONDS)
        if (!finished) { p.destroyForcibly(); false }
        else p.exitValue() == 0
      }
    }

    (fromConfig.toList ++ List("python3", "python", "py")).distinct.find(isRunnable)
  }

  private def canImportPandasAndPlotly(python: String): Boolean = {
    val pTry = Try(
      new ProcessBuilder(python, "-c", "import pandas, plotly").redirectErrorStream(true).start()
    )
    pTry.toOption.exists { p =>
      val finished = p.waitFor(60, TimeUnit.SECONDS)
      if (!finished) { p.destroyForcibly(); false }
      else p.exitValue() == 0
    }
  }

  // Driver executed by the runtime test below. It stubs only the pytexera import seam;
  // the generated module runs unmodified, and the layout is called directly so the
  // positions it computes are what is read, rather than a rendered picture.
  private val runtimeDriverScript: String =
    """import base64
      |import sys
      |import types
      |from typing import Iterator, Optional
      |
      |import pandas as pd
      |
      |class UDFTableOperator:
      |    def decode_python_template(self, data):
      |        return base64.b64decode(data).decode("utf-8")
      |
      |stub = types.ModuleType("pytexera")
      |stub.UDFTableOperator = UDFTableOperator
      |stub.overrides = lambda fn: fn
      |stub.Table = pd.DataFrame
      |stub.TableLike = object
      |stub.Iterator = Iterator
      |stub.Optional = Optional
      |sys.modules["pytexera"] = stub
      |
      |ns = {"__name__": "generated_tree_plot"}
      |with open(sys.argv[1]) as f:
      |    exec(compile(f.read(), sys.argv[1], "exec"), ns)
      |op = ns["ProcessTableOperator"]()
      |
      |cases = [
      |    ("tree", [("a", "b"), ("a", "c"), ("b", "d"), ("b", "e")]),
      |    ("shared", [("a", "c"), ("b", "c")]),
      |    ("cycle", [("a", "b"), ("b", "c"), ("c", "a")]),
      |    ("selfloop", [("a", "a"), ("a", "b")]),
      |    ("forest", [("a", "b"), ("x", "y")]),
      |]
      |
      |for cid, edges in cases:
      |    labels, coords = op.build_tree_layout(edges)
      |    placed = " ".join(
      |        "%s=%g,%g" % (label, x, y) for label, (x, y) in zip(labels, coords)
      |    )
      |    print("CASE %s %s" % (cid, placed))
      |""".stripMargin

  /** `label -> (x, y)` per case, read out of the driver's output. One run for the
    * whole suite: the driver lays every case out in the same process.
    */
  private lazy val layouts: Map[String, Map[String, (Double, Double)]] = {
    val python = resolvePythonExecutable().getOrElse(
      cancel("No runnable python executable (udf.conf python.path, python3, python, py)")
    )
    if (!canImportPandasAndPlotly(python)) {
      cancel(s"'$python' cannot import pandas and plotly; skipping runtime verification")
    }

    opDesc.edgeListColumn = "edge_pairs"
    val moduleFile = Files.createTempFile("tree_plot_op_", ".py")
    val driverFile = Files.createTempFile("tree_plot_driver_", ".py")
    try {
      Files.write(moduleFile, opDesc.generatePythonCode().getBytes(StandardCharsets.UTF_8))
      Files.write(driverFile, runtimeDriverScript.getBytes(StandardCharsets.UTF_8))

      val process = new ProcessBuilder(python, driverFile.toString, moduleFile.toString)
        .redirectErrorStream(true)
        .start()
      val finished = process.waitFor(120, TimeUnit.SECONDS)
      if (!finished) {
        process.destroyForcibly()
        fail("Layout driver timed out after 120s")
      }
      val output = new String(process.getInputStream.readAllBytes(), StandardCharsets.UTF_8)
      withClue(s"Driver output:\n$output\n") {
        process.exitValue() shouldBe 0
      }
      "CASE (\\S+) (.*)".r
        .findAllMatchIn(output)
        .map { m =>
          m.group(1) -> m
            .group(2)
            .trim
            .split(" ")
            .filter(_.nonEmpty)
            .map { entry =>
              val Array(label, xy) = entry.split("=", 2)
              val Array(x, y) = xy.split(",", 2)
              label -> (x.toDouble, y.toDouble)
            }
            .toMap
        }
        .toMap
    } finally {
      Files.deleteIfExists(moduleFile)
      Files.deleteIfExists(driverFile)
    }
  }

  it should "lay a tree out top-down, with every parent centred over its own children" in {
    val tree = layouts("tree")
    // Depth picks the row and the axis is inverted, so a child sits below its parent.
    tree.map { case (label, (_, y)) => label -> y } shouldBe
      Map("a" -> 0.0, "b" -> -1.0, "c" -> -1.0, "d" -> -2.0, "e" -> -2.0)
    // Leaves take consecutive free columns left to right, in the order they are reached.
    tree("d")._1 shouldBe 0.0
    tree("e")._1 shouldBe 1.0
    tree("c")._1 shouldBe 2.0
    // A parent sits at the mean of its own children: b over d and e, a over b and c.
    tree("b")._1 shouldBe (tree("d")._1 + tree("e")._1) / 2
    tree("a")._1 shouldBe (tree("b")._1 + tree("c")._1) / 2
  }

  it should "place a shared child once, under the parent that reaches it first" in {
    val shared = layouts("shared")
    shared.keySet shouldBe Set("a", "b", "c")
    // c is claimed by a, so it hangs below a and b is left as a childless root.
    shared("c") shouldBe (0.0, -1.0)
    shared("a") shouldBe (0.0, 0.0)
    shared("b")._2 shouldBe 0.0
    shared("b")._1 should not be shared("a")._1
  }

  it should "terminate on a cycle and still place every node once" in {
    val cycle = layouts("cycle")
    // No node is a root, so the layout starts from the first label it saw and the
    // edge that closes the ring is dropped rather than followed a second time.
    cycle shouldBe Map("a" -> (0.0, 0.0), "b" -> (0.0, -1.0), "c" -> (0.0, -2.0))
  }

  it should "survive a self loop" in {
    val selfLoop = layouts("selfloop")
    selfLoop.keySet shouldBe Set("a", "b")
    selfLoop("a")._2 shouldBe 0.0
    selfLoop("b")._2 shouldBe -1.0
  }

  it should "lay each tree of a forest out beside the other" in {
    val forest = layouts("forest")
    forest.keySet shouldBe Set("a", "b", "x", "y")
    // Two roots, each over its own child, in columns that do not overlap.
    forest("a")._2 shouldBe 0.0
    forest("x")._2 shouldBe 0.0
    forest("a")._1 should not be forest("x")._1
    forest("b") shouldBe (forest("a")._1, -1.0)
    forest("y") shouldBe (forest("x")._1, -1.0)
  }
}
