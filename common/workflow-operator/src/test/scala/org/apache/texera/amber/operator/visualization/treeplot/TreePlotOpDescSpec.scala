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

import org.apache.texera.amber.operator.visualization.ChartPythonTestSupport.withPytexeraTableDriver
import org.apache.texera.amber.util.PythonTestSupport.NeedsPythonPackages
import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.texera.amber.operator.GeneratedCodeAssertionChecks

class TreePlotOpDescSpec extends AnyFlatSpec with BeforeAndAfter with GeneratedCodeAssertionChecks {

  var opDesc: TreePlotOpDesc = _

  before {
    opDesc = new TreePlotOpDesc()
  }

  // The not-blank assert lives directly in generatePythonCode().
  it should "throw AssertionError naming the Edge List Column when it is left empty" in {
    val ex = expectError[AssertionError]("cannot be empty")(opDesc.generatePythonCode())
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
  it should "not reach for igraph in either generated path" in {
    opDesc.edgeListColumn = "edge_pairs"
    opDesc.generatePythonCode().toLowerCase should not include "igraph"
    opDesc.generateStandaloneCode().toLowerCase should not include "igraph"
  }

  // Driver executed by the runtime test below. It stubs only the pytexera import seam;
  // the generated module runs unmodified, and the layout is called directly so the
  // positions it computes are what is read, rather than a rendered picture.
  private val runtimeDriverBody: String =
    """cases = [
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
      |    print("CASE %s %s" % (cid, placed))""".stripMargin

  /** `label -> (x, y)` per case, read out of the driver's output. One run for the
    * whole suite: the driver lays every case out in the same process.
    */
  private lazy val layouts: Map[String, Map[String, (Double, Double)]] = {
    opDesc.edgeListColumn = "edge_pairs"
    withPytexeraTableDriver(
      "tree_plot",
      opDesc.generatePythonCode(),
      "generated_tree_plot",
      "ProcessTableOperator",
      runtimeDriverBody,
      timeoutMessage = "Layout driver timed out after 120s"
    ) { (exitCode, output) =>
      withClue(s"Driver output:\n$output\n") {
        exitCode shouldBe 0
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
    }
  }

  it should "lay a tree out top-down, with every parent centred over its own children" taggedAs NeedsPythonPackages in {
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

  it should "place a shared child once, under the parent that reaches it first" taggedAs NeedsPythonPackages in {
    val shared = layouts("shared")
    shared.keySet shouldBe Set("a", "b", "c")
    // c is claimed by a, so it hangs below a and b is left as a childless root.
    shared("c") shouldBe (0.0, -1.0)
    shared("a") shouldBe (0.0, 0.0)
    shared("b")._2 shouldBe 0.0
    shared("b")._1 should not be shared("a")._1
  }

  it should "terminate on a cycle and still place every node once" taggedAs NeedsPythonPackages in {
    val cycle = layouts("cycle")
    // No node is a root, so the layout starts from the first label it saw and the
    // edge that closes the ring is dropped rather than followed a second time.
    cycle shouldBe Map("a" -> (0.0, 0.0), "b" -> (0.0, -1.0), "c" -> (0.0, -2.0))
  }

  it should "survive a self loop" taggedAs NeedsPythonPackages in {
    val selfLoop = layouts("selfloop")
    selfLoop.keySet shouldBe Set("a", "b")
    selfLoop("a")._2 shouldBe 0.0
    selfLoop("b")._2 shouldBe -1.0
  }

  it should "lay each tree of a forest out beside the other" taggedAs NeedsPythonPackages in {
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
