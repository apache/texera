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

package org.apache.texera.amber.operator.visualization.networkGraph

import org.scalatest.BeforeAndAfter
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.Base64

class NetworkGraphOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: NetworkGraphOpDesc = _

  before {
    opDesc = new NetworkGraphOpDesc()
  }

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def carries(output: String, name: String): Boolean =
    output.contains(name) || output.contains(b64(name))

  private def fieldPart(msg: String): String =
    msg.toLowerCase.replace("cannot be empty", "")

  it should "throw AssertionError naming the Source Column when both fields are empty" in {
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("source")
  }

  it should "throw AssertionError naming the Destination Column when only source is set" in {
    opDesc.source = "from_node"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("destination")
  }

  it should "throw AssertionError naming the Source Column when only destination is set" in {
    opDesc.destination = "to_node"
    val ex = intercept[AssertionError](opDesc.manipulateTable())
    ex.getMessage should not be null
    ex.getMessage should include("cannot be empty")
    fieldPart(ex.getMessage) should include("source")
  }

  it should "render both configured columns when source and destination are set" in {
    opDesc.source = "from_node"
    opDesc.destination = "to_node"
    val plain = opDesc.manipulateTable().plain
    assert(carries(plain, "from_node"))
    assert(carries(plain, "to_node"))
    plain should include("dropna")
  }

  it should "generate python code carrying source, destination, and title" in {
    opDesc.source = "from_node"
    opDesc.destination = "to_node"
    opDesc.title = "My Graph"
    val code = opDesc.generatePythonCode()
    assert(carries(code, "from_node"))
    assert(carries(code, "to_node"))
    assert(carries(code, "My Graph"))
    code should include("class ProcessTableOperator(UDFTableOperator)")
  }

  it should "build the node set as a union rather than by adding the two columns" in {
    opDesc.source = "from_node"
    opDesc.destination = "to_node"
    val code = opDesc.generatePythonCode()

    // `sources + destinations` is element-wise on two Series, so it glued each
    // source to its destination and those strings entered the graph as nodes.
    code should not include "set(sources + destinations)"
    code should include("pd.concat([sources, destinations])")

    // Ordered de-duplication, not a set: a set iterates strings in an order that
    // varies between processes, which would move the nodes from run to run.
    code should include("dict.fromkeys")
  }

  it should "seed the layout so the same graph draws the same coordinates every run" in {
    opDesc.source = "from_node"
    opDesc.destination = "to_node"
    val code = opDesc.generatePythonCode()

    // spring_layout starts from random positions, so an unseeded call gives the
    // same graph different coordinates on every run.
    val layout = code.linesIterator
      .find(_.contains("nx.spring_layout"))
      .getOrElse(fail("generated code no longer calls nx.spring_layout"))
    layout should include("seed=0")
  }
}
