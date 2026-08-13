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

class NetworkGraphOpDescSpec extends AnyFlatSpec with BeforeAndAfter with Matchers {

  var opDesc: NetworkGraphOpDesc = _

  before {
    opDesc = new NetworkGraphOpDesc()
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
}
