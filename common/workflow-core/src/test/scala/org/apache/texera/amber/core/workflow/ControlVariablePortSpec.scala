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

package org.apache.texera.amber.core.workflow

import org.scalatest.flatspec.AnyFlatSpec

class ControlVariablePortSpec extends AnyFlatSpec {

  "ControlVariablePort.Id" should "be a reserved external id that no data port uses" in {
    assert(ControlVariablePort.Id == PortIdentity(-1, internal = false))
    assert(ControlVariablePort.is(PortIdentity(-1)))
    assert(!ControlVariablePort.is(PortIdentity(0)))
    assert(!ControlVariablePort.is(PortIdentity(-1, internal = true)))
  }

  "ControlVariablePort.inputPort" should "declare the reserved id with no dependencies" in {
    val port = ControlVariablePort.inputPort
    assert(port.id == ControlVariablePort.Id)
    assert(port.displayName == "control variables")
    assert(port.dependencies.isEmpty)
    assert(!port.disallowMultiLinks)
  }
}
