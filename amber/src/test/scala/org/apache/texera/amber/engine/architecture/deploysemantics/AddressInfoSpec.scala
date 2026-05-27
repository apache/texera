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

package org.apache.texera.amber.engine.architecture.deploysemantics

import org.apache.pekko.actor.Address
import org.scalatest.flatspec.AnyFlatSpec

class AddressInfoSpec extends AnyFlatSpec {

  private def addr(host: String, port: Int): Address =
    Address("pekko", "Amber", host, port)

  "AddressInfo" should "expose the addresses it was constructed with" in {
    val nodes = Array(addr("h1", 2552), addr("h2", 2552), addr("h3", 2552))
    val controller = addr("ctrl", 2552)
    val info = AddressInfo(nodes, controller)
    assert(info.allAddresses.toList == nodes.toList)
    assert(info.controllerAddress == controller)
  }

  it should "preserve the order of allAddresses" in {
    // The cluster scheduler picks workers based on this list's order, so
    // any reorder is observable.
    val nodes = Array(addr("c", 1), addr("a", 2), addr("b", 3))
    val info = AddressInfo(nodes, addr("ctrl", 0))
    assert(info.allAddresses.map(_.host.get).toList == List("c", "a", "b"))
  }

  it should "accept an empty allAddresses array" in {
    // Edge case: no worker nodes (e.g., controller-only configuration).
    val info = AddressInfo(Array.empty[Address], addr("ctrl", 0))
    assert(info.allAddresses.isEmpty)
    assert(info.controllerAddress.host.contains("ctrl"))
  }

  it should "allow the controller to also appear in allAddresses (collocated)" in {
    val controller = addr("ctrl", 2552)
    val info = AddressInfo(Array(controller, addr("worker", 2552)), controller)
    assert(info.allAddresses.contains(controller))
    assert(info.controllerAddress == controller)
  }

  it should "support copy(), allowing one field to change while the other is preserved" in {
    val a = AddressInfo(Array(addr("h1", 1)), addr("ctrl-a", 0))
    val b = a.copy(controllerAddress = addr("ctrl-b", 0))
    assert(b.controllerAddress.host.contains("ctrl-b"))
    assert(b.allAddresses.toList == a.allAddresses.toList)
    // original is unchanged
    assert(a.controllerAddress.host.contains("ctrl-a"))
  }

  it should "use Array reference equality (not element-wise) for the allAddresses field" in {
    // Case-class equality on `Array` fields uses array reference equality,
    // not element-wise equality. Two AddressInfo values that hold the SAME
    // array instance compare equal; two AddressInfo values that hold
    // distinct arrays with the SAME elements do NOT. Lock this down so a
    // future change to (say) Seq doesn't silently flip equality semantics
    // for callers.
    val nodes = Array(addr("h", 1))
    val ctrl = addr("ctrl", 0)
    val sameRef = AddressInfo(nodes, ctrl)
    val sameRefAgain = AddressInfo(nodes, ctrl) // shares the same array reference
    val differentRef = AddressInfo(Array(addr("h", 1)), ctrl) // different array reference
    assert(sameRef == sameRefAgain, "shared Array reference → equal")
    assert(sameRef != differentRef, "distinct Array references → not equal (no element-wise check)")
  }
}
