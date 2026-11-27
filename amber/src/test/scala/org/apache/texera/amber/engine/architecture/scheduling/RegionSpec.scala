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

package org.apache.texera.amber.engine.architecture.scheduling

import org.apache.texera.amber.core.workflow.{
  GlobalPortIdentity,
  InputPort,
  OutputPort,
  PhysicalOp,
  PortIdentity
}
import org.apache.texera.amber.engine.architecture.scheduling.config.{
  IntermediateInputPortConfig,
  OutputPortConfig,
  ResourceConfig
}
import org.apache.texera.amber.engine.common.BarePhysicalPlanFixture
import org.scalatest.flatspec.AnyFlatSpec

import java.net.URI

class RegionSpec extends AnyFlatSpec with BarePhysicalPlanFixture {

  private def wiredOp(opId: String): PhysicalOp =
    op(opId)
      .withInputPorts(List(InputPort(PortIdentity(0))))
      .withOutputPorts(List(OutputPort(PortIdentity(0))))

  "Region" should "expose the physical operators provided at construction" in {
    val a = op("a")
    val b = op("b")
    val region = Region(RegionIdentity(1), Set(a, b), Set.empty)

    assert(region.getOperators == Set(a, b))
  }

  it should "expose the physical links provided at construction" in {
    val a = op("a")
    val b = op("b")
    val ab = link("a", "b")
    val region = Region(RegionIdentity(1), Set(a, b), Set(ab))

    assert(region.getLinks == Set(ab))
  }

  it should "default ports to an empty set" in {
    val region = Region(RegionIdentity(1), Set(op("a")), Set.empty)
    assert(region.getPorts.isEmpty)
  }

  it should "expose the ports provided at construction" in {
    val portId = GlobalPortIdentity(physicalOpId("a"), PortIdentity(0), input = true)
    val region = Region(RegionIdentity(1), Set(op("a")), Set.empty, ports = Set(portId))
    assert(region.getPorts == Set(portId))
  }

  it should "default skipped to false and accept true" in {
    assert(!Region(RegionIdentity(1), Set(op("a")), Set.empty).skipped)
    assert(Region(RegionIdentity(1), Set(op("a")), Set.empty, skipped = true).skipped)
  }

  "Region.getOperator" should "look up a physical operator by id" in {
    val a = op("a")
    val b = op("b")
    val region = Region(RegionIdentity(1), Set(a, b), Set.empty)

    assert(region.getOperator(physicalOpId("a")) == a)
    assert(region.getOperator(physicalOpId("b")) == b)
  }

  it should "throw NoSuchElementException for an unknown operator id" in {
    val region = Region(RegionIdentity(1), Set(op("a")), Set.empty)
    assertThrows[NoSuchElementException] {
      region.getOperator(physicalOpId("missing"))
    }
  }

  "Region.topologicalIterator" should "yield operators in topological order based on physical links" in {
    val a = op("a")
    val b = op("b")
    val c = op("c")
    val region = Region(RegionIdentity(1), Set(a, b, c), Set(link("a", "b"), link("b", "c")))

    assert(
      region.topologicalIterator().toList ==
        List(physicalOpId("a"), physicalOpId("b"), physicalOpId("c"))
    )
  }

  "Region.getSourceOperators" should "treat operators without input ports as sources" in {
    val a = op("a")
    val b = op("b")
    val region = Region(RegionIdentity(1), Set(a, b), Set.empty)

    assert(region.getSourceOperators == Set(a, b))
  }

  it should "exclude operators fed by internal links and keep operators fed only from outside" in {
    val internalLink = link("a", "b")
    val externalLink = link("external", "c")
    val a = wiredOp("a").addOutputLink(internalLink)
    val b = wiredOp("b").addInputLink(internalLink)
    val c = wiredOp("c").addInputLink(externalLink)
    val region = Region(RegionIdentity(1), Set(a, b, c), Set(internalLink))

    assert(region.getSourceOperators == Set(a, c))
  }

  it should "count only the region's own links, not every link an operator lists" in {
    // b lists an input link from a, and both are in the region, yet the link is not one of the
    // region's links (for instance when b reads a's saved result): b is a source of the region.
    val ab = link("a", "b")
    val a = wiredOp("a").addOutputLink(ab)
    val b = wiredOp("b").addInputLink(ab)
    assert(Region(RegionIdentity(1), Set(a, b), Set.empty).getSourceOperators == Set(a, b))
    // With the link among the region's links, b is not a source.
    assert(Region(RegionIdentity(1), Set(a, b), Set(ab)).getSourceOperators == Set(a))
  }

  "Region.getStarterOperators" should "match getSourceOperators when no resource config is provided" in {
    val a = op("a")
    val b = op("b")
    val region = Region(RegionIdentity(1), Set(a, b), Set.empty)

    assert(region.getStarterOperators == region.getSourceOperators)
  }

  it should "promote only operators reading from materialized input ports in resourceConfig" in {
    val ab = link("a", "b")
    val ac = link("a", "c")
    val a = wiredOp("a").addOutputLink(ab).addOutputLink(ac)
    val b = wiredOp("b").addInputLink(ab)
    val c = wiredOp("c").addInputLink(ac)
    val baseUri = new URI("ram:///materialized/")
    val resourceConfig = ResourceConfig(
      portConfigs = Map(
        GlobalPortIdentity(b.id, PortIdentity(0), input = true) ->
          IntermediateInputPortConfig(List(baseUri)),
        GlobalPortIdentity(c.id, PortIdentity(0), input = true) ->
          IntermediateInputPortConfig(List.empty),
        GlobalPortIdentity(c.id, PortIdentity(0), input = false) ->
          OutputPortConfig(baseUri)
      )
    )
    val region = Region(
      RegionIdentity(1),
      Set(a, b, c),
      Set(ab, ac),
      resourceConfig = Some(resourceConfig)
    )

    assert(region.getStarterOperators == Set(a, b))
  }
}
