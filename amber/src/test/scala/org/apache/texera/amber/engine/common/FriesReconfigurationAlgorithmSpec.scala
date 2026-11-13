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

package org.apache.texera.amber.engine.common

import org.apache.texera.amber.core.executor.OpExecInitInfo
import org.apache.texera.amber.core.virtualidentity.{
  ExecutionIdentity,
  OperatorIdentity,
  PhysicalOpIdentity,
  WorkflowIdentity
}
import org.apache.texera.amber.core.workflow.{
  InputPort,
  OutputPort,
  PhysicalLink,
  PhysicalOp,
  PortIdentity
}
import org.apache.texera.amber.engine.architecture.coordinator.execution.WorkflowExecution
import org.apache.texera.amber.engine.architecture.rpc.controlcommands.{
  UpdateExecutorRequest,
  WorkflowReconfigureRequest
}
import org.apache.texera.amber.engine.architecture.scheduling.{
  Region,
  RegionIdentity,
  WorkflowExecutionManager
}
import org.apache.texera.amber.engine.common.FriesReconfigurationAlgorithm.FriesComponent
import org.scalatest.flatspec.AnyFlatSpec

class FriesReconfigurationAlgorithmSpec extends AnyFlatSpec {

  private def physicalOpId(opId: String): PhysicalOpIdentity =
    PhysicalOpIdentity(OperatorIdentity(opId), "main")

  private def op(opId: String, oneToMany: Boolean = false): PhysicalOp =
    PhysicalOp(
      physicalOpId(opId),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    ).withInputPorts(List(InputPort(PortIdentity())))
      .withOutputPorts(List(OutputPort(PortIdentity())))
      .withIsOneToManyOp(oneToMany)

  private def link(fromOp: String, toOp: String, fromPort: Int = 0, toPort: Int = 0): PhysicalLink =
    PhysicalLink(
      physicalOpId(fromOp),
      PortIdentity(fromPort),
      physicalOpId(toOp),
      PortIdentity(toPort)
    )

  /**
    * Builds a region with the given links, whose operators also list those links on their
    * ports, as a compiled plan's operators do. `operatorOnlyLinks` are listed by the operators
    * of the region at their ends but are not links of the region, like a link from outside the
    * region or a link whose reader reads the port's saved result instead.
    */
  private def buildRegion(
      regionId: Long,
      ops: Set[PhysicalOp],
      links: Set[PhysicalLink],
      operatorOnlyLinks: Set[PhysicalLink] = Set.empty
  ): Region = {
    val wiredOps =
      (links ++ operatorOnlyLinks).foldLeft(
        ops.map(physicalOp => physicalOp.id -> physicalOp).toMap
      ) { (currentOps, physicalLink) =>
        val withOutput = currentOps
          .get(physicalLink.fromOpId)
          .fold(currentOps)(from =>
            currentOps.updated(physicalLink.fromOpId, from.addOutputLink(physicalLink))
          )
        withOutput
          .get(physicalLink.toOpId)
          .fold(withOutput)(to =>
            withOutput.updated(physicalLink.toOpId, to.addInputLink(physicalLink))
          )
      }
    Region(RegionIdentity(regionId), wiredOps.values.toSet, links)
  }

  private def managerWithRegions(regions: Region*): WorkflowExecutionManager =
    new WorkflowExecutionManager(WorkflowExecution(), null, null) {
      override def getExecutingRegions: Set[Region] = regions.toSet
    }

  private def reconfigureRequest(opIds: String*): WorkflowReconfigureRequest =
    WorkflowReconfigureRequest(
      reconfiguration =
        opIds.map(opId => UpdateExecutorRequest(physicalOpId(opId), OpExecInitInfo.Empty)),
      reconfigurationId = "reconfiguration-1"
    )

  private def singleComponent(components: Set[FriesComponent]): FriesComponent = {
    assert(components.size == 1)
    components.head
  }

  "getReconfigurations" should "limit the scope to the reconfigured operator when no one-to-many operator is upstream" in {
    val region = buildRegion(
      1,
      Set(op("a"), op("b"), op("c")),
      Set(link("a", "b"), link("b", "c"))
    )
    val request = reconfigureRequest("b")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("b")))
    assert(component.sources == Set(physicalOpId("b")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
    assert(!component.scope.contains(physicalOpId("a")))
    assert(!component.scope.contains(physicalOpId("c")))
  }

  it should "extend the scope up to a one-to-many operator that has a reconfigured descendant" in {
    val region = buildRegion(
      1,
      Set(op("a", oneToMany = true), op("b"), op("c")),
      Set(link("a", "b"), link("b", "c"))
    )
    val request = reconfigureRequest("c")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("a"), physicalOpId("b"), physicalOpId("c")))
    assert(component.sources == Set(physicalOpId("a")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
  }

  it should "ignore one-to-many operators that have no reconfigured descendant" in {
    val region = buildRegion(
      1,
      Set(op("a"), op("b"), op("x", oneToMany = true), op("y")),
      Set(link("a", "b"), link("x", "y"))
    )
    val request = reconfigureRequest("b")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("b")))
    assert(!component.scope.contains(physicalOpId("x")))
    assert(!component.scope.contains(physicalOpId("y")))
  }

  it should "not extend the scope to a one-to-many operator downstream of the reconfigured operator" in {
    val region = buildRegion(
      1,
      Set(op("a"), op("b"), op("m", oneToMany = true), op("c")),
      Set(link("a", "b"), link("b", "m"), link("m", "c"))
    )
    val request = reconfigureRequest("b")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("b")))
    assert(!component.scope.contains(physicalOpId("m")))
    assert(!component.scope.contains(physicalOpId("c")))
  }

  it should "exclude side branches that do not lead back to a reconfigured operator" in {
    val region = buildRegion(
      1,
      Set(op("s", oneToMany = true), op("b"), op("c"), op("x")),
      Set(link("s", "b"), link("b", "c"), link("s", "x"))
    )
    val request = reconfigureRequest("c")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("s"), physicalOpId("b"), physicalOpId("c")))
    assert(component.sources == Set(physicalOpId("s")))
    assert(!component.scope.contains(physicalOpId("x")))
  }

  it should "include all parallel branches between a one-to-many operator and the reconfigured operator" in {
    val region = buildRegion(
      1,
      Set(op("s", oneToMany = true), op("a"), op("b"), op("t")),
      Set(link("s", "a"), link("s", "b"), link("a", "t"), link("b", "t"))
    )
    val request = reconfigureRequest("t")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(
      component.scope ==
        Set(physicalOpId("s"), physicalOpId("a"), physicalOpId("b"), physicalOpId("t"))
    )
    assert(component.sources == Set(physicalOpId("s")))
  }

  it should "split disconnected closures into separate components with their own reconfigurations" in {
    val region = buildRegion(
      1,
      Set(op("a1"), op("b1"), op("a2"), op("b2")),
      Set(link("a1", "b1"), link("a2", "b2"))
    )
    val request = reconfigureRequest("b1", "b2")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    assert(components.size == 2)
    assert(components.map(_.scope) == Set(Set(physicalOpId("b1")), Set(physicalOpId("b2"))))
    val componentOfB1 = components.find(_.scope.contains(physicalOpId("b1"))).get
    val componentOfB2 = components.find(_.scope.contains(physicalOpId("b2"))).get
    assert(componentOfB1.sources == Set(physicalOpId("b1")))
    assert(componentOfB2.sources == Set(physicalOpId("b2")))
    assert(componentOfB1.reconfigurations.map(_.targetOpId) == Set(physicalOpId("b1")))
    assert(componentOfB2.reconfigurations.map(_.targetOpId) == Set(physicalOpId("b2")))
  }

  it should "merge reconfigured operators connected by a link into a single component" in {
    val region = buildRegion(
      1,
      Set(op("a"), op("b"), op("c"), op("d")),
      Set(link("a", "b"), link("b", "c"), link("c", "d"))
    )
    val request = reconfigureRequest("b", "c")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("b"), physicalOpId("c")))
    assert(component.sources == Set(physicalOpId("b")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
    assert(!component.scope.contains(physicalOpId("a")))
    assert(!component.scope.contains(physicalOpId("d")))
  }

  it should "handle a single-operator region" in {
    val region = buildRegion(1, Set(op("only")), Set.empty)
    val request = reconfigureRequest("only")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("only")))
    assert(component.sources == Set(physicalOpId("only")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
  }

  it should "select the reconfigured operator itself as the marker source when it has no upstream" in {
    val region = buildRegion(1, Set(op("a"), op("b")), Set(link("a", "b")))
    val request = reconfigureRequest("a")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("a")))
    assert(component.sources == Set(physicalOpId("a")))
    assert(!component.scope.contains(physicalOpId("b")))
  }

  it should "produce components only for regions that contain reconfigured operators" in {
    val regionWithReconfiguration =
      buildRegion(1, Set(op("a1"), op("b1")), Set(link("a1", "b1")))
    val regionWithoutReconfiguration =
      buildRegion(2, Set(op("a2"), op("b2")), Set(link("a2", "b2")))
    val request = reconfigureRequest("b1")

    val components = FriesReconfigurationAlgorithm.getReconfigurations(
      managerWithRegions(regionWithReconfiguration, regionWithoutReconfiguration),
      request
    )

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("b1")))
  }

  it should "schedule reconfigurations independently for each executing region" in {
    val firstRegion = buildRegion(1, Set(op("a1"), op("b1")), Set(link("a1", "b1")))
    val secondRegion = buildRegion(2, Set(op("a2"), op("b2")), Set(link("a2", "b2")))
    val request = reconfigureRequest("b1", "b2")

    val components = FriesReconfigurationAlgorithm.getReconfigurations(
      managerWithRegions(firstRegion, secondRegion),
      request
    )

    assert(components.size == 2)
    assert(components.map(_.scope) == Set(Set(physicalOpId("b1")), Set(physicalOpId("b2"))))
    val componentOfB1 = components.find(_.scope.contains(physicalOpId("b1"))).get
    val componentOfB2 = components.find(_.scope.contains(physicalOpId("b2"))).get
    assert(componentOfB1.reconfigurations.map(_.targetOpId) == Set(physicalOpId("b1")))
    assert(componentOfB2.reconfigurations.map(_.targetOpId) == Set(physicalOpId("b2")))
  }

  it should "return no components when no executing region contains a reconfigured operator" in {
    val region = buildRegion(1, Set(op("a"), op("b")), Set(link("a", "b")))
    val request = reconfigureRequest("elsewhere")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    assert(components.isEmpty)
  }

  it should "keep a single-operator scope when the reconfigured operator is itself one-to-many" in {
    // The descendant-based pull-in only inspects OTHER one-to-many operators upstream;
    // a reconfigured one-to-many operator does not widen its own scope, so
    // ReconfigurationHandler's scope.size == 1 fast path applies.
    val region = buildRegion(
      1,
      Set(op("a"), op("m", oneToMany = true), op("c")),
      Set(link("a", "m"), link("m", "c"))
    )
    val request = reconfigureRequest("m")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("m")))
    assert(component.sources == Set(physicalOpId("m")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
    assert(!component.scope.contains(physicalOpId("a")))
    assert(!component.scope.contains(physicalOpId("c")))
  }

  it should "select multiple marker sources when several one-to-many operators converge on the reconfigured operator" in {
    val region = buildRegion(
      1,
      Set(op("s1", oneToMany = true), op("s2", oneToMany = true), op("t")),
      Set(link("s1", "t"), link("s2", "t"))
    )
    val request = reconfigureRequest("t")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("s1"), physicalOpId("s2"), physicalOpId("t")))
    assert(component.sources == Set(physicalOpId("s1"), physicalOpId("s2")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
  }

  it should "traverse links on every input port when branches arrive on different ports" in {
    val targetOp = PhysicalOp(
      physicalOpId("t"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    ).withInputPorts(List(InputPort(PortIdentity()), InputPort(PortIdentity(1))))
      .withOutputPorts(List(OutputPort(PortIdentity())))
    val region = buildRegion(
      1,
      Set(
        op("m0", oneToMany = true),
        op("b0"),
        op("m1", oneToMany = true),
        op("b1"),
        targetOp
      ),
      Set(
        link("m0", "b0"),
        link("b0", "t", toPort = 0),
        link("m1", "b1"),
        link("b1", "t", toPort = 1)
      )
    )
    val request = reconfigureRequest("t")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(
      component.scope == Set(
        physicalOpId("m0"),
        physicalOpId("b0"),
        physicalOpId("m1"),
        physicalOpId("b1"),
        physicalOpId("t")
      )
    )
    assert(component.sources == Set(physicalOpId("m0"), physicalOpId("m1")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
  }

  it should "count only the region's links, not a link that only its operators list" in {
    // Split is fed from outside the region. U1 reads split's port 0 from its saved result, so
    // the link from split's port 0 to U1 carries no data: split and U1 list it, the region does
    // not. U1 is on no path of the region's links from split to U2, so reconfiguring split and
    // U2 involves only the two of them.
    val split = PhysicalOp(
      physicalOpId("split"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    ).withInputPorts(List(InputPort(PortIdentity())))
      .withOutputPorts(List(OutputPort(PortIdentity()), OutputPort(PortIdentity(1))))
    val region = buildRegion(
      1,
      Set(split, op("r"), op("u1"), op("u2")),
      Set(link("r", "u1"), link("u1", "u2"), link("split", "u2", fromPort = 1)),
      operatorOnlyLinks = Set(link("src", "split"), link("split", "u1", fromPort = 0))
    )
    assert(region.getOperator(physicalOpId("u1")).getInputLinks().size == 2)
    val request = reconfigureRequest("split", "u2")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("split"), physicalOpId("u2")))
    assert(component.sources == Set(physicalOpId("split")))
    assert(component.reconfigurations == request.reconfiguration.toSet)
  }

  it should "not reach a reconfigured operator over a link that only its operators list" in {
    // p feeds x and q, and q feeds y. x runs for r, and y reads x's port 0 from its saved
    // result, so the link from x's port 0 to y carries no data: x and y list it, the region
    // does not. Reconfiguring p and y takes in q, which lies between them, but not x: no link
    // of the region leads from x to y.
    val x = PhysicalOp(
      physicalOpId("x"),
      WorkflowIdentity(0),
      ExecutionIdentity(0),
      OpExecInitInfo.Empty
    ).withInputPorts(List(InputPort(PortIdentity())))
      .withOutputPorts(List(OutputPort(PortIdentity()), OutputPort(PortIdentity(1))))
    val region = buildRegion(
      1,
      Set(op("p"), x, op("q"), op("y"), op("r")),
      Set(
        link("p", "x"),
        link("p", "q"),
        link("q", "y"),
        link("x", "r", fromPort = 1),
        link("y", "r")
      ),
      operatorOnlyLinks = Set(link("x", "y", fromPort = 0))
    )
    assert(region.getOperator(physicalOpId("x")).getOutputLinks(PortIdentity(0)).size == 1)
    val request = reconfigureRequest("p", "y")

    val components =
      FriesReconfigurationAlgorithm.getReconfigurations(managerWithRegions(region), request)

    val component = singleComponent(components)
    assert(component.scope == Set(physicalOpId("p"), physicalOpId("q"), physicalOpId("y")))
    assert(component.sources == Set(physicalOpId("p")))
  }
}
